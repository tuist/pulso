defmodule Pulso.LogQL.Evaluator do
  @moduledoc """
  End-to-end LogQL query evaluation.

  `evaluate_log/3` runs a `%Pulso.LogQL.AST.LogQuery{}` and returns
  streams (Loki `resultType: "streams"`). `evaluate_metric/3` runs a
  metric expression and returns a `matrix` or `vector`.

  The load-bearing performance move happens in `push_down/2`: as much of
  the selector and line-filter work as possible is translated into
  `Pulso.Storage.query/2` options so it lands in the Rust Parquet
  decoder. Rejected rows never allocate an Erlang term.
  """

  alias Pulso.LogQL.AST
  alias Pulso.LogQL.Entry
  alias Pulso.LogQL.MetricEval
  alias Pulso.LogQL.Pipeline
  alias Pulso.LogQL.QueryValidation
  alias Pulso.LogQL.StreamMatchers
  alias Pulso.PromQL.Evaluator
  alias Pulso.QueryRunner
  alias Pulso.Record.Log
  alias Pulso.Storage

  @type opts :: %{
          optional(:start_ts_ns) => integer(),
          optional(:end_ts_ns) => integer(),
          optional(:limit) => pos_integer(),
          optional(:direction) => :forward | :backward,
          optional(:step_ns) => pos_integer(),
          optional(:categorize_labels) => boolean()
        }

  @type stream ::
          {labels :: map(), entries :: [{integer(), String.t()} | {integer(), String.t(), map()}]}
  @type series :: {labels :: map(), samples :: [{ts :: integer(), value :: float()}] | float()}

  # ---------------------------------------------------------------------------
  # Log queries
  # ---------------------------------------------------------------------------

  @spec evaluate_log(AST.LogQuery.t(), Storage.tenant(), opts()) ::
          {:ok, [stream()]} | {:error, term()}
  def evaluate_log(%AST.LogQuery{} = query, tenant, opts \\ %{}) when is_binary(tenant) do
    Pulso.SelfMetrics.track(:query, :logql_log, fn ->
      QueryRunner.run(
        tenant,
        :logql,
        fn deadline ->
          Process.put(:query_deadline_ms, deadline)
          do_evaluate_log(query, tenant, opts)
        end,
        opts
      )
    end)
  end

  # With a limit, keep only the `limit` newest (backward) or oldest (forward)
  # entries across all streams while folding, as Loki does, instead of
  # materializing every match.
  defp do_evaluate_log(query, tenant, %{limit: limit} = opts) when is_integer(limit) and limit > 0 do
    with {:ok, entries} <- do_select_entries(query, tenant, opts, fn entry, _record -> entry end) do
      entries
      |> group_streams(Map.get(opts, :categorize_labels, false))
      |> apply_direction(Map.get(opts, :direction, :backward))
      |> ok()
    end
  end

  defp do_evaluate_log(query, tenant, opts) do
    with {:ok, entries} <- evaluate_log_raw(query, tenant, opts) do
      entries
      |> group_streams(Map.get(opts, :categorize_labels, false))
      |> apply_direction(Map.get(opts, :direction, :backward))
      |> apply_limit(Map.get(opts, :limit))
      |> ok()
    end
  end

  @doc """
  The `opts.limit` newest (`direction: :backward`, the default) or oldest
  selected entries across all streams, newest or oldest first, each as
  `project.(entry, record)`. Reads like a limited log query, keeping only the
  retained entries while folding. Optional `finish` runs only on the final
  selected values, inside the same admitted query task.
  """
  @spec select_entries(AST.LogQuery.t(), Storage.tenant(), opts(), (Entry.t(), Log.t() -> term())) ::
          {:ok, [term()]} | {:error, term()}
  def select_entries(query, tenant, opts, project, finish \\ &Function.identity/1)

  def select_entries(%AST.LogQuery{} = query, tenant, %{limit: limit} = opts, project, finish)
      when is_binary(tenant) and is_integer(limit) and limit > 0 do
    QueryRunner.run(
      tenant,
      :logql,
      fn deadline ->
        Process.put(:query_deadline_ms, deadline)

        with {:ok, selected} <- do_select_entries(query, tenant, opts, project) do
          {:ok, Enum.map(selected, finish)}
        end
      end,
      opts
    )
  end

  defp do_select_entries(query, tenant, %{limit: limit} = opts, project) do
    direction = Map.get(opts, :direction, :backward)
    stop = if direction == :backward, do: &newest_complete?(&1, &2, limit)
    retain = &retain(&1, &2, &3, limit, direction, project)

    with {:ok, {retained, _size}} <- fold_log(query, tenant, opts, {:gb_sets.empty(), 0}, retain, stop) do
      ordered =
        if direction == :backward, do: Enum.reverse(:gb_sets.to_list(retained)), else: :gb_sets.to_list(retained)

      {:ok, Enum.map(ordered, &elem(&1, tuple_size(&1) - 1))}
    end
  end

  @doc """
  Evaluate a log query and return the flat list of pipeline entries,
  ungrouped. This is the entry point the metric evaluator uses so it can
  bucket by time before grouping by label set.
  """
  @spec evaluate_log_raw(AST.LogQuery.t(), Storage.tenant(), opts()) ::
          {:ok, [Entry.t()]} | {:error, term()}
  def evaluate_log_raw(%AST.LogQuery{} = query, tenant, opts) when is_binary(tenant) do
    with :ok <- QueryValidation.validate(query),
         {storage_opts, remaining_stages} <- push_down(query, opts),
         {:ok, records} <- query_storage(tenant, storage_opts) do
      if Process.get(:promql_budget) do
        Evaluator.charge(:samples, length(records))
        Evaluator.charge(:work, length(records))
      end

      compiled = Pipeline.compile(remaining_stages)

      entries =
        records
        |> Stream.map(&Entry.from_record(&1, Map.get(opts, :categorize_labels, false)))
        |> then(&Pipeline.run(compiled, &1))
        |> Enum.to_list()

      {:ok, entries}
    end
  catch
    {:logql_error, reason} -> {:error, reason}
  end

  @doc """
  Fold the entries a log query selects, one storage batch at a time, as
  `fun.(entry, record, acc)`. Validation, pushdown and the pipeline are those
  of `evaluate_log_raw/3`, but no complete result is materialized, so memory
  follows one batch plus the accumulator. Entries arrive in no defined order.
  """
  @spec fold_log(AST.LogQuery.t(), Storage.tenant(), opts(), acc, (Entry.t(), Log.t(), acc -> acc)) ::
          {:ok, acc} | {:error, term()}
        when acc: term()
  def fold_log(%AST.LogQuery{} = query, tenant, opts, acc, fun, stop \\ nil) when is_binary(tenant) do
    with :ok <- QueryValidation.validate(query),
         {storage_opts, remaining_stages} <- push_down(query, opts) do
      compiled = Pipeline.compile(remaining_stages)

      storage_opts = if stop, do: Keyword.put(storage_opts, :log_fold_stop, stop), else: storage_opts
      categorize_labels? = Map.get(opts, :categorize_labels, false)

      storage_result(
        Storage.fold_logs(tenant, storage_opts, acc, &fold_records(&1, &2, compiled, fun, categorize_labels?))
      )
    end
  catch
    {:logql_error, reason} -> {:error, reason}
  end

  defp fold_records(records, acc, compiled, fun, categorize_labels?) do
    # Log lines share the cross-language sample quota, as when materialized.
    if Process.get(:promql_budget), do: Evaluator.charge(:work, length(records))

    Enum.reduce(records, acc, &fold_record(&1, &2, compiled, fun, categorize_labels?))
  end

  defp fold_record(record, acc, compiled, fun, categorize_labels?) do
    case Pipeline.apply(compiled, Entry.from_record(record, categorize_labels?)) do
      {:keep, entry} -> fun.(entry, record, acc)
      :drop -> acc
    end
  end

  defp query_storage(tenant, opts), do: storage_result(Storage.query(:logs, tenant, opts))

  defp storage_result(result) do
    case result do
      {:error, reason}
      when reason in [
             :query_scan_limit,
             :query_sample_limit,
             :query_timeout,
             :manifest_freshness_unavailable,
             :signal_authority_missing
           ] ->
        {:error, reason}

      {:error, reason} ->
        {:error, {:storage_error, reason}}

      result ->
        result
    end
  end

  defp ok(value), do: {:ok, value}

  # ---------------------------------------------------------------------------
  # Metric queries — dispatch, actual computation lives in RangeAgg / VectorAgg
  # ---------------------------------------------------------------------------

  @spec evaluate_metric(term(), Storage.tenant(), opts()) ::
          {:ok, MetricEval.result()} | {:error, term()}
  def evaluate_metric(expr, tenant, opts \\ %{}) when is_binary(tenant) do
    Pulso.SelfMetrics.track(:query, :logql_metric, fn ->
      QueryRunner.run(
        tenant,
        :logql,
        fn deadline ->
          Process.put(:query_deadline_ms, deadline)
          do_evaluate_metric(expr, tenant, opts)
        end,
        opts
      )
    end)
  end

  defp do_evaluate_metric(expr, tenant, opts) do
    with :ok <- QueryValidation.validate(expr) do
      MetricEval.evaluate(expr, tenant, opts)
    end
  end

  # ---------------------------------------------------------------------------
  # Pushdown: split a LogQuery into (Storage.query opts, remaining stages)
  # ---------------------------------------------------------------------------

  @doc false
  @spec push_down(AST.LogQuery.t(), opts()) :: {keyword(), [struct()]}
  def push_down(%AST.LogQuery{selector: sel, stages: stages}, opts) do
    {service, matchers_wo_service} = StreamMatchers.split(sel.matchers)

    {pushdown_line_filters, remaining} = split_head_line_filters(stages)

    storage_opts =
      []
      |> maybe_put(:start_ts, Map.get(opts, :start_ts_ns))
      |> maybe_put(:end_ts, Map.get(opts, :end_ts_ns))
      |> maybe_put(:service, service)
      |> maybe_put_list(:matchers, matchers_wo_service)
      |> maybe_put_list(:line_filters, pushdown_line_filters)

    deadline = Process.get(:query_deadline_ms)
    storage_opts = storage_opts ++ Evaluator.logql_storage_opts(deadline)
    {storage_opts, remaining}
  end

  # A consecutive run of `LineFilter` stages at the head of the pipeline
  # only reads `body`, which Rust already has, so we push them down. Any
  # subsequent line filter (after a parser or format stage) stays in
  # Elixir — a parser may not modify `body`, but keeping the split simple
  # avoids reasoning about which stages preserve the raw bytes.
  #
  # IP and pattern filters stay in the Elixir pipeline. Their presence ends
  # this head run, preserving the authored order of subsequent filters.
  defp split_head_line_filters(stages) do
    {head, rest} = Enum.split_while(stages, &AST.LineFilter.native?/1)

    pushable =
      Enum.map(head, fn %AST.LineFilter{op: op, value: value} ->
        {op, line_filter_value(value)}
      end)

    {pushable, rest}
  end

  defp line_filter_value({:string, s}), do: s
  defp line_filter_value({:re, s}), do: s

  defp maybe_put(kw, _key, nil), do: kw
  defp maybe_put(kw, key, value), do: Keyword.put(kw, key, value)

  defp maybe_put_list(kw, _key, []), do: kw
  defp maybe_put_list(kw, key, list), do: Keyword.put(kw, key, list)

  # ---------------------------------------------------------------------------
  # Streams shaping
  # ---------------------------------------------------------------------------

  # Group entries by label set. Loki streams are `{labels, entries}` where
  # entries is `[[ts_ns_string, line], ...]` — we emit `{ts_ns, line}`
  # tuples and let the marshaller stringify. Timestamp-descending within
  # each stream matches Loki's `direction=backward` default.
  # Keys order entries by time, then the canonical record tie-breakers; the
  # sequence number keeps otherwise identical entries distinct.
  defp retain(entry, record, {set, size}, limit, direction, project) do
    key =
      {entry.timestamp_ns || 0, record.observed_timestamp_ns || 0, record.trace_id || "", record.span_id || "",
       entry.line, System.unique_integer([:monotonic])}

    cond do
      size < limit ->
        {:gb_sets.add(retainable(key, project.(entry, record)), set), size + 1}

      better?(key, worst(set, direction), direction) ->
        {:gb_sets.add(retainable(key, project.(entry, record)), drop_worst(set, direction)), size}

      true ->
        {set, size}
    end
  end

  defp worst(set, :backward), do: :gb_sets.smallest(set)
  defp worst(set, :forward), do: :gb_sets.largest(set)
  defp drop_worst(set, :backward), do: elem(:gb_sets.take_smallest(set), 1)
  defp drop_worst(set, :forward), do: elem(:gb_sets.take_largest(set), 1)
  defp better?(key, worst, :backward), do: key > Tuple.delete_at(worst, tuple_size(worst) - 1)
  defp better?(key, worst, :forward), do: key < Tuple.delete_at(worst, tuple_size(worst) - 1)

  # Retained values are copied so they never keep a decoded chunk alive.
  defp retainable(key, value), do: Tuple.insert_at(Pulso.JSON.detach(key), tuple_size(key), Pulso.JSON.detach(value))

  defp newest_complete?({set, size}, next_max_ts, limit),
    do: size >= limit and is_integer(next_max_ts) and next_max_ts < elem(:gb_sets.smallest(set), 0)

  defp group_streams(entries, true) do
    entries
    |> Enum.map(fn entry ->
      {stream, categories} = Entry.categorized_labels(entry)
      {stream, {entry.timestamp_ns || 0, entry.line, categories}}
    end)
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Enum.map(fn {stream, values} -> {stream, Enum.sort_by(values, &elem(&1, 0), :desc)} end)
  end

  defp group_streams(entries, false) do
    entries
    |> Enum.group_by(fn %Entry{labels: l} -> l end)
    |> Enum.map(fn {labels, group} ->
      values =
        group
        |> Enum.map(fn %Entry{timestamp_ns: ts, line: line} -> {ts || 0, line} end)
        |> Enum.sort_by(&elem(&1, 0), :desc)

      {labels, values}
    end)
  end

  defp apply_direction(streams, :backward), do: streams

  defp apply_direction(streams, :forward) do
    Enum.map(streams, fn {labels, entries} -> {labels, Enum.reverse(entries)} end)
  end

  defp apply_limit(streams, nil), do: streams

  defp apply_limit(streams, limit) when is_integer(limit) and limit > 0 do
    take_limit(streams, limit, [])
  end

  # `limit: 0` or a negative value is treated as "no limit" rather than
  # crashing the caller (Loki's contract makes 0 undefined; a hard
  # crash on a bad param is worse than a permissive read).
  defp apply_limit(streams, _), do: streams

  defp take_limit([], _remaining, acc), do: Enum.reverse(acc)
  defp take_limit(_streams, 0, acc), do: Enum.reverse(acc)

  defp take_limit([{labels, entries} | rest], remaining, acc) do
    taken = Enum.take(entries, remaining)
    used = length(taken)

    if used == 0 do
      take_limit(rest, remaining, acc)
    else
      take_limit(rest, remaining - used, [{labels, taken} | acc])
    end
  end
end
