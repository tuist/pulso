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
  alias Pulso.Storage

  @type opts :: %{
          optional(:start_ts_ns) => integer(),
          optional(:end_ts_ns) => integer(),
          optional(:limit) => pos_integer(),
          optional(:direction) => :forward | :backward,
          optional(:step_ns) => pos_integer()
        }

  @type stream :: {labels :: map(), entries :: [{ts :: integer(), line :: String.t()}]}
  @type series :: {labels :: map(), samples :: [{ts :: integer(), value :: float()}] | float()}

  # ---------------------------------------------------------------------------
  # Log queries
  # ---------------------------------------------------------------------------

  @spec evaluate_log(AST.LogQuery.t(), Storage.tenant(), opts()) ::
          {:ok, [stream()]} | {:error, term()}
  def evaluate_log(%AST.LogQuery{} = query, tenant, opts \\ %{}) when is_binary(tenant) do
    with {:ok, entries} <- evaluate_log_raw(query, tenant, opts) do
      entries
      |> group_streams()
      |> apply_direction(Map.get(opts, :direction, :backward))
      |> apply_limit(Map.get(opts, :limit))
      |> ok()
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
         {:ok, records} <- Storage.query(:logs, tenant, storage_opts) do
      compiled = Pipeline.compile(remaining_stages)

      entries =
        records
        |> Stream.map(&Entry.from_record/1)
        |> then(&Pipeline.run(compiled, &1))
        |> Enum.to_list()

      {:ok, entries}
    end
  end

  defp ok(value), do: {:ok, value}

  # ---------------------------------------------------------------------------
  # Metric queries — dispatch, actual computation lives in RangeAgg / VectorAgg
  # ---------------------------------------------------------------------------

  @spec evaluate_metric(term(), Storage.tenant(), opts()) ::
          {:ok, {:matrix, [series()]} | {:vector, [series()]}} | {:error, term()}
  def evaluate_metric(expr, tenant, opts \\ %{}) when is_binary(tenant) do
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
    matchers = Enum.map(sel.matchers, &to_matcher_tuple/1)

    {service, matchers_wo_service} = extract_service_matcher(matchers)

    {pushdown_line_filters, remaining} = split_head_line_filters(stages)

    storage_opts =
      []
      |> maybe_put(:start_ts, Map.get(opts, :start_ts_ns))
      |> maybe_put(:end_ts, Map.get(opts, :end_ts_ns))
      |> maybe_put(:service, service)
      |> maybe_put_list(:matchers, matchers_wo_service)
      |> maybe_put_list(:line_filters, pushdown_line_filters)

    {storage_opts, remaining}
  end

  # `service` and `service_name` project onto the dedicated Parquet
  # `service` column, which is dictionary-encoded and faster to filter
  # than an equivalent matcher against `resource`. So we peel it out and
  # let the storage layer route it through the `service` option.
  defp extract_service_matcher(matchers) do
    Enum.reduce(matchers, {nil, []}, fn
      {name, :eq, value}, {nil, acc} when name in ["service", "service_name"] ->
        {value, acc}

      m, {service, acc} ->
        {service, [m | acc]}
    end)
    |> then(fn {service, acc} -> {service, Enum.reverse(acc)} end)
  end

  # A consecutive run of `LineFilter` stages at the head of the pipeline
  # only reads `body`, which Rust already has, so we push them down. Any
  # subsequent line filter (after a parser or format stage) stays in
  # Elixir — a parser may not modify `body`, but keeping the split simple
  # avoids reasoning about which stages preserve the raw bytes.
  #
  # `ip(...)` line filters are validated out by
  # `Pulso.LogQL.QueryValidation` before we get here, so we only handle
  # string and regex forms.
  defp split_head_line_filters(stages) do
    {head, rest} = Enum.split_while(stages, &match?(%AST.LineFilter{}, &1))

    pushable =
      Enum.map(head, fn %AST.LineFilter{op: op, value: value} ->
        {op, line_filter_value(value)}
      end)

    {pushable, rest}
  end

  defp line_filter_value({:string, s}), do: s
  defp line_filter_value({:re, s}), do: s

  defp to_matcher_tuple(%AST.Matcher{name: n, op: op, value: v}), do: {n, op, v}

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
  defp group_streams(entries) do
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
