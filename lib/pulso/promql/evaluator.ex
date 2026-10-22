defmodule Pulso.PromQL.Evaluator do
  @moduledoc """
  Evaluates float metric selectors, range functions, and vector aggregations.

  Each selector loads its whole evaluation interval once through Pulso.Storage,
  including the lookback or range window. Series identity is the label map,
  never the series hash. Rates correct counter resets and extrapolate using
  Prometheus's float-sample algorithm, including the counter's zero boundary.
  Results use Prometheus's envelope with numeric timestamps in seconds.

  This first evaluator folds selected samples in Elixir. Columnar aggregation
  in Rust is a future optimization; unsupported expression types fail in parsing.
  """
  alias Pulso.Codec.NIF
  alias Pulso.LogQL.AST.LogQuery
  alias Pulso.LogQL.AST.RangeAgg
  alias Pulso.LogQL.MetricEval
  alias Pulso.LogQL.QueryValidation
  alias Pulso.PromQL.NativeHistogram
  alias Pulso.PromQL.NativeRange
  alias Pulso.PromQL.Number
  alias Pulso.PromQL.Operations
  alias Pulso.PromQL.Parser
  alias Pulso.PromQL.RangeMath
  alias Pulso.PromQL.Time
  alias Pulso.QueryRunner
  alias Pulso.Record.NativeHistogram, as: Histogram
  alias Pulso.Runtime
  alias Pulso.Storage

  @lookback_ns 300_000_000_000
  @max_steps Pulso.QueryLimits.max_evaluation_steps()
  @range_functions Map.new(
                     ~w(rate increase irate delta sum_over_time avg_over_time min_over_time max_over_time count_over_time stddev_over_time stdvar_over_time absent_over_time)a,
                     &{Atom.to_string(&1), &1}
                   )

  def query(query, tenant, opts \\ %{}) do
    Pulso.SelfMetrics.track(:query, :promql, fn -> do_query(query, tenant, opts) end)
  end

  defp do_query(query, tenant, opts) do
    QueryRunner.run(
      tenant,
      Map.get(opts, :query_class, :promql),
      fn deadline ->
        Process.put(:promql_deadline_ms, deadline)
        Process.put(:pulso_require_fresh_manifest, Map.get(opts, :require_fresh_manifest, false))

        with {:ok, expr} <- Parser.parse(query),
             {:ok, kind, steps} <- evaluation_steps(opts) do
          run_query(expr, tenant, steps, kind)
        end
      end,
      opts
    )
  end

  @doc false
  def query_batch(queries, tenant, finish, opts \\ %{}) when is_list(queries) and is_function(finish, 2) do
    QueryRunner.run(
      tenant,
      Map.get(opts, :query_class, :promql),
      &batch_worker(queries, tenant, finish, &1, opts),
      opts
    )
  end

  defp batch_worker(queries, tenant, finish, deadline, opts) do
    Process.put(:pulso_require_fresh_manifest, Map.get(opts, :require_fresh_manifest, false))
    Process.put(:promql_deadline_ms, deadline)
    Process.put(:query_deadline_ms, deadline)
    Process.put(:promql_budget, %{samples: 0, work: 0, points: 0, materialized: 0, log_samples: 0})

    with {:ok, parsed} <- parse_batch(queries),
         selectors = Enum.reduce(parsed, 0, fn {_, expr, _}, count -> count + selectors(expr) end),
         Process.put(:promql_batch_selectors, max(1, selectors)),
         {:ok, results} <- run_batch(parsed, tenant),
         do: finish.(results, deadline)
  end

  defp parse_batch(queries) when length(queries) <= 128 do
    refs =
      Enum.map(queries, fn
        {ref, _, _} -> ref
        _ -> nil
      end)

    if Enum.any?(refs, &(not is_binary(&1) or byte_size(&1) not in 1..128)) or length(Enum.uniq(refs)) != length(refs) do
      {:error, :invalid_query_batch}
    else
      Enum.reduce_while(queries, {:ok, []}, &parse_batch_query/2)
    end
  end

  defp parse_batch(_), do: {:error, :invalid_query_batch}

  defp parse_batch_query({ref, query, opts}, {:ok, acc}) when is_map(opts) do
    with {:ok, expr} <- parse_source(query, Map.get(opts, :language, :promql)),
         {:ok, kind, steps} <- evaluation_steps(opts) do
      {:cont, {:ok, [{ref, expr, {kind, steps, opts}} | acc]}}
    else
      error -> {:halt, error}
    end
  end

  defp parse_batch_query(_, _), do: {:halt, {:error, :invalid_query_batch}}

  defp parse_source(query, :promql), do: Parser.parse(query)

  defp parse_source(query, :logql) do
    with {:ok, expr} <- Pulso.LogQL.Parser.parse(query),
         false <- match?(%LogQuery{}, expr),
         :ok <- QueryValidation.validate(expr) do
      {:ok, {:logql, expr}}
    else
      true -> {:error, :unsupported_source_query}
      {:error, :query_expression_limit} = error -> error
      {:error, _} -> {:error, :invalid_logql}
    end
  end

  defp parse_source(_, _), do: {:error, :invalid_query_batch}

  defp log_selectors(%RangeAgg{}), do: 1
  defp log_selectors(term) when is_map(term), do: term |> Map.values() |> log_selectors()
  defp log_selectors(term) when is_tuple(term), do: term |> Tuple.to_list() |> log_selectors()
  defp log_selectors(term) when is_list(term), do: Enum.reduce(term, 0, &(log_selectors(&1) + &2))
  defp log_selectors(_), do: 0
  defp selectors({:logql, expr}), do: log_selectors(expr)
  defp selectors({:selector, _, _}), do: 1
  defp selectors({:range, _, _, _}), do: 1
  defp selectors(value) when is_tuple(value), do: value |> Tuple.to_list() |> selectors()
  defp selectors(values) when is_list(values), do: Enum.reduce(values, 0, &(selectors(&1) + &2))
  defp selectors(_), do: 0

  defp run_batch(parsed, tenant) do
    Enum.reduce_while(Enum.reverse(parsed), {:ok, %{}}, fn {ref, expr, {kind, steps, opts}}, {:ok, acc} ->
      case run_source(expr, tenant, steps, kind, opts) do
        {:ok, result} -> {:cont, {:ok, Map.put(acc, ref, result)}}
        error -> {:halt, error}
      end
    end)
  end

  defp run_source({:logql, expr}, tenant, _steps, _kind, opts) do
    with {:ok, {kind, series}} <- MetricEval.evaluate(expr, tenant, opts),
         do: {:ok, {:logql, kind, series}}
  end

  defp run_source(expr, tenant, steps, kind, _opts), do: run_query(expr, tenant, steps, kind)

  defp run_query(expr, tenant, steps, kind) do
    if !Process.get(:promql_batch_selectors),
      do: Process.put(:promql_budget, %{samples: 0, work: 0, points: 0, materialized: 0, log_samples: 0})

    with {:ok, series, warnings} <- evaluate(expr, tenant, steps) do
      series = Operations.coalesce(series)
      points = Enum.reduce(series, 0, fn {_, values}, total -> total + length(values) end)
      if points > limits().points, do: throw({:promql_error, :query_result_limit})
      if Process.get(:promql_batch_selectors), do: charge(:points, points)

      if length(Enum.uniq_by(series, &elem(&1, 0))) != length(series),
        do: throw({:promql_error, :duplicate_result_label_sets})

      result = envelope(kind, series, Parser.type(expr), ordered?(expr))
      {:ok, if(warnings == [], do: result, else: Map.put(result, "warnings", Enum.uniq(warnings)))}
    end
  rescue
    ArithmeticError -> {:error, :nonfinite_result}
  catch
    {:promql_error, reason} -> {:error, reason}
  end

  @doc false
  def charge(key, count) do
    budget = Process.get(:promql_budget)
    used = Map.get(budget, key, 0) + count
    if used > Map.fetch!(limits(), key), do: throw({:promql_error, budget_error(key)})

    if System.monotonic_time(:millisecond) >= Process.get(:promql_deadline_ms),
      do: throw({:promql_error, :query_timeout})

    Process.put(:promql_budget, Map.put(budget, key, used))
  end

  @doc false
  # What remains of `key`'s budget in the current query.
  def remaining(key) do
    used = Process.get(:promql_budget, %{}) |> Map.get(key, 0)
    max(Map.fetch!(limits(), key) - used, 0)
  end

  defp budget_error(:samples), do: :query_sample_limit
  defp budget_error(:log_samples), do: :query_sample_limit
  defp budget_error(:work), do: :query_work_limit
  defp budget_error(:points), do: :query_result_limit
  defp budget_error(:materialized), do: :query_intermediate_limit
  defp ordered?({:call, name, _}), do: name in ["sort", "sort_desc"]
  defp ordered?({:parameter_aggregate, _, _, _, _}), do: true
  defp ordered?(_), do: false

  defp limits do
    config = Runtime.get_env(:pulso, __MODULE__, [])

    %{
      samples: Keyword.get(config, :max_samples, 100_000),
      log_samples: Keyword.get(config, :max_log_samples, 5_000_000),
      work: Keyword.get(config, :max_work, 5_000_000),
      points: Keyword.get(config, :max_result_points, 100_000),
      materialized: Keyword.get(config, :max_materialized_samples, 100_000),
      segments: Keyword.get(config, :max_scan_segments, 1024),
      bytes: Keyword.get(config, :max_scan_bytes, 134_217_728),
      rows: Keyword.get(config, :max_scan_rows, 1_000_000)
    }
  end

  defp evaluation_steps(opts) do
    finish = Map.get(opts, :end_ts_ns, System.system_time(:nanosecond))

    finish = if timestamp?(finish), do: finish, else: :invalid

    case Map.fetch(opts, :step_ns) do
      :error when is_integer(finish) and not is_map_key(opts, :start_ts_ns) ->
        {:ok, :vector, [finish]}

      {:ok, step} when is_integer(step) and step > 0 and is_integer(finish) ->
        range_steps(Map.get(opts, :start_ts_ns), Map.get(opts, :end_ts_ns), step)

      _ ->
        {:error, :invalid_evaluation_time_or_step}
    end
  end

  defp timestamp?(value) when is_integer(value),
    do: value >= -9_223_372_036_854_775_808 and value <= 9_223_372_036_854_775_807

  defp timestamp?(_value), do: false

  defp range_steps(start, finish, step)
       when is_integer(start) and is_integer(finish) and start <= finish and start >= -9_223_372_036_854_775_808 and
              start <= 9_223_372_036_854_775_807 do
    count = div(finish - start, step)

    if count < @max_steps do
      {:ok, :matrix, Enum.map(0..count, &(start + &1 * step))}
    else
      {:error, :invalid_range_or_too_many_steps}
    end
  end

  defp range_steps(_, _, _), do: {:error, :invalid_range_or_too_many_steps}

  defp evaluate({:scalar, value}, _tenant, steps), do: {:ok, [{%{}, Enum.map(steps, &{&1, value})}], []}
  defp evaluate({:string, _} = string, _tenant, _steps), do: {:ok, string, []}

  defp evaluate({:unary, op, inner}, tenant, steps) do
    with {:ok, series, warnings} <- evaluate(inner, tenant, steps) do
      result =
        Enum.map(series, fn {labels, samples} ->
          {if(op == "-", do: Map.delete(labels, "__name__"), else: labels),
           Enum.map(samples, fn {ts, value} -> {ts, if(op == "-", do: negate(value), else: value)} end)}
        end)

      {:ok, result, warnings}
    end
  end

  defp evaluate({:binary, op, modifiers, left, right}, tenant, steps) do
    with {:ok, lhs, lw} <- evaluate(left, tenant, steps), {:ok, rhs, rw} <- evaluate(right, tenant, steps) do
      {:ok, Operations.binary(op, modifiers, lhs, rhs, Parser.type(left), Parser.type(right), steps), lw ++ rw}
    end
  end

  defp evaluate({:call, "timestamp", [{:selector, matchers, offset}]}, tenant, steps),
    do: select(tenant, matchers, offset, @lookback_ns, steps, :timestamp)

  defp evaluate({:call, "absent", [inner]}, tenant, steps) do
    with {:ok, vector, warnings} <- evaluate(inner, tenant, steps) do
      {:ok, Operations.absent(vector, absent_labels(inner), steps), warnings}
    end
  end

  defp evaluate({:call, "predict_linear", [range, parameter]}, tenant, steps) do
    offset = elem(range, tuple_size(range) - 1)

    with {:ok, [{_, values}], warnings} <- evaluate(parameter, tenant, steps),
         {:ok, result, extra} <-
           evaluate_range(range, tenant, steps, {:predict_linear, Map.new(values), offset}) do
      {:ok, result, warnings ++ extra}
    end
  end

  defp evaluate({:call, name, [range]}, tenant, steps) when is_map_key(@range_functions, name),
    do: evaluate({:function, Map.fetch!(@range_functions, name), range}, tenant, steps)

  defp evaluate({:call, name, args}, tenant, steps) do
    with {:ok, values, warnings} <- evaluate_args(args, tenant, steps) do
      {:ok, Operations.call(name, values, steps), warnings}
    end
  end

  defp evaluate({:parameter_aggregate, op, grouping, parameter, inner}, tenant, steps) do
    with {:ok, k, kw} <- evaluate(parameter, tenant, steps),
         {:ok, vector, warnings} <- evaluate(inner, tenant, steps) do
      {:ok, Operations.topk(op, grouping, k, vector, steps), kw ++ warnings}
    end
  end

  defp evaluate({:aggregate, op, grouping, inner}, tenant, steps) do
    with {:ok, series, warnings} <- evaluate(inner, tenant, steps) do
      grouped = for {labels, samples} <- series, {ts, value} <- samples, do: {{group_key(labels, grouping), ts}, value}
      charge(:work, length(grouped))

      result =
        grouped
        |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
        |> Enum.flat_map(&aggregate_point(op, &1))
        |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
        |> Enum.map(fn {labels, samples} -> {labels, Enum.sort(samples)} end)

      {:ok, result, warnings}
    end
  end

  defp evaluate({:selector, matchers, offset}, tenant, steps) do
    select(tenant, matchers, offset, @lookback_ns, steps, :latest)
  end

  defp evaluate({:function, :absent_over_time, inner}, tenant, steps) do
    with {:ok, vector, warnings} <- evaluate_range(inner, tenant, steps, :count_over_time) do
      {:ok, Operations.absent(vector, absent_labels(inner), steps), warnings}
    end
  end

  defp evaluate({:function, op, range}, tenant, steps), do: evaluate_range(range, tenant, steps, op)

  defp evaluate_range({:range, matchers, window, offset}, tenant, steps, op),
    do: select(tenant, matchers, offset, window, steps, op)

  defp evaluate_range({:subquery, inner, window, resolution, offset}, tenant, steps, op) do
    # Subquery grids align independently to epoch multiples, not outer steps.
    first = (Integer.floor_div(hd(steps) - offset - window, resolution) + 1) * resolution
    last = Integer.floor_div(List.last(steps) - offset, resolution) * resolution
    count = max(0, div(last - first, resolution) + 1)
    if count > @max_steps, do: throw({:promql_error, :query_work_limit})
    charge(:work, count)
    inner_steps = if count == 0, do: [], else: Enum.map(0..(count - 1), &(first + &1 * resolution))
    subquery_groups(inner, tenant, inner_steps, {steps, offset, window, op})
  end

  defp subquery_groups(_inner, _tenant, [], _context), do: {:ok, [], []}

  defp subquery_groups(inner, tenant, inner_steps, {steps, offset, window, op}) do
    with {:ok, series, warnings} <- evaluate(inner, tenant, inner_steps) do
      count = Enum.sum(Enum.map(series, fn {_, points} -> length(points) end))
      charge(:work, count)
      charge(:materialized, count)

      groups =
        Enum.map(series, fn {labels, points} ->
          {labels, Enum.map(points, fn {ts, value} -> %{timestamp_ns: ts, value: value} end)}
        end)

      with {:ok, result} <- evaluate_groups(groups, steps, offset, window, op, limits()),
           do: {:ok, result, warnings}
    end
  end

  defp evaluate_args(args, tenant, steps) do
    Enum.reduce_while(args, {:ok, [], []}, fn arg, {:ok, acc, warnings} ->
      case evaluate(arg, tenant, steps) do
        {:ok, value, extra} -> {:cont, {:ok, acc ++ [value], warnings ++ extra}}
        error -> {:halt, error}
      end
    end)
  end

  defp select(tenant, matchers, offset, window, steps, op) do
    limits = selector_limits()

    opts = [
      start_ts: max(-9_223_372_036_854_775_808, hd(steps) - offset - window + 1),
      end_ts: max(-9_223_372_036_854_775_808, List.last(steps) - offset),
      matchers: Enum.map(matchers, &anchored_matcher/1),
      max_records: limits.samples,
      max_scan_segments: limits.segments,
      max_scan_bytes: limits.bytes,
      max_scan_rows: limits.rows,
      deadline_ms: Process.get(:promql_deadline_ms),
      require_fresh_manifest: Process.get(:pulso_require_fresh_manifest, false)
    ]

    with {:ok, samples} <- query_samples(tenant, opts),
         {:ok, groups, warnings} <- prepare_series(samples),
         {:ok, result} <- evaluate_groups(groups, steps, offset, window, op, limits) do
      {:ok, Operations.coalesce(result), warnings}
    end
  end

  defp selector_limits do
    limits = limits()

    case Process.get(:promql_batch_selectors) do
      nil ->
        limits

      divisor ->
        batch_selector_limits(limits, divisor)
    end
  end

  defp batch_selector_limits(limits, divisor) do
    runner = QueryRunner.storage_opts(Process.get(:promql_deadline_ms))
    keys = %{segments: :max_scan_segments, bytes: :max_scan_bytes, rows: :max_scan_rows}
    scans = Map.new(keys, fn {key, option} -> {key, div(min(Map.fetch!(limits, key), runner[option]), divisor)} end)
    if Enum.any?(scans, fn {_, value} -> value < 1 end), do: throw({:promql_error, :query_scan_limit})
    remaining = limits.samples - Process.get(:promql_budget).samples
    if remaining < 1, do: throw({:promql_error, :query_sample_limit})
    limits |> Map.merge(scans) |> Map.put(:samples, remaining)
  end

  @doc false
  def logql_storage_opts(deadline) do
    if Process.get(:promql_batch_selectors) do
      limits = selector_limits()

      [
        max_records: limits.samples,
        max_scan_segments: limits.segments,
        max_scan_bytes: limits.bytes,
        max_scan_rows: limits.rows,
        deadline_ms: deadline,
        require_fresh_manifest: Process.get(:pulso_require_fresh_manifest, false)
      ]
    else
      Keyword.delete(QueryRunner.storage_opts(deadline), :max_records)
    end
  end

  defp query_samples(tenant, opts) do
    case Storage.query(:metrics, tenant, opts) do
      {:ok, samples} ->
        charge(:samples, length(samples))
        {:ok, samples}

      {:error, reason}
      when reason in [
             :query_sample_limit,
             :query_scan_limit,
             :query_timeout,
             :manifest_freshness_unavailable,
             :signal_authority_missing
           ] ->
        {:error, reason}

      {:error, :non_finite_sample_value} ->
        {:error, :invalid_stored_sample}

      {:error, reason} ->
        {:error, {:storage_error, reason}}
    end
  end

  defp evaluate_groups(groups, steps, offset, window, op, limits) do
    Enum.reduce_while(groups, {:ok, [], 0, 0}, fn group, acc ->
      evaluate_group(group, acc, steps, offset, window, op, limits)
    end)
    |> case do
      {:ok, result, work, _} ->
        charge(:work, work)
        {:ok, result}

      error ->
        error
    end
  end

  defp evaluate_group(group, {:ok, result, work, points}, steps, offset, window, op, limits) do
    case evaluate_series(group, steps, offset, window, op, work, limits.work) do
      {:ok, {labels, values}, work} when points + length(values) <= limits.points ->
        result = if values == [], do: result, else: [{labels, values} | result]
        {:cont, {:ok, result, work, points + length(values)}}

      {:ok, _, _} ->
        {:halt, {:error, :query_result_limit}}

      error ->
        {:halt, error}
    end
  end

  defp evaluate_series({labels, ordered}, steps, offset, window, op, work, max_work) do
    state = %{remaining: ordered, bucket: :queue.new(), values: [], work: work}

    result =
      Enum.reduce_while(steps, {:ok, state}, fn ts, {:ok, state} ->
        step_bucket(ts, state, offset, window, op, max_work)
      end)

    case result do
      {:ok, state} ->
        labels = if op == :latest, do: labels, else: Map.delete(labels, "__name__")
        {:ok, {labels, Enum.reverse(state.values)}, state.work}

      error ->
        error
    end
  end

  defp step_bucket(ts, state, offset, window, op, max_work) do
    finish = ts - offset
    {arrivals, remaining} = Enum.split_while(state.remaining, &(&1.timestamp_ns <= finish))
    bucket = :queue.join(state.bucket, :queue.from_list(arrivals)) |> expire(finish - window)
    work = state.work + length(arrivals) + if(op in [:latest, :timestamp], do: 1, else: max(1, :queue.len(bucket)))

    if work > max_work do
      {:halt, {:error, :query_work_limit}}
    else
      value = bucket_value(op, bucket, finish - window, finish)
      values = if is_nil(value), do: state.values, else: [{ts, value} | state.values]
      {:cont, {:ok, %{remaining: remaining, bucket: bucket, work: work, values: values}}}
    end
  end

  defp expire(bucket, start) do
    case :queue.peek(bucket) do
      {:value, sample} when sample.timestamp_ns <= start -> expire(:queue.drop(bucket), start)
      _ -> bucket
    end
  end

  defp bucket_value(op, bucket, _, _) when op in [:latest, :timestamp] do
    case :queue.peek_r(bucket) do
      {:value, %{value: :stale}} -> nil
      {:value, sample} -> if(op == :timestamp, do: Time.seconds(sample.timestamp_ns), else: sample.value)
      :empty -> nil
    end
  end

  defp bucket_value(op, bucket, start, finish) do
    value(op, Enum.reject(:queue.to_list(bucket), &(&1.value == :stale)), start, finish)
  end

  # Both storage adapters use unanchored matching; Prometheus selectors anchor
  # the entire label and let dot match newline. Push the wrapped pattern down.
  defp anchored_matcher({name, op, pattern}) when op in [:re, :nre], do: {name, op, "(?s:\\A(?:#{pattern})\\z)"}
  defp anchored_matcher(matcher), do: matcher

  # Rows that only carry exemplars have no value to query.
  defp prepare_series(samples) do
    samples
    |> Enum.reject(&(is_nil(&1.value) and is_nil(&1.histogram)))
    |> Enum.group_by(& &1.labels)
    |> Enum.reduce_while({:ok, [], []}, &prepare_group/2)
  end

  defp prepare_group({labels, group}, {:ok, acc, warnings}) do
    group = Enum.map(group, &point/1)

    if Enum.any?(group, &(not is_integer(&1.timestamp_ns) or not point_value?(&1.value))) do
      {:halt, {:error, :unsupported_sample_value}}
    else
      ordered =
        group
        |> Enum.sort(fn a, b ->
          if a.timestamp_ns == b.timestamp_ns,
            do: sample_priority(a.value, b.value),
            else: a.timestamp_ns < b.timestamp_ns
        end)
        |> Enum.uniq_by(& &1.timestamp_ns)

      conflicts = length(Enum.uniq_by(group, &{&1.timestamp_ns, &1.value})) != length(ordered)

      warnings =
        if conflicts,
          do: [conflict_warning(group) | warnings],
          else: warnings

      {:cont, {:ok, [{labels, ordered} | acc], warnings}}
    end
  end

  # Staleness is carried in a native histogram's Sum, not its scalar column.
  # Normalize before ordering/windowing so existing stale precedence applies.
  defp point(%{histogram: %Histogram{sum: :stale}} = sample), do: %{sample | value: :stale}
  defp point(%{histogram: %Histogram{} = h} = sample), do: %{sample | value: h}
  defp point(sample), do: sample

  defp point_value?(%Histogram{}), do: true
  defp point_value?(value), do: Number.valid?(value)

  defp negate(%Histogram{} = h), do: NativeHistogram.scale(h, -1.0)
  defp negate(value), do: Number.negate(value)

  defp conflict_warning(group) do
    if Enum.any?(group, &(&1.value == :stale)),
      do: "Conflicting samples at the same timestamp were resolved using staleness first, then the maximum value.",
      else: "Conflicting samples at the same timestamp were resolved using the maximum value."
  end

  # Prometheus computes these over windows of only histograms, or only
  # floats; a mixed window has no result. `count_over_time` counts every
  # sample, `irate` looks at the last two, and every other function only
  # reads floats.
  @histogram_range_ops [:rate, :increase, :delta, :sum_over_time, :avg_over_time]

  defp value(:count_over_time, samples, _, _), do: if(samples != [], do: length(samples) * 1.0)

  defp value(:irate, samples, start, finish) do
    case Enum.take(samples, -2) do
      [%{value: %Histogram{}}, %{value: %Histogram{}}] = last -> NativeRange.value(:irate, last, start, finish)
      last -> if !Enum.any?(last, &histogram?/1), do: float_value(:irate, last, start, finish)
    end
  end

  defp value(op, samples, start, finish) do
    {histograms, floats} = Enum.split_with(samples, &histogram?/1)

    cond do
      op not in @histogram_range_ops -> float_value(op, floats, start, finish)
      floats == [] and histograms != [] -> NativeRange.value(op, histograms, start, finish)
      histograms == [] -> float_value(op, floats, start, finish)
      true -> nil
    end
  end

  defp histogram?(%{value: %Histogram{}}), do: true
  defp histogram?(_), do: false

  defp float_value(_, [], _, _), do: nil

  defp float_value(op, samples, start, finish) when op in [:rate, :increase, :delta] do
    if length(samples) >= 2, do: extrapolated_change(op, samples, start, finish)
  end

  defp float_value(:irate, samples, _, _) do
    case Enum.take(samples, -2) do
      [first, last] ->
        change =
          if Number.compare("<", last.value, first.value),
            do: last.value,
            else: Number.binary("-", last.value, first.value)

        Number.divide(change, Time.seconds(last.timestamp_ns - first.timestamp_ns))

      _ ->
        nil
    end
  end

  defp float_value(:sum_over_time, samples, _, _), do: aggregate(:sum, Enum.map(samples, & &1.value))
  defp float_value(:avg_over_time, samples, _, _), do: aggregate(:avg, Enum.map(samples, & &1.value))
  defp float_value(:min_over_time, samples, _, _), do: aggregate(:min, Enum.map(samples, & &1.value))
  defp float_value(:max_over_time, samples, _, _), do: aggregate(:max, Enum.map(samples, & &1.value))
  defp float_value(:stddev_over_time, samples, _, _), do: RangeMath.deviation(samples)
  defp float_value(:stdvar_over_time, samples, _, _), do: RangeMath.variance(samples)

  defp float_value({:predict_linear, horizons, offset}, samples, _, finish),
    do: RangeMath.predict(samples, finish + offset, Map.fetch!(horizons, finish + offset))

  defp extrapolated_change(op, samples, start, finish) do
    first = hd(samples)
    last = List.last(samples)
    change = Number.binary("-", last.value, first.value)

    {change, _} =
      Enum.reduce(tl(samples), {change, first.value}, fn sample, {delta, previous} ->
        reset = if op != :delta and Number.compare("<", sample.value, previous), do: previous, else: 0
        {Number.add(delta, reset), sample.value}
      end)

    interval = Time.seconds(last.timestamp_ns - first.timestamp_ns)
    average = interval / (length(samples) - 1)
    left = boundary_duration(Time.seconds(first.timestamp_ns - start), average)
    right = boundary_duration(Time.seconds(finish - last.timestamp_ns), average)

    left =
      if op != :delta and is_number(change) and change > 0 and is_number(first.value) and first.value >= 0,
        do: min(left, interval * first.value / change),
        else: left

    factor = (interval + left + right) / interval
    factor = if op == :rate, do: factor / Time.seconds(finish - start), else: factor
    Number.multiply(change, factor)
  end

  defp boundary_duration(distance, average) do
    if distance >= average * 1.1, do: average / 2, else: distance
  end

  defp sample_priority(:stale, _), do: true
  defp sample_priority(_, :stale), do: false
  defp sample_priority(%Histogram{}, _), do: true
  defp sample_priority(_, %Histogram{}), do: false
  defp sample_priority(:nan, _), do: false
  defp sample_priority(_, :nan), do: true
  defp sample_priority(a, b), do: not Number.less?(a, b)

  defp aggregate_point(op, {{labels, ts}, values}) do
    case aggregate_values(op, values) do
      nil -> []
      value -> [{labels, {ts, value}}]
    end
  end

  # As in Prometheus: `count` and `group` take every sample; `sum` and `avg`
  # combine groups of only histograms (mixed groups have no result); every
  # other aggregation ignores histograms.
  defp aggregate_values(op, values) when op in [:count, :group], do: aggregate(op, values)

  defp aggregate_values(op, values) do
    {histograms, floats} = Enum.split_with(values, &match?(%Histogram{}, &1))

    cond do
      histograms == [] -> aggregate(op, floats)
      op in [:sum, :avg] and floats == [] -> NativeRange.aggregate(op, histograms)
      op in [:sum, :avg] or floats == [] -> nil
      true -> aggregate(op, floats)
    end
  end

  defp aggregate(:sum, values), do: Number.sum(values)
  defp aggregate(:avg, values), do: Number.average(values)
  defp aggregate(:min, values), do: Enum.reduce(values, :nan, &Number.min/2)
  defp aggregate(:max, values), do: Enum.reduce(values, :nan, &Number.max/2)
  defp aggregate(:count, values), do: length(values) * 1.0
  defp aggregate(:group, _values), do: 1.0
  defp absent_labels({:selector, matchers, _}), do: absence_labels(matchers)
  defp absent_labels({:range, matchers, _, _}), do: absence_labels(matchers)
  defp absent_labels(_), do: %{}

  # Prometheus omits metric names, regex labels and repeated matcher labels
  # when inferring the single absent-series label set.
  defp absence_labels(matchers) do
    matchers
    |> order_absence_matchers()
    |> Enum.reduce({%{}, MapSet.new()}, fn
      {"__name__", _, _}, state ->
        state

      {name, :eq, value}, {labels, seen} ->
        labels =
          if MapSet.member?(seen, name) or value == "", do: Map.delete(labels, name), else: Map.put(labels, name, value)

        {labels, MapSet.put(seen, name)}

      {name, _, _}, {labels, seen} ->
        {Map.delete(labels, name), seen}
    end)
    |> elem(0)
  end

  # TSDB PostingsForMatchers mutates the AST matcher slice before absent()
  # infers labels: intersecting matchers precede subtracting matchers. Match
  # that observable engine behavior, not only functions.go's ordered walk.
  defp order_absence_matchers(matchers) do
    annotated = Enum.map(matchers, fn matcher -> {matcher, matcher_matches_empty?(matcher)} end)
    required = MapSet.new(for {{name, _, _}, false} <- annotated, do: name)

    {intersecting, subtracting} =
      Enum.split_with(annotated, fn {{name, op, _}, empty?} ->
        MapSet.member?(required, name) and not (op in [:neq, :nre] and empty?)
      end)

    Enum.map(intersecting ++ subtracting, &elem(&1, 0))
  end

  defp matcher_matches_empty?({_, :eq, value}), do: value == ""
  defp matcher_matches_empty?({_, :neq, value}), do: value != ""

  defp matcher_matches_empty?({_, op, pattern}) do
    matched = NIF.match_metric_regex("(?s:\\A(?:#{pattern})\\z)", "")
    if op == :re, do: matched, else: not matched
  end

  defp group_key(_, nil), do: %{}
  defp group_key(labels, {:grouping, :by, names}), do: Map.take(labels, names)
  defp group_key(labels, {:grouping, :without, names}), do: Map.drop(labels, ["__name__" | names])

  # Match Prometheus's shortest-decimal wire formatting and exponent cutoffs.
  @doc false
  # Go `strconv.FormatFloat(v, 'f', -1, 64)`-style text, as Prometheus and Loki return.
  def format_value(value) when is_atom(value), do: Number.format(value)
  def format_value(value) when is_integer(value), do: Integer.to_string(value)

  def format_value(value) do
    text = Float.to_string(value)

    case String.split(text, "e") do
      [plain] -> String.trim_trailing(plain, ".0")
      [mantissa, exponent] -> format_exponent(mantissa, String.to_integer(exponent), abs(value))
    end
  end

  defp format_exponent(mantissa, exponent, magnitude) when magnitude < 1.0e-6 or magnitude >= 1.0e21 do
    sign = if exponent < 0, do: "-", else: "+"
    String.trim_trailing(mantissa, ".0") <> "e" <> sign <> String.pad_leading(Integer.to_string(abs(exponent)), 2, "0")
  end

  defp format_exponent(mantissa, exponent, _) do
    sign = if String.starts_with?(mantissa, "-"), do: "-", else: ""
    [whole, fraction] = mantissa |> String.trim_leading("-") |> String.split(".")
    digits = whole <> fraction
    position = byte_size(whole) + exponent

    decimal =
      cond do
        position <= 0 -> "0." <> String.duplicate("0", -position) <> digits
        position >= byte_size(digits) -> digits <> String.duplicate("0", position - byte_size(digits))
        true -> binary_part(digits, 0, position) <> "." <> binary_part(digits, position, byte_size(digits) - position)
      end

    trimmed =
      if String.contains?(decimal, "."),
        do: decimal |> String.trim_trailing("0") |> String.trim_trailing("."),
        else: decimal

    sign <> trimmed
  end

  # Prometheus reports histogram points under `histogram` (instant vectors) or
  # `histograms` (ranges), beside float `value`/`values`.
  defp result_series(:vector, labels, [{ts, %Histogram{} = h}]),
    do: %{"metric" => labels, "histogram" => [Time.seconds(ts), format_histogram(h)]}

  defp result_series(:vector, labels, [{ts, value}]),
    do: %{"metric" => labels, "value" => [Time.seconds(ts), format_value(value)]}

  defp result_series(:matrix, labels, samples) do
    {histograms, floats} = Enum.split_with(samples, &match?({_, %Histogram{}}, &1))
    series = %{"metric" => labels}
    series = if floats == [], do: series, else: Map.put(series, "values", Enum.map(floats, &format_point/1))
    if histograms == [], do: series, else: Map.put(series, "histograms", Enum.map(histograms, &format_point/1))
  end

  defp format_point({ts, %Histogram{} = h}), do: [Time.seconds(ts), format_histogram(h)]
  defp format_point({ts, value}), do: [Time.seconds(ts), format_value(value)]

  @doc false
  # Prometheus' JSON histogram: count, sum and its non-empty buckets as
  # `[boundary_rule, lower, upper, count]`.
  def format_histogram(%Histogram{} = h) do
    buckets =
      for {rule, lower, upper, count} <- NativeHistogram.buckets(h),
          do: [rule, format_value(lower), format_value(upper), format_value(count)]

    base = %{"count" => format_value(h.count), "sum" => format_value(h.sum)}
    if buckets == [], do: base, else: Map.put(base, "buckets", buckets)
  end

  defp envelope(:vector, [{_, [{ts, value}]}], :scalar, _ordered) do
    %{
      "status" => "success",
      "data" => %{"resultType" => "scalar", "result" => [Time.seconds(ts), format_value(value)]}
    }
  end

  defp envelope(kind, series, _type, ordered) do
    series = if kind == :vector and ordered, do: series, else: Enum.sort_by(series, &elem(&1, 0))

    result =
      series
      |> Enum.map(fn {labels, samples} -> result_series(kind, labels, samples) end)

    %{"status" => "success", "data" => %{"resultType" => Atom.to_string(kind), "result" => result}}
  end
end
