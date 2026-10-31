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
  alias Pulso.PromQL.Duration
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
                     ~w(rate increase irate idelta delta changes resets deriv sum_over_time avg_over_time min_over_time max_over_time count_over_time stddev_over_time stdvar_over_time mad_over_time absent_over_time present_over_time first_over_time last_over_time ts_of_first_over_time ts_of_last_over_time ts_of_min_over_time ts_of_max_over_time)a,
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
          run_query(expr, tenant, steps, kind, opts)
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

  defp run_source(expr, tenant, steps, kind, opts), do: run_query(expr, tenant, steps, kind, opts)

  defp run_query(expr, tenant, steps, kind, opts) do
    lookback = Map.get(opts, :lookback_ns, @lookback_ns)

    if not (is_integer(lookback) and lookback > 0 and lookback <= 9_223_372_036_854_775_807),
      do: throw({:promql_error, :invalid_lookback})

    # Set this for every source in a batch so one source cannot change another's
    # default. Nested selectors and timestamp() share the current source's value.
    Process.put(:promql_lookback_ns, lookback)
    Process.put(:promql_infos, MapSet.new())
    Process.put(:promql_warnings, MapSet.new())
    Process.put(:promql_range_query, kind == :matrix)
    Process.put(:promql_query_context, query_context(opts, steps))

    Process.put(
      :promql_range_sort_warning,
      kind == :matrix and Map.get(opts, :start_ts_ns) != Map.get(opts, :end_ts_ns)
    )

    if !Process.get(:promql_batch_selectors),
      do: Process.put(:promql_budget, %{samples: 0, work: 0, points: 0, materialized: 0, log_samples: 0})

    with :ok <- query_result_type(expr, kind),
         {:ok, expr, work} <- Duration.resolve(expr, Process.get(:promql_query_context)),
         charge(:work, work),
         {:ok, series, warnings} <- evaluate(expr, tenant, steps) do
      finish_result(expr, series, warnings, kind, steps)
    end
  rescue
    ArithmeticError -> {:error, :nonfinite_result}
  catch
    {:promql_error, reason} -> {:error, reason}
  end

  defp finish_result(expr, series, warnings, kind, steps) do
    series =
      case series do
        {:string, value} -> [{%{}, [{hd(steps), value}]}]
        series -> Operations.coalesce(series)
      end

    points = Enum.reduce(series, 0, fn {_, values}, total -> total + length(values) end)
    if points > limits().points, do: throw({:promql_error, :query_result_limit})
    if Process.get(:promql_batch_selectors), do: charge(:points, points)

    if length(Enum.uniq_by(series, &elem(&1, 0))) != length(series),
      do: throw({:promql_error, :duplicate_result_label_sets})

    result = envelope(kind, series, Parser.type(expr), ordered?(expr))
    {:ok, annotate(result, warnings)}
  end

  # Like upstream, a range request accepts only scalars and instant vectors,
  # even when start == end. Reject before resolving or reading any selector.
  defp query_result_type(expr, :matrix) do
    if Parser.type(expr) in [:scalar, :vector], do: :ok, else: {:error, :invalid_result_type}
  end

  defp query_result_type(_expr, :vector), do: :ok

  defp query_context(opts, steps) do
    # Use requested outer bounds, not the last grid point or a subquery's grid.
    # An instant query's default time is already fixed by evaluation_steps/1.
    first = Map.get(opts, :start_ts_ns, hd(steps))
    last = Map.get(opts, :end_ts_ns, hd(steps))

    %{
      "start" => Time.seconds(first),
      "end" => Time.seconds(last),
      "start_ts_ns" => first,
      "end_ts_ns" => last,
      "step" => if(first == last, do: 0.0, else: Time.seconds(Map.fetch!(opts, :step_ns))),
      "range" => Time.seconds(min(last - first, 9_223_372_036_854_775_807))
    }
  end

  defp annotate(result, warnings) do
    warnings = warnings ++ Enum.sort(Process.get(:promql_warnings))
    result = if warnings == [], do: result, else: Map.put(result, "warnings", Enum.uniq(warnings))
    infos = Enum.sort(Process.get(:promql_infos))
    if infos == [], do: result, else: Map.put(result, "infos", infos)
  end

  @doc false
  def info(message), do: Process.put(:promql_infos, MapSet.put(Process.get(:promql_infos), message))

  @doc false
  def warning(message), do: Process.put(:promql_warnings, MapSet.put(Process.get(:promql_warnings), message))

  @doc false
  def quantile_warning(q) do
    if Number.nan?(q) or Number.compare("<", q, 0) or Number.compare(">", q, 1) do
      warning = "PromQL warning: quantile value should be between 0 and 1, got " <> Number.format(q, :general)
      Process.put(:promql_warnings, MapSet.put(Process.get(:promql_warnings), warning))
    end
  end

  @doc false
  def ratio_warning(value, capped) do
    warning =
      "PromQL warning: ratio value should be between -1 and 1, got " <>
        Number.format(value, :general) <>
        ", capping to " <> Number.format(capped, :general)

    Process.put(:promql_warnings, MapSet.put(Process.get(:promql_warnings), warning))
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
  defp ordered?({:call, name, _}), do: name in ["sort", "sort_desc", "sort_by_label", "sort_by_label_desc"]
  defp ordered?({:parameter_aggregate, name, _, _, _}), do: name in ["topk", "bottomk"]
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
    do: select(tenant, matchers, offset, Process.get(:promql_lookback_ns), steps, :timestamp)

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

  defp evaluate({:call, "quantile_over_time", [parameter, range]}, tenant, steps) do
    offset = elem(range, tuple_size(range) - 1)

    with {:ok, [{_, values}], warnings} <- evaluate(parameter, tenant, steps),
         {:ok, result, extra} <-
           evaluate_range(range, tenant, steps, {:quantile_over_time, Map.new(values), offset}) do
      {:ok, result, warnings ++ extra}
    end
  end

  defp evaluate({:call, name, [range, smoothing, trend]}, tenant, steps)
       when name in ["double_exponential_smoothing", "holt_winters"] do
    offset = elem(range, tuple_size(range) - 1)

    with {:ok, [[{_, sf}], [{_, tf}]], warnings} <- evaluate_args([smoothing, trend], tenant, steps) do
      factors = Map.new(Enum.zip_with(sf, tf, fn {time, sf}, {_, tf} -> {time, {sf, tf}} end))

      with {:ok, result, extra} <-
             evaluate_range(range, tenant, steps, {:double_exponential_smoothing, factors, offset}),
           do: {:ok, result, warnings ++ extra}
    end
  end

  defp evaluate({:call, name, [range]}, tenant, steps) when is_map_key(@range_functions, name),
    do: evaluate({:function, Map.fetch!(@range_functions, name), range}, tenant, steps)

  defp evaluate({:call, name, args}, tenant, steps)
       when name in ["sort", "sort_desc", "sort_by_label", "sort_by_label_desc"] do
    with {:ok, [vector | _] = values, warnings} <- evaluate_args(args, tenant, steps) do
      if Process.get(:promql_range_sort_warning) do
        message = "PromQL warning: sort is ineffective for range queries since results are always ordered by labels"
        Process.put(:promql_warnings, MapSet.put(Process.get(:promql_warnings), message))
      end

      result = if Process.get(:promql_range_query), do: vector, else: Operations.call(name, values, steps)
      {:ok, result, warnings}
    end
  end

  defp evaluate({:call, name, args}, tenant, steps) do
    with {:ok, values, warnings} <- evaluate_args(args, tenant, steps) do
      {:ok, Operations.call(name, values, steps), warnings}
    end
  end

  defp evaluate({:parameter_aggregate, op, grouping, parameter, inner}, tenant, steps) do
    with {:ok, k, kw} <- evaluate(parameter, tenant, steps),
         {:ok, vector, warnings} <- evaluate(inner, tenant, steps) do
      {:ok, Operations.parameter_aggregate(op, grouping, k, vector, steps), kw ++ warnings}
    end
  end

  defp evaluate({:aggregate, op, grouping, inner}, tenant, steps) do
    with {:ok, series, warnings} <- evaluate(inner, tenant, steps) do
      grouped =
        for {labels, samples} <- series,
            {ts, value} <- samples,
            do: {{group_key(labels, grouping), ts}, {value, Map.get(labels, "__name__", "")}}

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
    select(tenant, matchers, offset, Process.get(:promql_lookback_ns), steps, :latest)
  end

  defp evaluate(expr, tenant, steps) when elem(expr, 0) in [:range, :subquery],
    do: evaluate_range(expr, tenant, steps, :raw_range)

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
    first = (Integer.floor_div(source_time(hd(steps), offset) - window, resolution) + 1) * resolution
    last = Integer.floor_div(source_time(List.last(steps), offset), resolution) * resolution
    count = max(0, div(last - first, resolution) + 1)
    if count > @max_steps, do: throw({:promql_error, :query_work_limit})

    if count > 0 and not (timestamp?(first) and timestamp?(last)),
      do: throw({:promql_error, :invalid_evaluation_time_or_step})

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
      start_ts: max(-9_223_372_036_854_775_808, source_time(hd(steps), offset) - window + 1),
      end_ts: min(9_223_372_036_854_775_807, source_time(List.last(steps), offset)),
      matchers: Enum.map(matchers, &anchored_matcher/1),
      max_records: limits.samples,
      max_scan_segments: limits.segments,
      max_scan_bytes: limits.bytes,
      max_scan_rows: limits.rows,
      deadline_ms: Process.get(:promql_deadline_ms),
      require_fresh_manifest: Process.get(:pulso_require_fresh_manifest, false)
    ]

    if opts[:start_ts] > opts[:end_ts] do
      {:ok, [], []}
    else
      with {:ok, samples} <- query_samples(tenant, opts),
           {:ok, groups, warnings} <- prepare_series(samples),
           {:ok, result} <- evaluate_groups(groups, steps, offset, window, op, limits) do
        {:ok, Operations.coalesce(result), warnings}
      end
    end
  end

  defp source_time(_step, {:at, anchor, shift}) do
    anchor = if is_binary(anchor), do: Map.fetch!(Process.get(:promql_query_context), anchor <> "_ts_ns"), else: anchor
    anchor - shift
  end

  defp source_time(step, shift), do: step - shift

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

  defp evaluate_series({labels, ordered}, [step], offset, window, :raw_range, work, max_work) do
    finish = source_time(step, offset)
    work = work + length(ordered)

    if work > max_work do
      {:error, :query_work_limit}
    else
      points =
        for sample <- ordered,
            sample.timestamp_ns > finish - window and sample.timestamp_ns <= finish and sample.value != :stale,
            do: {sample.timestamp_ns, sample.value}

      {:ok, {labels, points}, work}
    end
  end

  defp evaluate_series({labels, ordered}, steps, offset, window, op, work, max_work) do
    state = %{
      remaining: ordered,
      bucket: :queue.new(),
      values: [],
      work: work,
      metric_name: Map.get(labels, "__name__", "")
    }

    result =
      Enum.reduce_while(steps, {:ok, state}, fn ts, {:ok, state} ->
        step_bucket(ts, state, offset, window, op, max_work)
      end)

    case result do
      {:ok, state} ->
        labels = if op in [:latest, :first_over_time, :last_over_time], do: labels, else: Map.delete(labels, "__name__")
        {:ok, {labels, Enum.reverse(state.values)}, state.work}

      error ->
        error
    end
  end

  defp step_bucket(ts, state, offset, window, op, max_work) do
    finish = source_time(ts, offset)
    {arrivals, remaining} = Enum.split_while(state.remaining, &(&1.timestamp_ns <= finish))
    bucket = :queue.join(state.bucket, :queue.from_list(arrivals)) |> expire(finish - window)
    work = state.work + length(arrivals) + if(op in [:latest, :timestamp], do: 1, else: max(1, :queue.len(bucket)))

    if work > max_work do
      {:halt, {:error, :query_work_limit}}
    else
      # Pin only the data window. Scalar parameters and prediction reference
      # time still belong to this evaluation step, not the @ timestamp.
      op =
        case op do
          {name, parameters, _shift} -> {name, parameters, ts - finish}
          other -> other
        end

      value = bucket_value(op, bucket, finish - window, finish, state.metric_name)
      values = if is_nil(value), do: state.values, else: [{ts, value} | state.values]
      {:cont, {:ok, %{state | remaining: remaining, bucket: bucket, work: work, values: values}}}
    end
  end

  defp expire(bucket, start) do
    case :queue.peek(bucket) do
      {:value, sample} when sample.timestamp_ns <= start -> expire(:queue.drop(bucket), start)
      _ -> bucket
    end
  end

  defp bucket_value(op, bucket, _, _, _metric_name) when op in [:latest, :timestamp] do
    case :queue.peek_r(bucket) do
      {:value, %{value: :stale}} -> nil
      {:value, sample} -> if(op == :timestamp, do: Time.seconds(sample.timestamp_ns), else: sample.value)
      :empty -> nil
    end
  end

  defp bucket_value(op, bucket, start, finish, metric_name) do
    samples = Enum.reject(:queue.to_list(bucket), &(&1.value == :stale))
    note_mixed_histograms(op, samples, metric_name)

    if op in [:rate, :increase] and Enum.all?(samples, &histogram?/1),
      do: NativeRange.value(op, samples, start, finish, &note_histogram_counter(&1, metric_name)),
      else: value(op, samples, start, finish)
  end

  # Both storage adapters use unanchored matching; Prometheus selectors anchor
  # the entire label and let dot match newline. Push the wrapped pattern down.
  defp anchored_matcher({name, op, pattern}) when op in [:re, :nre], do: {name, op, Pulso.QueryRegex.anchor(pattern)}
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

  defp negate(%Histogram{} = h) do
    charge(:work, length(h.positive_buckets) + length(h.negative_buckets))
    NativeHistogram.scale(h, -1.0)
  end

  defp negate(value), do: Number.negate(value)

  defp conflict_warning(group) do
    if Enum.any?(group, &(&1.value == :stale)),
      do: "Conflicting samples at the same timestamp were resolved using staleness first, then the maximum value.",
      else: "Conflicting samples at the same timestamp were resolved using the maximum value."
  end

  # Prometheus computes these over windows of only histograms, or only
  # floats; a mixed window has no result. `count_over_time` counts every
  # sample; presence, first/last selection and transition counts accept either
  # type. `irate`/`idelta` look at the last two; the remaining functions only read floats.
  @histogram_range_ops [:rate, :increase, :delta, :sum_over_time, :avg_over_time]

  defp value(:count_over_time, samples, _, _), do: if(samples != [], do: length(samples) * 1.0)
  defp value(:present_over_time, samples, _, _), do: if(samples != [], do: 1.0)
  defp value(:first_over_time, samples, _, _), do: if(samples != [], do: hd(samples).value)
  defp value(:last_over_time, samples, _, _), do: if(samples != [], do: List.last(samples).value)
  defp value(:ts_of_first_over_time, samples, _, _), do: if(samples != [], do: Time.seconds(hd(samples).timestamp_ns))

  defp value(:ts_of_last_over_time, samples, _, _),
    do: if(samples != [], do: Time.seconds(List.last(samples).timestamp_ns))

  defp value(op, [], _, _) when op in [:changes, :resets], do: nil

  defp value(op, [first | rest], _, _) when op in [:changes, :resets] do
    {count, _} =
      Enum.reduce(rest, {0.0, first.value}, fn sample, {count, previous} ->
        increment = if counted_transition?(op, previous, sample.value), do: 1.0, else: 0.0
        {count + increment, sample.value}
      end)

    count
  end

  defp value(op, samples, start, finish) when op in [:irate, :idelta] do
    case Enum.take(samples, -2) do
      [%{value: %Histogram{}}, %{value: %Histogram{}}] = last -> NativeRange.value(op, last, start, finish)
      last -> if !Enum.any?(last, &histogram?/1), do: float_value(op, last, start, finish)
    end
  end

  defp value({:double_exponential_smoothing, _, _}, [], _, _), do: nil

  defp value({:double_exponential_smoothing, factors, offset}, samples, _, finish) do
    {sf, tf} = Map.fetch!(factors, finish + offset)

    # Validate nonempty windows before discarding histograms or requiring two
    # floats. NaN passes these comparisons, as in Prometheus's IEEE arithmetic.
    for {name, factor} <- [smoothing: sf, trend: tf] do
      if Number.compare("<=", factor, 0) or Number.compare(">=", factor, 1),
        do: throw({:promql_error, {:invalid_smoothing_factor, name, factor}})
    end

    samples |> Enum.reject(&histogram?/1) |> RangeMath.smooth(sf, tf)
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

  defp note_mixed_histograms(op, samples, metric_name) when op in [:idelta, :irate] do
    case Enum.take(samples, -2) do
      [%{value: %Histogram{} = first}, %{value: %Histogram{} = last}] ->
        note_histogram_difference(op, first, last, metric_name)

      [first, last] ->
        if histogram?(first) != histogram?(last),
          do:
            warning(
              "PromQL warning: encountered a mix of histograms and floats for metric name " <>
                Pulso.JSON.encode!(metric_name)
            )

      _ ->
        :ok
    end
  end

  defp note_mixed_histograms(:delta, [first, _second | _] = samples, metric_name) do
    {histograms, floats} = Enum.split_with(samples, &histogram?/1)

    cond do
      histograms == [] ->
        :ok

      floats != [] ->
        warning(
          "PromQL warning: encountered a mix of histograms and floats for metric name " <>
            Pulso.JSON.encode!(metric_name)
        )

      first.value.schema == Histogram.custom_buckets_schema() !=
          (List.last(samples).value.schema == Histogram.custom_buckets_schema()) ->
        note_histogram_subtraction(first.value, List.last(samples).value, metric_name)

      true ->
        note_histogram_difference(:delta, first.value, List.last(samples).value, metric_name)
    end
  end

  defp note_mixed_histograms(op, samples, metric_name) when op in [:sum_over_time, :avg_over_time] do
    {histograms, floats} = Enum.split_with(samples, &histogram?/1)

    cond do
      histograms == [] ->
        :ok

      floats != [] ->
        warning(
          "PromQL warning: encountered a mix of histograms and floats for metric name " <>
            Pulso.JSON.encode!(metric_name)
        )

      true ->
        note_histogram_aggregation(Enum.map(histograms, &{&1.value, metric_name}))
    end
  end

  defp note_mixed_histograms(op, samples, metric_name) when op in [:rate, :increase] do
    {histograms, floats} = Enum.split_with(samples, &histogram?/1)

    cond do
      histograms != [] and floats != [] ->
        warning(
          "PromQL warning: encountered a mix of histograms and floats for metric name " <>
            Pulso.JSON.encode!(metric_name)
        )

      length(histograms) >= 2 ->
        work =
          Enum.reduce(histograms, 0, fn %{value: h}, total ->
            total + length(h.positive_buckets) + length(h.negative_buckets) + length(h.custom_values)
          end)

        charge(:work, work)

      true ->
        :ok
    end
  end

  defp note_mixed_histograms(op, samples, metric_name) do
    annotate? =
      op in [:deriv, :mad_over_time, :ts_of_min_over_time, :ts_of_max_over_time] or
        match?({name, _, _} when name in [:predict_linear, :quantile_over_time, :double_exponential_smoothing], op)

    if annotate? and Enum.any?(samples, &histogram?/1) and Enum.any?(samples, &(not histogram?(&1))) do
      info(
        "PromQL info: ignored histograms in a range containing both floats and histograms for metric name " <>
          Pulso.JSON.encode!(metric_name)
      )
    end
  end

  defp note_histogram_counter(:not_counter, metric_name),
    do: warning("PromQL warning: this native histogram metric is not a counter: " <> Pulso.JSON.encode!(metric_name))

  defp note_histogram_counter(:mixed_schema, metric_name),
    do:
      warning(
        "PromQL warning: vector contains a mix of histograms with exponential and custom buckets schemas for metric name " <>
          Pulso.JSON.encode!(metric_name)
      )

  defp note_histogram_counter(:reconciled_addition, _metric_name),
    do: info("PromQL info: mismatched custom buckets were reconciled during addition")

  defp note_histogram_counter(:reconciled_subtraction, _metric_name),
    do: info("PromQL info: mismatched custom buckets were reconciled during subtraction")

  defp note_histogram_aggregation([{first, _} | rest] = histograms) do
    work =
      Enum.reduce(histograms, 0, fn {h, _}, total ->
        total + length(h.positive_buckets) + length(h.negative_buckets) + length(h.custom_values)
      end)

    charge(:work, work)
    custom? = first.schema == Histogram.custom_buckets_schema()

    # Track original hints, not the accumulator's combined hint: an intervening
    # gauge or unknown must not hide a reset/no-reset collision. Stop at the
    # first incompatible family, just as the arithmetic does.
    Enum.reduce_while(rest, MapSet.new([first.counter_reset_hint]), fn {h, metric_name}, hints ->
      hints = MapSet.put(hints, h.counter_reset_hint)

      if MapSet.member?(hints, :yes) and MapSet.member?(hints, :no),
        do: warning("PromQL warning: conflicting counter resets during histogram aggregation")

      cond do
        custom? != (h.schema == Histogram.custom_buckets_schema()) ->
          warning(
            "PromQL warning: vector contains a mix of histograms with exponential and custom buckets schemas for metric name " <>
              Pulso.JSON.encode!(metric_name)
          )

          {:halt, hints}

        custom? and first.custom_values != h.custom_values ->
          info("PromQL info: mismatched custom buckets were reconciled during aggregation")
          {:cont, hints}

        true ->
          {:cont, hints}
      end
    end)
  end

  defp note_histogram_difference(op, first, last, metric_name) do
    charge(
      :work,
      length(first.positive_buckets) + length(first.negative_buckets) + length(first.custom_values) +
        length(last.positive_buckets) + length(last.negative_buckets) + length(last.custom_values)
    )

    gauges = Enum.count([first, last], &(&1.counter_reset_hint == :gauge))
    invalid_type? = if op == :irate, do: gauges > 0, else: gauges < 2

    if invalid_type? do
      expected = if op == :irate, do: "counter", else: "gauge"
      warning("PromQL warning: this native histogram metric is not a #{expected}: " <> Pulso.JSON.encode!(metric_name))
    end

    # A counter reset uses the last histogram directly, without subtraction.
    # In that case differing schemas/bounds cannot produce subtraction notices.
    if op != :irate or not NativeHistogram.detect_reset?(last, first),
      do: note_histogram_subtraction(first, last, metric_name)
  end

  defp note_histogram_subtraction(first, last, metric_name) do
    first_custom? = first.schema == Histogram.custom_buckets_schema()
    last_custom? = last.schema == Histogram.custom_buckets_schema()

    cond do
      first_custom? != last_custom? ->
        warning(
          "PromQL warning: vector contains a mix of histograms with exponential and custom buckets schemas for metric name " <>
            Pulso.JSON.encode!(metric_name)
        )

      first_custom? and first.custom_values != last.custom_values ->
        info("PromQL info: mismatched custom buckets were reconciled during subtraction")

      true ->
        :ok
    end
  end

  defp counted_transition?(:changes, %Histogram{} = previous, %Histogram{} = current),
    do: not NativeHistogram.equal?(previous, current)

  defp counted_transition?(:resets, %Histogram{} = previous, %Histogram{} = current),
    do: NativeHistogram.detect_reset?(current, previous)

  defp counted_transition?(_, %Histogram{}, _), do: true
  defp counted_transition?(_, _, %Histogram{}), do: true
  defp counted_transition?(:resets, previous, current), do: Number.less?(current, previous)
  defp counted_transition?(:changes, :nan, :nan), do: false
  defp counted_transition?(:changes, previous, current), do: not Number.compare("==", previous, current)

  defp histogram?(%{value: %Histogram{}}), do: true
  defp histogram?(_), do: false

  defp float_value(_, [], _, _), do: nil

  defp float_value(op, samples, start, finish) when op in [:rate, :increase, :delta] do
    if length(samples) >= 2, do: extrapolated_change(op, samples, start, finish)
  end

  defp float_value(:idelta, [first, last], _, _), do: Number.binary("-", last.value, first.value)
  defp float_value(:idelta, _, _, _), do: nil

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
  defp float_value(:deriv, samples, _, _), do: RangeMath.deriv(samples)

  defp float_value(op, [first | rest], _, _) when op in [:ts_of_min_over_time, :ts_of_max_over_time] do
    comparison = if op == :ts_of_min_over_time, do: "<=", else: ">="

    # Samples are chronological: equality replaces the best with the newest.
    # A NaN best is replaceable; a later NaN cannot displace a non-NaN value.
    best =
      Enum.reduce(rest, first, fn sample, best ->
        if Number.nan?(best.value) or Number.compare(comparison, sample.value, best.value), do: sample, else: best
      end)

    Time.seconds(best.timestamp_ns)
  end

  defp float_value(:mad_over_time, samples, _, _) do
    count = length(samples)
    # Reserve both sorts before allocating their lists. The window scan is
    # charged separately; logarithmic sorting work must not look linear.
    charge(:work, 2 * count * length(Integer.digits(count, 2)))
    RangeMath.mad(samples)
  end

  defp float_value({:quantile_over_time, quantiles, offset}, samples, _, finish) do
    q = Map.fetch!(quantiles, finish + offset)
    count = length(samples)
    charge(:work, count * length(Integer.digits(count, 2)))
    quantile_warning(q)
    RangeMath.quantile(q, samples)
  end

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
    {histograms, floats} = Enum.split_with(values, &match?({%Histogram{}, _}, &1))
    floats = Enum.map(floats, &elem(&1, 0))

    if histograms != [] and op in [:stddev, :stdvar],
      do: info("PromQL info: ignored histogram in #{op} aggregation")

    cond do
      histograms == [] ->
        aggregate(op, floats)

      op in [:sum, :avg] and floats == [] ->
        note_histogram_aggregation(histograms)
        NativeRange.aggregate(op, Enum.map(histograms, &elem(&1, 0)))

      op in [:sum, :avg] ->
        warning("PromQL warning: encountered a mix of histograms and floats for aggregation")
        nil

      floats == [] ->
        nil

      true ->
        aggregate(op, floats)
    end
  end

  defp aggregate(:sum, values), do: Number.sum(values)
  defp aggregate(:avg, values), do: Number.average(values)
  defp aggregate(:min, values), do: Enum.reduce(values, :nan, &Number.min/2)
  defp aggregate(:max, values), do: Enum.reduce(values, :nan, &Number.max/2)
  defp aggregate(:count, values), do: length(values) * 1.0
  defp aggregate(:group, _values), do: 1.0

  defp aggregate(:stdvar, values) do
    # Mimir's aggregation uses ordinary Welford updates, distinct from the
    # compensated range-vector algorithm. Nonfinite singletons yield NaN.
    {count, _, sum} =
      Enum.reduce(values, {0, 0.0, 0.0}, fn value, {count, mean, sum} ->
        delta = Number.binary("-", value, mean)
        mean = Number.add(mean, Number.divide(delta, count + 1))
        sum = Number.add(sum, Number.multiply(delta, Number.binary("-", value, mean)))
        {count + 1, mean, sum}
      end)

    Number.divide(sum, count)
  end

  defp aggregate(:stddev, values) do
    case aggregate(:stdvar, values) do
      value when is_number(value) and value >= 0 -> :math.sqrt(value)
      :infinity -> :infinity
      _ -> :nan
    end
  end

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
    matched = NIF.match_metric_regex(Pulso.QueryRegex.anchor(pattern), "")
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

  defp envelope(:vector, [{_, [{ts, value}]}], :string, _ordered) do
    %{"status" => "success", "data" => %{"resultType" => "string", "result" => [Time.seconds(ts), value]}}
  end

  defp envelope(kind, series, type, ordered) do
    kind = if type == :range, do: :matrix, else: kind
    series = if kind == :vector and ordered, do: series, else: Enum.sort_by(series, &elem(&1, 0))

    result =
      series
      |> Enum.map(fn {labels, samples} -> result_series(kind, labels, samples) end)

    %{"status" => "success", "data" => %{"resultType" => Atom.to_string(kind), "result" => result}}
  end
end
