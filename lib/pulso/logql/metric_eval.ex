defmodule Pulso.LogQL.MetricEval do
  @moduledoc """
  Metric-query evaluator.

  A metric expression is a tree over `RangeAgg`, `VectorAgg`,
  `BinaryOp`, and `NumberLit`. `evaluate/3` walks the tree and produces
  either a `matrix` (a set of time series over the requested steps) or a
  `vector` (one sample per series, at the query's evaluation timestamp).

  Steps and evaluation timestamps come from the caller's `opts`:

    * `start_ts_ns`, `end_ts_ns`, `step_ns` — matrix mode; buckets are
      centered at `start_ts, start_ts + step, ..., end_ts`.
    * `end_ts_ns` alone (no `step_ns`) — vector mode; one sample per
      series at the given timestamp.

  Every range aggregation reads the underlying `%LogQuery{}` via
  `Pulso.LogQL.Evaluator.evaluate_log_raw/3`, so all the same Rust
  pushdown applies. The evaluator groups the returned entries into
  buckets in Elixir and folds the aggregator over each bucket.
  """

  alias Pulso.LogQL.AST
  alias Pulso.LogQL.Entry
  alias Pulso.LogQL.Evaluator
  alias Pulso.LogQL.NativeRange
  alias Pulso.LogQL.Quantile
  alias Pulso.LogQL.Quantity
  alias Pulso.PromQL.Evaluator, as: Budget
  alias Pulso.PromQL.FloatParser
  alias Pulso.PromQL.Number
  alias Pulso.PromQL.Operations
  alias Pulso.PromQL.RangeMath
  alias Pulso.Storage.SortOrder

  @default_step_ns 60 * 1_000_000_000
  @max_steps Pulso.QueryLimits.max_evaluation_steps()
  @minimum_timestamp -9_223_372_036_854_775_808

  @over_time_ops [:sum_over_time, :avg_over_time, :max_over_time, :min_over_time, :stddev_over_time, :stdvar_over_time]

  @type result :: {:matrix | :vector, [{map(), term()}]}

  @spec evaluate(term(), Pulso.Storage.tenant(), Evaluator.opts()) :: {:ok, result()} | {:error, term()}
  def evaluate(expr, tenant, opts) when is_binary(tenant) and is_map(opts) do
    if !Process.get(:promql_budget),
      do: Process.put(:promql_budget, %{samples: 0, work: 0, points: 0, materialized: 0, log_samples: 0})

    if !Process.get(:promql_deadline_ms),
      do:
        Process.put(:promql_deadline_ms, Process.get(:query_deadline_ms, System.monotonic_time(:millisecond) + 10_000))

    with :ok <- validate_steps(opts),
         {:ok, kind, series} <- eval(expr, tenant, opts) do
      {:ok, {kind, series}}
    end
  catch
    {:promql_error, reason} -> {:error, reason}
    {:logql_error, reason} -> {:error, reason}
  end

  # ---------------------------------------------------------------------------
  # Number literal
  # ---------------------------------------------------------------------------

  defp eval(expr, tenant, opts) do
    Budget.charge(:work, 1)

    with {:ok, kind, series} <- do_eval(expr, tenant, opts) do
      points = Enum.reduce(series, 0, fn item, n -> n + point_count(item, kind) end)
      Budget.charge(:points, points)
      {:ok, kind, series}
    end
  end

  defp do_eval(%type{value: v}, _tenant, opts) when type in [AST.NumberLit, AST.VectorLit] do
    kind = result_kind(opts)

    series =
      case kind do
        :matrix ->
          samples = for ts <- steps(opts), do: {ts, v * 1.0}
          [{%{}, samples}]

        :vector ->
          [{%{}, {evaluation_ts(opts), v * 1.0}}]
      end

    {:ok, kind, series}
  end

  # ---------------------------------------------------------------------------
  # Range aggregation
  # ---------------------------------------------------------------------------

  # Range aggregations stream the selected entries, reducing each to a
  # compact `{timestamp, tie, value}` sample of its series instead of holding
  # pipeline entries. Every window then reads a slice of the series' samples
  # in the canonical log order, so results match evaluating the materialized
  # entries while memory follows two numbers per log line.
  defp do_eval(%AST.RangeAgg{op: :absent_over_time} = agg, tenant, opts) do
    kind = result_kind(opts)
    times = if kind == :matrix, do: steps(opts), else: [evaluation_ts(opts)]
    offset = agg.offset_ns || 0
    {first, last} = Enum.min_max(times)
    spec = %{op: :absent_over_time, unwrap: extract_unwrap(agg.inner), grouping: :all}

    with {:ok, collected} <-
           collect(remove_unwrap(agg.inner), spec, tenant, opts, first - offset - agg.range_ns + 1, last - offset) do
      samples = collected |> Map.values() |> List.flatten() |> ordered()

      points =
        for t <- times, window(samples, t - offset - agg.range_ns + 1, t - offset) == :empty, do: {t, 1.0}

      labels = absence_labels(agg.inner.selector.matchers)
      series = if points == [], do: [], else: [{labels, points}]
      {:ok, kind, result_series(series, kind)}
    end
  end

  defp do_eval(%AST.RangeAgg{op: op, range_ns: range, offset_ns: offset, param: param} = agg, tenant, opts) do
    kind = result_kind(opts)
    offset = offset || 0
    times = if kind == :matrix, do: steps(opts), else: [evaluation_ts(opts)]
    spec = %{op: op, param: param, unwrap: extract_unwrap(agg.inner), grouping: agg.grouping}

    if times == [] do
      {:ok, kind, []}
    else
      {first, last} = Enum.min_max(times)

      with {:ok, collected} <-
             collect(remove_unwrap(agg.inner), spec, tenant, opts, first - offset - range + 1, last - offset) do
        {:ok, kind, Enum.flat_map(collected, &series_points(&1, times, spec, range, offset, kind))}
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Vector aggregation
  # ---------------------------------------------------------------------------

  defp do_eval(%AST.VectorAgg{op: op, inner: inner, grouping: g, param: param}, tenant, opts) do
    with {:ok, kind, inner_series} <- eval(inner, tenant, opts) do
      grouped =
        case kind do
          :matrix -> aggregate_matrix(inner_series, op, g, param)
          :vector -> aggregate_vector(inner_series, op, g, param)
        end

      {:ok, kind, grouped}
    end
  end

  # ---------------------------------------------------------------------------
  # Binary operator
  # ---------------------------------------------------------------------------

  defp do_eval(%AST.BinaryOp{op: op, left: l, right: r, bool: bool, matching: matching}, tenant, opts) do
    with {:ok, kind, lser} <- eval(l, tenant, opts),
         {:ok, ^kind, rser} <- eval(r, tenant, opts) do
      times = if kind == :matrix, do: steps(opts), else: [evaluation_ts(opts)]

      series =
        Operations.binary(
          operator(op),
          modifiers(bool, matching),
          normalized(lser, kind),
          normalized(rser, kind),
          expression_type(l),
          expression_type(r),
          times
        )

      result = result_series(series, kind)
      {:ok, kind, result}
    end
  end

  # ---------------------------------------------------------------------------
  # Range helpers
  # ---------------------------------------------------------------------------

  defp entry_label_key(%Entry{labels: labels}, unwrap, grouping) do
    labels = if unwrap, do: Map.delete(labels, unwrap.label), else: labels

    case grouping do
      nil -> labels
      %AST.Grouping{mode: :by, labels: names} -> Map.take(labels, names)
      %AST.Grouping{mode: :without, labels: names} -> Map.drop(labels, names)
    end
  end

  defp extract_unwrap(%AST.LogQuery{stages: stages}) do
    Enum.find(stages, &match?(%AST.Unwrap{}, &1))
  end

  defp remove_unwrap(%AST.LogQuery{stages: stages} = q) do
    %{q | stages: Enum.reject(stages, &match?(%AST.Unwrap{}, &1))}
  end

  # ---------------------------------------------------------------------------
  # Streamed samples
  # ---------------------------------------------------------------------------

  # Every entry in (start, finish] becomes one sample of its series. A
  # pipeline error fails the query wherever it occurs, as before; an unwrap
  # failure is kept as `:error` and fails only a window that reads it.
  defp collect(_inner, _spec, _tenant, _opts, _start, finish) when finish < @minimum_timestamp, do: {:ok, %{}}

  defp collect(inner, spec, tenant, opts, start, finish) do
    query_opts =
      opts
      |> Map.put(:start_ts_ns, max(start, @minimum_timestamp))
      |> Map.put(:end_ts_ns, finish)
      |> Map.drop([:limit, :direction, :step_ns])

    case NativeRange.collect(inner, spec, tenant, query_opts) do
      :fallback -> collect_entries(inner, spec, tenant, query_opts)
      result -> result
    end
  end

  defp collect_entries(inner, spec, tenant, query_opts) do
    Evaluator.fold_log(inner, tenant, query_opts, %{}, &add_entry(&1, &2, &3, spec))
  end

  defp add_entry(entry, record, acc, spec) do
    check_pipeline_error(entry)

    case entry.timestamp_ns do
      ts when is_integer(ts) ->
        Budget.charge(:log_samples, 1)
        add_sample(acc, series_key(entry, spec), {ts, tie(spec.op, record), entry_value(spec, entry)})

      _ ->
        acc
    end
  end

  defp add_sample(acc, key, sample) do
    case acc do
      %{^key => samples} -> %{acc | key => [sample | samples]}
      # Labels can be slices of a decoded chunk; copy the first so a series
      # never keeps its chunk alive.
      _ -> Map.put(acc, :erlang.binary_to_term(:erlang.term_to_binary(key)), [sample])
    end
  end

  defp series_key(_entry, %{grouping: :all}), do: %{}
  defp series_key(entry, %{unwrap: unwrap, grouping: grouping}), do: entry_label_key(entry, unwrap, grouping)

  # Equal timestamps keep the canonical log order only where it decides the
  # result (first/last); the key is copied so it never pins segment memory.
  defp tie(op, record) when op in [:first_over_time, :last_over_time],
    do: record |> SortOrder.log_key() |> :erlang.term_to_binary() |> :erlang.binary_to_term()

  defp tie(_op, _record), do: nil

  defp entry_value(%{op: op}, entry) when op in [:bytes_over_time, :bytes_rate], do: byte_size(entry.line)
  defp entry_value(%{op: :count_over_time}, _entry), do: 1.0
  defp entry_value(%{unwrap: nil}, _entry), do: 1.0

  # absent_over_time checks every unwrapped entry of its whole range.
  defp entry_value(%{op: :absent_over_time, unwrap: unwrap}, entry) do
    if unwrapped(entry, unwrap) == :error, do: throw({:logql_error, :sample_extraction_error}), else: 1.0
  end

  defp entry_value(%{unwrap: unwrap}, entry), do: unwrapped(entry, unwrap)

  defp unwrapped(%Entry{labels: labels}, %AST.Unwrap{label: name, conversion: conv}) do
    with value when is_binary(value) <- Map.get(labels, name),
         number when not is_nil(number) <- parse_unwrap(conv, value) do
      number
    else
      _ -> :error
    end
  end

  # Descending timestamp, then the canonical tie: the order a materialized,
  # sorted result presents entries in. Windows are contiguous slices.
  # Samples are a tuple of `{ts, tie, value}` (Elixir pipeline) or native
  # `{:packed, binary}` of `<<ts::64, tag::8, value::64>>` records, which stay
  # off the process heap: windows decode only the slice they read.
  defp ordered({:packed, _} = packed), do: packed
  defp ordered(samples), do: samples |> Enum.sort(:desc) |> List.to_tuple()

  defp size({:packed, binary}), do: div(byte_size(binary), 17)
  defp size(samples), do: tuple_size(samples)

  defp ts_at({:packed, binary}, i) do
    <<ts::signed-64>> = binary_part(binary, i * 17, 8)
    ts
  end

  defp ts_at(samples, i), do: elem(elem(samples, i), 0)

  defp value_at({:packed, binary}, i) do
    case binary_part(binary, i * 17 + 8, 9) do
      <<0, value::float-64>> -> value
      <<1, _::binary>> -> :error
      <<2, value::signed-64>> -> value
    end
  end

  defp value_at(samples, i), do: elem(elem(samples, i), 2)

  defp window(samples, start, finish) do
    first = search(samples, &(&1 <= finish))
    last = search(samples, &(&1 < start)) - 1
    if first > last, do: :empty, else: {first, last}
  end

  # First index whose timestamp satisfies a predicate monotone in the order.
  defp search(samples, predicate), do: search(samples, predicate, 0, size(samples))
  defp search(_samples, _predicate, low, high) when low >= high, do: low

  defp search(samples, predicate, low, high) do
    middle = div(low + high, 2)

    if predicate.(ts_at(samples, middle)),
      do: search(samples, predicate, low, middle),
      else: search(samples, predicate, middle + 1, high)
  end

  defp window_point(t, samples, spec, range, offset) do
    finish = t - offset
    start = finish - range + 1

    case window(samples, start, finish) do
      :empty ->
        nil

      {first, last} ->
        Budget.charge(:work, last - first + 1)
        {t, aggregate(spec, samples, first, last, start, finish)}
    end
  end

  defp series_points({labels, samples}, times, spec, range, offset, kind) do
    samples = ordered(samples)
    points = for t <- times, point = window_point(t, samples, spec, range, offset), point != nil, do: point
    range_series(labels, points, kind)
  end

  defp range_series(_labels, [], _kind), do: []
  defp range_series(labels, points, :matrix), do: [{labels, points}]
  defp range_series(labels, [point], :vector), do: [{labels, point}]

  defp aggregate(%{op: :count_over_time}, _samples, first, last, _start, _finish), do: (last - first + 1) * 1.0

  defp aggregate(%{op: :rate, unwrap: unwrap}, samples, first, last, start, finish) do
    seconds = (finish - start + 1) / 1_000_000_000
    total = if unwrap, do: over_time(:sum_over_time, values!(samples, first, last)), else: (last - first + 1) * 1.0
    Number.binary("/", total, seconds)
  end

  defp aggregate(%{op: :bytes_over_time}, samples, first, last, _start, _finish),
    do: samples |> slice(first, last) |> Enum.sum() |> Kernel.*(1.0)

  defp aggregate(%{op: :bytes_rate}, samples, first, last, start, finish) do
    seconds = (finish - start + 1) / 1_000_000_000
    total = samples |> slice(first, last) |> Enum.sum()
    if seconds > 0, do: total / seconds, else: 0.0
  end

  # Ascending by time, first keeps the earliest timestamp's first entry in
  # canonical order and last keeps the latest timestamp's last entry.
  defp aggregate(%{op: :first_over_time}, samples, first, last, _start, _finish) do
    validate_window!(samples, first, last)
    earliest = ts_at(samples, last)
    index = search(samples, &(&1 <= earliest))
    checked(samples, max(index, first))
  end

  defp aggregate(%{op: :last_over_time}, samples, first, last, _start, _finish) do
    validate_window!(samples, first, last)
    latest = ts_at(samples, first)
    index = search(samples, &(&1 < latest)) - 1
    checked(samples, min(index, last))
  end

  defp aggregate(%{op: op}, samples, first, last, _start, _finish) when op in @over_time_ops,
    do: over_time(op, values!(samples, first, last))

  defp aggregate(%{op: :quantile_over_time, param: param}, samples, first, last, _start, _finish),
    do: Quantile.calculate(param, values!(samples, first, last))

  defp slice(samples, first, last), do: for(i <- first..last//1, do: value_at(samples, i))

  defp values!(samples, first, last) do
    values = slice(samples, first, last)
    if Enum.member?(values, :error), do: throw({:logql_error, :sample_extraction_error}), else: values
  end

  # First/last need no materialized values list, but every selected sample in
  # this window must validate. Gaps between sparse evaluation windows do not.
  defp validate_window!(samples, first, last) do
    Enum.each(first..last, &checked(samples, &1))
  end

  defp checked(samples, index) do
    case value_at(samples, index) do
      :error -> throw({:logql_error, :sample_extraction_error})
      value -> value
    end
  end

  defp absence_labels(matchers) do
    duplicates =
      matchers
      |> Enum.frequencies_by(& &1.name)
      |> Enum.filter(fn {_, count} -> count > 1 end)
      |> Enum.map(&elem(&1, 0))

    for %AST.Matcher{op: :eq, name: name, value: value} <- matchers,
        name != "__name__" and name not in duplicates,
        into: %{},
        do: {name, value}
  end

  defp over_time(:sum_over_time, values), do: Enum.reduce(values, 0.0, &Number.add/2)
  defp over_time(:avg_over_time, []), do: :nan
  defp over_time(:avg_over_time, values), do: Number.average(values)
  defp over_time(:max_over_time, values), do: Enum.reduce(values, :nan, &Number.max/2)
  defp over_time(:min_over_time, values), do: Enum.reduce(values, :nan, &Number.min/2)
  defp over_time(:stddev_over_time, values), do: stddev(values)
  defp over_time(:stdvar_over_time, values), do: variance(values)

  defp parse_unwrap(:none, v) do
    case parse_number_str(v) do
      {:ok, n} -> n
      :error -> nil
    end
  end

  defp parse_unwrap(conv, v) when conv in [:duration, :duration_seconds] do
    case parse_duration(v) do
      {:ok, ns} -> if conv == :duration_seconds, do: ns / 1_000_000_000, else: ns / 1_000_000_000
      :error -> nil
    end
  end

  defp parse_unwrap(:bytes, v) do
    case parse_bytes(v) do
      {:ok, b} -> b * 1.0
      :error -> nil
    end
  end

  # ---------------------------------------------------------------------------
  # Vector aggregation
  # ---------------------------------------------------------------------------

  defp aggregate_vector(series, op, grouping, param) do
    series
    |> Enum.group_by(fn {labels, _} -> group_key(labels, grouping) end)
    |> Enum.flat_map(fn {group_labels, group} ->
      values = for {_, {_ts, v}} <- group, do: v
      apply_vector_op(op, group_labels, group, values, param)
    end)
  end

  defp aggregate_matrix(series, op, grouping, param) do
    per_ts =
      Enum.reduce(series, %{}, fn {labels, samples}, acc ->
        Enum.reduce(samples, acc, fn {ts, v}, acc2 ->
          key = {ts, group_key(labels, grouping)}
          Map.update(acc2, key, [{labels, v}], fn existing -> [{labels, v} | existing] end)
        end)
      end)

    result =
      Enum.reduce(per_ts, %{}, fn {{ts, group_labels}, entries}, acc ->
        values = Enum.map(entries, fn {_l, v} -> v end)

        entries_v =
          apply_vector_op_matrix(op, group_labels, entries, values, param)

        Enum.reduce(entries_v, acc, fn {labels, v}, acc2 ->
          Map.update(acc2, labels, [{ts, v}], fn existing -> [{ts, v} | existing] end)
        end)
      end)

    Enum.map(result, fn {labels, samples} ->
      {labels, Enum.sort_by(samples, &elem(&1, 0))}
    end)
  end

  defp apply_vector_op(op, group_labels, group, values, _param)
       when op in [:sum, :avg, :min, :max, :count, :stddev, :stdvar] do
    {_labels, {ts, _value}} = hd(group)
    [{group_labels, {ts, aggregator(op, values)}}]
  end

  defp apply_vector_op(:topk, _group_labels, group, _values, k) when is_integer(k) do
    group
    |> sort_group(:desc, :vector)
    |> Enum.take(max(k, 0))
  end

  defp apply_vector_op(:bottomk, _group_labels, group, _values, k) when is_integer(k) do
    group
    |> sort_group(:asc, :vector)
    |> Enum.take(max(k, 0))
  end

  defp apply_vector_op(:sort, _group_labels, group, _values, _), do: sort_group(group, :asc, :vector)
  defp apply_vector_op(:sort_desc, _group_labels, group, _values, _), do: sort_group(group, :desc, :vector)

  defp apply_vector_op_matrix(op, group_labels, _entries, values, _param)
       when op in [:sum, :avg, :min, :max, :count, :stddev, :stdvar] do
    [{group_labels, aggregator(op, values)}]
  end

  defp apply_vector_op_matrix(:topk, _group_labels, entries, _values, k) when is_integer(k) do
    entries
    |> sort_group(:desc, :matrix)
    |> Enum.take(max(k, 0))
  end

  defp apply_vector_op_matrix(:bottomk, _group_labels, entries, _values, k) when is_integer(k) do
    entries
    |> sort_group(:asc, :matrix)
    |> Enum.take(max(k, 0))
  end

  defp apply_vector_op_matrix(op, _group_labels, entries, _values, _param) when op in [:sort, :sort_desc], do: entries

  defp sort_group(group, direction, kind) do
    Enum.sort(group, fn left, right -> ordered?(sample_value(left, kind), sample_value(right, kind), direction) end)
  end

  defp sample_value({_, {_, value}}, :vector), do: value
  defp sample_value({_, value}, :matrix), do: value
  defp ordered?(:nan, :nan, _), do: true
  defp ordered?(:nan, _, _), do: false
  defp ordered?(_, :nan, _), do: true
  defp ordered?(left, right, :asc), do: not Number.less?(right, left)
  defp ordered?(left, right, :desc), do: not Number.less?(left, right)

  defp aggregator(:sum, values), do: Enum.reduce(values, 0.0, &Number.add/2)
  defp aggregator(:count, values), do: length(values) * 1.0
  defp aggregator(:avg, values), do: over_time(:avg_over_time, values)
  defp aggregator(:min, values), do: over_time(:min_over_time, values)
  defp aggregator(:max, values), do: over_time(:max_over_time, values)
  defp aggregator(:stddev, values), do: stddev(values)
  defp aggregator(:stdvar, values), do: variance(values)

  defp group_key(_labels, nil), do: %{}

  defp group_key(labels, %AST.Grouping{mode: :by, labels: names}) do
    Map.take(labels, names)
  end

  defp group_key(labels, %AST.Grouping{mode: :without, labels: names}) do
    Map.drop(labels, names)
  end

  # ---------------------------------------------------------------------------
  # Binary op helpers
  # ---------------------------------------------------------------------------

  @operators %{
    add: "+",
    sub: "-",
    mul: "*",
    div: "/",
    mod: "%",
    pow: "^",
    eq: "==",
    neq: "!=",
    lt: "<",
    lte: "<=",
    gt: ">",
    gte: ">=",
    and: "and",
    or: "or",
    unless: "unless"
  }
  defp operator(op), do: Map.fetch!(@operators, op)
  defp expression_type(%AST.NumberLit{}), do: :scalar

  defp expression_type(%AST.BinaryOp{left: left, right: right}) do
    if expression_type(left) == :scalar and expression_type(right) == :scalar, do: :scalar, else: :vector
  end

  defp expression_type(_), do: :vector
  defp point_count({_, values}, :matrix), do: length(values)
  defp point_count(_, :vector), do: 1
  defp result_series(series, :matrix), do: series
  defp result_series(series, :vector), do: Enum.map(series, fn {labels, [point]} -> {labels, point} end)
  defp normalized(series, :matrix), do: series
  defp normalized(series, :vector), do: Enum.map(series, fn {labels, point} -> {labels, [point]} end)

  defp modifiers(bool, matching) do
    boolean = if bool, do: ["bool"], else: []
    boolean ++ matching_modifiers(matching)
  end

  defp matching_modifiers(nil), do: []

  defp matching_modifiers(%AST.VectorMatching{mode: mode, labels: labels, group: group, group_labels: included}) do
    match = [[Atom.to_string(mode), labels]]
    if group, do: match ++ [["group_#{group}", included]], else: match
  end

  # ---------------------------------------------------------------------------
  # Bucketing / opts helpers
  # ---------------------------------------------------------------------------

  defp check_pipeline_error(%{labels: %{"__error__" => error}}) when error != "",
    do: throw({:logql_error, :pipeline_error})

  defp check_pipeline_error(_), do: :ok

  defp result_kind(opts) do
    if Map.get(opts, :step_ns), do: :matrix, else: :vector
  end

  defp validate_steps(opts) do
    case Map.get(opts, :step_ns) do
      nil ->
        :ok

      step ->
        validate_step_count(
          Map.get(opts, :start_ts_ns, 0),
          Map.get(opts, :end_ts_ns, Map.get(opts, :start_ts_ns, 0)),
          step
        )
    end
  end

  defp validate_step_count(start, stop, step)
       when is_integer(start) and is_integer(stop) and is_integer(step) and step > 0 and start <= stop do
    if div(stop - start, step) < @max_steps, do: :ok, else: {:error, :invalid_range_or_too_many_steps}
  end

  defp validate_step_count(_start, _stop, _step), do: {:error, :invalid_range_or_too_many_steps}

  defp steps(opts) do
    start = Map.get(opts, :start_ts_ns) || 0
    stop = Map.get(opts, :end_ts_ns) || start
    step = Map.get(opts, :step_ns) || @default_step_ns

    Stream.iterate(start, &(&1 + step))
    |> Stream.take_while(&(&1 <= stop))
    |> Enum.to_list()
  end

  defp evaluation_ts(opts), do: Map.get(opts, :end_ts_ns) || 0

  # ---------------------------------------------------------------------------
  # Numeric helpers
  # ---------------------------------------------------------------------------

  defp parse_number_str(str), do: FloatParser.parse(str)

  defp parse_duration(str), do: Quantity.duration(str)
  defp parse_bytes(str), do: Quantity.bytes(str)

  defp stddev(values), do: RangeMath.deviation(Enum.map(values, &%{value: &1}))
  defp variance(values), do: RangeMath.variance(Enum.map(values, &%{value: &1}))
end
