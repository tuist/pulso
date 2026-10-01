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

  @default_step_ns 60 * 1_000_000_000

  @over_time_ops [:sum_over_time, :avg_over_time, :max_over_time, :min_over_time, :stddev_over_time, :stdvar_over_time]

  @type result :: {:matrix | :vector, [{map(), term()}]}

  @spec evaluate(term(), Pulso.Storage.tenant(), Evaluator.opts()) :: {:ok, result()} | {:error, term()}
  def evaluate(expr, tenant, opts) when is_binary(tenant) and is_map(opts) do
    with {:ok, kind, series} <- eval(expr, tenant, opts) do
      {:ok, {kind, series}}
    end
  end

  # ---------------------------------------------------------------------------
  # Number literal
  # ---------------------------------------------------------------------------

  defp eval(%AST.NumberLit{value: v}, _tenant, opts) do
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

  defp eval(%AST.RangeAgg{} = agg, tenant, opts) do
    kind = result_kind(opts)
    unwrap = extract_unwrap(agg.inner)
    inner_no_unwrap = remove_unwrap(agg.inner)

    case kind do
      :matrix -> run_range_matrix(agg, inner_no_unwrap, unwrap, tenant, opts)
      :vector -> run_range_vector(agg, inner_no_unwrap, unwrap, tenant, opts)
    end
  end

  # ---------------------------------------------------------------------------
  # Vector aggregation
  # ---------------------------------------------------------------------------

  defp eval(%AST.VectorAgg{op: op, inner: inner, grouping: g, param: param}, tenant, opts) do
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

  defp eval(%AST.BinaryOp{op: op, left: l, right: r, bool: bool, matching: matching}, tenant, opts) do
    with {:ok, lk, lser} <- eval(l, tenant, opts),
         {:ok, rk, rser} <- eval(r, tenant, opts) do
      # If either side is a scalar (single %{} labeled series), broadcast.
      cond do
        scalar?(lser) and scalar?(rser) ->
          combine_scalars(lk, rk, lser, rser, op, bool)

        scalar?(lser) ->
          {:ok, rk, broadcast_scalar_op(rser, extract_scalar(lser), op, bool, :left)}

        scalar?(rser) ->
          {:ok, lk, broadcast_scalar_op(lser, extract_scalar(rser), op, bool, :right)}

        true ->
          kind = if lk == :matrix or rk == :matrix, do: :matrix, else: :vector
          {:ok, kind, combine_vectors(lser, rser, op, bool, matching, kind)}
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Range helpers
  # ---------------------------------------------------------------------------

  # One storage query for the whole matrix: fetch (min_step - offset - r,
  # max_step - offset] once, then bucket in Elixir per step. Before this
  # refactor a `rate({...}[1h])` at step=15s issued 240 Storage.query
  # calls; now it issues one, at the cost of holding one range's worth of
  # entries in memory for the fold.
  defp run_range_matrix(%AST.RangeAgg{op: op, range_ns: r, offset_ns: off, param: p}, inner, unwrap, tenant, opts) do
    offset = off || 0
    step_ts_list = steps(opts)

    case step_ts_list do
      [] ->
        {:ok, :matrix, []}

      _ ->
        {min_step, max_step} = Enum.min_max(step_ts_list)
        union_start = min_step - offset - r + 1
        union_end = max_step - offset

        query_opts =
          opts
          |> Map.put(:start_ts_ns, union_start)
          |> Map.put(:end_ts_ns, union_end)
          |> Map.drop([:limit, :direction, :step_ns])

        with {:ok, entries} <- Evaluator.evaluate_log_raw(inner, tenant, query_opts) do
          by_labels = Enum.group_by(entries, &entry_label_key/1)

          series =
            Enum.map(by_labels, fn {labels, es} ->
              samples =
                Enum.map(step_ts_list, fn t ->
                  bucket_end = t - offset
                  bucket_start = bucket_end - r + 1
                  bucketed = filter_by_bucket(es, bucket_start, bucket_end)
                  {t, compute_range_agg(op, bucketed, unwrap, bucket_start, bucket_end, p)}
                end)

              {labels, samples}
            end)

          {:ok, :matrix, series}
        end
    end
  end

  defp run_range_vector(%AST.RangeAgg{op: op, range_ns: r, offset_ns: off, param: p}, inner, unwrap, tenant, opts) do
    offset = off || 0
    ts = evaluation_ts(opts)
    end_ts = ts - offset
    start_ts = end_ts - r + 1

    query_opts =
      opts
      |> Map.put(:start_ts_ns, start_ts)
      |> Map.put(:end_ts_ns, end_ts)
      |> Map.drop([:limit, :direction, :step_ns])

    with {:ok, entries} <- Evaluator.evaluate_log_raw(inner, tenant, query_opts) do
      series =
        entries
        |> Enum.group_by(&entry_label_key/1)
        |> Enum.map(fn {labels, es} ->
          {labels, {ts, compute_range_agg(op, es, unwrap, start_ts, end_ts, p)}}
        end)

      {:ok, :vector, series}
    end
  end

  defp filter_by_bucket(entries, start_ts, end_ts) do
    Enum.filter(entries, fn %Entry{timestamp_ns: ts} ->
      is_integer(ts) and ts >= start_ts and ts <= end_ts
    end)
  end

  defp entry_label_key(%Entry{labels: labels}), do: labels

  defp extract_unwrap(%AST.LogQuery{stages: stages}) do
    Enum.find(stages, &match?(%AST.Unwrap{}, &1))
  end

  defp remove_unwrap(%AST.LogQuery{stages: stages} = q) do
    %{q | stages: Enum.reject(stages, &match?(%AST.Unwrap{}, &1))}
  end

  # `param` is the AST-level scalar argument (currently only
  # `quantile_over_time` uses it). Passing it explicitly avoids the
  # process-dictionary hop that broke on future concurrent evaluation.
  defp compute_range_agg(:count_over_time, entries, _, _, _, _), do: length(entries) * 1.0

  defp compute_range_agg(:rate, entries, _, start_ts, end_ts, _) do
    seconds = (end_ts - start_ts) / 1_000_000_000
    if seconds > 0, do: length(entries) / seconds, else: 0.0
  end

  defp compute_range_agg(:rate_counter, entries, unwrap, start_ts, end_ts, param) do
    compute_range_agg(:rate, entries, unwrap, start_ts, end_ts, param)
  end

  defp compute_range_agg(:bytes_over_time, entries, _, _, _, _) do
    entries |> Enum.map(&byte_size(&1.line)) |> Enum.sum() |> Kernel.*(1.0)
  end

  defp compute_range_agg(:bytes_rate, entries, _, start_ts, end_ts, _) do
    seconds = (end_ts - start_ts) / 1_000_000_000
    total = entries |> Enum.map(&byte_size(&1.line)) |> Enum.sum()
    if seconds > 0, do: total / seconds, else: 0.0
  end

  defp compute_range_agg(:absent_over_time, entries, _, _, _, _) do
    if entries == [], do: 1.0, else: 0.0
  end

  defp compute_range_agg(:first_over_time, entries, unwrap, _, _, _) do
    entries
    |> Enum.sort_by(& &1.timestamp_ns)
    |> List.first()
    |> unwrap_value(unwrap, 0.0)
  end

  defp compute_range_agg(:last_over_time, entries, unwrap, _, _, _) do
    entries
    |> Enum.sort_by(& &1.timestamp_ns)
    |> List.last()
    |> unwrap_value(unwrap, 0.0)
  end

  defp compute_range_agg(op, entries, unwrap, _start_ts, _end_ts, _) when op in @over_time_ops do
    values = for e <- entries, v = unwrap_value(e, unwrap, nil), v != nil, do: v
    over_time(op, values)
  end

  defp compute_range_agg(:quantile_over_time, entries, unwrap, _, _, param) do
    values = for e <- entries, v = unwrap_value(e, unwrap, nil), v != nil, do: v
    q = if is_number(param), do: param * 1.0, else: 0.99
    if values == [], do: 0.0, else: quantile(values, q)
  end

  defp over_time(:sum_over_time, values), do: Enum.sum(values) * 1.0
  defp over_time(:avg_over_time, []), do: 0.0
  defp over_time(:avg_over_time, values), do: Enum.sum(values) / length(values)
  defp over_time(:max_over_time, []), do: 0.0
  defp over_time(:max_over_time, values), do: Enum.max(values) * 1.0
  defp over_time(:min_over_time, []), do: 0.0
  defp over_time(:min_over_time, values), do: Enum.min(values) * 1.0
  defp over_time(:stddev_over_time, values), do: stddev(values)
  defp over_time(:stdvar_over_time, values), do: variance(values)

  defp unwrap_value(entry, nil, default), do: length_or_default(entry, default)

  defp unwrap_value(nil, _unwrap, default), do: default

  defp unwrap_value(%Entry{labels: labels}, %AST.Unwrap{label: name, conversion: conv}, default) do
    case Map.get(labels, name) do
      nil ->
        default

      v ->
        parse_unwrap(conv, v) || default
    end
  end

  defp length_or_default(nil, default), do: default
  defp length_or_default(_entry, _default), do: 1.0

  defp parse_unwrap(:none, v) do
    case parse_number_str(v) do
      {:ok, n} -> n * 1.0
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

  defp apply_vector_op(op, group_labels, _group, values, _param)
       when op in [:sum, :avg, :min, :max, :count, :stddev, :stdvar] do
    [{group_labels, {0, aggregator(op, values)}}]
  end

  defp apply_vector_op(:topk, _group_labels, group, _values, k) when is_integer(k) do
    group
    |> Enum.sort_by(fn {_l, {_ts, v}} -> v end, :desc)
    |> Enum.take(k)
  end

  defp apply_vector_op(:bottomk, _group_labels, group, _values, k) when is_integer(k) do
    group
    |> Enum.sort_by(fn {_l, {_ts, v}} -> v end)
    |> Enum.take(k)
  end

  defp apply_vector_op(:sort, _group_labels, group, _values, _), do: Enum.sort_by(group, fn {_l, {_ts, v}} -> v end)

  defp apply_vector_op(:sort_desc, _group_labels, group, _values, _),
    do: Enum.sort_by(group, fn {_l, {_ts, v}} -> v end, :desc)

  defp apply_vector_op_matrix(op, group_labels, _entries, values, _param)
       when op in [:sum, :avg, :min, :max, :count, :stddev, :stdvar] do
    [{group_labels, aggregator(op, values)}]
  end

  defp apply_vector_op_matrix(:topk, _group_labels, entries, _values, k) when is_integer(k) do
    entries
    |> Enum.sort_by(fn {_l, v} -> v end, :desc)
    |> Enum.take(k)
  end

  defp apply_vector_op_matrix(:bottomk, _group_labels, entries, _values, k) when is_integer(k) do
    entries
    |> Enum.sort_by(fn {_l, v} -> v end)
    |> Enum.take(k)
  end

  defp apply_vector_op_matrix(op, group_labels, _entries, values, _param) when op in [:sort, :sort_desc] do
    [{group_labels, aggregator(op, values)}]
  end

  defp aggregator(:sum, values), do: Enum.sum(values) * 1.0
  defp aggregator(:count, values), do: length(values) * 1.0
  defp aggregator(:avg, []), do: 0.0
  defp aggregator(:avg, values), do: Enum.sum(values) / length(values)
  defp aggregator(:min, []), do: 0.0
  defp aggregator(:min, values), do: Enum.min(values) * 1.0
  defp aggregator(:max, []), do: 0.0
  defp aggregator(:max, values), do: Enum.max(values) * 1.0
  defp aggregator(:stddev, values), do: stddev(values)
  defp aggregator(:stdvar, values), do: variance(values)
  defp aggregator(:sort, values), do: Enum.min(values, fn -> 0.0 end) * 1.0
  defp aggregator(:sort_desc, values), do: Enum.max(values, fn -> 0.0 end) * 1.0

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

  # A scalar is a *single* series with an *empty* label bag. The
  # `%{}` pattern alone would match any map (it means "at least these
  # keys"); the `map_size == 0` guard is what makes it a true scalar.
  defp scalar?([{labels, _}]) when is_map(labels), do: map_size(labels) == 0
  defp scalar?(_), do: false

  defp extract_scalar([{_labels, {_ts, v}}]), do: v
  defp extract_scalar([{_labels, samples}]) when is_list(samples), do: samples

  defp combine_scalars(lk, _rk, [{_, {ts, lv}}], [{_, {_, rv}}], op, bool) do
    case apply_binop(op, lv, rv, bool) do
      :drop -> {:ok, lk, []}
      value -> {:ok, lk, [{%{}, {ts, value}}]}
    end
  end

  defp combine_scalars(lk, _rk, l, r, op, bool) do
    lv = extract_scalar(l)
    rv = extract_scalar(r)
    combine_scalars(lk, lk, [{%{}, {0, lv}}], [{%{}, {0, rv}}], op, bool)
  end

  # A `:drop` from `apply_binop` (an un-`bool`ed comparison that failed)
  # means "this series/sample is filtered out of the result". Every caller
  # must drop it — leaking it through would crash `Envelope.value_str`
  # which only handles numeric values.
  defp broadcast_scalar_op(series, scalar, op, bool, side) do
    series
    |> Enum.map(fn
      {labels, {ts, v}} ->
        {a, b} = if side == :left, do: {scalar, v}, else: {v, scalar}

        case apply_binop(op, a, b, bool) do
          :drop -> nil
          value -> {labels, {ts, value}}
        end

      {labels, samples} when is_list(samples) ->
        new =
          for {ts, v} <- samples,
              {a, b} = if(side == :left, do: {scalar, v}, else: {v, scalar}),
              value = apply_binop(op, a, b, bool),
              value != :drop do
            {ts, value}
          end

        if new != [], do: {labels, new}
    end)
    |> Enum.reject(&is_nil/1)
  end

  defp combine_vectors(left, right, op, bool, matching, :vector) do
    right_index = index_by(right, matching)

    left
    |> Enum.map(fn {labels, {ts, lv}} ->
      key = match_key(labels, matching)

      case Map.get(right_index, key) do
        nil ->
          nil

        {rlabels, {_rts, rv}} ->
          case apply_binop(op, lv, rv, bool) do
            :drop -> nil
            value -> {result_labels(labels, rlabels, matching), {ts, value}}
          end
      end
    end)
    |> Enum.reject(&is_nil/1)
  end

  defp combine_vectors(left, right, op, bool, matching, :matrix) do
    right_index = index_by(right, matching)

    left
    |> Enum.flat_map(fn {labels, samples} ->
      key = match_key(labels, matching)

      case Map.get(right_index, key) do
        nil ->
          []

        {rlabels, rsamples} ->
          rmap = Map.new(rsamples)

          combined =
            for {ts, lv} <- samples,
                rv = Map.get(rmap, ts),
                rv != nil,
                value = apply_binop(op, lv, rv, bool),
                value != :drop do
              {ts, value}
            end

          if combined == [], do: [], else: [{result_labels(labels, rlabels, matching), combined}]
      end
    end)
  end

  defp index_by(series, matching) do
    Map.new(series, fn {labels, samples} -> {match_key(labels, matching), {labels, samples}} end)
  end

  defp match_key(labels, nil), do: labels

  defp match_key(labels, %AST.VectorMatching{mode: :on, labels: names}), do: Map.take(labels, names)

  defp match_key(labels, %AST.VectorMatching{mode: :ignoring, labels: names}), do: Map.drop(labels, names)

  defp result_labels(l, _r, nil), do: l

  defp result_labels(l, _r, %AST.VectorMatching{group: nil}), do: l

  defp result_labels(l, r, %AST.VectorMatching{group: :left, group_labels: add}) do
    Map.merge(l, Map.take(r, add))
  end

  defp result_labels(l, r, %AST.VectorMatching{group: :right, group_labels: add}) do
    Map.merge(r, Map.take(l, add))
  end

  defp apply_binop(:add, a, b, _), do: (a + b) * 1.0
  defp apply_binop(:sub, a, b, _), do: (a - b) * 1.0
  defp apply_binop(:mul, a, b, _), do: a * b * 1.0
  defp apply_binop(:div, _, 0, _), do: 0.0
  defp apply_binop(:div, _, +0.0, _), do: 0.0
  defp apply_binop(:div, a, b, _), do: a / b * 1.0
  defp apply_binop(:mod, _, 0, _), do: 0.0
  defp apply_binop(:mod, a, b, _) when is_integer(a) and is_integer(b), do: rem(a, b) * 1.0
  defp apply_binop(:mod, a, b, _), do: (a - Float.floor(a / b) * b) * 1.0
  defp apply_binop(:pow, a, b, _), do: :math.pow(a, b)
  defp apply_binop(:eq, a, b, true), do: if(a == b, do: 1.0, else: 0.0)
  defp apply_binop(:eq, a, b, false), do: if(a == b, do: a * 1.0, else: :drop)
  defp apply_binop(:neq, a, b, true), do: if(a == b, do: 0.0, else: 1.0)
  defp apply_binop(:neq, a, b, false), do: if(a == b, do: :drop, else: a * 1.0)
  defp apply_binop(:lt, a, b, true), do: if(a < b, do: 1.0, else: 0.0)
  defp apply_binop(:lt, a, b, false), do: if(a < b, do: a * 1.0, else: :drop)
  defp apply_binop(:lte, a, b, true), do: if(a <= b, do: 1.0, else: 0.0)
  defp apply_binop(:lte, a, b, false), do: if(a <= b, do: a * 1.0, else: :drop)
  defp apply_binop(:gt, a, b, true), do: if(a > b, do: 1.0, else: 0.0)
  defp apply_binop(:gt, a, b, false), do: if(a > b, do: a * 1.0, else: :drop)
  defp apply_binop(:gte, a, b, true), do: if(a >= b, do: 1.0, else: 0.0)
  defp apply_binop(:gte, a, b, false), do: if(a >= b, do: a * 1.0, else: :drop)
  defp apply_binop(:and, _, _, _), do: 1.0
  defp apply_binop(:or, _, _, _), do: 1.0
  defp apply_binop(:unless, _, _, _), do: 1.0

  # ---------------------------------------------------------------------------
  # Bucketing / opts helpers
  # ---------------------------------------------------------------------------

  defp result_kind(opts) do
    if Map.get(opts, :step_ns), do: :matrix, else: :vector
  end

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

  defp parse_number_str(str) when is_binary(str) do
    case Integer.parse(str) do
      {n, ""} ->
        {:ok, n}

      _ ->
        case Float.parse(str) do
          {n, ""} -> {:ok, n}
          _ -> :error
        end
    end
  end

  defp parse_number_str(_), do: :error

  defp parse_duration(str) when is_binary(str) do
    case Integer.parse(str) do
      {n, "ns"} -> {:ok, n}
      {n, "us"} -> {:ok, n * 1_000}
      {n, "ms"} -> {:ok, n * 1_000_000}
      {n, "s"} -> {:ok, n * 1_000_000_000}
      {n, "m"} -> {:ok, n * 60 * 1_000_000_000}
      {n, "h"} -> {:ok, n * 3_600 * 1_000_000_000}
      _ -> :error
    end
  end

  @byte_factors %{
    "" => 1,
    "B" => 1,
    "kB" => 1_000,
    "MB" => 1_000_000,
    "GB" => 1_000_000_000,
    "KiB" => 1024,
    "MiB" => 1024 * 1024,
    "GiB" => 1024 * 1024 * 1024
  }

  defp parse_bytes(str) when is_binary(str) do
    case Integer.parse(str) do
      {n, unit} ->
        case Map.get(@byte_factors, unit) do
          nil -> :error
          f -> {:ok, n * f}
        end

      :error ->
        :error
    end
  end

  defp stddev([]), do: 0.0
  defp stddev(values), do: :math.sqrt(variance(values))

  defp variance([]), do: 0.0

  defp variance(values) do
    mean = Enum.sum(values) / length(values)
    sum_sq = Enum.reduce(values, 0.0, fn v, acc -> acc + (v - mean) * (v - mean) end)
    sum_sq / length(values)
  end

  defp quantile(values, q) when q >= 0.0 and q <= 1.0 do
    sorted = Enum.sort(values)
    idx = min(length(sorted) - 1, max(0, trunc(q * (length(sorted) - 1))))
    Enum.at(sorted, idx) * 1.0
  end
end
