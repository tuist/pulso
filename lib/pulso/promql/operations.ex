defmodule Pulso.PromQL.Operations do
  @moduledoc false
  alias Pulso.PromQL.Evaluator
  alias Pulso.PromQL.Histogram
  alias Pulso.PromQL.LabelReplace
  alias Pulso.PromQL.Number

  @comparisons ["==", "!=", "<", ">", "<=", ">="]
  @sets ["and", "or", "unless"]

  def binary(op, modifiers, left, right, left_type, right_type, steps) do
    left = by_time(left)
    right = by_time(right)
    at_steps(steps, fn ts -> binary_step(op, modifiers, at(left, ts), at(right, ts), left_type, right_type) end)
  end

  defp binary_step(op, _modifiers, lhs, rhs, :scalar, :scalar), do: [{%{}, result(op, value(lhs), value(rhs))}]

  defp binary_step(op, modifiers, lhs, rhs, :scalar, :vector),
    do: scalar_vector(op, value(lhs), rhs, true, "bool" in modifiers)

  defp binary_step(op, modifiers, lhs, rhs, :vector, :scalar),
    do: scalar_vector(op, value(rhs), lhs, false, "bool" in modifiers)

  defp binary_step(op, modifiers, lhs, rhs, _, _) when op in @sets, do: set(op, modifiers, lhs, rhs)
  defp binary_step(op, modifiers, lhs, rhs, _, _), do: vector_vector(op, modifiers, lhs, rhs)

  defp scalar_vector(op, scalar, vector, scalar_left?, boolean) do
    Enum.flat_map(vector, fn sample -> scalar_pair(op, scalar, sample, scalar_left?, boolean) end)
  end

  defp scalar_pair(op, scalar, {labels, value}, scalar_left?, boolean) do
    {a, b} = if scalar_left?, do: {scalar, value}, else: {value, scalar}

    if filter?(op, boolean) do
      filtered_sample(labels, value, Number.compare(op, a, b))
    else
      [{Map.delete(labels, "__name__"), result(op, a, b)}]
    end
  end

  defp filtered_sample(labels, value, true), do: [{labels, value}]
  defp filtered_sample(_, _, false), do: []
  defp filter?(op, boolean), do: op in @comparisons and not boolean

  defp vector_vector(_op, _modifiers, [], _rhs), do: []
  defp vector_vector(_op, _modifiers, _lhs, []), do: []

  defp vector_vector(op, modifiers, lhs, rhs) do
    grouping = grouping(modifiers)
    right_many? = match?(["group_right" | _], grouping)
    {many, one} = if right_many?, do: {rhs, lhs}, else: {lhs, rhs}

    context = %{
      op: op,
      modifiers: modifiers,
      grouping: grouping,
      right_many?: right_many?,
      boolean: "bool" in modifiers,
      index: unique_index(one, modifiers)
    }

    {pairs, _seen} = Enum.map_reduce(many, MapSet.new(), &matched_output(&1, &2, context))
    pairs |> List.flatten() |> unique!()
  end

  defp vector_pair({labels, value}, context) do
    key = signature(labels, context.modifiers)

    case Map.get(context.index, key, []) do
      [] ->
        []

      [{other_labels, other_value}] ->
        {a, b} = if context.right_many?, do: {other_value, value}, else: {value, other_value}
        matched_pair(labels, other_labels, a, b, context)

      _ ->
        throw({:promql_error, :many_to_many_matching})
    end
  end

  defp unique_index(one, modifiers) do
    index = Enum.group_by(one, fn {labels, _} -> signature(labels, modifiers) end)
    if Enum.any?(index, fn {_, samples} -> length(samples) > 1 end), do: throw({:promql_error, :many_to_many_matching})
    index
  end

  defp matched_output({labels, _} = sample, seen, context) do
    output = vector_pair(sample, context)
    key = signature(labels, context.modifiers)

    if output != [] and is_nil(context.grouping) and MapSet.member?(seen, key),
      do: throw({:promql_error, :many_to_many_matching})

    seen = if output == [], do: seen, else: MapSet.put(seen, key)
    {output, seen}
  end

  defp matched_pair(labels, other, a, b, context) do
    output_labels = result_labels(labels, other, context)

    if filter?(context.op, context.boolean) do
      filtered_sample(output_labels, a, Number.compare(context.op, a, b))
    else
      [{output_labels, result(context.op, a, b)}]
    end
  end

  defp result_labels(labels, other, context) do
    labels = matched_labels(labels, other, context.modifiers, context.grouping)
    if context.op not in @comparisons or context.boolean, do: Map.delete(labels, "__name__"), else: labels
  end

  defp matched_labels(labels, _other, modifiers, nil) do
    case matching(modifiers) do
      ["on", names] -> Map.take(labels, names)
      ["ignoring", names] -> Map.drop(labels, names)
      nil -> labels
    end
  end

  defp matched_labels(labels, other, _modifiers, grouping) do
    names =
      case grouping do
        [_, names] -> names
        _ -> []
      end

    Enum.reduce(names, labels, &include_label(&1, other, &2))
  end

  defp include_label(name, other, labels) do
    case Map.get(other, name, "") do
      "" -> Map.delete(labels, name)
      value -> Map.put(labels, name, value)
    end
  end

  defp set(op, modifiers, lhs, rhs) do
    left_keys = MapSet.new(lhs, fn {labels, _} -> signature(labels, modifiers) end)
    right_keys = MapSet.new(rhs, fn {labels, _} -> signature(labels, modifiers) end)

    case op do
      "and" -> Enum.filter(lhs, fn {labels, _} -> MapSet.member?(right_keys, signature(labels, modifiers)) end)
      "unless" -> Enum.reject(lhs, fn {labels, _} -> MapSet.member?(right_keys, signature(labels, modifiers)) end)
      "or" -> lhs ++ Enum.reject(rhs, fn {labels, _} -> MapSet.member?(left_keys, signature(labels, modifiers)) end)
    end
  end

  defp signature(labels, modifiers) do
    labels =
      case matching(modifiers) do
        ["on", names] -> Map.take(labels, names)
        ["ignoring", names] -> Map.drop(labels, ["__name__" | names])
        nil -> Map.delete(labels, "__name__")
      end

    Map.reject(labels, fn {_, value} -> value == "" end)
  end

  defp matching(modifiers), do: Enum.find(modifiers, &match?([mode, _] when mode in ["on", "ignoring"], &1))
  defp grouping(modifiers), do: Enum.find(modifiers, &match?([mode | _] when mode in ["group_left", "group_right"], &1))
  defp result(op, a, b) when op in @comparisons, do: if(Number.compare(op, a, b), do: 1.0, else: 0.0)
  defp result(op, a, b), do: Number.binary(op, a, b)

  def call("time", [], steps), do: [{%{}, Enum.map(steps, &{&1, &1 / 1_000_000_000})}]
  def call("vector", [scalar], _steps), do: scalar

  def call("scalar", [vector], steps) do
    vector = by_time(vector)
    at_steps(steps, fn ts -> [{%{}, scalar_value(at(vector, ts))}] end)
  end

  def call("label_replace", [vector | args], _steps), do: vector |> LabelReplace.run(args) |> unique!()

  def call("histogram_quantile", [quantile, buckets], steps) do
    quantile = by_time(quantile)
    buckets = by_time(buckets)
    at_steps(steps, fn ts -> Histogram.quantile(value(at(quantile, ts)), at(buckets, ts)) end)
  end

  def call(name, [vector | params], steps)
      when name in ["clamp_min", "clamp_max", "round", "abs", "sort", "sort_desc"] do
    vector = by_time(vector)
    params = Enum.map(params, &by_time/1)
    at_steps(steps, fn ts -> transform_vector(name, at(vector, ts), parameter(params, ts)) end)
  end

  defp parameter([], _ts), do: 1.0
  defp parameter([scalar], ts), do: value(at(scalar, ts))
  defp scalar_value([{_, value}]), do: value
  defp scalar_value(_), do: :nan

  defp transform_vector(name, vector, _parameter) when name in ["sort", "sort_desc"],
    do: sort(vector, name == "sort_desc")

  defp transform_vector(name, vector, parameter) do
    Enum.map(vector, fn {labels, value} -> {Map.delete(labels, "__name__"), transform(name, value, parameter)} end)
  end

  defp transform("clamp_min", v, p), do: clamp(v, p, :min)
  defp transform("clamp_max", v, p), do: clamp(v, p, :max)
  defp transform("round", v, p), do: round_value(v, p)
  defp transform("abs", :nan, _), do: :nan
  defp transform("abs", v, _), do: if(Number.sign(v) < 0, do: Number.negate(v), else: v)
  defp clamp(:nan, _, _), do: :nan
  defp clamp(_, :nan, _), do: :nan
  defp clamp(v, p, :min) when v == 0 and p == 0, do: if(Number.sign(v) == 1 or Number.sign(p) == 1, do: 0.0, else: -0.0)

  defp clamp(v, p, :max) when v == 0 and p == 0,
    do: if(Number.sign(v) == -1 or Number.sign(p) == -1, do: -0.0, else: 0.0)

  defp clamp(v, p, :min), do: Number.max(v, p)
  defp clamp(v, p, :max), do: Number.min(v, p)

  defp round_value(value, nearest) do
    inverse = Number.divide(1.0, nearest)
    rounded = Number.add(Number.multiply(value, inverse), 0.5)
    rounded = if is_number(rounded), do: :math.floor(rounded), else: rounded
    Number.divide(rounded, inverse)
  end

  def topk(op, grouping, parameter, vector, steps) do
    parameter = by_time(parameter)
    vector = by_time(vector)
    at_steps(steps, fn ts -> topk_step(op, grouping, value(at(parameter, ts)), at(vector, ts)) end)
  end

  defp topk_step(op, grouping, k, vector) do
    k = rank_count(k)

    vector
    |> Enum.group_by(fn {labels, _} -> group_key(labels, grouping) end)
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.flat_map(fn {_, values} -> values |> sort(op == "topk") |> Enum.take(k) end)
  end

  defp rank_count(k) do
    cond do
      Number.compare("<", k, 1) -> 0
      not is_number(k) or k >= 9_223_372_036_854_775_808 -> throw({:promql_error, :invalid_aggregation_parameter})
      true -> trunc(k)
    end
  end

  def group_key(_, nil), do: %{}
  def group_key(labels, {:grouping, :by, names}), do: Map.take(labels, names)
  def group_key(labels, {:grouping, :without, names}), do: Map.drop(labels, ["__name__" | names])

  def sort(values, descending?) do
    Enum.sort(values, fn {la, a}, {lb, b} ->
      cond do
        a == b -> la <= lb
        a == :nan -> false
        b == :nan -> true
        descending? -> Number.less?(b, a)
        true -> Number.less?(a, b)
      end
    end)
  end

  defp value([{_, value}]), do: value
  defp at(index, ts), do: Map.get(index, ts, [])

  defp by_time(series) do
    rows = for {labels, samples} <- series, {ts, value} <- samples, do: {ts, {labels, value}}
    Evaluator.charge(:work, length(rows))
    Enum.group_by(rows, &elem(&1, 0), &elem(&1, 1))
  end

  defp at_steps(steps, fun) do
    rows =
      Enum.flat_map(steps, fn ts ->
        values = fun.(ts) |> unique!()
        Evaluator.charge(:work, length(values))
        for {labels, value} <- values, do: {labels, {ts, value}}
      end)

    # Preserve operation ordering for instant topk/sort results.
    order = rows |> Enum.map(&elem(&1, 0)) |> Enum.uniq()
    Evaluator.charge(:points, length(rows))
    groups = Enum.group_by(rows, &elem(&1, 0), &elem(&1, 1))
    Enum.map(order, &{&1, Map.fetch!(groups, &1)})
  end

  defp unique!(values) do
    if length(values) != length(Enum.uniq_by(values, &elem(&1, 0))),
      do: throw({:promql_error, :duplicate_result_label_sets})

    values
  end
end
