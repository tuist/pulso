defmodule Pulso.PromQL.Operations do
  @moduledoc false
  alias Pulso.PromQL.Evaluator
  alias Pulso.PromQL.Histogram
  alias Pulso.PromQL.LabelReplace
  alias Pulso.PromQL.NativeHistogram
  alias Pulso.PromQL.Number
  alias Pulso.PromQL.Time
  alias Pulso.Record.NativeHistogram, as: HistogramSample

  @comparisons ["==", "!=", "<", ">", "<=", ">="]
  @histogram_property_functions %{
    "histogram_count" => :count,
    "histogram_sum" => :sum,
    "histogram_avg" => :avg,
    "histogram_stddev" => :stddev,
    "histogram_stdvar" => :stdvar
  }
  @histogram_properties Map.keys(@histogram_property_functions)
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

    cond do
      histogram?(a) or histogram?(b) -> histogram_pair(op, labels, a, b, boolean)
      filter?(op, boolean) -> filtered_sample(labels, value, Number.compare(op, a, b))
      true -> [{Map.delete(labels, "__name__"), result(op, a, b)}]
    end
  end

  # Prometheus' `vectorElemBinop` for histogram operands: scaling by a float,
  # adding or subtracting histograms, and comparing two for equality. Any
  # other combination has no result.
  defp histogram_pair(op, labels, a, b, boolean) do
    case {op, a, b} do
      {"*", %HistogramSample{}, b} when not is_struct(b) ->
        [{Map.delete(labels, "__name__"), compact(NativeHistogram.scale(a, b))}]

      {"*", a, %HistogramSample{}} when not is_struct(a) ->
        [{Map.delete(labels, "__name__"), compact(NativeHistogram.scale(b, a))}]

      {"/", %HistogramSample{}, b} when not is_struct(b) ->
        [{Map.delete(labels, "__name__"), compact(NativeHistogram.divide(a, b))}]

      {op, %HistogramSample{}, %HistogramSample{}} when op in ["+", "-"] ->
        combined(op, labels, a, b)

      {op, %HistogramSample{}, %HistogramSample{}} when op in ["==", "!="] ->
        equality(op, labels, a, b, boolean)

      _ ->
        []
    end
  end

  defp combined(op, labels, a, b) do
    result = if op == "+", do: NativeHistogram.add(a, b), else: NativeHistogram.sub(a, b)

    case result do
      {:ok, h} -> [{Map.delete(labels, "__name__"), compact(h)}]
      _ -> []
    end
  end

  defp equality(op, labels, a, b, boolean) do
    keep = NativeHistogram.equal?(a, b) == (op == "==")

    cond do
      boolean -> [{Map.delete(labels, "__name__"), if(keep, do: 1.0, else: 0.0)}]
      keep -> [{labels, a}]
      true -> []
    end
  end

  defp compact(h), do: NativeHistogram.compact(h)
  defp histogram?(value), do: is_struct(value, HistogramSample)

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

    cond do
      histogram?(a) or histogram?(b) ->
        histogram_pair(context.op, output_labels, a, b, context.boolean)

      filter?(context.op, context.boolean) ->
        filtered_sample(output_labels, a, Number.compare(context.op, a, b))

      true ->
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

  def absent(vector, labels, steps) do
    vector = by_time(vector)
    at_steps(steps, fn ts -> if at(vector, ts) == [], do: [{labels, 1.0}], else: [] end)
  end

  def call("timestamp", [vector], _steps) do
    Enum.map(vector, fn {labels, samples} ->
      {Map.delete(labels, "__name__"), Enum.map(samples, fn {ts, _value} -> {ts, Time.seconds(ts)} end)}
    end)
  end

  def call("time", [], steps), do: [{%{}, Enum.map(steps, &{&1, Time.seconds(&1)})}]
  def call("vector", [scalar], _steps), do: scalar

  def call("scalar", [vector], steps) do
    vector = by_time(vector)
    at_steps(steps, fn ts -> [{%{}, scalar_value(at(vector, ts))}] end)
  end

  def call("label_replace", [vector | args], _steps), do: vector |> LabelReplace.run(args) |> coalesce()

  # Native histograms answer for themselves; float samples are classic `le`
  # buckets, as Prometheus combines them.
  def call("histogram_quantile", [quantile, buckets], steps) do
    quantile = by_time(quantile)
    buckets = by_time(buckets)

    at_steps(steps, fn ts ->
      q = value(at(quantile, ts))
      {natives, classic} = Enum.split_with(at(buckets, ts), fn {_, v} -> histogram?(v) end)
      native = for {labels, h} <- natives, do: {Map.delete(labels, "__name__"), NativeHistogram.quantile(q, h)}
      native ++ Histogram.quantile(q, classic)
    end)
  end

  def call("histogram_fraction", [lower, upper, vector], steps) do
    {lower, upper, vector} = {by_time(lower), by_time(upper), by_time(vector)}

    at_steps(steps, fn ts ->
      {l, u} = {value(at(lower, ts)), value(at(upper, ts))}

      for {labels, h} <- at(vector, ts),
          histogram?(h),
          do: {Map.delete(labels, "__name__"), NativeHistogram.fraction(l, u, h)}
    end)
  end

  def call(name, [vector], _steps) when name in @histogram_properties do
    property = Map.fetch!(@histogram_property_functions, name)

    for {labels, samples} <- vector,
        points = for({ts, h} <- samples, histogram?(h), do: {ts, apply(NativeHistogram, property, [h])}),
        points != [],
        do: {Map.delete(labels, "__name__"), points}
  end

  def call(name, [vector | params], steps)
      when name in [
             "clamp_min",
             "clamp_max",
             "round",
             "abs",
             "sort",
             "sort_desc",
             "sgn",
             "floor",
             "ceil",
             "sqrt",
             "exp",
             "ln",
             "log2",
             "log10"
           ] do
    vector = by_time(vector)
    params = Enum.map(params, &by_time/1)
    at_steps(steps, fn ts -> transform_vector(name, at(vector, ts), parameter(params, ts)) end)
  end

  defp parameter([], _ts), do: 1.0
  defp parameter([scalar], ts), do: value(at(scalar, ts))
  defp scalar_value([{_, %HistogramSample{}}]), do: :nan
  defp scalar_value([{_, value}]), do: value
  defp scalar_value(_), do: :nan

  # These functions read floats; Prometheus leaves histograms out.
  defp transform_vector(name, vector, parameter),
    do: vector |> Enum.reject(fn {_, v} -> histogram?(v) end) |> transform_floats(name, parameter)

  defp transform_floats(vector, name, _parameter) when name in ["sort", "sort_desc"],
    do: sort(vector, name == "sort_desc")

  defp transform_floats(vector, name, parameter) do
    Enum.map(vector, fn {labels, value} -> {Map.delete(labels, "__name__"), transform(name, value, parameter)} end)
  end

  defp transform("clamp_min", v, p), do: clamp(v, p, :min)
  defp transform("clamp_max", v, p), do: clamp(v, p, :max)
  defp transform("round", v, p), do: round_value(v, p)
  defp transform("floor", v, _) when is_number(v), do: :math.floor(v)
  defp transform("floor", v, _), do: v
  defp transform("ceil", v, _) when is_number(v), do: :math.ceil(v)
  defp transform("ceil", v, _), do: v
  defp transform("sqrt", v, _) when v in [:nan, :infinity], do: v
  defp transform("sqrt", v, _) when v == :negative_infinity or v < 0, do: :nan
  defp transform("sqrt", v, _), do: :math.sqrt(v)
  defp transform("exp", v, _) when v in [:nan, :infinity], do: v
  defp transform("exp", :negative_infinity, _), do: 0.0
  defp transform("exp", v, _), do: exponential(v)

  defp transform(name, v, _) when name in ["ln", "log2", "log10"] and v == 0, do: :negative_infinity
  defp transform(name, :infinity, _) when name in ["ln", "log2", "log10"], do: :infinity

  defp transform(name, v, _) when name in ["ln", "log2", "log10"] and (v in [:nan, :negative_infinity] or v < 0),
    do: :nan

  defp transform("ln", v, _), do: :math.log(v)
  defp transform("log2", v, _), do: binary_logarithm(v)
  defp transform("log10", v, _), do: :math.log10(v)
  defp transform("sgn", :nan, _), do: :nan
  defp transform("sgn", v, _) when v == 0, do: v
  defp transform("sgn", v, _), do: if(Number.compare("<", v, 0), do: -1.0, else: 1.0)
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

  # Adapted from Go 1.24.5 math/{log10.go,frexp.go,exp.go,exp_arm64.s}.
  # Copyright 2009, 2017 The Go Authors.
  # BSD license (also shipped in releases): priv/third_party/go-math-LICENSE.
  # The exp polynomial also retains its original permission notice:
  # Copyright (C) 2004 by Sun Microsystems, Inc. All rights reserved.
  # Permission to use, copy, modify, and distribute this software is freely
  # granted, provided that this notice is preserved.
  # Keep Go's reduction/scaling rather than libm: cancellation near log2(1)
  # and arm64 exponential underflow change observable PromQL predicates.
  defp binary_logarithm(value) do
    {value, adjustment} =
      if value < 2.2250738585072014e-308, do: {value * 4_503_599_627_370_496.0, -52}, else: {value, 0}

    <<0::1, exponent::11, fraction::52>> = <<value::float-64>>
    <<normalized::float-64>> = <<0::1, 1022::11, fraction::52>>
    exponent = exponent - 1022 + adjustment

    if normalized == 0.5 do
      exponent - 1.0
    else
      # Go arm64 fuses Log(frac)*(1/Ln2)+exp. Form the exact integer
      # numerator before the single float rounding; separate multiplication
      # and addition lose most significant digits immediately above one.
      <<1::1, log_exponent::11, log_fraction::52>> = <<:math.log(normalized)::float-64>>
      <<0::1, 1023::11, multiplier::52>> = <<1.4426950408889634::float-64>>
      denominator = Integer.pow(2, 1127 - log_exponent)
      product = (4_503_599_627_370_496 + log_fraction) * (4_503_599_627_370_496 + multiplier)
      (exponent * denominator - product) / denominator
    end
  end

  defp exponential(value) when value > 7.09782712893383973096e2, do: :infinity
  defp exponential(value) when value < -7.45133219101941108420e2, do: 0.0
  defp exponential(value) when abs(value) < 3.725290298461914e-9, do: 1.0 + value

  defp exponential(value) do
    exponent = trunc(1.44269504088896338700 * value + if(value < 0, do: -0.5, else: 0.5))
    hi = value - exponent * 6.93147180369123816490e-1
    lo = exponent * 1.90821492927058770002e-10
    remainder = hi - lo
    squared = remainder * remainder

    polynomial =
      1.66666666666666657415e-1 +
        squared *
          (-2.77777777770155933842e-3 +
             squared *
               (6.61375632143793436117e-5 +
                  squared * (-1.65339022054652515390e-6 + squared * 4.13813679705723846039e-8)))

    correction = remainder - squared * polynomial
    reduced = 1.0 - (lo - remainder * correction / (2.0 - correction) - hi)
    <<0::1, biased::11, fraction::52>> = <<reduced::float-64>>
    biased = biased + exponent

    cond do
      biased >= 2047 ->
        :infinity

      biased < 1 ->
        # This is specifically the pinned Go arm64 52-bit scaling, including
        # its early zero results at the smallest-subnormal boundary.
        <<scaled::float-64>> = <<0::1, biased + 52::11, fraction::52>>
        scaled * 2.220446049250313e-16

      true ->
        <<scaled::float-64>> = <<0::1, biased::11, fraction::52>>
        scaled
    end
  end

  def topk(op, grouping, parameter, vector, steps) do
    parameter = by_time(parameter)
    vector = by_time(vector)
    at_steps(steps, fn ts -> topk_step(op, grouping, value(at(parameter, ts)), at(vector, ts)) end)
  end

  defp topk_step(op, grouping, k, vector) do
    k = rank_count(k)

    vector
    |> Enum.reject(fn {_, v} -> histogram?(v) end)
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

  # Metric-name removal may merge disjoint series in a range response.
  # Only identical labels at the SAME evaluation step are a PromQL error.
  def coalesce(series) do
    rows = for {labels, samples} <- series, {ts, value} <- samples, do: {{labels, ts}, value}
    Evaluator.charge(:work, length(rows))
    if length(rows) != map_size(Map.new(rows)), do: throw({:promql_error, :duplicate_result_label_sets})
    order = Enum.uniq_by(series, &elem(&1, 0)) |> Enum.map(&elem(&1, 0))
    groups = Enum.group_by(rows, fn {{labels, _}, _} -> labels end, fn {{_, ts}, value} -> {ts, value} end)
    Enum.map(order, &{&1, Enum.sort(Map.fetch!(groups, &1))})
  end

  defp unique!(values) do
    if length(values) != length(Enum.uniq_by(values, &elem(&1, 0))),
      do: throw({:promql_error, :duplicate_result_label_sets})

    values
  end
end
