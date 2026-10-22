defmodule Pulso.Traces.SearchExpression.Evaluator do
  @moduledoc false
  alias Pulso.Codec.NIF
  alias Pulso.Traces.SearchExpression

  def prepare(program) do
    with :ok <- SearchExpression.validate(program) do
      {:ok, {:expression, Enum.map(program, &prepare_leaf/1)}}
    end
  end

  defp prepare_leaf({scope, key, op, value}) when op in ["=~", "!~"] do
    {:ok, regex} = NIF.compile_metric_capture_regex(value)
    {scope, key, op, regex}
  end

  defp prepare_leaf({scope, key, op, value}) when scope in ["resource.int", "span.int", "intrinsic"],
    do: {scope, key, op, String.to_integer(value)}

  defp prepare_leaf(leaf), do: leaf

  def evaluate(span, program, work) do
    resource = Map.new(span.otlp["resource"], &{&1["key"], &1["value"]})
    attrs = Map.new(span.otlp["attributes"], &{&1["key"], &1["value"]})
    work = work + 2 + map_size(resource) + map_size(attrs) + length(program)

    if work > 20_000_000 do
      {:error, :query_resource_limit}
    else
      {[matched], selected} = Enum.reduce(program, {[], %{}}, &instruction(&1, &2, span, resource, attrs))
      attributes = selected |> Enum.sort() |> Enum.map(fn {{key, _}, value} -> %{"key" => key, "value" => value} end)
      {:ok, matched, attributes, work}
    end
  end

  defp instruction({"boolean", "", op, ""}, {[rhs, lhs | tail], selected}, _, _, _),
    do: {[if(op == "&&", do: lhs and rhs, else: lhs or rhs) | tail], selected}

  defp instruction({scope, key, op, wanted}, {stack, selected}, span, resource, attrs) do
    {actual, value, owner} = actual(scope, key, span, resource, attrs)
    matched = match_value(actual, op, wanted)
    selected = if matched and value != nil, do: Map.put(selected, {key, owner}, value), else: selected
    {[matched | stack], selected}
  end

  defp actual("intrinsic", "duration", span, _, _),
    do: {{:int, span.end_timestamp_ns - span.timestamp_ns}, nil, "intrinsic"}

  defp actual("intrinsic", "status", span, _, _) do
    code = span.status["code"]
    {{:int, code}, %{"stringValue" => Enum.at(~w(unset ok error), code)}, "intrinsic"}
  end

  defp actual(scope, key, _, resource, attrs) do
    [owner, type] = String.split(scope, ".")
    value = Map.get(if(owner == "resource", do: resource, else: attrs), key)

    actual =
      case {type, value} do
        {"string", %{"stringValue" => text}} -> {:string, text}
        {"int", %{"intValue" => number}} -> {:int, String.to_integer(number)}
        _ -> nil
      end

    {actual, value, owner}
  end

  defp match_value({:string, value}, op, wanted) when op in ["=~", "!~"] do
    positive = NIF.metric_regex_captures(wanted, value) != nil
    if op == "!~", do: not positive, else: positive
  end

  defp match_value({_, actual}, "=", wanted), do: actual == wanted
  defp match_value({_, actual}, "!=", wanted), do: actual != wanted
  defp match_value({:int, actual}, "<", wanted), do: actual < wanted
  defp match_value({:int, actual}, "<=", wanted), do: actual <= wanted
  defp match_value({:int, actual}, ">", wanted), do: actual > wanted
  defp match_value({:int, actual}, ">=", wanted), do: actual >= wanted
  defp match_value(_, _, _), do: false
end
