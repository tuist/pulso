defmodule Pulso.PromQL.Histogram do
  @moduledoc false
  alias Pulso.PromQL.FloatParser
  alias Pulso.PromQL.Number

  def quantile(q, vector) do
    vector
    |> Enum.flat_map(&bucket/1)
    |> Enum.group_by(fn {labels, _, _} -> labels end, fn {_, bound, count} -> {bound, count} end)
    |> Enum.map(fn {labels, values} -> {Map.delete(labels, "__name__"), bucket_quantile(q, values)} end)
  end

  defp bucket({labels, count}) do
    case FloatParser.parse(Map.get(labels, "le")) do
      :error -> []
      {:ok, bound} -> [{Map.delete(labels, "le"), bound, count}]
    end
  end

  defp bucket_quantile(:nan, _), do: :nan

  defp bucket_quantile(q, values) do
    cond do
      Number.compare("<", q, 0) ->
        :negative_infinity

      Number.compare(">", q, 1) ->
        :infinity

      true ->
        values
        |> Enum.sort(fn {a, _}, {b, _} -> not Number.compare(">", a, b) end)
        |> coalesce()
        |> monotonic()
        |> quantile_result(q)
    end
  end

  defp quantile_result(buckets, q) do
    cond do
      length(buckets) < 2 -> :nan
      elem(List.last(buckets), 0) != :infinity -> :nan
      Number.compare("==", elem(List.last(buckets), 1), 0) -> :nan
      true -> interpolate(buckets, Number.multiply(q, elem(List.last(buckets), 1)))
    end
  end

  defp coalesce(buckets) do
    buckets |> Enum.reduce([], &coalesce_bucket/2) |> Enum.reverse()
  end

  defp coalesce_bucket({bound, count}, [{previous, previous_count} | rest] = acc) do
    if Number.compare("==", bound, previous),
      do: [{bound, Number.add(previous_count, count)} | rest],
      else: [{bound, count} | acc]
  end

  defp coalesce_bucket(bucket, []), do: [bucket]
  defp monotonic([]), do: []

  defp monotonic([first | rest]) do
    {result, _} =
      Enum.map_reduce(rest, elem(first, 1), fn {bound, count}, previous ->
        count = monotonic_count(count, previous)
        {{bound, count}, count}
      end)

    [first | result]
  end

  defp monotonic_count(count, previous) do
    cond do
      Number.compare("==", count, previous) -> count
      almost_equal?(count, previous) -> previous
      Number.compare("<", count, previous) -> previous
      true -> count
    end
  end

  defp almost_equal?(a, b) when is_number(a) and is_number(b), do: abs(a - b) <= 1.0e-12 * (abs(a) + abs(b))
  defp almost_equal?(_, _), do: false

  defp interpolate(buckets, rank) do
    # Prometheus uses sort.Search even for NaN counts; its predicate can be
    # non-monotone in that case, so a linear first-match scan is not equivalent.
    tuple = List.to_tuple(buckets)
    index = search(tuple, rank, 0, tuple_size(tuple) - 1)

    cond do
      index == tuple_size(tuple) - 1 -> elem(elem(tuple, index - 1), 0)
      index == 0 and Number.compare("<=", elem(elem(tuple, 0), 0), 0) -> elem(elem(tuple, 0), 0)
      true -> interpolate_bucket(tuple, index, rank)
    end
  end

  defp search(_buckets, _rank, low, high) when low >= high, do: low

  defp search(buckets, rank, low, high) do
    middle = div(low + high, 2)

    if Number.compare(">=", elem(elem(buckets, middle), 1), rank),
      do: search(buckets, rank, low, middle),
      else: search(buckets, rank, middle + 1, high)
  end

  defp interpolate_bucket(buckets, index, rank) do
    {upper, count} = elem(buckets, index)
    {lower, previous} = if index == 0, do: {0.0, 0.0}, else: elem(buckets, index - 1)
    count = Number.binary("-", count, previous)
    rank = Number.binary("-", rank, previous)
    Number.add(lower, Number.multiply(Number.binary("-", upper, lower), Number.divide(rank, count)))
  end
end
