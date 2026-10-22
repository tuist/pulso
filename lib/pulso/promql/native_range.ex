defmodule Pulso.PromQL.NativeRange do
  @moduledoc """
  Range functions over native histogram samples, following Prometheus 3.5.0
  (`promql/functions.go`: `extrapolatedRate`, `histogramRate`,
  `instantValue`, `funcSumOverTime`, `funcAvgOverTime`).

  Callers pass windows whose samples are all histograms; windows that mix
  floats and histograms produce no result, as in Prometheus. Incompatible
  histograms (custom bounds that differ, or custom mixed with exponential
  buckets) also produce no result.
  """

  alias Pulso.PromQL.NativeHistogram, as: H
  alias Pulso.PromQL.Time
  alias Pulso.Record.NativeHistogram

  @doc "`rate`, `increase`, `delta`, `irate`, `sum_over_time` or `avg_over_time` of `samples`, or nil."
  def value(op, samples, start, finish) when op in [:rate, :increase, :delta] do
    if length(samples) >= 2, do: extrapolated(op, samples, start, finish)
  end

  def value(:irate, samples, _start, _finish) do
    case Enum.take(samples, -2) do
      [first, last] when last.timestamp_ns > first.timestamp_ns ->
        change =
          if H.detect_reset?(last.value, first.value),
            do: {:ok, last.value},
            else: H.sub(last.value, first.value)

        case change do
          {:ok, h} ->
            h
            |> gauge()
            |> H.compact()
            |> H.divide(Time.seconds(last.timestamp_ns - first.timestamp_ns))

          _ ->
            nil
        end

      _ ->
        nil
    end
  end

  def value(:sum_over_time, samples, _start, _finish), do: aggregate(:sum, Enum.map(samples, & &1.value))
  def value(:avg_over_time, samples, _start, _finish), do: aggregate(:avg, Enum.map(samples, & &1.value))

  @doc "The `sum` or `avg` of `histograms`, or nil when they are incompatible."
  def aggregate(:sum, [first | rest]) do
    rest
    |> Enum.reduce_while({:ok, first}, fn h, {:ok, sum} ->
      case H.add(sum, h) do
        {:ok, _} = ok -> {:cont, ok}
        error -> {:halt, error}
      end
    end)
    |> ok_or_nil()
  end

  # Incremental mean, as Prometheus computes it for histograms.
  def aggregate(:avg, [first | rest]) do
    rest
    |> Enum.with_index(2)
    |> Enum.reduce_while({:ok, first}, fn {h, count}, {:ok, mean} ->
      with {:ok, step} <- H.sub(H.divide(h, count), H.divide(mean, count)),
           {:ok, _} = ok <- H.add(mean, step) do
        {:cont, ok}
      else
        error -> {:halt, error}
      end
    end)
    |> ok_or_nil()
  end

  defp extrapolated(op, samples, start, finish) do
    counter? = op != :delta

    case change(Enum.map(samples, & &1.value), counter?) do
      {:ok, h} ->
        first = hd(samples).timestamp_ns
        last = List.last(samples).timestamp_ns
        interval = Time.seconds(last - first)
        average = interval / (length(samples) - 1)
        left = boundary(Time.seconds(first - start), average)
        right = boundary(Time.seconds(finish - last), average)
        # Unlike floats, histograms are not extrapolated to the zero point.
        factor = (interval + left + right) / interval
        factor = if op == :rate, do: factor / Time.seconds(finish - start), else: factor
        H.scale(h, factor)

      _ ->
        nil
    end
  end

  defp boundary(distance, average), do: if(distance >= average * 1.1, do: average / 2, else: distance)

  # histogramRate: the last histogram minus the first, reconciled to the
  # lowest schema in the window, plus every value lost to a counter reset.
  defp change([first, second | _] = histograms, counter?) do
    last = List.last(histograms)

    # A reset between the first two samples makes the first irrelevant.
    first =
      if counter? and H.detect_reset?(second, first),
        do: %NativeHistogram{schema: second.schema, custom_values: second.custom_values},
        else: first

    # Counters reconcile every sample; gauges only the two they subtract.
    middle = if counter?, do: Enum.slice(histograms, 1..-2//1), else: []
    involved = [first, last | middle]
    custom? = &(&1.schema == NativeHistogram.custom_buckets_schema())

    with true <- Enum.all?(involved, &(custom?.(&1) == custom?.(first))) or :incompatible,
         lowest = involved |> Enum.map(& &1.schema) |> Enum.min(),
         {:ok, h} <- H.sub(H.reduce_resolution(last, lowest), first),
         {:ok, h} <- if(counter?, do: add_resets(h, [first | tl(histograms)]), else: {:ok, h}) do
      {:ok, h |> gauge() |> H.compact()}
    else
      _ -> :error
    end
  end

  defp add_resets(h, [previous | rest]) do
    rest
    |> Enum.reduce_while({:ok, h, previous}, &add_reset/2)
    |> case do
      {:ok, acc, _} -> {:ok, acc}
      error -> error
    end
  end

  # Each counter reset loses the value before it; add that back.
  defp add_reset(current, {:ok, acc, previous}) do
    with true <- H.detect_reset?(current, previous),
         {:ok, acc} <- H.add(acc, previous) do
      {:cont, {:ok, acc, current}}
    else
      false -> {:cont, {:ok, acc, current}}
      error -> {:halt, error}
    end
  end

  defp gauge(h), do: %{h | counter_reset_hint: :gauge}

  defp ok_or_nil({:ok, h}), do: h
  defp ok_or_nil(_), do: nil
end
