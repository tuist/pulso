defmodule Pulso.PromQL.RangeMath do
  @moduledoc false
  alias Pulso.PromQL.Number, as: N
  alias Pulso.PromQL.Time

  # Float algorithms from Prometheus v3.5.0 promql/functions.go, using the same
  # Neumaier compensated sums and constant-series regression special case.
  def variance(samples) do
    {count, _mean, _cm, aux, ca} =
      Enum.reduce(samples, {0, 0.0, 0.0, 0.0, 0.0}, fn sample, {count, mean, cm, aux, ca} ->
        count = count + 1
        delta = N.binary("-", sample.value, N.add(mean, cm))
        {mean, cm} = N.compensated_add(N.divide(delta, count), mean, cm)
        inc = N.multiply(delta, N.binary("-", sample.value, N.add(mean, cm)))
        {aux, ca} = N.compensated_add(inc, aux, ca)
        {count, mean, cm, aux, ca}
      end)

    N.divide(N.add(aux, ca), count)
  end

  def deviation(samples) do
    case variance(samples) do
      value when is_number(value) and value >= 0 -> :math.sqrt(value)
      :infinity -> :infinity
      _ -> :nan
    end
  end

  def predict(samples, origin, horizon) when length(samples) >= 2 do
    values = Enum.map(samples, & &1.value)
    first = hd(values)

    if Enum.all?(values, &N.compare("==", &1, first)) do
      intercept = if is_number(first), do: first, else: :nan
      N.add(N.multiply(0.0, horizon), intercept)
    else
      {sx, sy, sxy, sx2} = sums(samples, origin)
      n = length(samples)
      covariance = N.binary("-", sxy, N.divide(N.multiply(sx, sy), n))
      variance = N.binary("-", sx2, N.divide(N.multiply(sx, sx), n))
      slope = N.divide(covariance, variance)
      intercept = N.binary("-", N.divide(sy, n), N.divide(N.multiply(slope, sx), n))
      N.add(N.multiply(slope, horizon), intercept)
    end
  end

  def predict(_, _, _), do: nil

  defp sums(samples, origin) do
    initial = {{0.0, 0.0}, {0.0, 0.0}, {0.0, 0.0}, {0.0, 0.0}}

    {sx, sy, sxy, sx2} =
      Enum.reduce(samples, initial, fn sample, {sx, sy, sxy, sx2} ->
        x = Time.seconds(sample.timestamp_ns - origin)
        y = sample.value
        {inc(x, sx), inc(y, sy), inc(N.multiply(x, y), sxy), inc(N.multiply(x, x), sx2)}
      end)

    {total(sx), total(sy), total(sxy), total(sx2)}
  end

  defp inc(value, {sum, correction}), do: N.compensated_add(value, sum, correction)
  defp total({sum, correction}), do: N.add(sum, correction)
end
