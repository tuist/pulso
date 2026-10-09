defmodule Pulso.Codec.MetricJSONTest do
  use Pulso.Test.Case, async: true

  import Bitwise

  alias Pulso.Codec.NIF
  alias Pulso.Record.MetricSample

  defp fields(sample), do: Map.take(sample, [:series_id, :timestamp_ns, :value, :labels])

  test "native response encoding matches the Elixir reference across randomized batches" do
    for _ <- 1..200 do
      labels = %{"__name__" => "counter", "unicode" => "é😀", "escaped" => "\"\\\n\r\t\x00\x1f"}

      samples =
        for i <- 1..:rand.uniform(30) do
          %MetricSample{
            series_id: Enum.random([nil, -9_223_372_036_854_775_808, i]),
            timestamp_ns: Enum.random([nil, -i, 9_223_372_036_854_775_807]),
            value: Enum.random([nil, i, i / 3, -0.0]),
            labels: Enum.random([labels, %{}, Map.put(labels, "instance", "node-#{i}")])
          }
        end

      assert {:ok, encoded} = NIF.encode_metric_samples(samples)
      assert JSON.decode!(encoded) == JSON.decode!(JSON.encode!(Enum.map(samples, &fields/1)))
    end
  end

  test "repeated label ranges remain valid through large output reallocations" do
    labels = %{"annotation" => String.duplicate("é\\\"", 1000)}
    samples = for i <- 1..100, do: %MetricSample{series_id: i, timestamp_ns: i, value: i * 1.0, labels: labels}
    assert {:ok, binary} = NIF.encode_metric_samples(samples)
    assert byte_size(binary) > 500_000
    assert JSON.decode!(binary) == JSON.decode!(JSON.encode!(Enum.map(samples, &fields/1)))
    assert {:ok, "[]"} = NIF.encode_metric_samples([])
  end

  test "unsupported fields still fall back after a run of valid labels" do
    valid = %MetricSample{series_id: 1, timestamp_ns: 1, value: 1.0, labels: %{}}

    for invalid <- [
          %{valid | series_id: 1 <<< 80},
          %{valid | value: :invalid},
          %{valid | labels: %{"key" => :invalid}},
          %{valid | labels: %{atom_key: "value"}},
          %{},
          :invalid
        ] do
      assert :fallback = NIF.encode_metric_samples([valid, valid, invalid])
    end
  end
end
