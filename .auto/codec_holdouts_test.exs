defmodule PulsoCodecHoldouts do
  use ExUnit.Case, async: false
  alias Pulso.Storage.S3
  alias Pulso.Record.{Log, MetricSample}

  defp id(i), do: :crypto.hash(:sha256, <<i::64>>) |> Base.encode16(case: :lower)

  test "independent shared, missing and high-entropy identifier distributions remain lossless" do
    results = for n <- [50, 2500, 15000], shape <- [:missing, :shared, :unique] do
      input = for i <- 1..n do
        key = if shape == :shared, do: div(i, 100), else: i
        %Log{timestamp_ns: 1_700_000_000_000_000_000 + i * 117_821,
          service: "worker", severity_text: "INFO", body: "processed event #{rem(i, 31)}",
          trace_id: if(shape != :missing, do: id(key)),
          span_id: if(shape != :missing, do: String.slice(id(div(key, 2)), 0, 16)),
          resource: %{"host" => "node-1"}}
      end
      timings = for _ <- 1..3 do
        {us, {:ok, blob, _, _}} = :timer.tc(fn -> S3.encode_segment(:logs, input) end)
        {:ok, decoded} = S3.decode_segment(:logs, blob, nil, nil, [])
        assert Enum.sort(decoded) == Enum.sort(input)
        {us, byte_size(blob)}
      end
      {us, bytes} = timings |> Enum.sort() |> Enum.at(1)
      {"logs_#{shape}_#{n}", %{bytes: bytes, encode_us: us}}
    end
    metric_results = for n <- [50, 2500, 15000], shape <- [:repeated_gauge, :changing_counter, :float_entropy, :single_long_series] do
      input = for i <- 1..n do
        value = case shape do
          :repeated_gauge -> rem(i, 5) / 1
          :changing_counter -> i / 1
          :single_long_series -> i / 1
          :float_entropy -> :math.sin(i * 1.23456789) * 99999.12345
        end
        %MetricSample{timestamp_ns: 1_700_000_000_000_000_000 + i * 15_000_000_000,
          value: value, labels: %{"__name__" => "work", "host" => "node-#{if(shape == :single_long_series, do: 0, else: rem(i, 100))}"}}
      end
      timings = for _ <- 1..3 do
        {us, {:ok, blob, _, _}} = :timer.tc(fn -> S3.encode_segment(:metrics, input) end)
        {:ok, decoded} = S3.decode_segment(:metrics, blob, nil, nil, [])
        assert Enum.sort(Enum.map(decoded, &{&1.timestamp_ns, &1.labels, &1.value})) == Enum.sort(Enum.map(input, &{&1.timestamp_ns, &1.labels, &1.value}))
        ts = Enum.at(input, div(n, 2)).timestamp_ns
        {select_us, {:ok, selected}} = :timer.tc(fn -> S3.decode_segment(:metrics, blob, ts, ts, []) end)
        assert Enum.map(selected, & &1.timestamp_ns) == [ts]
        {us, byte_size(blob), select_us}
      end
      {us, bytes, select_us} = timings |> Enum.sort() |> Enum.at(1)
      {"metrics_#{shape}_#{n}", %{bytes: bytes, encode_us: us, select_us: select_us}}
    end
    File.write!(".auto/holdouts-latest.json", JSON.encode!(Map.new(results ++ metric_results)))
    for {name, values} <- results ++ metric_results, do: IO.puts("HOLDOUT #{name} bytes=#{values.bytes} median_encode_us=#{values.encode_us} select_us=#{Map.get(values, :select_us, 0)}")
  end
end
