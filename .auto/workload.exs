defmodule CapacityBench do
  alias Pulso.Codec.NIF
  alias Pulso.Record.{Log, MetricSample}

  def metrics(series, per_series) do
    for i <- 1..series, j <- 1..per_series do
      %MetricSample{series_id: i, timestamp_ns: j * 1_000_000_000, value: j / 3,
        labels: %{"__name__" => "requests_total_#{rem(i, 7)}", "instance" => "node-#{i}",
          "job" => "api", "region" => "region-#{rem(i, 4)}", "path" => "/v1/objects/#{rem(i, 19)}"}}
    end
  end

  def cases do
    repeated = metrics(80, 100)
    unique = metrics(2000, 1)
    {:ok, repeated_blob, _, _, _} = NIF.encode_metric_segment_parquet(repeated)
    {:ok, unique_blob, _, _, _} = NIF.encode_metric_segment_parquet(unique)
    logs = for i <- 1..2000 do
      %Log{timestamp_ns: i * 1_000_000_000, service: "svc-#{rem(i, 11)}", severity_text: "INFO",
        body: "request #{i} completed with status 200 in #{rem(i, 73)}ms " <> String.duplicate("x", rem(i, 180)),
        attributes: %{"http.status_code" => 200, "request_id" => "req-#{i}"},
        resource: %{"service.name" => "svc-#{rem(i, 11)}", "region" => "west"}}
    end
    {:ok, log_blob, _, _} = Pulso.Storage.S3.encode_segment(:logs, logs)
    json = JSON.encode!(Enum.map(logs, &Map.from_struct/1))
    [
      {"metric_read_repeated", fn -> {:ok, rows} = NIF.decode_metric_segment_parquet(repeated_blob, nil, nil, []); rows end},
      {"metric_read_unique", fn -> {:ok, rows} = NIF.decode_metric_segment_parquet(unique_blob, nil, nil, []); rows end},
      {"metric_read_filtered", fn -> {:ok, rows} = NIF.decode_metric_segment_parquet(repeated_blob, 20_000_000_000, 40_000_000_000, [{"region", :eq, "region-1"}]); rows end},
      {"log_read", fn -> {:ok, rows} = Pulso.Storage.S3.decode_segment(:logs, log_blob, nil, nil, []); rows end},
      {"json_roundtrip", fn -> rows = Pulso.JSON.decode!(json); {rows, Pulso.JSON.encode!(rows)} end},
      {"metric_write", fn -> {:ok, blob, _, _, 8000} = NIF.encode_metric_segment_parquet(repeated); blob end}
    ]
  end

  def retained(fun) do
    Task.async(fn ->
      result = fun.()
      :erlang.garbage_collect()
      {:memory, mem} = Process.info(self(), :memory)
      {:binary, bins} = Process.info(self(), :binary)
      bytes = bins |> Enum.uniq_by(&elem(&1, 0)) |> Enum.map(&elem(&1, 1)) |> Enum.sum()
      # Keep result genuinely live across GC and measurement.
      {mem + bytes, :erts_debug.size(result)}
    end) |> Task.await(:infinity)
  end

  def measure({name, fun}) do
    fun.()
    {bytes, words} = retained(fun)
    rates = for _ <- 1..5 do
      {us, counts} = :timer.tc(fn ->
        1..4 |> Task.async_stream(fn _ ->
          for _ <- 1..120 do
            result = fun.()
            if is_binary(result), do: byte_size(result), else: :erlang.phash2(result)
          end |> length()
        end, max_concurrency: 4, timeout: :infinity) |> Enum.map(fn {:ok, n} -> n end)
      end)
      Enum.sum(counts) * 1_000_000 / us
    end
    rate = rates |> Enum.sort() |> Enum.at(2)
    mb = bytes / 1_048_576
    IO.puts("METRIC #{name}_rps=#{rate}")
    IO.puts("METRIC #{name}_live_mb=#{mb}")
    IO.puts("METRIC #{name}_words=#{words}")
    {rate, mb}
  end

  def run do
    results = cases() |> Enum.map(&measure/1)
    geometric = fn values -> :math.exp(Enum.sum(Enum.map(values, &:math.log/1)) / length(values)) end
    IO.puts("METRIC throughput_rps=#{geometric.(Enum.map(results, &elem(&1, 0)))}")
    IO.puts("METRIC live_mb=#{geometric.(Enum.map(results, &elem(&1, 1)))}")
    IO.puts("METRIC capacity_index=#{geometric.(Enum.map(results, fn {rps, mb} -> rps / mb end))}")
  end
end
CapacityBench.run()
