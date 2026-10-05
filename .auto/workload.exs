Code.require_file(".auto/reference_nif.exs")

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

  def cases(nif) do
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
      {"metric_read_repeated", fn -> {:ok, rows} = nif.decode_metric_segment_parquet(repeated_blob, nil, nil, []); rows end},
      {"metric_read_unique", fn -> {:ok, rows} = nif.decode_metric_segment_parquet(unique_blob, nil, nil, []); rows end},
      {"metric_read_filtered", fn -> {:ok, rows} = nif.decode_metric_segment_parquet(repeated_blob, 20_000_000_000, 40_000_000_000, [{"region", :eq, "region-1"}]); rows end},
      {"log_read", fn -> {:ok, rows} = nif.decode_log_segment_parquet(log_blob, nil, nil, nil, [], []); rows end},
      {"json_roundtrip", fn -> rows = json_decode(nif, json); {rows, json_encode(nif, rows)} end},
      {"metric_write", fn -> {:ok, blob, _, _, 8000} = nif.encode_metric_segment_parquet(repeated); blob end}
    ]
  end

  # Same public Pulso.JSON dispatch for candidate and frozen control.
  def json_decode(nif, bytes) do
    result = if byte_size(bytes) <= 65536, do: nif.json_decode(bytes), else: nif.json_decode_dirty(bytes)
    case result do
      {:ok, term} -> term
      :fallback -> JSON.decode!(bytes)
    end
  end

  def json_encode(nif, term) do
    result = case nif.json_encode(term, 65536) do
      :too_big -> nif.json_encode_dirty(term)
      result -> result
    end
    case result do
      {:ok, bytes} -> bytes
      :fallback -> JSON.encode!(term)
    end
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

  def rate(fun) do
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

  def canonical("metric_write", blob) do
    {:ok, rows} = NIF.decode_metric_segment_parquet(blob, nil, nil, [])
    rows
  end
  def canonical("json_roundtrip", {rows, json}), do: {rows, JSON.decode!(json)}
  def canonical(_, result), do: result

  def measure({{name, fun}, {name, ref_fun}}) do
    unless canonical(name, fun.()) == canonical(name, ref_fun.()), do: raise("reference mismatch: #{name}")
    {bytes, words} = retained(fun)
    {ref_bytes, _} = retained(ref_fun)
    runs = for round <- 1..7 do
      # Alternate which version goes first, preserving identical work.
      {candidate, reference} = if rem(round, 2) == 1 do
        candidate = rate(fun)
        {candidate, rate(ref_fun)}
      else
        reference = rate(ref_fun)
        {rate(fun), reference}
      end
      {candidate, candidate / reference}
    end
    median = fn values -> values |> Enum.sort() |> Enum.at(3) end
    rps = median.(Enum.map(runs, &elem(&1, 0)))
    ratio = median.(Enum.map(runs, &elem(&1, 1)))
    mb = bytes / 1_048_576
    IO.puts("METRIC #{name}_rps=#{rps}")
    IO.puts("METRIC #{name}_live_mb=#{mb}")
    IO.puts("METRIC #{name}_words=#{words}")
    IO.puts("PAIR #{name} throughput_ratio=#{ratio} memory_ratio=#{ref_bytes / bytes}")
    {rps, mb, ratio * ref_bytes / bytes}
  end

  def run do
    results = Enum.zip(cases(NIF), cases(Pulso.AutoReferenceNIF)) |> Enum.map(&measure/1)
    geometric = fn values -> :math.exp(Enum.sum(Enum.map(values, &:math.log/1)) / length(values)) end
    IO.puts("METRIC throughput_rps=#{geometric.(Enum.map(results, &elem(&1, 0)))}")
    IO.puts("METRIC live_mb=#{geometric.(Enum.map(results, &elem(&1, 1)))}")
    IO.puts("METRIC capacity_index=#{geometric.(Enum.map(results, &elem(&1, 2)))}")
  end
end
CapacityBench.run()
