defmodule PulsoWeb.IngestLoadTest do
  @moduledoc """
  Mixed-burst ingest load test through the public HTTP ingest paths.

  Excluded by default. Run with:

      PULSO_LOAD_S3_LATENCY_MS=150..300 PULSO_LOAD_SECONDS=60 \\
        mix test test/pulso_web/ingest_load_test.exs --include ingest_load

  A remote-write replay flood (many shards that retry after 429) runs alongside
  steady open-loop Loki pushes and OTLP trace exports against the S3 adapter on
  a fake S3 server that delays every request by a uniformly drawn latency.
  The test prints throughput, status counts, accepted latency and storage
  request counts, and asserts that Loki and traces are still admitted.
  """
  use Pulso.Test.Case, async: true

  import Bitwise

  alias Pulso.Runtime
  alias Pulso.Storage
  alias Pulso.Storage.S3
  alias Pulso.Test.CompactionStore
  alias Pulso.Test.OTLPTraceFixture
  alias Pulso.Test.RuntimePlug

  @moduletag :ingest_load
  @moduletag timeout: :infinity

  defp env_int(name, default), do: name |> System.get_env(Integer.to_string(default)) |> String.to_integer()

  defp latency_range do
    case String.split(System.get_env("PULSO_LOAD_S3_LATENCY_MS", "150..300"), "..") do
      [low, high] -> {String.to_integer(low), String.to_integer(high)}
      [value] -> {String.to_integer(value), String.to_integer(value)}
    end
  end

  setup do
    Runtime.share()
    stats = :ets.new(:load_stats, [:public, :duplicate_bag, write_concurrency: true])
    {low, high} = latency_range()

    store =
      start_supervised!(
        {Agent,
         fn ->
           %{
             objects: %{},
             reads: [],
             version: 0,
             hook: nil,
             faults: %{},
             barriers: %{},
             lists: 0,
             deletes: [],
             requests: []
           }
         end}
      )

    # Trim the store's request journals and drop stored segment bodies (never
    # read back here), so VM memory reflects Pulso rather than the fake store.
    trim = fn ->
      Agent.update(store, fn state ->
        objects =
          Map.new(state.objects, fn
            {key, {etag, _body}} when binary_part(key, byte_size(key) - 8, 8) == ".parquet" -> {key, {etag, ""}}
            entry -> entry
          end)

        %{state | reads: [], requests: [], objects: objects}
      end)
    end

    delay = fn method, key, body ->
      kind = if String.ends_with?(key, "manifest.json"), do: :manifest, else: :object
      :ets.insert(stats, {:s3, method, kind, byte_size(body)})
      Process.sleep(low + :rand.uniform(high - low + 1) - 1)
    end

    s3 =
      start_supervised!(
        {Bandit,
         plug: {CompactionStore, agent: store, before_request: delay},
         ip: {127, 0, 0, 1},
         port: 0,
         thousand_island_options: [num_acceptors: 16, shutdown_timeout: 100]},
        id: :s3
      )

    {:ok, {_, s3_port}} = ThousandIsland.listener_info(s3)

    Runtime.put_env(:pulso, Storage, adapter: S3)

    Runtime.put_env(:pulso, S3, %{
      bucket: "pulso",
      region: "us-east-1",
      endpoint: "http://127.0.0.1:#{s3_port}",
      access_key_id: "load-#{System.unique_integer([:positive])}",
      secret_access_key: "load",
      allow_http: true,
      ingest_flush_interval_ms: env_int("PULSO_LOAD_FLUSH_INTERVAL_MS", 50)
    })

    Runtime.put_env(:pulso, Pulso.IngestAdmission, max_concurrent: env_int("PULSO_LOAD_MAX_CONCURRENT", 16))
    start_supervised!(S3.ManifestSupervision)
    start_supervised!(Pulso.IngestAdmission)

    api =
      start_supervised!(
        {Bandit,
         plug: {RuntimePlug, plug: PulsoWeb.Endpoint, runtime: Runtime.current()},
         ip: {127, 0, 0, 1},
         port: 0,
         thousand_island_options: [num_acceptors: 32, num_connections: 4096]},
        id: :api
      )

    {:ok, {_, api_port}} = ThousandIsland.listener_info(api)
    start_supervised!({Finch, name: LoadFinch, pools: %{default: [size: 512, count: 1]}})

    {:ok, api: "http://127.0.0.1:#{api_port}", stats: stats, trim: trim, latency: {low, high}}
  end

  test "a remote-write flood does not starve steady Loki and OTLP trace ingest", ctx do
    seconds = env_int("PULSO_LOAD_SECONDS", 30)
    shards = env_int("PULSO_LOAD_RW_SHARDS", 64)
    deadline = System.monotonic_time(:millisecond) + seconds * 1000
    results = :ets.new(:load_results, [:public, :duplicate_bag, write_concurrency: true])

    rw_bodies = for i <- 1..16, do: remote_write_body(i, env_int("PULSO_LOAD_RW_SAMPLES", 2000))
    loki_body = loki_body(100)
    trace_body = trace_body(50)

    memory = Task.async(fn -> sample_memory(deadline, 0) end)
    stats = ctx.stats

    :telemetry.attach(
      {__MODULE__, self()},
      [:pulso, :operation, :stop],
      fn _event, measurements, metadata, _ ->
        :ets.insert(stats, {:op, metadata.operation, metadata.purpose, measurements.duration})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach({__MODULE__, self()}) end)
    cpu_start = :scheduler.sample_all()

    flood =
      for shard <- 1..shards do
        Task.async(fn -> flood(ctx, results, deadline, Enum.at(rw_bodies, rem(shard, 16))) end)
      end

    loki = Task.async(fn -> open_loop(ctx, results, deadline, :loki, env_int("PULSO_LOAD_LOKI_RPS", 4), loki_body) end)

    traces =
      Task.async(fn -> open_loop(ctx, results, deadline, :traces, env_int("PULSO_LOAD_TRACE_RPS", 2), trace_body) end)

    trimmer = Task.async(fn -> trim_loop(ctx.trim, deadline) end)
    started = System.monotonic_time(:millisecond)
    Task.await_many([loki, traces | flood], :infinity)
    elapsed = (System.monotonic_time(:millisecond) - started) / 1000
    Task.await(trimmer, :infinity)
    peak = Task.await(memory, :infinity)
    cpu = :scheduler.utilization(cpu_start, :scheduler.sample_all())
    {:total, total, _} = List.keyfind(cpu, :total, 0)

    report = report(results, ctx, elapsed, peak, total)
    IO.puts(report.text)

    assert report.accepted.loki > 0
    assert report.accepted.traces > 0
    assert report.dropped.loki == 0, "Loki pushes dropped after retries: #{report.dropped.loki}"
    assert report.dropped.traces == 0, "trace exports dropped after retries: #{report.dropped.traces}"
  end

  # Alloy's remote-write shard: send, back off after a 429, repeat.
  defp flood(ctx, results, deadline, body) do
    if System.monotonic_time(:millisecond) < deadline do
      {status, ms} = post(ctx.api <> "/api/v1/write", body, remote_write_headers())
      :ets.insert(results, {:remote_write, :attempt, status, ms})
      if status == 429, do: Process.sleep(env_int("PULSO_LOAD_RW_BACKOFF_MS", 200))
      flood(ctx, results, deadline, body)
    else
      :ok
    end
  end

  # Fixed arrival rate; each push retries a 429 with exponential backoff, as
  # loki.write and the OTLP exporter do, and counts as dropped when it gives up.
  defp open_loop(ctx, results, deadline, signal, rps, body) do
    interval = div(1000, rps)
    start = System.monotonic_time(:millisecond)

    Stream.iterate(0, &(&1 + 1))
    |> Stream.map(fn n -> start + n * interval end)
    |> Stream.take_while(&(&1 < deadline))
    |> Enum.map(fn at ->
      wait = at - System.monotonic_time(:millisecond)
      if wait > 0, do: Process.sleep(wait)
      Task.async(fn -> deliver(ctx, results, signal, body, 500, 4) end)
    end)
    |> Task.await_many(:infinity)
  end

  defp deliver(_ctx, results, signal, _body, _backoff, 0), do: :ets.insert(results, {signal, :dropped})

  defp deliver(ctx, results, signal, body, backoff, tries) do
    {path, headers} = target(signal)
    {status, ms} = post(ctx.api <> path, body, headers)
    :ets.insert(results, {signal, :attempt, status, ms})

    if status in [429, 503] do
      Process.sleep(backoff)
      deliver(ctx, results, signal, body, backoff * 2, tries - 1)
    end
  end

  defp target(:loki), do: {"/loki/api/v1/push", [{"content-type", "application/json"}]}
  defp target(:traces), do: {"/v1/traces", [{"content-type", "application/json"}]}

  defp post(url, body, headers) do
    started = System.monotonic_time(:microsecond)

    status =
      case Req.post(url, body: body, headers: headers, retry: false, finch: [name: LoadFinch], receive_timeout: 60_000) do
        {:ok, response} -> response.status
        {:error, _} -> :transport_error
      end

    {status, (System.monotonic_time(:microsecond) - started) / 1000}
  end

  defp trim_loop(trim, deadline) do
    if System.monotonic_time(:millisecond) < deadline do
      trim.()
      Process.sleep(200)
      trim_loop(trim, deadline)
    end
  end

  defp sample_memory(deadline, peak) do
    peak = max(peak, :erlang.memory(:total))

    if System.monotonic_time(:millisecond) < deadline do
      Process.sleep(250)
      sample_memory(deadline, peak)
    else
      peak
    end
  end

  defp report(results, ctx, elapsed, peak, cpu) do
    rows = :ets.tab2list(results)
    s3 = :ets.tab2list(ctx.stats)

    per_signal =
      for signal <- [:remote_write, :loki, :traces], into: %{} do
        attempts = for {^signal, :attempt, status, ms} <- rows, do: {status, ms}
        ok = for {status, ms} <- attempts, is_integer(status) and status < 300, do: ms
        statuses = attempts |> Enum.frequencies_by(&elem(&1, 0)) |> Enum.sort()
        dropped = Enum.count(rows, &match?({^signal, :dropped}, &1))
        {signal, %{ok: length(ok), statuses: statuses, p50: pct(ok, 0.5), p90: pct(ok, 0.9), dropped: dropped}}
      end

    puts = for {:s3, "PUT", kind, bytes} <- s3, do: {kind, bytes}
    segment_puts = Enum.count(puts, &(elem(&1, 0) == :object))
    manifest_puts = Enum.count(puts, &(elem(&1, 0) == :manifest))
    manifest_bytes = puts |> Enum.filter(&(elem(&1, 0) == :manifest)) |> Enum.map(&elem(&1, 1)) |> Enum.max(fn -> 0 end)
    gets = Enum.count(s3, &match?({:s3, "GET", _, _}, &1))

    operations =
      for({:op, operation, purpose, duration} <- s3, do: {{operation, purpose}, duration})
      |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
      |> Enum.sort()
      |> Enum.map(fn {{operation, purpose}, durations} ->
        mean = durations |> Enum.sum() |> div(length(durations)) |> System.convert_time_unit(:native, :millisecond)
        "    #{operation}/#{purpose}: #{length(durations)} calls, mean #{mean} ms"
      end)

    lines =
      for {signal, row} <- per_signal do
        "  #{signal}: accepted #{row.ok} (#{Float.round(row.ok / elapsed, 1)}/s) " <>
          "p50 #{row.p50} ms p90 #{row.p90} ms statuses #{inspect(row.statuses)} dropped #{row.dropped}"
      end

    {low, high} = ctx.latency

    text =
      [
        "",
        "ingest load: #{Float.round(elapsed, 1)} s, S3 latency #{low}..#{high} ms, " <>
          "max_concurrent #{env_int("PULSO_LOAD_MAX_CONCURRENT", 16)}, " <>
          "flush #{env_int("PULSO_LOAD_FLUSH_INTERVAL_MS", 50)} ms",
        lines,
        "  total accepted #{Enum.sum(Enum.map(per_signal, fn {_, row} -> row.ok end)) |> Kernel./(elapsed) |> Float.round(1)} req/s",
        "  S3: #{segment_puts} segment PUTs, #{manifest_puts} manifest PUTs (max #{manifest_bytes} B), #{gets} GETs",
        "  object store operations:",
        operations,
        "  peak VM memory #{div(peak, 1024 * 1024)} MiB, scheduler utilization #{Float.round(cpu * 100, 1)}%"
      ]
      |> List.flatten()
      |> Enum.join("\n")

    %{
      text: text,
      accepted: Map.new(per_signal, fn {signal, row} -> {signal, row.ok} end),
      dropped: Map.new(per_signal, fn {signal, row} -> {signal, row.dropped} end)
    }
  end

  defp pct([], _), do: nil

  defp pct(values, q) do
    sorted = Enum.sort(values)
    sorted |> Enum.at(min(length(sorted) - 1, trunc(q * length(sorted)))) |> round()
  end

  # ---- payloads -----------------------------------------------------------

  defp remote_write_headers do
    [
      {"content-type", "application/x-protobuf"},
      {"content-encoding", "snappy"},
      {"x-prometheus-remote-write-version", "0.1.0"}
    ]
  end

  # One sample per series, like a replaying Alloy shard (max_samples_per_send 2000).
  defp remote_write_body(variant, samples) do
    now = System.system_time(:millisecond)

    series =
      for i <- 1..samples do
        labels = [
          {"__name__", "load_metric_#{rem(i, 50)}"},
          {"instance", "pod-#{rem(i, 37)}"},
          {"job", "load"},
          {"series", Integer.to_string(i)},
          {"variant", Integer.to_string(variant)}
        ]

        labels_bytes = Enum.map_join(labels, "", fn {n, v} -> field(1, field(1, n) <> field(2, v)) end)
        sample = tag(1, 1) <> <<i * 1.0::little-float-64>> <> tag(2, 0) <> varint(now - variant * 1000)
        field(1, labels_bytes <> field(2, sample))
      end

    {:ok, compressed} = :snappyer.compress(IO.iodata_to_binary(series))
    compressed
  end

  defp loki_body(lines) do
    now = System.system_time(:nanosecond)
    values = for i <- 1..lines, do: [Integer.to_string(now + i), "level=info msg=\"request #{i}\" duration=12ms"]

    Pulso.JSON.encode!(%{
      "streams" => [%{"stream" => %{"service_name" => "load", "env" => "staging"}, "values" => values}]
    })
  end

  defp trace_body(spans) do
    now = System.system_time(:nanosecond)

    spans =
      for i <- 1..spans do
        span = OTLPTraceFixture.span(now + i)
        id = i |> Integer.to_string(16) |> String.pad_leading(16, "0") |> String.downcase()
        %{span | "spanId" => id}
      end

    Pulso.JSON.encode!(OTLPTraceFixture.payload(spans))
  end

  defp varint(v) when v < 128, do: <<v>>
  defp varint(v), do: <<(v &&& 0x7F) ||| 0x80>> <> varint(v >>> 7)
  defp tag(field, wire), do: varint(field <<< 3 ||| wire)
  defp field(number, bytes), do: tag(number, 2) <> varint(byte_size(bytes)) <> bytes
end
