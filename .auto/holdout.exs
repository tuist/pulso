System.put_env("CAPACITY_BENCH_SKIP", "1")
Code.require_file(".auto/workload.exs")

defmodule HoldoutStorage do
  @behaviour Pulso.Storage

  alias Pulso.Storage.SortOrder

  def append(_, _, _, _), do: {:error, :read_only}

  def query(signal, _tenant, opts) do
    {nif, metrics, logs} = :persistent_term.get(__MODULE__)

    result =
      case signal do
        :metrics ->
          nif.decode_metric_segment_parquet(metrics, opts[:start_ts], opts[:end_ts], Keyword.get(opts, :matchers, []))

        :logs ->
          nif.decode_log_segment_parquet(logs, opts[:start_ts], opts[:end_ts], opts[:service], [], [])
      end

    with {:ok, rows} <- result do
      sorted = SortOrder.sort(rows, signal)
      {:ok, Enum.take(sorted, Keyword.get(opts, :limit, 5000))}
    end
  end
end

defmodule HoldoutBench do
  alias Pulso.AutoReferenceNIF, as: Ref
  alias Pulso.Codec.NIF
  alias Pulso.Record.Log
  alias Pulso.Storage.S3

  def median(values), do: values |> Enum.sort() |> Enum.at(div(length(values), 2))

  def writer(name, samples) do
    candidate = fn ->
      {:ok, blob, _, _, _} = NIF.encode_metric_segment_parquet(samples)
      blob
    end

    reference = fn ->
      {:ok, blob, _, _, _} = Ref.encode_metric_segment_parquet(samples)
      blob
    end

    if CapacityBench.canonical("metric_write", candidate.()) != CapacityBench.canonical("metric_write", reference.()),
      do: raise("holdout output mismatch")

    ratios =
      for round <- 1..5 do
        if rem(round, 2) == 1 do
          c = CapacityBench.rate(candidate)
          c / CapacityBench.rate(reference)
        else
          r = CapacityBench.rate(reference)
          CapacityBench.rate(candidate) / r
        end
      end

    IO.puts("METRIC holdout_#{name}_writer_ratio=#{median(ratios)}")
  end

  def request(url, tool) do
    params = %{
      "name" => tool,
      "arguments" => %{"tenant" => "holdout", "limit" => 2000},
      "_meta" => %{
        "io.modelcontextprotocol/protocolVersion" => "2026-07-28",
        "io.modelcontextprotocol/clientCapabilities" => %{}
      }
    }

    body = JSON.encode!(%{"jsonrpc" => "2.0", "id" => 1, "method" => "tools/call", "params" => params})

    Req.new(
      url: url <> "/mcp",
      method: :post,
      body: body,
      decode_body: false,
      retry: false,
      headers: [
        {"content-type", "application/json"},
        {"mcp-protocol-version", "2026-07-28"},
        {"mcp-method", "tools/call"},
        {"mcp-name", tool}
      ]
    )
  end

  def http_rate(req) do
    {us, results} =
      :timer.tc(fn ->
        for _ <- 1..120 do
          %{status: 200, body: body} = Req.request!(req)
          response = JSON.decode!(body)
          if response["result"]["isError"] == true, do: raise("HTTP tool failed: #{body}")
          # Validate result rows, not merely successful status codes.
          [content] = response["result"]["content"]
          if length(JSON.decode!(content["text"])) != 2000, do: raise("incomplete HTTP response")
          byte_size(body)
        end
      end)

    {length(results) * 1_000_000 / us, hd(results)}
  end

  def http do
    samples = CapacityBench.metrics(40, 50)

    logs =
      for i <- 1..2000 do
        %Log{
          timestamp_ns: i,
          service: "svc-#{rem(i, 5)}",
          severity_text: "INFO",
          body: "request #{i} " <> String.duplicate("a", 120),
          resource: %{"service.name" => "svc-#{rem(i, 5)}", "region" => "west"}
        }
      end

    {:ok, metric_blob, _, _, _} = NIF.encode_metric_segment_parquet(samples)
    {:ok, log_blob, _, _} = S3.encode_segment(:logs, logs)
    Application.put_env(:pulso, Pulso.Storage, adapter: HoldoutStorage)
    {:ok, server} = Bandit.start_link(plug: PulsoWeb.Endpoint, ip: {127, 0, 0, 1}, port: 0)
    {:ok, {_, port}} = ThousandIsland.listener_info(server)
    url = "http://127.0.0.1:#{port}"

    try do
      for tool <- ["query_metrics", "query_logs"] do
        req = request(url, tool)
        set = fn nif -> :persistent_term.put(HoldoutStorage, {nif, metric_blob, log_blob}) end
        set.(NIF)
        http_rate(req)
        set.(Ref)
        http_rate(req)

        runs =
          for round <- 1..5 do
            {c, r, bytes} =
              if rem(round, 2) == 1 do
                set.(NIF)
                {c, bytes} = http_rate(req)
                set.(Ref)
                {r, _} = http_rate(req)
                {c, r, bytes}
              else
                set.(Ref)
                {r, _} = http_rate(req)
                set.(NIF)
                {c, bytes} = http_rate(req)
                {c, r, bytes}
              end

            {c, c / r, bytes}
          end

        IO.puts("METRIC holdout_http_#{tool}_rps=#{median(Enum.map(runs, &elem(&1, 0)))}")
        IO.puts("METRIC holdout_http_#{tool}_ratio=#{median(Enum.map(runs, &elem(&1, 1)))}")
        IO.puts("HTTP #{tool} bytes=#{elem(hd(runs), 2)} concurrency=1 admission=unchanged")
      end
    after
      Supervisor.stop(server)
      :persistent_term.erase(HoldoutStorage)
    end
  end

  def run do
    writer("unique", CapacityBench.metrics(2000, 1))

    writer(
      "long_labels",
      Enum.map(CapacityBench.metrics(60, 50), fn s ->
        %{s | labels: Map.put(s.labels, "annotation", String.duplicate("label", 100))}
      end)
    )

    :rand.seed(:exsss, {51, 17, 901})
    writer("shuffled", Enum.shuffle(CapacityBench.metrics(80, 40)))
    http()
  end
end

HoldoutBench.run()
