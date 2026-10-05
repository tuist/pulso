defmodule PulsoCostStore do
  @behaviour Plug
  def init(opts), do: opts
  def call(conn, opts) do
    conn = Plug.Conn.register_before_send(conn, fn result ->
      meter = Keyword.fetch!(opts, :meter)
      list? = Map.has_key?(result.query_params, "list-type")
      billable? = result.status not in [301, 307, 400, 403, 405, 409, 411, 412, 416, 304, 500, 501]
      class = if result.method == "PUT" or list?, do: :a, else: :b
      bytes = if result.method == "GET" and result.status == 200, do: IO.iodata_length(result.resp_body), else: 0
      Agent.update(meter, fn m ->
        m |> Map.update!(class, &(&1 + if(billable?, do: 1, else: 0))) |> Map.update!(:read, &(&1 + bytes))
      end)
      result
    end)
    Pulso.Test.CompactionStore.call(conn, opts)
  end
end

defmodule PulsoCostBench do
  use ExUnit.Case, async: false
  alias Pulso.Storage.S3
  alias Pulso.Storage.S3.{ManifestCache, ManifestSupervision}
  alias Pulso.Record.{Log, MetricSample}

  defp token(i), do: :crypto.hash(:sha256, Integer.to_string(i)) |> Base.encode16(case: :lower)
  defp records(:logs, shape, n) do
    for i <- 1..n do
      id = if shape == :repeat, do: rem(i, 16), else: i
      %Log{timestamp_ns: 1_800_000_000_000_000_000 + i * 1_000_000,
        observed_timestamp_ns: 1_800_000_000_000_000_000 + i * 1_000_000 + rem(i, 100),
        service: "service-#{rem(i, 7)}", severity_text: Enum.at(["INFO", "ERROR", "DEBUG"], rem(i, 3)),
        severity_number: 9, body: "operation completed request=#{token(id)} duration=#{rem(i, 187)} peer=#{rem(i, 31)}",
        trace_id: token(i), span_id: String.slice(token(i + n), 0, 16),
        resource: %{"region" => "eu-west", "service.version" => "1.4.#{rem(i, 4)}", "host" => "host-#{rem(i, 16)}"},
        attributes: %{"context" => String.duplicate("peer=#{token(id)} ", 12), "attempt" => rem(i, 5), "ok" => rem(i, 9) != 0}}
    end
  end
  defp records(:metrics, shape, n) do
    for i <- 1..n do
      id = if shape == :repeat, do: rem(i, 32), else: i
      %MetricSample{timestamp_ns: 1_800_000_000_000_000_000 + div(i, 32) * 15_000_000_000,
        value: if(rem(i, 2) == 0, do: i / 3, else: rem(i, 17) / 13),
        labels: %{"__name__" => "metric_#{rem(i, 11)}", "job" => "job-#{rem(i, 4)}", "instance" => "host-#{id}",
          "route" => "/api/#{rem(i, 23)}", "request_id" => token(id)}}
    end
  end

  test "measured lossless storage cost matrix" do
    agent = start_supervised!({Agent, fn -> %{objects: %{}, reads: [], version: 0, hook: nil, faults: %{}, barriers: %{}, lists: 0, deletes: [], requests: []} end})
    meter = start_supervised!(Supervisor.child_spec({Agent, fn -> %{a: 0, b: 0, read: 0} end}, id: :meter))
    server = start_supervised!({Bandit, plug: {PulsoCostStore, agent: agent, meter: meter}, port: 0})
    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)
    config = %{bucket: "pulso", endpoint: "http://localhost:#{port}", region: "us-east-1", access_key_id: "test", secret_access_key: "test", allow_http: true, refresh_stale_ms: 0}
    Application.put_env(:pulso, S3, config)
    start_supervised!(ManifestSupervision)

    results = for signal <- [:logs, :metrics], shape <- [:repeat, :churn], size <- [64, 1024, 10000] do
      tenant = "cost-#{signal}-#{shape}-#{size}"
      input = records(signal, shape, size * 6)
      Agent.update(agent, &%{&1 | objects: %{}, requests: [], reads: []})
      Agent.update(meter, fn _ -> %{a: 0, b: 0, read: 0} end)
      {write_us, _} = :timer.tc(fn ->
        for batch <- Enum.chunk_every(input, size), do: assert(:ok == S3.append(signal, tenant, batch, idempotency_key: token(hd(batch).timestamp_ns)))
      end)
      write = Agent.get(meter, & &1)
      stored = Agent.get(agent, fn s -> Enum.sum(for {_, {_, body}} <- s.objects, do: byte_size(body)) end)
      # A codec roundtrip defines unavoidable normalization (e.g. calculated series_id).
      {:ok, reference_blob, _, _} = S3.encode_segment(signal, input)
      {:ok, reference} = S3.decode_segment(signal, reference_blob, nil, nil, [])
      time = Enum.at(input, div(length(input), 2)).timestamp_ns
      queries = [[], [start_ts: time], [matchers: [{if(signal == :logs, do: "service", else: "job"), :eq, "absent"}]]]
      Agent.update(meter, fn _ -> %{a: 0, b: 0, read: 0} end)
      {query_us, _} = :timer.tc(fn ->
        for cold <- [false, true], opts <- queries do
          if cold, do: ManifestCache.drop(tenant, Atom.to_string(signal))
          assert {:ok, actual} = S3.query(signal, tenant, opts)
          expected = cond do
            opts[:matchers] -> []
            opts[:start_ts] -> Enum.filter(reference, &(&1.timestamp_ns >= time))
            true -> reference
          end
          assert Enum.sort(actual) == Enum.sort(expected)
        end
      end)
      query = Agent.get(meter, & &1)
      scale = 1_000_000 / length(input)
      # Marginal steady-state cohort: one million records held for a month,
      # plus 1000 queries with the measured mix. No free-tier subtraction.
      retained = stored * scale
      a = write.a * scale + query.a * 1000 / 6
      b = write.b * scale + query.b * 1000 / 6
      cost = retained / 1_073_741_824 * 0.02 + a * 0.000005 + b * 0.0000005
      IO.puts("CASE #{tenant} usd=#{cost} bytes_per_record=#{stored / length(input)} write_a=#{write.a} query_b=#{query.b} read_bytes=#{query.read}")
      %{cost_usd_per_million: cost, retained_bytes_per_million: retained, class_a_per_million: a,
        class_b_per_1000_queries: query.b * 1000 / 6, read_bytes_per_1000_queries: query.read * 1000 / 6,
        encode_us_per_record: write_us / length(input), query_us_per_record: query_us / (length(input) * 6)}
    end
    for key <- Map.keys(hd(results)) do
      value = Enum.sum(Enum.map(results, &Map.fetch!(&1, key))) / length(results)
      IO.puts("METRIC #{key}=#{value}")
    end
  end
end
