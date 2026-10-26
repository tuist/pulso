defmodule Pulso.Storage.S3.TracesTest do
  use Pulso.Test.Case, async: true

  alias Pulso.MCP.Tools
  alias Pulso.OTLP.Traces
  alias Pulso.Record.SpanKey
  alias Pulso.Runtime
  alias Pulso.Runtime.Task
  alias Pulso.Storage.S3
  alias Pulso.Storage.S3.Manifest
  alias Pulso.Storage.S3.ManifestSupervision
  alias Pulso.Storage.S3.TraceSummary
  alias Pulso.Test.CompactionStore
  alias Pulso.Test.OTLPTraceFixture, as: Fixture
  alias Pulso.Traces.Tempo
  alias Pulso.Traces.TempoSearch

  setup do
    agent =
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

    server = start_supervised!({Bandit, plug: {CompactionStore, agent: agent}, ip: {127, 0, 0, 1}, port: 0})
    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)

    config = %{
      bucket: "pulso",
      endpoint: "http://127.0.0.1:#{port}",
      region: "us-east-1",
      access_key_id: "test-#{System.unique_integer([:positive])}",
      secret_access_key: "test",
      allow_http: true,
      refresh_stale_ms: 0
    }

    Runtime.put_env(:pulso, S3, config)
    Runtime.put_env(:pulso, Pulso.Storage, adapter: S3)

    start_supervised!(ManifestSupervision)
    tasks = start_supervised!(Task.Supervisor)
    tenant = "traces-#{System.unique_integer([:positive])}"
    {:ok, spans, 0} = Traces.decode(Fixture.payload([Fixture.span(10), Fixture.span(20)]))
    %{agent: agent, tenant: tenant, spans: spans, tasks: tasks}
  end

  @tag timeout: 120_000
  test "MCP limited searches retain only global top-k across 150k overlapping spans", ctx do
    [base | _] = ctx.spans
    # Rich tags duplicate projected attributes. Keep explicit headroom under
    # the unchanged 16 MiB full-key/result budgets for 5,000 surviving rows.
    attrs = Map.new(1..12, &{"attr.#{&1}", "v#{&1}"})
    typed = attrs |> Map.new(fn {key, value} -> {key, %{"stringValue" => value}} end) |> Fixture.pairs()
    otlp = %{base.otlp | "attributes" => typed, "events" => [], "links" => []}
    assert SpanKey.size(%{base | attributes: attrs, events: [], links: [], otlp: otlp}) * 5000 < 16_777_216

    for segment <- 1..30 do
      records =
        for row <- 1..5000 do
          %{
            base
            | timestamp_ns: 1_700_000_000_000_000_000 + row * 1000 + segment,
              end_timestamp_ns: 1_700_000_000_000_001_000 + row * 1000 + segment,
              trace_id: Base.encode16(<<segment::64, row::64>>, case: :lower),
              attributes: attrs,
              events: [],
              links: [],
              otlp: otlp
          }
        end

      assert :ok = S3.append(:traces, ctx.tenant, records)
    end

    assert {:ok, [%{"text" => text}]} =
             Tools.call("query_traces", %{
               "tenant" => ctx.tenant,
               "service" => "tuist-server",
               "limit" => 10,
               "fields" => ["timestamp_ns", "trace_id", "duration_ns"]
             })

    rows = Pulso.JSON.decode!(text)
    assert Enum.map(rows, & &1["timestamp_ns"]) == Enum.map(30..21//-1, &(1_700_000_000_005_000_000 + &1))
    assert Enum.all?(rows, &(&1["duration_ns"] == 1000))

    assert {:ok, [%{"text" => large}]} =
             Tools.call("query_traces", %{"tenant" => ctx.tenant, "limit" => 5000, "fields" => ["timestamp_ns"]})

    expected = for segment <- 1..30, row <- 1..5000, do: 1_700_000_000_000_000_000 + row * 1000 + segment
    assert Enum.map(Pulso.JSON.decode!(large), & &1["timestamp_ns"]) == expected |> Enum.sort(:desc) |> Enum.take(5000)
    # The full-metadata response stays native too, even with a deliberately
    # small BEAM heap. The off-heap result has its own native byte ceiling.
    Runtime.put_env(:pulso, Pulso.QueryRunner, max_heap_words: 200_000)
    assert {:ok, [%{"text" => full}]} = Tools.call("query_traces", %{"tenant" => ctx.tenant, "limit" => 5000})
    assert length(Pulso.JSON.decode!(full)) == 5000
    assert {:error, :query_sample_limit} = Tools.call("query_traces", %{"tenant" => ctx.tenant})
  end

  @tag timeout: 120_000
  test "cold-manifest trace lookup prunes over 1024 segments without partial scans", ctx do
    [base | _] = ctx.spans

    # This tests cold pruning, not 1030 serial CAS publications. Seed actual
    # encoded objects and a coherent owned authority after one public append.
    assert :ok = S3.append(:traces, ctx.tenant, [%{base | trace_id: Base.encode16(<<1::128>>, case: :lower)}])
    key = Manifest.manifest_key(ctx.tenant, "traces")
    {_, body} = Agent.get(ctx.agent, &Map.fetch!(&1.objects, key))
    {:ok, root} = Manifest.decode(body)
    template = hd(root.segments)

    entries =
      for n <- 1..1030 do
        record = %{base | trace_id: Base.encode16(<<n::128>>, case: :lower)}
        {:ok, blob, _, _} = S3.encode_segment(:traces, [record])

        segment = %{
          template
          | key: String.replace_suffix(template.key, ".parquet", "-#{n}.parquet"),
            byte_size: byte_size(blob),
            trace_summary: TraceSummary.build([record])
        }

        {segment, blob}
      end

    Agent.update(ctx.agent, fn state ->
      objects =
        Enum.reduce(entries, state.objects, fn {segment, blob}, objects ->
          Map.put(objects, segment.key, {"fixture", blob})
        end)

      body = %{root | segments: Enum.map(entries, &elem(&1, 0))} |> Manifest.encode() |> IO.iodata_to_binary()
      %{state | objects: Map.put(objects, key, {"seeded-authority", body})}
    end)

    assert :ok = stop_supervised(ManifestSupervision)
    start_supervised!(ManifestSupervision)
    id = Base.encode16(<<1::128>>, case: :lower)
    assert {:ok, [%{trace_id: ^id}]} = S3.query(:traces, ctx.tenant, trace_id: id, max_scan_segments: 1)
    assert {:ok, [%{"text" => text}]} = Tools.call("query_traces", %{"tenant" => ctx.tenant, "trace_id" => id})
    assert [%{"trace_id" => ^id}] = Pulso.JSON.decode!(text)
    assert {:error, :query_scan_limit} = S3.query(:traces, ctx.tenant, max_scan_segments: 1024)
    # Same-time old spans cannot be skipped: metadata ties may outrank a hit.
    assert {:error, :query_scan_limit} =
             S3.query(:traces, ctx.tenant, service: "tuist-server", limit: 1, max_scan_segments: 1)

    latest = %{base | timestamp_ns: 100, end_timestamp_ns: 1100}
    assert :ok = S3.append(:traces, ctx.tenant, [latest])
    assert {:ok, [^latest]} = S3.query(:traces, ctx.tenant, service: "tuist-server", limit: 1, max_scan_segments: 1)

    assert {:error, :query_scan_limit} =
             S3.query(:traces, ctx.tenant, service: "tuist-server", limit: 1, max_scan_rows: 0)
  end

  test "trace search scans every object before limiting traces; one hot trace cannot hide another", ctx do
    [base | _] = ctx.spans
    a = %{base | trace_id: Base.encode16(<<1::128>>, case: :lower), timestamp_ns: 1000, end_timestamp_ns: 2000}
    b = %{base | trace_id: Base.encode16(<<2::128>>, case: :lower), timestamp_ns: 100, end_timestamp_ns: 1100}
    for count <- [2000, 2000, 1001], do: assert(:ok = S3.append(:traces, ctx.tenant, List.duplicate(a, count)))
    assert :ok = S3.append(:traces, ctx.tenant, [b])

    resource =
      Enum.map(a.otlp["resource"], fn pair ->
        if pair["key"] == "service.namespace", do: %{pair | "value" => %{"stringValue" => "other"}}, else: pair
      end)

    context = %{
      a
      | timestamp_ns: 1,
        end_timestamp_ns: 1001,
        resource: Map.put(a.resource, "service.namespace", "other"),
        otlp: Map.put(a.otlp, "resource", resource)
    }

    assert :ok = S3.append(:traces, ctx.tenant, [context])

    args = %{
      "tenant" => ctx.tenant,
      "query" => ~s'{resource.service.namespace="tuist"} with (most_recent=true)',
      "start_ts_ns" => 0,
      "end_ts_ns" => 3000,
      "limit" => 2
    }

    Runtime.put_env(:pulso, Pulso.QueryRunner, max_heap_words: 200_000)
    assert {:ok, [%{"text" => text}]} = Tools.call("search_traces", args)
    result = Pulso.JSON.decode!(text)
    assert result["matched_trace_count"] == 2
    assert [row_b, row_a] = result["traces"]
    assert row_b["trace_id"] == b.trace_id
    assert row_a["trace_id"] == a.trace_id
    assert row_a["start_timestamp_ns"] == 1
    assert row_a["stored_span_count"] == 5002
    assert row_a["matching_span_count"] == 5001
    Runtime.put_env(:pulso, Pulso.QueryRunner, max_scan_segments: 1)
    assert {:error, :query_scan_limit} = Tools.call("search_traces", Map.put(args, "limit", 1))
  end

  test "search missing-object retry restarts native grouping and charges earlier work", ctx do
    [base | _] = ctx.spans
    a = %{base | trace_id: Base.encode16(<<1::128>>, case: :lower), timestamp_ns: 20, end_timestamp_ns: 1020}
    b = %{base | trace_id: Base.encode16(<<2::128>>, case: :lower), timestamp_ns: 10, end_timestamp_ns: 1010}
    assert :ok = S3.append(:traces, ctx.tenant, [a])
    assert :ok = S3.append(:traces, ctx.tenant, [b])

    key =
      Agent.get(ctx.agent, fn state ->
        Enum.find(
          Map.keys(state.objects),
          &(String.ends_with?(&1, ".parquet") and String.contains?(&1, "00000000000000000010-"))
        )
      end)

    assert is_binary(key)
    Agent.update(ctx.agent, &%{&1 | faults: %{{"GET", key} => {404, :before, 1}}})
    args = %{"tenant" => ctx.tenant, "query" => ~s'{span.request_kind="api"}', "start_ts_ns" => 0, "end_ts_ns" => 30}
    assert {:ok, [%{"text" => text}]} = Tools.call("search_traces", args)
    result = Pulso.JSON.decode!(text)
    assert result["matched_trace_count"] == 2
    assert Enum.map(result["traces"], & &1["stored_span_count"]) == [1, 1]
    Agent.update(ctx.agent, &%{&1 | faults: %{{"GET", key} => {404, :before, 1}}})
    Runtime.put_env(:pulso, Pulso.QueryRunner, max_scan_segments: 3)
    assert {:error, :query_scan_limit} = Tools.call("search_traces", args)
  end

  test "search charges actual footer rows even when a manifest undercounts them", ctx do
    assert :ok = S3.append(:traces, ctx.tenant, ctx.spans)
    key = Manifest.manifest_key(ctx.tenant, "traces")

    Agent.update(ctx.agent, fn state ->
      {etag, body} = Map.fetch!(state.objects, key)
      {:ok, root} = Manifest.decode(body)
      root = %{root | segments: Enum.map(root.segments, &%{&1 | row_count: 1})}
      body = root |> Manifest.encode() |> IO.iodata_to_binary()
      %{state | objects: Map.put(state.objects, key, {etag <> "-undercount", body})}
    end)

    Runtime.put_env(:pulso, Pulso.QueryRunner, max_scan_rows: 1)
    args = %{"tenant" => ctx.tenant, "query" => ~s'{span.request_kind="api"}', "start_ts_ns" => 0, "end_ts_ns" => 30}
    assert {:error, :query_scan_limit} = Tools.call("search_traces", args)

    assert {:error, :query_scan_limit} =
             Tools.call("get_trace", %{"tenant" => ctx.tenant, "trace_id" => hd(ctx.spans).trace_id})

    assert {:error, :query_scan_limit} = Tempo.lookup(ctx.tenant, hd(ctx.spans).trace_id, "protobuf_v2")
    Runtime.put_env(:pulso, TempoSearch, enabled: true)
    args = %{args | "query" => args["query"] <> " with (most_recent=true)", "start_ts_ns" => 1}
    assert {:error, :query_scan_limit} = Tools.call("search_trace_metadata", args)
    assert {:error, :query_scan_limit} = Tools.call("discover_trace_tags", %{"tenant" => ctx.tenant, "scope" => "span"})
    assert {:error, :query_scan_limit} = Tools.call("discover_trace_tags", %{"tenant" => ctx.tenant})

    assert {:error, :query_scan_limit} =
             Tools.call("discover_trace_tag_values", %{"tenant" => ctx.tenant, "scope" => "none", "key" => "shared"})

    assert {:error, :query_scan_limit} =
             Tools.call("discover_trace_tag_values", %{"tenant" => ctx.tenant, "scope" => "intrinsic", "key" => "name"})

    assert {:error, :query_scan_limit} =
             Tools.call("discover_trace_tags", %{
               "tenant" => ctx.tenant,
               "query" => ~s'{resource.service.name="absent"}'
             })

    assert {:error, :query_scan_limit} =
             Tools.call("discover_trace_tags", %{
               "tenant" => ctx.tenant,
               "scope" => "resource",
               "query" => ~s'{resource.service.name="absent"}'
             })

    assert {:error, :query_scan_limit} =
             Tools.call("discover_trace_tags", %{"tenant" => ctx.tenant, "scope" => "instrumentation"})

    assert {:error, :query_scan_limit} =
             Tools.call("discover_trace_tag_values", %{
               "tenant" => ctx.tenant,
               "scope" => "span",
               "key" => "request_kind",
               "query" => ~s'{resource.service.name="absent"}'
             })

    for {start, finish} <- [{1, 30}, {100, 200}],
        {tool, target} <- [
          {"discover_trace_tags", %{}},
          {"discover_trace_tags", %{"scope" => "span", "query" => ~s'{resource.service.name="absent"}'}},
          {"discover_trace_tag_values", %{"scope" => "none", "key" => "shared"}},
          {"discover_trace_tag_values",
           %{"scope" => "none", "key" => "shared", "query" => ~s'{resource.service.name="absent"}'}},
          {"discover_trace_tag_values", %{"scope" => "intrinsic", "key" => "name"}},
          {"discover_trace_tag_values", %{"scope" => "intrinsic", "key" => "rootName"}},
          {"discover_trace_tag_values", %{"scope" => "intrinsic", "key" => "trace:duration"}},
          {"discover_trace_tag_values",
           %{"scope" => "intrinsic", "key" => "trace:duration", "query" => ~s'{resource.service.name="absent"}'}},
          {"discover_trace_tag_values",
           %{"scope" => "intrinsic", "key" => "trace:rootService", "query" => ~s'{resource.service.name="absent"}'}},
          {"discover_trace_tag_values", %{"scope" => "intrinsic", "key" => "event:timeSinceStart"}},
          {"discover_trace_tag_values",
           %{"scope" => "intrinsic", "key" => "event:timeSinceStart", "query" => ~s'{resource.service.name="absent"}'}}
        ] do
      args = Map.merge(target, %{"tenant" => ctx.tenant, "start_ts_ns" => start, "end_ts_ns" => finish})
      assert {:error, :query_scan_limit} = Tools.call(tool, args)
    end

    assert Agent.get(ctx.agent, fn state -> Enum.any?(state.reads, &String.ends_with?(&1, ".parquet")) end)
  end

  test "filtered discovery refuses corrupt nonmatching stored data instead of publishing an empty set", ctx do
    assert :ok = S3.append(:traces, ctx.tenant, ctx.spans)
    blob = File.read!(Path.expand("../../../fixtures/traces/trace-invalid-version.parquet", __DIR__))

    Agent.update(ctx.agent, fn state ->
      objects =
        Map.new(state.objects, fn {key, value} ->
          if String.ends_with?(key, ".parquet"), do: {key, {"corrupt-fixture", blob}}, else: {key, value}
        end)

      %{state | objects: objects}
    end)

    Runtime.put_env(:pulso, TempoSearch, enabled: true)

    args = %{
      "tenant" => ctx.tenant,
      "scope" => "span",
      "key" => "retries",
      "query" => ~s'{resource.service.name="absent"}'
    }

    assert {:error, {:decode_failed, _}} = Tools.call("discover_trace_tag_values", args)
  end

  test "Tempo captured-input budget is clamped before reads and lower budgets survive lying sizes", ctx do
    assert :ok = S3.append(:traces, ctx.tenant, ctx.spans)
    key = Manifest.manifest_key(ctx.tenant, "traces")
    {:ok, predicates} = TempoSearch.parse(~s'{span.request_kind="api"} with (most_recent=true)')

    for {declared, allowed} <- [{33_554_433, 134_217_728}, {1, 1}] do
      Agent.update(ctx.agent, fn state ->
        {etag, body} = Map.fetch!(state.objects, key)
        {:ok, root} = Manifest.decode(body)
        root = %{root | segments: Enum.map(root.segments, &%{&1 | byte_size: declared})}
        body = root |> Manifest.encode() |> IO.iodata_to_binary()
        %{state | objects: Map.put(state.objects, key, {etag <> "-sized", body}), reads: []}
      end)

      assert :ok = stop_supervised(ManifestSupervision)
      start_supervised!(ManifestSupervision)

      assert {:error, :query_scan_limit} =
               S3.tempo_search_json(ctx.tenant, predicates,
                 trace_start: 1,
                 trace_end: 30,
                 limit: 100,
                 spss: 3,
                 max_scan_bytes: allowed
               )

      parquet_read? = Agent.get(ctx.agent, &Enum.any?(&1.reads, fn key -> String.ends_with?(key, ".parquet") end))
      assert parquet_read? == (declared == 1)
    end
  end

  test "legacy entries recover row counts from the footer before charging scan budgets", ctx do
    assert :ok = S3.append(:traces, ctx.tenant, ctx.spans)
    key = Manifest.manifest_key(ctx.tenant, "traces")

    Agent.update(ctx.agent, fn state ->
      {etag, body} = Map.fetch!(state.objects, key)
      {:ok, manifest} = Manifest.decode(body)
      segments = Enum.map(manifest.segments, &%{&1 | row_count: nil, trace_summary: nil})
      body = %{manifest | segments: segments} |> Manifest.encode() |> IO.iodata_to_binary()
      %{state | objects: Map.put(state.objects, key, {etag, body})}
    end)

    assert :ok = stop_supervised(ManifestSupervision)
    start_supervised!(ManifestSupervision)
    assert {:error, :query_scan_limit} = S3.query(:traces, ctx.tenant, limit: 1, max_scan_rows: 1)
    assert {:ok, [%{timestamp_ns: 20}]} = S3.query(:traces, ctx.tenant, limit: 1, max_scan_rows: 2)
  end

  test "ack follows segment PUT and manifest CAS, retries deduplicate, and filters isolate signals", ctx do
    manifest_key = Manifest.manifest_key(ctx.tenant, "traces")
    owner = self()
    Agent.update(ctx.agent, &%{&1 | barriers: %{{"PUT", manifest_key} => owner}})

    task =
      Task.Supervisor.async_nolink(ctx.tasks, fn ->
        S3.append(:traces, ctx.tenant, ctx.spans, idempotency_key: "retry")
      end)

    assert_receive {:storage_barrier, writer, "PUT", ^manifest_key}, 30_000
    assert Task.yield(task, 0) == nil
    Agent.update(ctx.agent, &%{&1 | barriers: %{}})
    send(writer, {:release_storage, manifest_key})
    assert Task.await(task, 10_000) == :ok
    assert :ok = S3.append(:traces, ctx.tenant, ctx.spans, idempotency_key: "retry")
    assert {:ok, rows} = S3.query(:traces, ctx.tenant, trace_id: Fixture.trace_id())
    assert Enum.map(rows, & &1.timestamp_ns) == [20, 10]

    assert {:ok, [%{timestamp_ns: 10}]} =
             S3.query(:traces, ctx.tenant,
               start_ts: 10,
               end_ts: 10,
               service: "tuist-server",
               attributes: %{"metrics.skip" => false}
             )

    assert {:ok, []} = S3.query(:traces, ctx.tenant, service: "other")
    assert {:ok, []} = S3.query(:traces, "other", [])
    assert {:ok, []} = S3.query(:logs, ctx.tenant, [])
    assert {:ok, []} = S3.query(:metrics, ctx.tenant, [])
    assert {:error, :query_scan_limit} = S3.query(:traces, ctx.tenant, max_scan_rows: 1)
    assert {:error, :query_sample_limit} = S3.query(:traces, ctx.tenant, max_records: 1)
  end
end
