defmodule Pulso.Storage.S3.TraceCompactorTest do
  use Pulso.Test.Case, async: true

  alias Pulso.ObjectStore
  alias Pulso.OTLP.Traces
  alias Pulso.Runtime
  alias Pulso.Storage.S3
  alias Pulso.Storage.S3.CompactionOwnership
  alias Pulso.Storage.S3.CompactionTasks
  alias Pulso.Storage.S3.CompactionWorker
  alias Pulso.Storage.S3.Manifest
  alias Pulso.Storage.S3.ManifestSupervision
  alias Pulso.Storage.S3.Retention
  alias Pulso.Storage.S3.TraceCompactor
  alias Pulso.Test.CompactionStore
  alias Pulso.Test.OTLPTraceFixture, as: Fixture

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
    {:ok, {_, port}} = ThousandIsland.listener_info(server)

    config = %{
      bucket: "pulso",
      endpoint: "http://127.0.0.1:#{port}",
      region: "us-east-1",
      access_key_id: "trace-#{System.unique_integer([:positive])}",
      secret_access_key: "test",
      allow_http: true,
      refresh_stale_ms: 0
    }

    Runtime.put_env(:pulso, S3, config)
    start_supervised!(ManifestSupervision)
    {:ok, [span], 0} = Traces.decode(Fixture.payload())
    %{agent: agent, config: config, tenant: "trace-compact-#{System.unique_integer([:positive])}", span: span}
  end

  defp load(ctx) do
    assert {:ok, manifest, _} = Retention.load(ctx.tenant, "traces", ctx.config)
    manifest
  end

  test "native assembly scans the full stored trace and never returns a budget-limited prefix", ctx do
    root = %{ctx.span | span_id: String.duplicate("a", 16), parent_span_id: nil, timestamp_ns: 1}
    assert :ok = S3.append(:traces, ctx.tenant, [root])
    assert :ok = S3.append(:traces, ctx.tenant, [ctx.span, ctx.span])
    assert {:ok, json} = S3.get_trace_json(ctx.tenant, root.trace_id, [])
    result = Pulso.JSON.decode!(json)
    assert result["span_count"] == 3
    assert result["root_span_ids"] == [root.span_id]
    assert result["duplicate_span_ids"] == [ctx.span.span_id]
    assert {:error, :query_scan_limit} = S3.get_trace_json(ctx.tenant, root.trace_id, max_scan_segments: 1)
    assert {:ok, json} = S3.get_trace_json("empty-tenant", root.trace_id, [])
    assert Pulso.JSON.decode!(json)["span_count"] == 0
  end

  test "flat roots discard unused ID hashes while retaining exact-ID pruning", ctx do
    assert :ok = S3.append(:traces, ctx.tenant, [ctx.span])
    [segment] = load(ctx).segments
    assert segment.trace_summary.lookup_hashes == nil
    assert segment.trace_summary.bits != nil
    assert {:ok, [_]} = S3.query(:traces, ctx.tenant, trace_id: ctx.span.trace_id)
  end

  test "LIST-rebuilt trace objects charge recovered footer rows", ctx do
    assert {:ok, blob, min, max} = S3.encode_segment(:traces, List.duplicate(ctx.span, 20))
    key = S3.object_key(ctx.tenant, "traces", min, max, "legacy", nil)
    assert {:ok, _} = ObjectStore.put(ctx.config, key, blob)
    assert {:error, :query_scan_limit} = S3.query(:traces, ctx.tenant, limit: 1, max_scan_rows: 10)
    assert [segment] = load(ctx).segments
    assert segment.row_count == nil
  end

  test "native merge preserves duplicates, nested metadata, summaries and durable recovery", ctx do
    a = %{ctx.span | attributes: Map.put(ctx.span.attributes, "zero", -0.0)}
    b = %{ctx.span | timestamp_ns: ctx.span.timestamp_ns + 1, attributes: Map.put(ctx.span.attributes, "zero", 0.0)}
    assert :ok = S3.append(:traces, ctx.tenant, [a, b], idempotency_key: "batch")
    assert :ok = S3.append(:traces, ctx.tenant, [a])
    assert {:ok, before} = S3.query(:traces, ctx.tenant, [])
    assert {:ok, %{merged: 2, replacement: replacement}} = TraceCompactor.compact(ctx.tenant, ctx.config, grace_ms: 0)
    manifest = load(ctx)
    assert [segment] = manifest.segments
    assert segment.key == replacement
    assert segment.row_count == 3
    assert segment.trace_summary.services == [ctx.span.service]
    assert map_size(manifest.retired) == 2
    assert :ok = stop_supervised(ManifestSupervision)
    start_supervised!(ManifestSupervision)
    assert {:ok, ^before} = S3.query(:traces, ctx.tenant, [])
    assert {:ok, 2} = TraceCompactor.cleanup(ctx.tenant, ctx.config)
    assert {:ok, 0} = TraceCompactor.cleanup(ctx.tenant, ctx.config)
    assert :ok = S3.append(:traces, ctx.tenant, [a, b], idempotency_key: "batch")
    assert {:ok, ^before} = S3.query(:traces, ctx.tenant, [])
    assert {:ok, 1} = TraceCompactor.cleanup(ctx.tenant, ctx.config)
  end

  test "lost conditional publication response is recognized without losing the replacement", ctx do
    for _ <- 1..2, do: assert(:ok = S3.append(:traces, ctx.tenant, [ctx.span]))
    key = Manifest.manifest_key(ctx.tenant, "traces")
    Agent.update(ctx.agent, &%{&1 | faults: %{{"PUT", key} => {500, :after, 1}}})
    assert {:ok, %{merged: 2}} = TraceCompactor.compact(ctx.tenant, ctx.config)
    assert [_] = load(ctx).segments
    assert {:ok, rows} = S3.query(:traces, ctx.tenant, [])
    assert length(rows) == 2
  end

  test "native payload ceiling refuses publication instead of dropping spans", ctx do
    row = %{ctx.span | name: :binary.copy(<<0>>, 2 * 1024 * 1024)}
    for _ <- 1..2, do: assert(:ok = S3.append(:traces, ctx.tenant, [row]))

    assert {:error, :payload_too_large} =
             TraceCompactor.compact(ctx.tenant, ctx.config,
               small_segment_bytes: 32 * 1024 * 1024,
               max_input_bytes: 64 * 1024 * 1024
             )

    assert length(load(ctx).segments) == 2
    assert load(ctx).retired == %{}
  end

  test "over-budget native merges shrink original source sets before publication", ctx do
    row = %{ctx.span | name: :binary.copy(<<0>>, 1024 * 1024)}
    for _ <- 1..4, do: assert(:ok = S3.append(:traces, ctx.tenant, [row]))

    assert {:ok, %{merged: 2}} =
             TraceCompactor.compact(ctx.tenant, ctx.config,
               small_segment_bytes: 32 * 1024 * 1024,
               max_input_bytes: 64 * 1024 * 1024
             )

    manifest = load(ctx)
    assert length(manifest.segments) == 3
    assert Enum.sum(Enum.map(manifest.segments, & &1.row_count)) == 4
    assert map_size(manifest.retired) == 2
  end

  test "trace-only workers join their own eligibility group and maintain idle durable tenants", ctx do
    for _ <- 1..2, do: assert(:ok = S3.append(:traces, ctx.tenant, [ctx.span]))

    config =
      Map.merge(ctx.config, %{compaction_enabled: false, trace_compaction_enabled: true, compaction_interval_ms: 60_000})

    start_supervised!(%{id: CompactionOwnership, start: {:pg, :start_link, [CompactionOwnership.scope()]}})
    start_supervised!({Task.Supervisor, name: CompactionTasks})
    worker = start_supervised!({CompactionWorker, config})
    assert CompactionOwnership.members(config, "metrics") == []
    assert CompactionOwnership.members(config, "traces") == [node()]
    send(worker, :compact)
    _ = :sys.get_state(worker, 60_000)
    assert [_] = load(ctx).segments
  end
end
