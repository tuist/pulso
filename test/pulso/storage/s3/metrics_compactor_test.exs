defmodule Pulso.Storage.S3.MetricsCompactorTest do
  use ExUnit.Case, async: false

  alias Pulso.ObjectStore
  alias Pulso.Record.Log
  alias Pulso.Record.MetricSample
  alias Pulso.Storage.S3
  alias Pulso.Storage.S3.CompactionWorker
  alias Pulso.Storage.S3.Manifest
  alias Pulso.Storage.S3.Manifest.Segment
  alias Pulso.Storage.S3.ManifestCache
  alias Pulso.Storage.S3.ManifestSupervision
  alias Pulso.Storage.S3.MetricsCompactor
  alias Pulso.Test.CompactionStore

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

    server = start_supervised!({Bandit, plug: {CompactionStore, agent: agent}, port: 0})
    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)

    config = %{
      bucket: "pulso",
      endpoint: "http://localhost:#{port}",
      region: "us-east-1",
      access_key_id: "test",
      secret_access_key: "test",
      allow_http: true,
      refresh_stale_ms: 0
    }

    previous = Application.get_env(:pulso, S3)
    Application.put_env(:pulso, S3, config)

    on_exit(fn ->
      if previous, do: Application.put_env(:pulso, S3, previous), else: Application.delete_env(:pulso, S3)
    end)

    start_supervised!(ManifestSupervision)
    tenant = "compact-#{System.unique_integer([:positive])}"

    {:ok, _} =
      ObjectStore.put(
        config,
        Manifest.manifest_key(tenant, "metrics"),
        Manifest.encode(Manifest.new()) |> IO.iodata_to_binary()
      )

    %{agent: agent, config: config, tenant: tenant}
  end

  defp sample(ts, value, host \\ "a") do
    %MetricSample{timestamp_ns: ts, value: value, labels: %{"__name__" => "requests", "host" => host}}
  end

  defp load(ctx) do
    {:ok, _, body} = ObjectStore.get_if_none_match(ctx.config, Manifest.manifest_key(ctx.tenant, "metrics"), nil)
    {:ok, manifest} = Manifest.decode(body)
    manifest
  end

  defp reads(agent) do
    Agent.get_and_update(agent, fn state ->
      {Enum.count(state.reads, &String.ends_with?(&1, ".parquet")), %{state | reads: []}}
    end)
  end

  test "identical filtered results with 16 times fewer object reads, durable grace and ingest retry", ctx do
    for ts <- 1..16 do
      assert :ok =
               S3.append(:metrics, ctx.tenant, [sample(ts, ts / 1), sample(ts, -ts / 1, "b")], idempotency_key: "#{ts}")
    end

    queries = [[], [start_ts: 4, end_ts: 12], [limit: 7], [matchers: [{"host", :eq, "a"}]]]
    before = Enum.map(queries, &S3.query(:metrics, ctx.tenant, &1))
    assert Enum.all?(before, &match?({:ok, _}, &1))
    reads(ctx.agent)
    assert {:ok, _} = S3.query(:metrics, ctx.tenant, [])
    assert reads(ctx.agent) == 16
    snapshot = load(ctx)
    assert {:ok, %{merged: 16}} = MetricsCompactor.compact(ctx.tenant, ctx.config)
    assert {:ok, 0} = MetricsCompactor.cleanup(ctx.tenant, ctx.config)
    assert Enum.map(queries, &S3.query(:metrics, ctx.tenant, &1)) == before
    reads(ctx.agent)
    assert {:ok, _} = S3.query(:metrics, ctx.tenant, [])
    assert reads(ctx.agent) == 1
    assert :ok = S3.append(:metrics, ctx.tenant, [sample(1, 1.0), sample(1, -1.0, "b")], idempotency_key: "1")
    assert S3.query(:metrics, ctx.tenant, []) == hd(before)
    # Expire the durable deadlines, simulating a later worker after restart.
    manifest = load(ctx)

    expired = %{
      manifest
      | retired: Map.new(manifest.retired, fn {key, retirement} -> {key, %{retirement | delete_after: 0}} end)
    }

    assert {:ok, _} =
             ObjectStore.put(
               ctx.config,
               Manifest.manifest_key(ctx.tenant, "metrics"),
               Manifest.encode(expired) |> IO.iodata_to_binary()
             )

    assert {:ok, 16} = MetricsCompactor.cleanup(ctx.tenant, ctx.config)
    assert {:ok, 0} = MetricsCompactor.cleanup(ctx.tenant, ctx.config)
    ManifestCache.put(ctx.tenant, "metrics", snapshot, "stale")
    # Deliberately serve a fresh-looking obsolete snapshot; missing files restart the scan.
    Application.put_env(:pulso, S3, Map.put(ctx.config, :refresh_stale_ms, 60_000))
    assert S3.query(:metrics, ctx.tenant, []) == hd(before)
  end

  test "concurrent append survives a conditional-write conflict", ctx do
    for ts <- 1..3, do: assert(:ok = S3.append(:metrics, ctx.tenant, [sample(ts, 1.0)]))
    {:ok, payload, mn, mx} = S3.encode_segment(:metrics, [sample(4, 2.0)])
    key = S3.object_key(ctx.tenant, "metrics", mn, mx, "extra", nil)
    assert {:ok, _} = ObjectStore.put(ctx.config, key, payload)
    extra = Segment.build(key, mn, mx, 1, byte_size(payload))
    manifest_key = Manifest.manifest_key(ctx.tenant, "metrics")

    Agent.update(ctx.agent, fn state ->
      %{
        state
        | hook: fn state ->
            {_, body} = Map.fetch!(state.objects, manifest_key)
            {:ok, current} = Manifest.decode(body)
            updated = Manifest.merge(current, [extra]) |> Manifest.encode() |> IO.iodata_to_binary()
            %{state | objects: Map.put(state.objects, manifest_key, {"\"concurrent\"", updated})}
          end
      }
    end)

    assert {:ok, %{merged: 3}} = MetricsCompactor.compact(ctx.tenant, ctx.config, grace_ms: 0)
    assert {:ok, records} = S3.query(:metrics, ctx.tenant, [])
    assert Enum.map(records, & &1.timestamp_ns) == [4, 3, 2, 1]
    assert {:ok, 3} = MetricsCompactor.cleanup(ctx.tenant, ctx.config)
  end

  test "overlapping compaction cannot publish twice", ctx do
    for ts <- 1..3, do: assert(:ok = S3.append(:metrics, ctx.tenant, [sample(ts, 1.0)]))
    manifest_key = Manifest.manifest_key(ctx.tenant, "metrics")

    Agent.update(ctx.agent, fn state ->
      %{
        state
        | hook: fn state ->
            {_, body} = Map.fetch!(state.objects, manifest_key)
            {:ok, current} = Manifest.decode(body)
            [first | _] = current.segments
            replacement = %{first | key: first.key <> "-winner"}
            {:ok, updated} = Manifest.replace(current, Enum.map(current.segments, & &1.key), replacement, 0)
            body = Manifest.encode(updated) |> IO.iodata_to_binary()
            %{state | objects: Map.put(state.objects, manifest_key, {"\"winner\"", body})}
          end
      }
    end)

    assert {:error, :compaction_conflict} = MetricsCompactor.compact(ctx.tenant, ctx.config)
    assert length(load(ctx).segments) == 1
  end

  test "duplicates and conflicting same-time samples retain deterministic limited results", ctx do
    for value <- [2.0, 1.0, 2.0, 3.0] do
      assert :ok = S3.append(:metrics, ctx.tenant, [sample(1, value)])
    end

    before = S3.query(:metrics, ctx.tenant, [])
    limited = S3.query(:metrics, ctx.tenant, limit: 2)
    assert {:ok, %{merged: 4}} = MetricsCompactor.compact(ctx.tenant, ctx.config)
    assert S3.query(:metrics, ctx.tenant, []) == before
    assert S3.query(:metrics, ctx.tenant, limit: 2) == limited
  end

  test "background worker discovers tenants and compacts and cleans up", ctx do
    for ts <- 1..3, do: assert(:ok = S3.append(:metrics, ctx.tenant, [sample(ts, 1.0)]))
    worker = start_supervised!({CompactionWorker, Map.put(ctx.config, :compaction_options, grace_ms: 0)})
    send(worker, :compact)
    _ = :sys.get_state(worker)
    assert length(load(ctx).segments) == 1
    assert {:ok, records} = S3.query(:metrics, ctx.tenant, [])
    assert length(records) == 3

    assert Agent.get(ctx.agent, fn state ->
             Enum.count(state.objects, fn {key, _} -> String.ends_with?(key, ".parquet") end)
           end) == 1
  end

  test "missing input fails before publication and cleanup never removes active segments", ctx do
    for ts <- 1..2, do: assert(:ok = S3.append(:metrics, ctx.tenant, [sample(ts, 1.0)]))
    before = load(ctx)
    [first | _] = before.segments
    assert :ok = ObjectStore.delete(ctx.config, first.key)
    assert {:error, :not_found} = MetricsCompactor.compact(ctx.tenant, ctx.config, grace_ms: 0)
    assert load(ctx) == before
    assert {:ok, 0} = MetricsCompactor.cleanup(ctx.tenant, ctx.config)
    assert {:error, :not_found} = S3.query(:metrics, ctx.tenant, [])
  end

  test "recompaction preserves all samples and cleanup keeps the current replacement", ctx do
    for ts <- 1..4, do: assert(:ok = S3.append(:metrics, ctx.tenant, [sample(ts, 1.0)]))
    assert {:ok, %{merged: 4, replacement: first}} = MetricsCompactor.compact(ctx.tenant, ctx.config, grace_ms: 0)
    for ts <- 5..8, do: assert(:ok = S3.append(:metrics, ctx.tenant, [sample(ts, 1.0)]))
    before = S3.query(:metrics, ctx.tenant, [])
    assert {:ok, %{merged: 5, replacement: second}} = MetricsCompactor.compact(ctx.tenant, ctx.config, grace_ms: 0)
    assert {:ok, 9} = MetricsCompactor.cleanup(ctx.tenant, ctx.config)
    assert {:error, :not_found} = ObjectStore.get(ctx.config, first)
    assert {:ok, _} = ObjectStore.get(ctx.config, second)
    assert S3.query(:metrics, ctx.tenant, []) == before
  end

  test "realistic scrape batches across midnight preserve selective and limited queries", ctx do
    # Twelve series, fifteen-second scrapes, out-of-order delivery and an hour/day
    # boundary. No aggregation or resampling is allowed during compaction.
    start = DateTime.to_unix(~U[2026-09-30 23:56:00Z], :nanosecond)

    batches =
      for scrape <- 0..31 do
        for host <- 1..6, name <- ["http_requests_total", "process_cpu_seconds_total"] do
          %MetricSample{
            timestamp_ns: start + scrape * 15_000_000_000,
            value: scrape * 10.0 + host / 10,
            labels: %{
              "__name__" => name,
              "instance" => "app-#{host}:9090",
              "job" => "checkout",
              "region" => if(host <= 3, do: "eu-west", else: "us-east")
            }
          }
        end
      end

    for {batch, index} <- Enum.with_index(Enum.reverse(batches)) do
      assert :ok = S3.append(:metrics, ctx.tenant, batch, idempotency_key: "scrape-#{index}")
    end

    queries = [
      [],
      [limit: 25],
      [start_ts: start + 180_000_000_000, end_ts: start + 300_000_000_000],
      [matchers: [{"__name__", :eq, "http_requests_total"}, {"region", :eq, "eu-west"}]],
      [matchers: [{"instance", :re, "app-[12]:9090"}], limit: 13],
      [matchers: [{"job", :eq, "absent"}]]
    ]

    before = Enum.map(queries, &S3.query(:metrics, ctx.tenant, &1))
    assert {:ok, all} = hd(before)
    assert length(all) == 384
    reads(ctx.agent)
    assert {:ok, _} = S3.query(:metrics, ctx.tenant, [])
    assert reads(ctx.agent) == 32
    assert {:ok, %{merged: 16}} = MetricsCompactor.compact(ctx.tenant, ctx.config, grace_ms: 0)
    assert {:ok, %{merged: 16}} = MetricsCompactor.compact(ctx.tenant, ctx.config, grace_ms: 0)
    assert {:ok, %{merged: 0}} = MetricsCompactor.compact(ctx.tenant, ctx.config)
    assert Enum.map(queries, &S3.query(:metrics, ctx.tenant, &1)) == before
    reads(ctx.agent)
    assert {:ok, _} = S3.query(:metrics, ctx.tenant, [])
    assert reads(ctx.agent) == 2
    assert {:ok, 32} = MetricsCompactor.cleanup(ctx.tenant, ctx.config)
    # Actually restart the local writer/cache supervision tree, then retry an
    # acknowledged scrape. Durable retirement metadata must prevent duplicates.
    stop_supervised!(ManifestSupervision)
    start_supervised!(ManifestSupervision)
    assert :ok = S3.append(:metrics, ctx.tenant, List.last(batches), idempotency_key: "scrape-0")
    assert Enum.map(queries, &S3.query(:metrics, ctx.tenant, &1)) == before
  end

  test "failed replacement upload keeps the original query generation intact", ctx do
    for ts <- 1..3, do: assert(:ok = S3.append(:metrics, ctx.tenant, [sample(ts, 1.0)]))
    before = S3.query(:metrics, ctx.tenant, [])
    manifest = load(ctx)
    Agent.update(ctx.agent, &%{&1 | faults: %{{"PUT", :replacement} => {403, :before, 1}}})
    assert {:error, _} = MetricsCompactor.compact(ctx.tenant, ctx.config, grace_ms: 0)
    assert load(ctx) == manifest
    assert S3.query(:metrics, ctx.tenant, []) == before
    assert {:ok, 0} = MetricsCompactor.cleanup(ctx.tenant, ctx.config)
  end

  test "cleanup resumes after a deletion failure without affecting active results", ctx do
    for ts <- 1..3, do: assert(:ok = S3.append(:metrics, ctx.tenant, [sample(ts, 1.0)]))
    before = S3.query(:metrics, ctx.tenant, [])
    source = hd(load(ctx).segments).key
    assert {:ok, %{merged: 3}} = MetricsCompactor.compact(ctx.tenant, ctx.config, grace_ms: 0)
    Agent.update(ctx.agent, &%{&1 | faults: %{{"DELETE", source} => {403, :before, 1}}})
    assert {:error, _} = MetricsCompactor.cleanup(ctx.tenant, ctx.config)
    assert S3.query(:metrics, ctx.tenant, []) == before
    assert {:ok, 1} = MetricsCompactor.cleanup(ctx.tenant, ctx.config)
    assert S3.query(:metrics, ctx.tenant, []) == before
  end

  test "lost publication response keeps the acknowledged samples visible", ctx do
    for ts <- 1..3, do: assert(:ok = S3.append(:metrics, ctx.tenant, [sample(ts, 1.0)]))
    before = S3.query(:metrics, ctx.tenant, [])
    key = Manifest.manifest_key(ctx.tenant, "metrics")
    Agent.update(ctx.agent, &%{&1 | faults: %{{"PUT", key} => {500, :after, 1}}})
    # The native client retries with the original condition after a successful
    # write whose response was lost. The data must remain valid either way.
    assert {:ok, %{merged: 3, replacement: replacement}} = MetricsCompactor.compact(ctx.tenant, ctx.config, grace_ms: 0)
    assert Enum.map(load(ctx).segments, & &1.key) == [replacement]

    assert Agent.get(ctx.agent, fn state ->
             Enum.count(state.objects, fn {key, _} -> String.contains?(key, "-compact-") end)
           end) == 1

    assert S3.query(:metrics, ctx.tenant, []) == before
    assert {:ok, 3} = MetricsCompactor.cleanup(ctx.tenant, ctx.config)
    assert S3.query(:metrics, ctx.tenant, []) == before
  end

  test "background maintenance isolates tenants and leaves log segments unchanged", ctx do
    other = ctx.tenant <> "-other"

    for tenant <- [ctx.tenant, other] do
      {:ok, _} =
        ObjectStore.put(
          ctx.config,
          Manifest.manifest_key(tenant, "metrics"),
          Manifest.encode(Manifest.new()) |> IO.iodata_to_binary()
        )

      for ts <- 1..3,
          do: assert(:ok = S3.append(:metrics, tenant, [sample(ts, if(tenant == other, do: 2.0, else: 1.0))]))
    end

    assert :ok = S3.append(:logs, ctx.tenant, [%Log{timestamp_ns: 1, body: "checkout completed", service: "checkout"}])
    before = for tenant <- [ctx.tenant, other], do: S3.query(:metrics, tenant, [])
    logs = S3.query(:logs, ctx.tenant, [])
    worker = start_supervised!({CompactionWorker, Map.put(ctx.config, :compaction_options, grace_ms: 0)})
    send(worker, :compact)
    _ = :sys.get_state(worker)
    assert for(tenant <- [ctx.tenant, other], do: S3.query(:metrics, tenant, [])) == before
    assert S3.query(:logs, ctx.tenant, []) == logs
    assert length(load(ctx).segments) == 1
    assert length(load(%{ctx | tenant: other}).segments) == 1
  end

  test "two real compactors race without leaving the losing replacement behind", ctx do
    for ts <- 1..4, do: assert(:ok = S3.append(:metrics, ctx.tenant, [sample(ts, ts / 1)]))
    before = S3.query(:metrics, ctx.tenant, [])
    owner = self()
    Agent.update(ctx.agent, &%{&1 | barriers: %{{"PUT", :replacement} => owner}})
    supervisor = start_supervised!(Task.Supervisor)

    tasks =
      for _ <- 1..2,
          do: Task.Supervisor.async_nolink(supervisor, fn -> MetricsCompactor.compact(ctx.tenant, ctx.config) end)

    assert_receive {:storage_barrier, first, "PUT", first_key}, 5_000
    assert_receive {:storage_barrier, second, "PUT", second_key}, 5_000
    assert first_key != second_key
    send(first, {:release_storage, first_key})
    send(second, {:release_storage, second_key})
    results = Enum.map(tasks, &Task.await(&1, 10_000))
    assert Enum.count(results, &match?({:ok, %{merged: 4}}, &1)) == 1
    assert Enum.count(results, &(&1 == {:error, :compaction_conflict})) == 1
    assert S3.query(:metrics, ctx.tenant, []) == before

    assert Agent.get(ctx.agent, fn state ->
             Enum.count(state.objects, fn {key, _} -> String.contains?(key, "-compact-") end)
           end) == 1
  end

  test "bounded cleanup rotates past permanent failures and never repeats completed deletions", ctx do
    for ts <- 1..3, do: assert(:ok = S3.append(:metrics, ctx.tenant, [sample(ts, 1.0)]))
    failed = load(ctx).segments |> Enum.map(& &1.key) |> Enum.min()
    assert {:ok, %{merged: 3}} = MetricsCompactor.compact(ctx.tenant, ctx.config, grace_ms: 0)
    Agent.update(ctx.agent, &%{&1 | faults: %{{"DELETE", failed} => {403, :before, 100}}})

    assert {:error, {:cleanup_failed, [{^failed, _}]}} =
             MetricsCompactor.cleanup(ctx.tenant, ctx.config, max_deletions: 1)

    assert {:ok, 1} = MetricsCompactor.cleanup(ctx.tenant, ctx.config, max_deletions: 1)
    assert {:ok, 1} = MetricsCompactor.cleanup(ctx.tenant, ctx.config, max_deletions: 1)

    assert {:error, {:cleanup_failed, [{^failed, _}]}} =
             MetricsCompactor.cleanup(ctx.tenant, ctx.config, max_deletions: 1)

    assert Agent.get(ctx.agent, &length(&1.deletes)) == 2
    manifest = load(ctx)
    assert Enum.count(manifest.retired, fn {_, retirement} -> retirement.deleted? end) == 2
    assert {:ok, records} = S3.query(:metrics, ctx.tenant, [])
    assert length(records) == 3
  end

  test "an ingest retry overlapping cleanup publication schedules its re-upload for deletion", ctx do
    for ts <- 1..3, do: assert(:ok = S3.append(:metrics, ctx.tenant, [sample(ts, 1.0)], idempotency_key: "#{ts}"))
    source = hd(load(ctx).segments)
    {:ok, blob} = ObjectStore.get(ctx.config, source.key)
    before = S3.query(:metrics, ctx.tenant, [])
    assert {:ok, %{merged: 3}} = MetricsCompactor.compact(ctx.tenant, ctx.config, grace_ms: 0)
    manifest_key = Manifest.manifest_key(ctx.tenant, "metrics")

    Agent.update(ctx.agent, fn state ->
      %{
        state
        | hook: fn state ->
            {_, body} = Map.fetch!(state.objects, manifest_key)
            {:ok, current} = Manifest.decode(body)
            updated = Manifest.merge(current, [source]) |> Manifest.encode() |> IO.iodata_to_binary()

            objects =
              state.objects
              |> Map.put(source.key, {"\"retry\"", blob})
              |> Map.put(manifest_key, {"\"retry-manifest\"", updated})

            %{state | objects: objects}
          end
      }
    end)

    assert {:ok, 3} = MetricsCompactor.cleanup(ctx.tenant, ctx.config)
    refute load(ctx).retired[source.key].deleted?
    assert {:ok, ^blob} = ObjectStore.get(ctx.config, source.key)
    assert {:ok, 1} = MetricsCompactor.cleanup(ctx.tenant, ctx.config)
    assert {:error, :not_found} = ObjectStore.get(ctx.config, source.key)
    assert {:ok, 0} = MetricsCompactor.cleanup(ctx.tenant, ctx.config)
    assert S3.query(:metrics, ctx.tenant, []) == before
    assert :ok = S3.append(:metrics, ctx.tenant, [sample(3, 1.0)], idempotency_key: "3")
    assert S3.query(:metrics, ctx.tenant, []) == before
    assert {:ok, 1} = MetricsCompactor.cleanup(ctx.tenant, ctx.config)
    assert {:error, :not_found} = ObjectStore.get(ctx.config, source.key)
  end

  test "background maintenance handles a poison tenant without recursively listing objects", ctx do
    for ts <- 1..3, do: assert(:ok = S3.append(:metrics, ctx.tenant, [sample(ts, 1.0)]))
    poison = ctx.tenant <> "-poison"
    prefix = "tenants/#{poison}/v4/signal=metrics/date=2026-10-01/hour=00/"

    sources =
      for i <- 1..2 do
        key = prefix <> "bad-#{i}.parquet"
        {:ok, _} = ObjectStore.put(ctx.config, key, "corrupt")
        Segment.build(key, i, i, 1, 7)
      end

    manifest = Manifest.merge(Manifest.new(), sources)

    {:ok, etag} =
      ObjectStore.put(
        ctx.config,
        Manifest.manifest_key(poison, "metrics"),
        Manifest.encode(manifest) |> IO.iodata_to_binary()
      )

    ManifestCache.put(poison, "metrics", manifest, etag)
    lists = Agent.get(ctx.agent, & &1.lists)
    worker = start_supervised!({CompactionWorker, ctx.config})
    send(worker, :compact)
    _ = :sys.get_state(worker)
    assert length(load(ctx).segments) == 1
    assert load(%{ctx | tenant: poison}).segments == manifest.segments
    assert Agent.get(ctx.agent, & &1.lists) == lists
  end

  test "a missing compacted manifest fails closed instead of rebuilding an incomplete snapshot", ctx do
    for ts <- 1..3, do: assert(:ok = S3.append(:metrics, ctx.tenant, [sample(ts, 1.0)]))
    assert {:ok, %{merged: 3}} = MetricsCompactor.compact(ctx.tenant, ctx.config, grace_ms: 0)
    assert {:ok, 3} = MetricsCompactor.cleanup(ctx.tenant, ctx.config)
    assert :ok = ObjectStore.delete(ctx.config, Manifest.manifest_key(ctx.tenant, "metrics"))
    stop_supervised!(ManifestSupervision)
    start_supervised!(ManifestSupervision)
    assert {:error, :compacted_manifest_missing} = S3.query(:metrics, ctx.tenant, [])
    assert {:error, :compacted_manifest_missing} = S3.append(:metrics, ctx.tenant, [sample(4, 1.0)])
  end

  test "background compaction requires explicit activation after writer upgrades" do
    assert CompactionWorker.children(%{}) == []
    assert CompactionWorker.children(%{compaction_enabled: false}) == []
    config = %{compaction_enabled: true}
    assert CompactionWorker.children(config) == [{CompactionWorker, config}]
    assert {:ok, %Manifest{version: 1, retired: %{}}} = Manifest.decode(~s({"v":1,"s":[]}))
    assert {:error, :unsupported_manifest_version} = Manifest.decode(~s({"v":99,"s":[]}))
    assert {:error, :invalid_manifest} = Manifest.decode(~s({"v":2,"s":[]}))
  end

  test "cleanup during a running query restarts the whole scan without duplicate samples", ctx do
    for ts <- 1..4, do: assert(:ok = S3.append(:metrics, ctx.tenant, [sample(ts, ts / 1)]))
    before = S3.query(:metrics, ctx.tenant, [])
    first_key = hd(load(ctx).segments).key
    owner = self()
    Agent.update(ctx.agent, &%{&1 | barriers: %{{"GET", first_key} => owner}})
    supervisor = start_supervised!(Task.Supervisor)
    query = Task.Supervisor.async_nolink(supervisor, fn -> S3.query(:metrics, ctx.tenant, []) end)
    assert_receive {:storage_barrier, reader, "GET", ^first_key}, 5_000
    Agent.update(ctx.agent, &%{&1 | barriers: %{}})
    assert {:ok, %{merged: 4}} = MetricsCompactor.compact(ctx.tenant, ctx.config, grace_ms: 0)
    assert {:ok, 4} = MetricsCompactor.cleanup(ctx.tenant, ctx.config)
    send(reader, {:release_storage, first_key})
    assert Task.await(query, 10_000) == before
  end

  test "a delayed query retry cannot overwrite a cache entry containing a later acknowledged append", ctx do
    for ts <- 1..3, do: assert(:ok = S3.append(:metrics, ctx.tenant, [sample(ts, 1.0)]))
    before = S3.query(:metrics, ctx.tenant, [])
    snapshot = load(ctx)
    assert {:ok, %{merged: 3}} = MetricsCompactor.compact(ctx.tenant, ctx.config, grace_ms: 0)
    assert {:ok, 3} = MetricsCompactor.cleanup(ctx.tenant, ctx.config)
    ManifestCache.put(ctx.tenant, "metrics", snapshot, "obsolete")
    Application.put_env(:pulso, S3, Map.put(ctx.config, :refresh_stale_ms, 60_000))
    key = Manifest.manifest_key(ctx.tenant, "metrics")
    owner = self()
    Agent.update(ctx.agent, &%{&1 | barriers: %{{"GET", key} => owner}})
    supervisor = start_supervised!(Task.Supervisor)
    query = Task.Supervisor.async_nolink(supervisor, fn -> S3.query(:metrics, ctx.tenant, []) end)
    assert_receive {:storage_barrier, reader, "GET", ^key}, 5_000
    Agent.update(ctx.agent, &%{&1 | barriers: %{}})
    assert :ok = S3.append(:metrics, ctx.tenant, [sample(4, 1.0)])
    current = ManifestCache.get(ctx.tenant, "metrics")
    send(reader, {:release_storage, key})
    assert Task.await(query, 10_000) == before
    assert ManifestCache.get(ctx.tenant, "metrics") == current
    assert {:ok, records} = S3.query(:metrics, ctx.tenant, [])
    assert Enum.map(records, & &1.timestamp_ns) == [4, 3, 2, 1]
  end

  test "repeated bounded merges converge without exceeding row or segment budgets", ctx do
    for batch <- 0..11 do
      records = for row <- 1..3, do: sample(batch * 3 + row, row / 1)
      assert :ok = S3.append(:metrics, ctx.tenant, records)
    end

    before = S3.query(:metrics, ctx.tenant, [])

    for _ <- 1..4 do
      assert {:ok, %{merged: 3, replacement: key}} =
               MetricsCompactor.compact(ctx.tenant, ctx.config, max_segments: 3, max_rows: 9)

      assert Enum.find(load(ctx).segments, &(&1.key == key)).row_count == 9
    end

    assert {:ok, %{merged: 0}} = MetricsCompactor.compact(ctx.tenant, ctx.config, max_segments: 3, max_rows: 9)
    assert length(load(ctx).segments) == 4
    assert S3.query(:metrics, ctx.tenant, []) == before
  end

  test "selection tries another candidate when an otherwise valid segment fills the byte budget alone" do
    manifest = %Manifest{
      segments: [
        Segment.build("hour/a", 1, 1, 1, 100),
        Segment.build("hour/b", 2, 2, 2, 45),
        Segment.build("hour/c", 3, 3, 2, 45)
      ]
    }

    selected = MetricsCompactor.select(manifest, max_input_bytes: 100, max_rows: 4)
    assert Enum.map(selected, & &1.key) == ["hour/b", "hour/c"]
  end

  test "background operation exceptions are isolated and maintenance recovers after configuration is repaired", ctx do
    for ts <- 1..3, do: assert(:ok = S3.append(:metrics, ctx.tenant, [sample(ts, 1.0)]))
    worker = start_supervised!({CompactionWorker, Map.delete(ctx.config, :bucket)})

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        send(worker, :compact)
        _ = :sys.get_state(worker)
      end)

    assert log =~ "raised tenant="
    :sys.replace_state(worker, fn _ -> ctx.config end)
    send(worker, :compact)
    _ = :sys.get_state(worker)
    assert length(load(ctx).segments) == 1
  end

  test "a cache miss reloads external publications instead of freshly stamping the owner's old snapshot", ctx do
    assert :ok = S3.append(:metrics, ctx.tenant, [sample(1, 1.0)])
    {:ok, payload, mn, mx} = S3.encode_segment(:metrics, [sample(2, 2.0)])
    key = S3.object_key(ctx.tenant, "metrics", mn, mx, "external", nil)
    assert {:ok, _} = ObjectStore.put(ctx.config, key, payload)
    manifest_key = Manifest.manifest_key(ctx.tenant, "metrics")
    {:ok, etag, body} = ObjectStore.get_if_none_match(ctx.config, manifest_key, nil)
    {:ok, current} = Manifest.decode(body)
    updated = Manifest.merge(current, [Segment.build(key, mn, mx, 1, byte_size(payload))])

    assert {:ok, _} =
             ObjectStore.put_if_match(ctx.config, manifest_key, Manifest.encode(updated) |> IO.iodata_to_binary(), etag)

    ManifestCache.drop(ctx.tenant, "metrics")
    assert {:ok, records} = S3.query(:metrics, ctx.tenant, [])
    assert Enum.map(records, & &1.timestamp_ns) == [2, 1]
  end

  test "an unconfigured object-storage adapter still fails with a clear configuration error" do
    Application.delete_env(:pulso, S3)
    assert_raise RuntimeError, ~r/is not configured/, fn -> S3.query(:metrics, "unconfigured", []) end
  end

  test "exhausted conditional conflicts reclaim a replacement proven never published", ctx do
    for ts <- 1..3, do: assert(:ok = S3.append(:metrics, ctx.tenant, [sample(ts, 1.0)]))
    before = S3.query(:metrics, ctx.tenant, [])
    manifest = load(ctx)
    key = Manifest.manifest_key(ctx.tenant, "metrics")
    Agent.update(ctx.agent, &%{&1 | faults: %{{"PUT", key} => {412, :before, 5}}})
    assert {:error, :cas_retries_exhausted} = MetricsCompactor.compact(ctx.tenant, ctx.config)
    assert load(ctx) == manifest
    assert S3.query(:metrics, ctx.tenant, []) == before

    assert Agent.get(ctx.agent, fn state ->
             Enum.count(state.objects, fn {key, _} -> String.contains?(key, "-compact-") end)
           end) == 0
  end

  test "a repeatedly failing cleanup key does not rewrite unchanged completion state", ctx do
    for ts <- 1..2, do: assert(:ok = S3.append(:metrics, ctx.tenant, [sample(ts, 1.0)]))
    assert {:ok, %{merged: 2}} = MetricsCompactor.compact(ctx.tenant, ctx.config, grace_ms: 0)
    retired = load(ctx).retired |> Map.keys() |> Enum.sort()
    Agent.update(ctx.agent, &%{&1 | faults: Map.new(retired, fn key -> {{"DELETE", key}, {403, :before, 100}} end)})
    assert {:error, {:cleanup_failed, _}} = MetricsCompactor.cleanup(ctx.tenant, ctx.config)
    key = Manifest.manifest_key(ctx.tenant, "metrics")
    {:ok, etag, body} = ObjectStore.get_if_none_match(ctx.config, key, nil)
    assert {:error, {:cleanup_failed, _}} = MetricsCompactor.cleanup(ctx.tenant, ctx.config)
    assert {:ok, ^etag, ^body} = ObjectStore.get_if_none_match(ctx.config, key, nil)
  end

  test "an owner loaded before compaction preserves the new metadata when background compaction is disabled", ctx do
    for ts <- 1..3, do: assert(:ok = S3.append(:metrics, ctx.tenant, [sample(ts, 1.0)]))
    assert load(ctx).version == 1
    assert CompactionWorker.children(ctx.config) == []
    assert {:ok, %{merged: 3}} = MetricsCompactor.compact(ctx.tenant, ctx.config, grace_ms: 0)
    assert {:ok, 3} = MetricsCompactor.cleanup(ctx.tenant, ctx.config)
    retired = load(ctx)
    assert :ok = S3.append(:metrics, ctx.tenant, [sample(4, 1.0)])
    updated = load(ctx)
    assert updated.version == 2
    assert updated.retired == retired.retired
    assert updated.cleanup_cursor == retired.cleanup_cursor
    assert {:ok, records} = S3.query(:metrics, ctx.tenant, [])
    assert Enum.map(records, & &1.timestamp_ns) == [4, 3, 2, 1]
  end

  test "selection respects bytes, rows, segment count and hour boundaries" do
    segments = for i <- 1..6, do: Segment.build("hour/a#{i}", i, i, 10, 100)

    manifest = %Manifest{
      segments: segments ++ [Segment.build("other/b", 0, 0, 1, 100), Segment.build("hour/unknown", 0, 0, 1)]
    }

    assert length(MetricsCompactor.select(manifest, max_segments: 2)) == 2
    assert length(MetricsCompactor.select(manifest, max_input_bytes: 300)) == 3
    assert length(MetricsCompactor.select(manifest, max_rows: 20)) == 2
    assert MetricsCompactor.select(manifest, max_input_bytes: 100) == []
    assert MetricsCompactor.select(manifest, small_segment_bytes: 99) == []
  end
end
