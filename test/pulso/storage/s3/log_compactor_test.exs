defmodule Pulso.Storage.S3.LogCompactorTest do
  use Pulso.Test.Case, async: true

  alias Pulso.Codec.NIF
  alias Pulso.Record.Log
  alias Pulso.Runtime
  alias Pulso.Storage.S3
  alias Pulso.Storage.S3.CompactionOwnership
  alias Pulso.Storage.S3.CompactionTasks
  alias Pulso.Storage.S3.CompactionWorker
  alias Pulso.Storage.S3.LogCompactor
  alias Pulso.Storage.S3.Manifest.Segment
  alias Pulso.Storage.S3.ManifestSupervision
  alias Pulso.Storage.S3.Retention
  alias Pulso.Storage.SortOrder
  alias Pulso.Test.CompactionStore
  alias Pulso.Test.RandomTerms

  defp decode(blob) do
    {:ok, records} = NIF.decode_log_segment_parquet(blob, nil, nil, nil, [], [])
    records
  end

  defp canonical(records), do: records |> SortOrder.sort(:logs) |> Enum.sort_by(&:erlang.term_to_binary/1)

  describe "native merge" do
    test "random segments merge into the same records, bounds and service summary as one encode" do
      for _ <- 1..20 do
        records = RandomTerms.logs(:rand.uniform(200))
        parts = Enum.chunk_every(records, max(1, div(length(records), :rand.uniform(5))))
        blobs = Enum.map(parts, fn part -> part |> NIF.encode_log_segment_parquet() |> elem(1) end)

        assert {:ok, merged, min, max, count, services} = NIF.compact_log_segments(blobs, 100_000)

        assert canonical(decode(merged)) == canonical(Enum.flat_map(blobs, &decode/1))
        assert {:ok, _, ^min, ^max, ^count} = NIF.encode_log_segment_parquet(records)
        assert services == Segment.summarize_logs(%Segment{}, records).log_services
      end
    end

    test "merged rows are ordered by service, then timestamp" do
      a = [%Log{timestamp_ns: 3, service: "b", body: "x"}, %Log{timestamp_ns: 9, service: "a", body: "y"}]
      b = [%Log{timestamp_ns: 1, service: "b", body: "z"}, %Log{timestamp_ns: 2, service: nil, body: "w"}]
      blobs = Enum.map([a, b], &elem(NIF.encode_log_segment_parquet(&1), 1))

      assert {:ok, merged, 1, 9, 4, nil} = NIF.compact_log_segments(blobs, 10)
      assert Enum.map(decode(merged), &{&1.service, &1.timestamp_ns}) == [{nil, 2}, {"a", 9}, {"b", 1}, {"b", 3}]
    end

    test "legacy ten-column segments merge with current ones without inventing metadata" do
      legacy = File.read!("test/fixtures/logs/legacy_log.parquet")
      {:ok, current, _, _, _} = NIF.encode_log_segment_parquet([%Log{timestamp_ns: 5, service: "api", body: "new"}])

      assert {:ok, merged, _, _, count, _} = NIF.compact_log_segments([legacy, current], 100_000)
      assert count == length(decode(legacy)) + 1
      assert canonical(decode(merged)) == canonical(decode(legacy) ++ decode(current))
    end

    test "row and payload ceilings refuse a merge instead of truncating it" do
      {:ok, blob, _, _, _} = NIF.encode_log_segment_parquet(RandomTerms.logs(10))
      assert {:error, :query_sample_limit} = NIF.compact_log_segments([blob, blob], 15)

      big = %Log{timestamp_ns: 1, body: String.duplicate("x", 9 * 1024 * 1024)}
      {:ok, large, _, _, _} = NIF.encode_log_segment_parquet([big])
      assert {:error, :payload_too_large} = NIF.compact_log_segments([large, large, large, large], 100)
    end

    test "corrupt or foreign segments are rejected" do
      {:ok, metric, _, _, _} = NIF.encode_metric_segment_parquet([])
      assert :fallback = NIF.compact_log_segments([metric], 100)
      # Its footer claims an impossible row count, refused before any decode.
      corrupt = File.read!("test/fixtures/logs/corrupt_footer.parquet")
      refute match?({:ok, _, _, _, _, _}, NIF.compact_log_segments([corrupt], 100_000))
      assert :fallback = NIF.compact_log_segments([], 0)
    end
  end

  describe "storage maintenance" do
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
        access_key_id: "logs-#{System.unique_integer([:positive])}",
        secret_access_key: "test",
        allow_http: true,
        refresh_stale_ms: 0
      }

      Runtime.put_env(:pulso, S3, config)
      start_supervised!(ManifestSupervision)
      %{config: config, tenant: "log-compact-#{System.unique_integer([:positive])}"}
    end

    defp segments(ctx) do
      assert {:ok, manifest, _} = Retention.load(ctx.tenant, "logs", ctx.config)
      manifest.segments
    end

    defp record(ts, service, pod),
      do: %Log{timestamp_ns: ts, service: service, body: "line #{ts}", resource: %{"pod" => pod}}

    test "small segments merge, keep query results and keyed retries, and survive restart", ctx do
      assert :ok =
               S3.append(:logs, ctx.tenant, [record(1, "api", "a"), record(2, "api", "a")], idempotency_key: "batch")

      assert :ok = S3.append(:logs, ctx.tenant, [record(3, "web", "b")])
      assert :ok = S3.append(:logs, ctx.tenant, [record(4, "api", "c")])
      assert {:ok, before} = S3.query(:logs, ctx.tenant, [])

      assert {:ok, %{merged: 3, replacement: replacement}} = LogCompactor.compact(ctx.tenant, ctx.config, grace_ms: 0)
      assert [%Segment{key: ^replacement, row_count: 4, log_services: ["api", "web"]}] = segments(ctx)

      assert :ok = stop_supervised(ManifestSupervision)
      start_supervised!(ManifestSupervision)
      assert {:ok, ^before} = S3.query(:logs, ctx.tenant, [])
      assert {:ok, [_, _, _]} = S3.query(:logs, ctx.tenant, service: "api")

      assert {:ok, 3} = LogCompactor.cleanup(ctx.tenant, ctx.config)

      assert :ok =
               S3.append(:logs, ctx.tenant, [record(1, "api", "a"), record(2, "api", "a")], idempotency_key: "batch")

      assert {:ok, ^before} = S3.query(:logs, ctx.tenant, [])
    end

    test "retention cleans flat log retirements even after log compaction is disabled", ctx do
      for ts <- [1, 2], do: assert(:ok = S3.append(:logs, ctx.tenant, [record(ts, "api", "a")]))
      assert {:ok, %{merged: 2}} = LogCompactor.compact(ctx.tenant, ctx.config, grace_ms: 0)
      config = Map.put(ctx.config, :log_compaction_enabled, false)
      assert {:ok, 2} = Retention.cleanup_retired(ctx.tenant, "logs", config)
      assert {:ok, manifest, _} = Retention.load(ctx.tenant, "logs", config)
      assert Enum.all?(manifest.retired, fn {_key, retired} -> retired.deleted? end)
    end

    test "segments stay unmerged until they settle, so live tail reads them first", ctx do
      now = System.os_time(:nanosecond)
      for i <- 1..2, do: assert(:ok = S3.append(:logs, ctx.tenant, [record(now + i, "api", "a")]))
      assert {:ok, %{merged: 0}} = LogCompactor.compact(ctx.tenant, ctx.config, grace_ms: 0)
      assert {:ok, %{merged: 2}} = LogCompactor.compact(ctx.tenant, ctx.config, grace_ms: 0, settle_ms: 0)
    end

    test "stream discovery reads the merged segment", ctx do
      for {ts, pod} <- [{1, "a"}, {2, "b"}], do: assert(:ok = S3.append(:logs, ctx.tenant, [record(ts, "api", pod)]))
      assert {:ok, %{merged: 2}} = LogCompactor.compact(ctx.tenant, ctx.config, grace_ms: 0)

      assert {:ok, ~s(["a","b"])} = S3.log_discovery_json(ctx.tenant, "values", "pod", start_ts: 0, end_ts: 10)
    end

    test "log-only workers join the logs eligibility group and catch up on a backlog in one pass", ctx do
      # More segments than one merge combines, as a per-second log shipper creates.
      for ts <- 1..40, do: assert(:ok = S3.append(:logs, ctx.tenant, [record(ts, "api", "a")]))

      config =
        Map.merge(ctx.config, %{
          compaction_enabled: false,
          trace_compaction_enabled: false,
          log_compaction_enabled: true,
          compaction_interval_ms: 60_000
        })

      start_supervised!(%{id: CompactionOwnership, start: {:pg, :start_link, [CompactionOwnership.scope()]}})
      start_supervised!({Task.Supervisor, name: CompactionTasks})
      worker = start_supervised!({CompactionWorker, config})
      assert CompactionOwnership.members(config, "logs") == [node()]
      assert CompactionOwnership.members(config, "metrics") == []
      send(worker, :compact)
      _ = :sys.get_state(worker, 60_000)
      assert [_] = segments(ctx)
    end
  end
end
