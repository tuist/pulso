defmodule Pulso.PromQL.SegmentReadsTest do
  @moduledoc """
  Counts object-store segment GETs per PromQL query against an in-process S3
  fake: every candidate segment is read once per query, however many
  selectors need it, and scan budgets apply to the distinct segments read.
  """
  use Pulso.Test.Case, async: true

  alias Pulso.ObjectStore
  alias Pulso.PromQL.Evaluator
  alias Pulso.QueryLimit
  alias Pulso.Record.MetricSample
  alias Pulso.Runtime
  alias Pulso.Runtime.Task
  alias Pulso.Storage
  alias Pulso.Storage.Memory
  alias Pulso.Storage.S3
  alias Pulso.Storage.S3.Manifest
  alias Pulso.Storage.S3.ManifestSupervision
  alias Pulso.Test.CompactionStore

  @segments 4
  @and_on ~s|sum by (pod) (rate(requests_total{cluster="c"}[1m])) and on (pod) max by (pod) (node_info{cluster="c"})|

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
      access_key_id: "reads-#{System.unique_integer([:positive])}",
      secret_access_key: "test",
      allow_http: true,
      refresh_stale_ms: 0
    }

    Runtime.put_env(:pulso, S3, config)
    start_supervised!(ManifestSupervision)
    tenant = "reads-#{System.unique_integer([:positive])}"

    {:ok, _} =
      ObjectStore.put(
        config,
        Manifest.manifest_key(tenant, "metrics"),
        IO.iodata_to_binary(Manifest.encode(Manifest.new()))
      )

    # Each append is one segment holding both metrics, as one remote-write
    # batch from one scrape does.
    records =
      for segment <- 1..@segments, pod <- ["a", "b"] do
        ts = segment * 15_000_000_000

        [
          %MetricSample{
            timestamp_ns: ts,
            value: segment * 10.0,
            labels: %{"__name__" => "requests_total", "cluster" => "c", "pod" => pod}
          },
          %MetricSample{
            timestamp_ns: ts,
            value: 1.0,
            labels: %{"__name__" => "node_info", "cluster" => "c", "pod" => pod}
          }
        ]
      end

    for chunk <- Enum.chunk_every(records, 2), do: assert(:ok = S3.append(:metrics, tenant, List.flatten(chunk)))

    Runtime.put_env(:pulso, Storage, adapter: S3)
    reads(agent)
    %{agent: agent, tenant: tenant, records: List.flatten(records)}
  end

  defp reads(agent) do
    Agent.get_and_update(agent, fn state ->
      {Enum.count(state.reads, &String.ends_with?(&1, ".parquet")), %{state | reads: []}}
    end)
  end

  defp range, do: %{start_ts_ns: 30_000_000_000, end_ts_ns: 60_000_000_000, step_ns: 15_000_000_000}

  test "a two-selector and-on query reads each segment once and matches memory storage", ctx do
    assert {:ok, s3} = Evaluator.query(@and_on, ctx.tenant, range())
    assert reads(ctx.agent) == @segments

    Runtime.put_env(:pulso, Storage, adapter: Memory)
    assert :ok = Storage.append(:metrics, ctx.tenant, ctx.records)
    assert {:ok, ^s3} = Evaluator.query(@and_on, ctx.tenant, range())
    assert length(s3["data"]["result"]) == 2
  end

  test "selectors repeated with different windows and subqueries share one pass", ctx do
    query =
      ~s|max_over_time(requests_total{cluster="c"}[1m:15s]) + on (pod) group_left increase(requests_total{cluster="c"}[45s]) * on (pod) group_left node_info{cluster="c"}|

    assert {:ok, %{"data" => %{"result" => [_, _]}}} = Evaluator.query(query, ctx.tenant, range())
    assert reads(ctx.agent) == @segments
  end

  test "the segment budget applies to the distinct segments a query reads", ctx do
    Runtime.put_env(:pulso, Evaluator, max_scan_segments: @segments)
    assert {:ok, _} = Evaluator.query(@and_on, ctx.tenant, range())
    assert reads(ctx.agent) == @segments

    Runtime.put_env(:pulso, Evaluator, max_scan_segments: @segments - 1)
    assert {:error, :query_scan_limit} = Evaluator.query(@and_on, ctx.tenant, range())
    assert reads(ctx.agent) == 0
    # Paged manifests stop listing candidates as soon as the budget is exceeded.
    assert %{limit: :max_scan_segments, maximum: 3, stage: :candidates} = detail = QueryLimit.take()
    assert QueryLimit.describe(detail) =~ "max_scan_segments=3 exceeded"
    assert QueryLimit.describe(detail) =~ "PULSO_QUERY_MAX_SCAN_SEGMENTS"
  end

  test "alert batches share one read per source instead of a 1/N split per selector", ctx do
    # Two sources share the batch's 8 segments: 4 each. The two-selector
    # source needs its 4 distinct segments, read once for both selectors.
    Runtime.put_env(:pulso, Evaluator, max_scan_segments: 2 * @segments)
    batch = [{"a", @and_on, %{end_ts_ns: 60_000_000_000}}, {"b", "node_info", %{end_ts_ns: 60_000_000_000}}]

    assert {:ok, {%{"a" => _, "b" => _}, _}} =
             Evaluator.query_batch(batch, ctx.tenant, fn results, _deadline -> {:ok, {results, :done}} end)

    assert reads(ctx.agent) == 2 * @segments
  end

  test "the deadline covers the whole query: a stalled read answers at the deadline and reads no further", ctx do
    owner = self()

    Agent.update(ctx.agent, fn state ->
      %{state | barriers: %{{"GET", :segment} => owner}}
    end)

    query = Task.async(fn -> Evaluator.query(@and_on, ctx.tenant, Map.put(range(), :timeout_ms, 1_000)) end)
    assert_receive {:storage_barrier, request, "GET", key}, 5_000
    assert {:error, :query_timeout} = Task.await(query, 5_000)

    Agent.update(ctx.agent, &%{&1 | barriers: %{}})
    send(request, {:release_storage, key})
    assert reads(ctx.agent) == 1
    # The worker that issued the stalled read is gone, so a new query reads afresh.
    assert {:ok, _} = Evaluator.query(@and_on, ctx.tenant, range())
    assert reads(ctx.agent) == @segments
  end
end
