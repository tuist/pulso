defmodule Pulso.PromQL.BatchTest do
  use Pulso.Test.Case, async: true

  alias Pulso.PromQL.Evaluator
  alias Pulso.Record.Log
  alias Pulso.Record.MetricSample
  alias Pulso.Runtime
  alias Pulso.Runtime.Task
  alias Pulso.Storage
  alias Pulso.Storage.Memory
  alias Pulso.Test.BatchQueryStorage
  alias Pulso.Test.BlockingMetricStorage

  defp finish(results, deadline), do: {:ok, {results, deadline}}
  defp queries, do: [{"A", "one", %{end_ts_ns: 10}}, {"B", "two", %{end_ts_ns: 10}}]

  defp sample(name, value),
    do: %MetricSample{series_id: 1, timestamp_ns: 10, value: value, labels: %{"__name__" => name}}

  test "source results and final callback share a tenant-scoped admitted worker/deadline" do
    Memory.reset()
    assert :ok = Storage.append(:metrics, "acme", [sample("one", 4.0), sample("two", 8.0)])
    assert :ok = Storage.append(:metrics, "beta", [sample("one", 99.0)])
    assert {:ok, {results, deadline}} = Evaluator.query_batch(queries(), "acme", &finish/2)
    assert is_integer(deadline)
    assert results["A"]["data"]["result"] |> hd() |> Map.get("value") |> List.last() == "4"
    assert results["B"]["data"]["result"] |> hd() |> Map.get("value") |> List.last() == "8"
  end

  test "sample and output allocation budgets accumulate across queries, not per-node resets" do
    Memory.reset()
    assert :ok = Storage.append(:metrics, "acme", [sample("one", 4.0), sample("two", 8.0)])
    Runtime.put_env(:pulso, Evaluator, max_samples: 1)
    assert {:error, :query_sample_limit} = Evaluator.query_batch(queries(), "acme", &finish/2)
    Runtime.put_env(:pulso, Evaluator, max_result_points: 1)
    assert {:error, :query_result_limit} = Evaluator.query_batch(queries(), "acme", &finish/2)
    assert {:ok, _} = Evaluator.query("two", "acme", %{end_ts_ns: 10})
  end

  test "one slot covers all sources; aggregate scan ceilings are divided across every selector" do
    Runtime.put_env(:pulso, Storage, adapter: BatchQueryStorage)
    Runtime.put_env(:pulso, BatchQueryStorage, self())
    Runtime.put_env(:pulso, Pulso.QueryRunner, max_per_tenant: 1, class_limits: %{promql: 1})
    Runtime.put_env(:pulso, Evaluator, max_scan_segments: 9, max_scan_bytes: 101, max_scan_rows: 999)
    callers = start_supervised!({Task.Supervisor, name: __MODULE__.Calls})
    owner = self()

    {:ok, _} =
      Task.Supervisor.start_child(callers, fn ->
        send(
          owner,
          {:batch_result,
           Evaluator.query_batch(
             [{"A", "one+two", %{end_ts_ns: 10}}, {"B", "three", %{end_ts_ns: 10}}],
             "acme",
             &finish/2
           )}
        )
      end)

    assert_receive {:batch_query, worker, "acme", first}, 5000
    ref = Process.monitor(worker)
    assert first[:max_scan_segments] == 3
    assert first[:max_scan_bytes] == 33
    assert first[:max_scan_rows] == 333
    send(worker, {:release_batch, {:ok, []}})
    assert_receive {:batch_query, ^worker, "acme", second}, 5000
    assert second[:deadline_ms] == first[:deadline_ms]
    send(worker, {:release_batch, {:ok, []}})
    assert_receive {:batch_query, ^worker, "acme", third}, 5000
    assert third[:max_scan_bytes] == 33
    send(worker, {:release_batch, {:ok, []}})
    assert_receive {:batch_result, {:ok, _}}, 5000
    assert_receive {:DOWN, ^ref, :process, ^worker, :normal}, 5000
  end

  test "mixed languages share one slot, deadline and aggregate selector scan allowance" do
    Runtime.put_env(:pulso, Storage, adapter: BatchQueryStorage)
    Runtime.put_env(:pulso, BatchQueryStorage, self())
    Runtime.put_env(:pulso, Pulso.QueryRunner, max_per_tenant: 1, class_limits: %{promql: 1}, max_scan_segments: 6)
    Runtime.put_env(:pulso, Evaluator, max_scan_segments: 9, max_scan_bytes: 101, max_scan_rows: 999)
    callers = start_supervised!({Task.Supervisor, name: __MODULE__.Calls})
    owner = self()

    batch = [
      {"A", "one", %{end_ts_ns: 10}},
      {"B", ~s'count_over_time({service="api"}[1m]) + count_over_time({service="api"}[2m])',
       %{end_ts_ns: 10, language: :logql}}
    ]

    {:ok, _} =
      Task.Supervisor.start_child(callers, fn ->
        send(owner, {:mixed_result, Evaluator.query_batch(batch, "acme", &finish/2)})
      end)

    assert_receive {:batch_query, worker, "acme", first}, 5000
    monitor = Process.monitor(worker)
    assert first[:max_scan_segments] == 2
    assert first[:max_scan_bytes] == 33
    send(worker, {:release_batch, {:ok, []}})

    for _ <- 1..2 do
      assert_receive {:batch_query, ^worker, "acme", opts}, 5000
      assert opts[:max_scan_segments] == 2
      assert opts[:max_scan_bytes] == 33
      assert opts[:max_scan_rows] == 333
      assert opts[:max_records] == 100_000
      assert opts[:deadline_ms] == first[:deadline_ms]
      send(worker, {:release_batch, {:ok, []}})
    end

    assert_receive {:mixed_result, {:ok, {%{"B" => {:logql, :vector, []}}, _}}}, 5000
    assert_receive {:DOWN, ^monitor, :process, ^worker, :normal}, 5000
  end

  test "mixed source failures cannot reset budgets or return a partial callback" do
    Memory.reset()
    assert :ok = Storage.append(:metrics, "acme", [sample("one", 4.0)])
    assert :ok = Storage.append(:logs, "acme", [%Log{timestamp_ns: 10, service: "api", body: "hello"}])
    metric = {"A", "one", %{end_ts_ns: 10}}
    logs = {"B", ~s'count_over_time({service="api"}[1m])', %{end_ts_ns: 10, language: :logql}}
    # The metric fits its sample quota; the log source exceeds its own.
    Runtime.put_env(:pulso, Evaluator, max_samples: 1, max_log_samples: 0)

    for batch <- [[metric, logs], [logs, metric]] do
      assert {:error, :query_sample_limit} =
               Evaluator.query_batch(batch, "acme", fn _, _ -> flunk("partial callback") end)
    end

    assert {:error, :invalid_logql} =
             Evaluator.query_batch([{"L", "not logql", %{language: :logql}}], "acme", &finish/2)

    assert {:error, :unsupported_source_query} =
             Evaluator.query_batch([{"L", ~s'{service="api"}', %{language: :logql}}], "acme", &finish/2)
  end

  test "work and output points also accumulate across language boundaries in either order" do
    logs = {"L", "1", %{end_ts_ns: 10, language: :logql}}

    {:ok, work} =
      Evaluator.query_batch([{"P", "1+2", %{end_ts_ns: 10}}], "acme", fn _, _ ->
        {:ok, Process.get(:promql_budget).work}
      end)

    assert work > 0

    for {limits, query, reason} <- [
          {[max_work: work], "1+2", :query_work_limit},
          {[max_result_points: 1], "1", :query_result_limit}
        ] do
      Runtime.put_env(:pulso, Evaluator, limits)
      metric = {"P", query, %{end_ts_ns: 10}}
      assert {:ok, _} = Evaluator.query_batch([metric], "acme", &finish/2)
      assert {:ok, _} = Evaluator.query_batch([logs], "acme", &finish/2)

      for batch <- [[metric, logs], [logs, metric]] do
        assert {:error, ^reason} = Evaluator.query_batch(batch, "acme", fn _, _ -> flunk("partial callback") end)
      end
    end
  end

  test "invalid/cyclic identity batches and indivisible scan budgets fail without partial callback" do
    callback = fn _, _ -> flunk("partial callback") end

    for batch <- [[{"A", "one", %{}}, {"A", "two", %{}}], [nil], List.duplicate({"A", "one", %{}}, 129)] do
      assert {:error, :invalid_query_batch} = Evaluator.query_batch(batch, "acme", callback)
    end

    Runtime.put_env(:pulso, Evaluator, max_scan_segments: 1)
    assert {:error, :query_scan_limit} = Evaluator.query_batch(queries(), "acme", callback)
  end

  test "source errors are not a successful empty graph and the finish callback is inside the deadline" do
    Runtime.put_env(:pulso, Storage, adapter: BlockingMetricStorage)
    Runtime.put_env(:pulso, BlockingMetricStorage, {:error, :backend_unavailable})

    assert {:error, {:storage_error, :backend_unavailable}} =
             Evaluator.query_batch(queries(), "acme", fn _, _ -> flunk("source failure") end)

    assert {:error, :query_timeout} =
             Evaluator.query_batch(
               [],
               "acme",
               fn _, _ ->
                 receive do
                   :never -> :unexpected
                 end
               end,
               %{timeout_ms: 10}
             )
  end
end
