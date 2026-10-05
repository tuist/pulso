defmodule Pulso.PromQL.EvaluatorTest do
  use ExUnit.Case, async: false

  alias Pulso.PromQL.Evaluator
  alias Pulso.PromQL.QuerySlots
  alias Pulso.PromQL.TaskSupervisor
  alias Pulso.Record.MetricSample
  alias Pulso.Storage
  alias Pulso.Storage.Memory
  alias Pulso.Test.BlockingMetricStorage

  setup do
    Memory.reset()
    :ok
  end

  defp append(name, points, extra \\ %{}, tenant \\ "acme") do
    samples =
      Enum.map(points, fn {seconds, value} ->
        %MetricSample{
          series_id: 1,
          timestamp_ns: seconds * 1_000_000_000,
          value: value * 1.0,
          labels: Map.put(extra, "__name__", name)
        }
      end)

    :ok = Storage.append(:metrics, tenant, samples)
  end

  defp instant(query, time, tenant \\ "acme") do
    Evaluator.query(query, tenant, %{end_ts_ns: time * 1_000_000_000})
  end

  defp values(result), do: Enum.map(result["data"]["result"], & &1["value"])

  test "the operator heap ceiling is configurable" do
    original = Application.get_env(:pulso, Evaluator, [])
    Application.put_env(:pulso, Evaluator, max_heap_words: 1000)
    on_exit(fn -> Application.put_env(:pulso, Evaluator, original) end)
    append("memory", Enum.map(1..1000, &{&1, &1}))
    assert {:error, :query_resource_limit} = instant("memory", 1000)
  end

  test "real blocked queries enforce tenant admission and release their slots" do
    adapter = BlockingMetricStorage
    original = Application.get_env(:pulso, Pulso.Storage)
    Application.put_env(:pulso, Pulso.Storage, adapter: adapter)
    Application.put_env(:pulso, adapter, self())

    on_exit(fn ->
      Application.put_env(:pulso, Pulso.Storage, original)
      Application.delete_env(:pulso, adapter)
    end)

    callers = start_supervised!({Task.Supervisor, name: __MODULE__.CallerSupervisor})
    owner = self()

    workers =
      for _ <- 1..2 do
        {:ok, _} =
          Task.Supervisor.start_child(callers, fn -> send(owner, {:finished_metric_query, instant("m", 1, "acme")}) end)

        assert_receive {:blocked_metric_query, worker, "acme"}
        worker
      end

    assert {:error, :query_overloaded} = instant("m", 1, "acme")

    {:ok, _} =
      Task.Supervisor.start_child(callers, fn -> send(owner, {:finished_metric_query, instant("m", 1, "beta")}) end)

    assert_receive {:blocked_metric_query, beta, "beta"}
    refs = Enum.map([beta | workers], &Process.monitor/1)
    Enum.each([beta | workers], &send(&1, {:release, {:ok, []}}))
    for _ <- 1..3, do: assert_receive({:finished_metric_query, {:ok, _}})
    # A result reaches the caller before its worker necessarily exits.
    # Observe worker termination before asserting that slots are released.
    for ref <- refs, do: assert_receive({:DOWN, ^ref, :process, _, :normal})
    _ = :sys.get_state(QuerySlots)
    assert Registry.lookup(QuerySlots, {"acme", 0}) == []
    assert Registry.lookup(QuerySlots, {"acme", 1}) == []
  end

  test "one tenant cannot occupy every node slot" do
    owner = self()

    children =
      for slot <- 0..1 do
        {:ok, pid} =
          Task.Supervisor.start_child(TaskSupervisor, fn ->
            {:ok, _} = Registry.register(QuerySlots, {"acme", slot}, nil)
            send(owner, {:tenant_slot, self()})
            receive do: (:release -> :ok)
          end)

        assert_receive {:tenant_slot, ^pid}
        pid
      end

    on_exit(fn -> Enum.each(children, &Task.Supervisor.terminate_child(TaskSupervisor, &1)) end)
    assert {:error, :query_overloaded} = instant("m", 1, "acme")
    assert {:ok, _} = instant("m", 1, "beta")
  end

  test "unexpected task crashes are execution failures" do
    original = Application.get_env(:pulso, Pulso.Storage)
    Application.put_env(:pulso, Pulso.Storage, adapter: Pulso.MissingAdapter)
    on_exit(fn -> Application.put_env(:pulso, Pulso.Storage, original) end)

    ExUnit.CaptureLog.capture_log(fn ->
      assert {:error, :query_execution_failed} = instant("m", 1)
    end)
  end

  test "query admission rejects excess work and releases capacity" do
    owner = self()

    children =
      for _ <- 1..4 do
        {:ok, pid} =
          Task.Supervisor.start_child(TaskSupervisor, fn ->
            send(owner, {:query_slot, self()})
            receive do: (:release -> :ok)
          end)

        assert_receive {:query_slot, ^pid}
        pid
      end

    on_exit(fn -> Enum.each(children, &Task.Supervisor.terminate_child(TaskSupervisor, &1)) end)
    assert {:error, :query_overloaded} = instant("m", 1)

    Enum.each(children, fn pid ->
      ref = Process.monitor(pid)
      send(pid, :release)
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    end)

    assert {:ok, _} = instant("m", 1)
  end

  test "sample strings use Prometheus decimal and exponent cutoffs" do
    for {number, expected} <- [
          {1.0e21, "1e+21"},
          {1.0e-7, "1e-07"},
          {1.0e-6, "0.000001"},
          {1.0e20, "100000000000000000000"},
          {-0.0, "-0"},
          {1.25, "1.25"}
        ] do
      Memory.reset()
      append("format", [{1, number}])
      assert {:ok, result} = instant("format", 1)
      assert values(result) == [[1.0, expected]]
    end
  end

  test "selectors use the newest sample in a left-open five-minute lookback" do
    append("gauge", [{0, 1}, {10, 2}, {20, 3}])
    assert {:ok, result} = instant("gauge", 15)
    assert values(result) == [[15.0, "2"]]
    assert {:ok, result} = instant("gauge", 320)
    assert values(result) == []
    assert {:ok, result} = instant("gauge offset 10s", 25)
    assert values(result) == [[25.0, "2"]]
    assert {:ok, result} = instant("gauge", 20, "other")
    assert values(result) == []
  end

  test "selector regular expressions are fully anchored and missing labels are empty" do
    append("gauge", [{1, 1}], %{"job" => "api"})
    append("gauge", [{1, 2}], %{"job" => "myapi"})
    assert {:ok, result} = instant(~s(gauge{job=~"api",missing=""}), 1)
    assert values(result) == [[1.0, "1"]]
    assert {:ok, result} = instant(~s(gauge{job!~"api"}), 1)
    assert values(result) == [[1.0, "2"]]
  end

  test "rates extrapolate, correct resets, and preserve evaluation timestamps" do
    append("requests_total", [{0, 100}, {10, 110}, {20, 120}, {30, 5}])
    assert {:ok, result} = instant("rate(requests_total[30s])", 30)
    assert values(result) == [[30.0, "0.75"]]
    assert result["data"]["result"] |> hd() |> Map.fetch!("metric") == %{}
    assert {:ok, result} = instant("increase(requests_total[30s])", 30)
    assert values(result) == [[30.0, "22.5"]]
    assert {:ok, result} = instant("irate(requests_total[30s])", 30)
    assert values(result) == [[30.0, "0.5"]]
    assert {:ok, result} = instant("delta(requests_total[30s])", 30)
    assert values(result) == [[30.0, "-157.5"]]
  end

  test "rates do not extrapolate counters below zero or across distant boundaries" do
    append("new_total", [{10, 0}, {20, 10}])
    assert {:ok, result} = instant("increase(new_total[30s])", 30)
    assert values(result) == [[30.0, "20"]]
    assert {:ok, result} = instant("increase(new_total[100s])", 100)
    assert values(result) == [[100.0, "15"]]
    assert {:ok, result} = instant("rate(new_total[5s])", 20)
    assert values(result) == []
  end

  test "range functions omit empty buckets and aggregations group per evaluation time" do
    append("gauge", [{10, 2}, {20, 4}], %{"job" => "api", "instance" => "a"})
    append("gauge", [{10, 6}, {20, 8}], %{"job" => "api", "instance" => "b"})
    assert {:ok, result} = instant("sum by (job) (avg_over_time(gauge[20s]))", 20)
    assert result["data"]["result"] == [%{"metric" => %{"job" => "api"}, "value" => [20.0, "10"]}]
    assert {:ok, result} = instant("count(gauge) without(instance)", 20)
    assert result["data"]["result"] == [%{"metric" => %{"job" => "api"}, "value" => [20.0, "2"]}]

    assert {:ok, result} =
             Evaluator.query("sum(gauge)", "acme", %{start_ts_ns: 0, end_ts_ns: 20_000_000_000, step_ns: 10_000_000_000})

    assert result["data"]["resultType"] == "matrix"
    assert result["data"]["result"] == [%{"metric" => %{}, "values" => [[10.0, "8"], [20.0, "12"]]}]
  end

  test "each supported over-time function and vector aggregation folds real values" do
    append("gauge", [{10, 2}, {20, 4}], %{"instance" => "a"})
    append("gauge", [{10, 6}, {20, 8}], %{"instance" => "b"})

    for {op, expected} <- [{"sum", "12"}, {"avg", "6"}, {"min", "4"}, {"max", "8"}, {"count", "2"}] do
      assert {:ok, result} = instant("#{op}(gauge)", 20)
      assert values(result) == [[20.0, expected]]
    end

    for {op, expected} <- [
          {"sum", ["6", "14"]},
          {"avg", ["3", "7"]},
          {"min", ["2", "6"]},
          {"max", ["4", "8"]},
          {"count", ["2", "2"]}
        ] do
      assert {:ok, result} = instant("#{op}_over_time(gauge[20s])", 20)
      assert values(result) == Enum.map(expected, &[20.0, &1])
    end
  end

  test "instant and range evaluation agree at every step, including counter reset and offset" do
    append("requests_total", [{0, 100}, {10, 110}, {20, 120}, {30, 5}])
    opts = %{start_ts_ns: 10_000_000_000, end_ts_ns: 40_000_000_000, step_ns: 10_000_000_000}
    query = "sum(rate(requests_total[30s] offset 10s))"
    assert {:ok, matrix} = Evaluator.query(query, "acme", opts)

    expected =
      for time <- [10, 20, 30, 40],
          {:ok, vector} = instant(query, time),
          [_, value] <- values(vector),
          do: [time * 1.0, value]

    assert matrix["data"]["result"] == [%{"metric" => %{}, "values" => expected}]
  end

  test "same series hash does not merge different label sets; overlapping retries deduplicate" do
    append("gauge", [{1, 1}, {1, 1}], %{"instance" => "a"})
    append("gauge", [{1, 2}], %{"instance" => "b"})
    assert {:ok, result} = instant("sum(gauge)", 1)
    assert values(result) == [[1.0, "3"]]
    append("gauge", [{1, 5}], %{"instance" => "a"})
    assert {:ok, result} = instant("gauge", 1)
    assert values(result) == [[1.0, "5"], [1.0, "2"]]
    assert result["warnings"] == ["Conflicting samples at the same timestamp were resolved using the maximum value."]
    append("gauge", [{20, 9}], %{"instance" => "a"})
    assert {:ok, result} = instant(~s/gauge{instance="a"}/, 30)
    assert values(result) == [[30.0, "9"]]
  end

  test "invalid times and excessive step counts fail before scanning" do
    for opts <- [
          %{start_ts_ns: 0},
          %{step_ns: 1, start_ts_ns: 0},
          %{step_ns: 0},
          %{end_ts_ns: "bad"},
          %{step_ns: 1, start_ts_ns: 0, end_ts_ns: 11_000},
          %{step_ns: 1, start_ts_ns: 10, end_ts_ns: 0}
        ] do
      assert {:error, _} = Evaluator.query("gauge", "acme", opts)
    end
  end

  test "sample, work, and result limits return errors instead of partial aggregates" do
    original = Application.get_env(:pulso, Evaluator)
    on_exit(fn -> Application.put_env(:pulso, Evaluator, original || []) end)
    append("gauge", [{10, 1}, {20, 2}, {30, 3}])
    Application.put_env(:pulso, Evaluator, max_samples: 2)
    assert {:error, :query_sample_limit} = instant("sum(gauge)", 30)
    Application.put_env(:pulso, Evaluator, max_work: 1)
    assert {:error, :query_work_limit} = instant("sum_over_time(gauge[30s])", 30)
    Application.put_env(:pulso, Evaluator, max_result_points: 1)

    assert {:error, :query_result_limit} =
             Evaluator.query("gauge", "acme", %{
               start_ts_ns: 20_000_000_000,
               end_ts_ns: 30_000_000_000,
               step_ns: 10_000_000_000
             })
  end
end
