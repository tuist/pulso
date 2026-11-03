defmodule PulsoWeb.IngestAdmissionTest do
  use PulsoWeb.ConnCase, async: true

  alias Pulso.IngestAdmission
  alias Pulso.Test.OTLPTraceFixture

  setup do
    # 8 slots: 2 reserved for each of metrics, logs and traces, 2 shared.
    start_supervised!(IngestAdmission)
    Pulso.Runtime.put_env(:pulso, IngestAdmission, max_concurrent: 8, reserved_per_signal: 2)
    :ok
  end

  defp hold(signal) do
    owner = self()

    pid =
      spawn(
        Pulso.Runtime.capture(fn ->
          send(owner, {:held, self(), IngestAdmission.acquire(signal)})
          receive do: (:stop -> :ok)
        end)
      )

    assert_receive {:held, ^pid, result}
    {pid, result}
  end

  defp post_signal(path, body, type) do
    build_conn() |> put_req_header("content-type", type) |> post(path, body)
  end

  defp loki, do: post_signal("/loki/api/v1/push", ~s({"streams":[]}), "application/json")
  defp traces, do: post_signal("/v1/traces", Pulso.JSON.encode!(OTLPTraceFixture.payload()), "application/json")

  defp remote_write do
    build_conn()
    |> put_req_header("content-type", "application/x-protobuf")
    |> put_req_header("content-encoding", "snappy")
    |> put_req_header("x-prometheus-remote-write-version", "0.1.0")
    |> post("/api/v1/write", "")
  end

  test "a saturated signal is refused with Retry-After while other signals keep their reserves" do
    # Metrics take their reserve plus the whole shared pool, and no more.
    assert for(_ <- 1..4, do: elem(hold(:metrics), 1)) == List.duplicate(:ok, 4)
    assert {_pid, {:error, :ingest_overloaded}} = hold(:metrics)

    conn = remote_write()
    assert conn.status == 429
    assert get_resp_header(conn, "retry-after") == ["1"]

    # Logs and traces are still admitted through their reserved slots.
    assert loki().status == 204
    assert traces().status == 200

    # A logs burst can use only its own reserve once the pool is gone,
    # never the traces reserve.
    {logs, :ok} = hold(:logs)
    {_, :ok} = hold(:logs)
    assert loki().status == 429
    assert traces().status == 200

    # A released slot returns to its own reserve.
    ref = Process.monitor(logs)
    send(logs, :stop)
    assert_receive {:DOWN, ^ref, :process, _, _}
    _ = :sys.get_state(Pulso.Runtime.whereis(IngestAdmission))
    assert IngestAdmission.in_flight(:logs) == 1
    assert loki().status == 204

    metrics = Pulso.SelfMetrics.render()
    assert metrics =~ ~s(pulso_ingest_admission_total{signal="metrics",outcome="admitted"} 4\n)
    assert metrics =~ ~s(pulso_ingest_admission_total{signal="metrics",outcome="rejected"} 2\n)
    assert metrics =~ ~s(pulso_ingest_admission_total{signal="logs",outcome="rejected"} 1\n)
    assert metrics =~ ~s(pulso_ingest_admission_total{signal="traces",outcome="rejected"} 0\n)
    assert metrics =~ ~s(pulso_ingest_admission_in_flight{signal="metrics"} 4\n)
    assert metrics =~ ~s(pulso_ingest_admission_in_flight{signal="logs"} 1\n)
  end

  test "handoff frees the slot only within the pending budget and returns its bytes on release" do
    Pulso.Runtime.put_env(:pulso, IngestAdmission, max_concurrent: 1, reserved_per_signal: 0, max_pending_bytes: 100)
    owner = self()

    waiter = fn bytes ->
      spawn(
        Pulso.Runtime.capture(fn ->
          :ok = IngestAdmission.acquire(:metrics)
          IngestAdmission.handoff(bytes)
          send(owner, {:handed_off, self()})
          receive do: (:stop -> :ok)
        end)
      )
    end

    first = waiter.(60)
    assert_receive {:handed_off, ^first}
    assert IngestAdmission.in_flight() == 0
    assert IngestAdmission.pending_bytes() == 60

    # Over budget: the second request keeps its slot, so the node is full again.
    second = waiter.(60)
    assert_receive {:handed_off, ^second}
    assert IngestAdmission.in_flight() == 1
    assert IngestAdmission.pending_bytes() == 60
    assert {_, {:error, :ingest_overloaded}} = hold(:logs)

    for pid <- [first, second] do
      ref = Process.monitor(pid)
      send(pid, :stop)
      assert_receive {:DOWN, ^ref, :process, _, _}
    end

    _ = :sys.get_state(Pulso.Runtime.whereis(IngestAdmission))
    assert IngestAdmission.in_flight() == 0
    assert IngestAdmission.pending_bytes() == 0
  end
end
