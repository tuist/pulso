defmodule Pulso.Storage.S3.AppendBufferTest do
  use ExUnit.Case, async: false

  alias Pulso.Record.Log
  alias Pulso.Storage.S3
  alias Pulso.Storage.S3.AppendBuffer
  alias Pulso.Storage.S3.AppendRegistry
  alias Pulso.Storage.S3.Manifest
  alias Pulso.Storage.S3.ManifestCache
  alias Pulso.Storage.S3.ManifestSupervision
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
      refresh_stale_ms: 0,
      ingest_flush_interval_ms: 1000
    }

    previous = Application.get_env(:pulso, S3)
    Application.put_env(:pulso, S3, config)

    on_exit(fn ->
      if previous, do: Application.put_env(:pulso, S3, previous), else: Application.delete_env(:pulso, S3)
    end)

    start_supervised!(ManifestSupervision)
    tasks = start_supervised!(Task.Supervisor)
    tenant = "buffer-#{System.unique_integer([:positive])}"
    key = {:crypto.hash(:sha256, :erlang.term_to_binary(config, [:deterministic])), tenant, :logs}
    buffer = start_supervised!({AppendBuffer, key: key, signal: :logs, tenant: tenant, config: config})
    :erlang.trace(buffer, true, [:receive])
    [{^buffer, admission}] = Registry.lookup(AppendRegistry, key)
    %{agent: agent, config: config, tasks: tasks, tenant: tenant, buffer: buffer, admission: admission}
  end

  defp enqueue(ctx, batches) do
    # Control the flush explicitly, without sleeps or a scheduler-timing race.
    token = make_ref()
    :sys.replace_state(ctx.buffer, fn state -> %{state | timer: {token, token}} end)

    tasks =
      Enum.map(batches, fn records ->
        Task.Supervisor.async_nolink(ctx.tasks, fn -> S3.append(:logs, ctx.tenant, records) end)
      end)

    buffer = ctx.buffer
    for _ <- batches, do: assert_receive({:trace, ^buffer, :receive, {:"$gen_call", _, {:append, _, _, _}}}, 5000)
    state = :sys.get_state(buffer)
    assert length(state.pending) == length(batches)
    {_ref, token} = state.timer
    send(buffer, {:flush, token})
    tasks
  end

  defp segment_puts(agent),
    do:
      Agent.get(
        agent,
        &Enum.count(&1.requests, fn {method, key} -> method == "PUT" and String.ends_with?(key, ".parquet") end)
      )

  test "concurrent unkeyed appends share a segment and ACK only after complete publication", ctx do
    # Include identical rows: buffering must not introduce deduplication.
    record = %Log{timestamp_ns: 1, service: "api", body: "same"}
    manifest_key = Manifest.manifest_key(ctx.tenant, "logs")
    owner = self()
    Agent.update(ctx.agent, &%{&1 | barriers: %{{"PUT", manifest_key} => owner}})
    tasks = enqueue(ctx, [[record], [record]])
    assert_receive {:storage_barrier, writer, "PUT", ^manifest_key}, 5000
    assert Enum.all?(tasks, &(Task.yield(&1, 0) == nil))
    assert ManifestCache.get(ctx.tenant, "logs") == nil
    assert [{:pending, 2, bytes, 2}] = :ets.lookup(ctx.admission, :pending)
    assert bytes > 0
    Agent.update(ctx.agent, &%{&1 | barriers: %{}})
    send(writer, {:release_storage, manifest_key})
    assert Enum.all?(tasks, &(Task.await(&1, 10_000) == :ok))
    _ = :sys.get_state(ctx.buffer)
    assert :ets.lookup(ctx.admission, :pending) == [{:pending, 0, 0, 0}]
    assert segment_puts(ctx.agent) == 1
    assert {:ok, [^record, ^record]} = S3.query(:logs, ctx.tenant, [])
  end

  test "keyed retries bypass buffers and keep original idempotent fingerprints", ctx do
    record = %Log{timestamp_ns: 1, service: "api"}
    for _ <- 1..2, do: assert(:ok = S3.append(:logs, ctx.tenant, [record], idempotency_key: "retry"))
    assert :sys.get_state(ctx.buffer).pending == []
    assert :ets.lookup(ctx.admission, :pending) == [{:pending, 0, 0, 0}]
    assert {:ok, [^record]} = S3.query(:logs, ctx.tenant, [])
  end

  test "byte, row and caller reservations cannot grow a full mailbox; overflow publishes directly", ctx do
    for counters <- [{:pending, 128, 0, 0}, {:pending, 0, 10 * 1024 * 1024, 0}, {:pending, 0, 0, 100_000}] do
      :ets.insert(ctx.admission, counters)
      assert :ok = S3.append(:logs, ctx.tenant, [%Log{timestamp_ns: 1, service: "direct"}])
      assert :ets.lookup(ctx.admission, :pending) == [counters]
      assert :sys.get_state(ctx.buffer).pending == []
    end

    assert segment_puts(ctx.agent) == 3
    assert {:ok, records} = S3.query(:logs, ctx.tenant, [])
    assert length(records) == 3
  end

  test "oversized terms bypass the buffer without truncating their contents", ctx do
    body = String.duplicate("x", 11 * 1024 * 1024)
    record = %Log{timestamp_ns: 1, service: "large", body: body}
    assert :ok = S3.append(:logs, ctx.tenant, [record])
    assert :sys.get_state(ctx.buffer).pending == []
    assert :ets.lookup(ctx.admission, :pending) == [{:pending, 0, 0, 0}]
    assert {:ok, [^record]} = S3.query(:logs, ctx.tenant, service: "large")
  end

  test "a caller timeout cannot release queued input reservations before publication", ctx do
    record = %Log{timestamp_ns: 1, service: "timed-out"}
    records = [record]
    bytes = :erlang.external_size(records)
    :ets.insert(ctx.admission, {:pending, 1, bytes, 1})
    token = make_ref()
    :sys.replace_state(ctx.buffer, fn state -> %{state | timer: {token, token}} end)

    caller =
      Task.Supervisor.async_nolink(ctx.tasks, fn ->
        try do
          GenServer.call(ctx.buffer, {:append, records, bytes, 1}, 0)
        catch
          :exit, {:timeout, _} -> :timed_out
        end
      end)

    assert Task.await(caller) == :timed_out
    assert length(:sys.get_state(ctx.buffer).pending) == 1
    assert :ets.lookup(ctx.admission, :pending) == [{:pending, 1, bytes, 1}]
    send(ctx.buffer, {:flush, token})
    _ = :sys.get_state(ctx.buffer)
    assert :ets.lookup(ctx.admission, :pending) == [{:pending, 0, 0, 0}]
    assert {:ok, [^record]} = S3.query(:logs, ctx.tenant, [])
  end

  test "invalid encoding does not poison a valid request sharing its flush", ctx do
    valid = %Log{timestamp_ns: 1, service: "api", body: "valid"}
    invalid = %{valid | body: make_ref()}
    [good, bad] = enqueue(ctx, [[valid], [invalid]])
    assert Task.await(good, 10_000) == :ok
    assert {:error, {:encode_failed, _}} = Task.await(bad, 10_000)
    assert segment_puts(ctx.agent) == 1
    assert {:ok, [^valid]} = S3.query(:logs, ctx.tenant, [])
    assert :ets.lookup(ctx.admission, :pending) == [{:pending, 0, 0, 0}]
  end

  test "storage errors fan out without retrying individual inputs after an ambiguous publication", ctx do
    key = Manifest.manifest_key(ctx.tenant, "logs")
    Agent.update(ctx.agent, &%{&1 | faults: %{{"PUT", key} => {400, :before, 1}}})
    tasks = enqueue(ctx, [[%Log{timestamp_ns: 1}], [%Log{timestamp_ns: 2}]])
    for task <- tasks, do: assert({:error, _} = Task.await(task, 10_000))
    assert segment_puts(ctx.agent) == 1
    assert ManifestCache.get(ctx.tenant, "logs") == nil
    assert :ets.lookup(ctx.admission, :pending) == [{:pending, 0, 0, 0}]
  end
end
