defmodule Pulso.Storage.S3.AppendBufferTest do
  use ExUnit.Case, async: false

  alias Pulso.Record.Log
  alias Pulso.Record.MetricSample
  alias Pulso.Storage.S3
  alias Pulso.Storage.S3.AppendBuffer
  alias Pulso.Storage.S3.AppendRegistry
  alias Pulso.Storage.S3.Manifest
  alias Pulso.Storage.S3.ManifestCache
  alias Pulso.Storage.S3.ManifestSupervision
  alias Pulso.Test.CompactionStore

  setup context do
    signal = Map.get(context, :signal, :logs)

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

    # Cached native clients leave keep-alive sockets open. All publications
    # are awaited below; do not spend 15s draining each idle socket at teardown.
    server =
      start_supervised!(
        {Bandit, plug: {CompactionStore, agent: agent}, port: 0, thousand_island_options: [shutdown_timeout: 100]}
      )

    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)

    config = %{
      bucket: "pulso",
      endpoint: "http://localhost:#{port}",
      region: "us-east-1",
      # Native clients cache connection pools by full config. Unique test
      # credentials prevent reusing a stale pool if the OS recycles a port.
      access_key_id: "test-#{System.unique_integer([:positive])}",
      secret_access_key: "test",
      allow_http: true,
      refresh_stale_ms: 0,
      ingest_flush_interval_ms: Map.get(context, :flush_interval_ms, 1000)
    }

    previous = Application.get_env(:pulso, S3)
    Application.put_env(:pulso, S3, config)

    on_exit(fn ->
      if previous, do: Application.put_env(:pulso, S3, previous), else: Application.delete_env(:pulso, S3)
    end)

    start_supervised!(ManifestSupervision)
    tasks = start_supervised!(Task.Supervisor)
    tenant = "buffer-#{System.unique_integer([:positive])}"
    key = {:crypto.hash(:sha256, :erlang.term_to_binary(config, [:deterministic])), tenant, signal}
    buffer = start_supervised!({AppendBuffer, key: key, signal: signal, tenant: tenant, config: config})
    :erlang.trace(buffer, true, [:receive])
    [{^buffer, admission}] = Registry.lookup(AppendRegistry, key)
    %{agent: agent, config: config, tasks: tasks, tenant: tenant, signal: signal, buffer: buffer, admission: admission}
  end

  defp enqueue(ctx, batches) do
    # Control the flush explicitly, without sleeps or a scheduler-timing race.
    token = make_ref()
    :sys.replace_state(ctx.buffer, fn state -> %{state | timer: {token, token}} end)

    tasks =
      Enum.map(batches, fn records ->
        Task.Supervisor.async_nolink(ctx.tasks, fn -> S3.append(ctx.signal, ctx.tenant, records) end)
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
    # Scraping reads reservation tables, even while the owner is blocked in I/O.
    assert AppendBuffer.stats() == {1, 2, bytes, 2}
    report = Pulso.Metrics.render()
    assert report =~ "pulso_ingest_buffer_reserved_calls 2\n"
    assert report =~ "pulso_ingest_buffer_input_bytes #{bytes}\n"
    assert report =~ "pulso_ingest_buffer_rows 2\n"
    Agent.update(ctx.agent, &%{&1 | barriers: %{}})
    send(writer, {:release_storage, manifest_key})
    assert Enum.all?(tasks, &(Task.await(&1, 10_000) == :ok))
    _ = :sys.get_state(ctx.buffer)
    assert :ets.lookup(ctx.admission, :pending) == [{:pending, 0, 0, 0}]
    assert AppendBuffer.stats() == {1, 0, 0, 0}
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
    key = Manifest.manifest_key(ctx.tenant, "logs")
    owner = self()
    Agent.update(ctx.agent, &%{&1 | barriers: %{{"PUT", key} => owner}})
    send(ctx.buffer, {:flush, token})
    assert_receive {:storage_barrier, writer, "PUT", ^key}, 5000
    assert :ets.lookup(ctx.admission, :pending) == [{:pending, 1, bytes, 1}]
    Agent.update(ctx.agent, &%{&1 | barriers: %{}})
    send(writer, {:release_storage, key})
    _ = :sys.get_state(ctx.buffer)
    assert :ets.lookup(ctx.admission, :pending) == [{:pending, 0, 0, 0}]
    assert %{etag: etag} = ManifestCache.get(ctx.tenant, "logs")
    assert is_binary(etag)
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

  defp heterogeneous_batches(signal, producers) do
    for producer <- 1..producers do
      for row <- 1..Enum.at([1, 17, 131, 509], rem(producer - 1, 4)) do
        id = producer * 1000 + row

        producer_record(signal, producer, id)
      end
    end
  end

  defp producer_record(:logs, producer, id) do
    %Log{
      timestamp_ns: id,
      service: "worker-#{rem(producer, 3)}",
      body: "request-#{id}",
      resource: %{"instance" => "host-#{producer}"}
    }
  end

  defp producer_record(:metrics, producer, id) do
    %MetricSample{
      timestamp_ns: id,
      value: id / 13,
      labels: %{"__name__" => "work", "job" => "worker-#{rem(producer, 3)}", "instance" => "host-#{producer}"}
    }
  end

  defp producer_label(:logs, record), do: record.service
  defp producer_label(:metrics, record), do: record.labels["job"]

  defp normalized(signal, records) do
    assert {:ok, blob, _, _} = S3.encode_segment(signal, records)
    assert {:ok, decoded} = S3.decode_segment(signal, blob, nil, nil, [])
    Enum.sort(decoded)
  end

  for signal <- [:logs, :metrics], producers <- [8, 32] do
    @tag signal: signal
    test "#{signal}: #{producers} heterogeneous producers preserve every row", ctx do
      batches = heterogeneous_batches(ctx.signal, unquote(producers))
      tasks = enqueue(ctx, batches)
      assert Enum.all?(tasks, &(Task.await(&1, 30_000) == :ok))
      assert segment_puts(ctx.agent) == 1
      assert {:ok, actual} = S3.query(ctx.signal, ctx.tenant, [])
      expected = normalized(ctx.signal, List.flatten(batches))
      assert Enum.sort(actual) == expected
      label = if ctx.signal == :logs, do: "service", else: "job"
      assert {:ok, selected} = S3.query(ctx.signal, ctx.tenant, matchers: [{label, :eq, "worker-1"}])
      matching = Enum.filter(expected, &(producer_label(ctx.signal, &1) == "worker-1"))
      assert Enum.sort(selected) == matching
      _ = :sys.get_state(ctx.buffer)
      assert :ets.lookup(ctx.admission, :pending) == [{:pending, 0, 0, 0}]
    end

    @tag signal: signal, flush_interval_ms: 0
    test "#{signal}: disabled coalescing publishes one segment per each of #{producers} producers", ctx do
      # As in the original holdout, publish a keyed bootstrap before the
      # burst so this checks direct appends, not first-write reconstruction.
      bootstrap = producer_record(ctx.signal, 99, 99_000)
      assert :ok = S3.append(ctx.signal, ctx.tenant, [bootstrap], idempotency_key: "bootstrap")
      puts_before = segment_puts(ctx.agent)
      batches = heterogeneous_batches(ctx.signal, unquote(producers))

      tasks =
        Enum.map(batches, fn records ->
          Task.Supervisor.async_nolink(ctx.tasks, fn -> S3.append(ctx.signal, ctx.tenant, records) end)
        end)

      assert Enum.all?(tasks, &(Task.await(&1, 30_000) == :ok))
      assert segment_puts(ctx.agent) - puts_before == unquote(producers)
      assert {:ok, actual} = S3.query(ctx.signal, ctx.tenant, [])
      assert Enum.sort(actual) == normalized(ctx.signal, [bootstrap | List.flatten(batches)])
      assert :sys.get_state(ctx.buffer).pending == []
      assert :ets.lookup(ctx.admission, :pending) == [{:pending, 0, 0, 0}]
    end

    @tag signal: signal
    test "#{signal}: one invalid request cannot poison #{producers - 1} valid producers", ctx do
      [first | rest] = heterogeneous_batches(ctx.signal, unquote(producers))
      [record | tail] = first
      field = if ctx.signal == :logs, do: :body, else: :value
      invalid = Map.put(record, field, make_ref())
      [bad | good] = enqueue(ctx, [[invalid | tail] | rest])
      assert {:error, {:encode_failed, _}} = Task.await(bad, 30_000)
      assert Enum.all?(good, &(Task.await(&1, 30_000) == :ok))
      assert segment_puts(ctx.agent) == unquote(producers) - 1
      assert {:ok, actual} = S3.query(ctx.signal, ctx.tenant, [])
      assert Enum.sort(actual) == normalized(ctx.signal, List.flatten(rest))
      _ = :sys.get_state(ctx.buffer)
      assert :ets.lookup(ctx.admission, :pending) == [{:pending, 0, 0, 0}]
    end

    @tag signal: signal
    test "#{signal}: publication failure does not retry #{producers} producers separately", ctx do
      key = Manifest.manifest_key(ctx.tenant, Atom.to_string(ctx.signal))
      Agent.update(ctx.agent, &%{&1 | faults: %{{"PUT", key} => {400, :before, 1}}})
      tasks = enqueue(ctx, heterogeneous_batches(ctx.signal, unquote(producers)))
      for task <- tasks, do: assert({:error, _} = Task.await(task, 30_000))
      assert segment_puts(ctx.agent) == 1
      refute Agent.get(ctx.agent, &Map.has_key?(&1.objects, key))
      _ = :sys.get_state(ctx.buffer)
      assert :ets.lookup(ctx.admission, :pending) == [{:pending, 0, 0, 0}]
    end
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
