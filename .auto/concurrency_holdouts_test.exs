defmodule PulsoConcurrencyHoldouts do
  use ExUnit.Case, async: false

  alias Pulso.Record.{Log, MetricSample}
  alias Pulso.Storage.S3
  alias Pulso.Storage.S3.{AppendBuffer, AppendRegistry, Manifest, ManifestSupervision}

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

    meter = start_supervised!(Supervisor.child_spec({Agent, fn -> %{a: 0, b: 0, read: 0} end}, id: :meter))
    server = start_supervised!({Bandit, plug: {PulsoCostStore, agent: agent, meter: meter}, port: 0})
    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)

    config = %{
      bucket: "pulso",
      endpoint: "http://localhost:#{port}",
      region: "us-east-1",
      access_key_id: "heldout-#{System.unique_integer([:positive])}",
      secret_access_key: "test",
      allow_http: true,
      refresh_stale_ms: 0,
      ingest_flush_interval_ms: 50
    }

    previous = Application.get_env(:pulso, S3)
    Application.put_env(:pulso, S3, config)

    on_exit(fn ->
      if previous, do: Application.put_env(:pulso, S3, previous), else: Application.delete_env(:pulso, S3)
    end)

    start_supervised!(ManifestSupervision)
    tasks = start_supervised!(Task.Supervisor)
    %{agent: agent, meter: meter, config: config, tasks: tasks}
  end

  defp token(i), do: :crypto.hash(:sha256, Integer.to_string(i)) |> Base.encode16(case: :lower)

  defp batch(signal, producer) do
    # Unequal sizes, independent of the primary benchmark and buffer thresholds.
    size = Enum.at([1, 17, 131, 509], rem(producer - 1, 4))

    for row <- 1..size do
      id = producer * 1000 + row
      ts = 1_800_000_000_000_000_000 + id * 1_000_000

      case signal do
        :logs ->
          %Log{
            timestamp_ns: ts,
            service: "worker-#{rem(producer, 3)}",
            body: "request=#{token(id)}",
            resource: %{"region" => "east", "instance" => "host-#{producer}"},
            attributes: %{"context" => String.duplicate("peer=#{token(id)} ", 3), "row" => row}
          }

        :metrics ->
          %MetricSample{
            timestamp_ns: ts,
            value: id / 13,
            labels: %{
              "__name__" => "heldout_#{rem(row, 7)}",
              "job" => "worker-#{rem(producer, 3)}",
              "instance" => "host-#{producer}",
              "route" => "/work/#{rem(row, 11)}"
            }
          }
      end
    end
  end

  defp launch(ctx, signal, tenant, batches) do
    parent = self()

    tasks =
      Enum.map(batches, fn records ->
        Task.Supervisor.async_nolink(ctx.tasks, fn ->
          send(parent, {:producer_ready, self()})

          receive do
            :publish -> S3.append(signal, tenant, records)
          after
            10_000 -> raise "producer launch barrier timed out"
          end
        end)
      end)

    for %{pid: pid} <- tasks, do: assert_receive({:producer_ready, ^pid}, 10_000)
    tasks
  end

  defp segment_puts(agent, tenant) do
    Agent.get(agent, fn state ->
      Enum.count(state.requests, fn {method, key} ->
        method == "PUT" and String.starts_with?(key, "tenants/#{tenant}/") and String.ends_with?(key, ".parquet")
      end)
    end)
  end

  defp reference(signal, records) do
    assert {:ok, blob, _, _} = S3.encode_segment(signal, records)
    assert {:ok, decoded} = S3.decode_segment(signal, blob, nil, nil, [])
    Enum.sort(decoded)
  end

  test "natural 50ms batching across fan-in and heterogeneous producers", ctx do
    results =
      for signal <- [:logs, :metrics], producers <- [1, 2, 8, 32], interval <- [0, 50] do
        Application.put_env(:pulso, S3, %{ctx.config | ingest_flush_interval_ms: interval})
        tenant = "holdout-#{signal}-#{producers}-#{interval}"
        batches = for producer <- 1..producers, do: batch(signal, producer)
        bootstrap = batch(signal, 99)
        assert :ok = S3.append(signal, tenant, bootstrap, idempotency_key: "bootstrap")
        before = Agent.get(ctx.meter, & &1)
        puts_before = segment_puts(ctx.agent, tenant)
        tasks = launch(ctx, signal, tenant, batches)

        {wall_us, replies} =
          :timer.tc(fn ->
            Enum.each(tasks, &send(&1.pid, :publish))
            Enum.map(tasks, &Task.await(&1, 30_000))
          end)

        assert Enum.all?(replies, &(&1 == :ok))
        after_write = Agent.get(ctx.meter, & &1)
        puts = segment_puts(ctx.agent, tenant) - puts_before
        if interval == 0, do: assert(puts == producers)
        assert puts >= 1 and puts <= producers
        input = bootstrap ++ List.flatten(batches)
        expected = reference(signal, input)
        assert {:ok, actual} = S3.query(signal, tenant, [])
        assert Enum.sort(actual) == expected
        label = if signal == :logs, do: "service", else: "job"
        assert {:ok, selected} = S3.query(signal, tenant, matchers: [{label, :eq, "worker-1"}])

        matching =
          Enum.filter(expected, fn record ->
            if signal == :logs, do: record.service == "worker-1", else: record.labels["job"] == "worker-1"
          end)

        assert Enum.sort(selected) == matching

        stored =
          Agent.get(ctx.agent, fn state ->
            for {key, {_, body}} <- state.objects, String.starts_with?(key, "tenants/#{tenant}/"), reduce: 0 do
              bytes -> bytes + byte_size(body)
            end
          end)

        result = %{
          signal: signal,
          producers: producers,
          interval_ms: interval,
          rows: length(input) - length(bootstrap),
          segment_puts: puts,
          class_a: after_write.a - before.a,
          class_b: after_write.b - before.b,
          retained_bytes: stored,
          wall_us: wall_us
        }

        IO.puts("HOLDOUT " <> Pulso.JSON.encode!(result))
        result
      end

    File.write!(".auto/concurrency-holdouts-latest.json", Pulso.JSON.encode!(results))
  end

  defp controlled_flush(ctx, signal, tenant, batches) do
    key = {:crypto.hash(:sha256, :erlang.term_to_binary(ctx.config, [:deterministic])), tenant, signal}
    spec = Supervisor.child_spec({AppendBuffer, key: key, signal: signal, tenant: tenant, config: ctx.config}, id: key)
    buffer = start_supervised!(spec)
    [{^buffer, admission}] = Registry.lookup(AppendRegistry, key)
    token = make_ref()
    :sys.replace_state(buffer, fn state -> %{state | timer: {token, token}} end)
    :erlang.trace(buffer, true, [:receive])
    tasks = launch(ctx, signal, tenant, batches)
    Enum.each(tasks, &send(&1.pid, :publish))

    for _ <- batches do
      assert_receive {:trace, ^buffer, :receive, {:"$gen_call", _, {:append, _, _, _}}}, 10_000
    end

    assert length(:sys.get_state(buffer).pending) == length(batches)
    send(buffer, {:flush, token})
    replies = Enum.map(tasks, &Task.await(&1, 30_000))
    _ = :sys.get_state(buffer)
    assert :ets.lookup(admission, :pending) == [{:pending, 0, 0, 0}]
    replies
  end

  test "32 producers preserve good inputs when one request cannot encode", ctx do
    for signal <- [:logs, :metrics] do
      tenant = "heldout-invalid-#{signal}"
      batches = for producer <- 1..32, do: batch(signal, producer)
      [first | rest] = batches
      [record | tail] = first
      invalid = if signal == :logs, do: %{record | body: make_ref()}, else: %{record | value: make_ref()}
      replies = controlled_flush(ctx, signal, tenant, [[invalid | tail] | rest])
      assert {:error, {:encode_failed, _}} = hd(replies)
      assert Enum.all?(tl(replies), &(&1 == :ok))
      assert segment_puts(ctx.agent, tenant) == 31
      assert {:ok, actual} = S3.query(signal, tenant, [])
      assert Enum.sort(actual) == reference(signal, List.flatten(rest))
      IO.puts("HOLDOUT_FAILURE #{signal} producers=32 invalid=1 durable_good_requests=31")
    end
  end

  test "8 and 32 producer publication failures do not retry inputs separately", ctx do
    for signal <- [:logs, :metrics], producers <- [8, 32] do
      tenant = "heldout-publication-#{signal}-#{producers}"
      manifest_key = Manifest.manifest_key(tenant, Atom.to_string(signal))
      Agent.update(ctx.agent, &%{&1 | faults: Map.put(&1.faults, {"PUT", manifest_key}, {400, :before, 1})})
      batches = for producer <- 1..producers, do: batch(signal, producer)
      replies = controlled_flush(ctx, signal, tenant, batches)
      assert Enum.all?(replies, &match?({:error, _}, &1))
      assert segment_puts(ctx.agent, tenant) == 1
      refute Agent.get(ctx.agent, &Map.has_key?(&1.objects, manifest_key))
      IO.puts("HOLDOUT_FAILURE #{signal} producers=#{producers} publication_failure=1 segment_puts=1 acknowledgments=0")
    end
  end
end
