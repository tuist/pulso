defmodule Pulso.Storage.S3.AppendBuffer do
  @moduledoc """
  Optional node-local coalescing for appends without an idempotency key.

  Reservations bound queued and executing input by caller count, estimated term
  bytes, and rows before a request enters the mailbox. Overflow uses the original
  unbuffered path; this is not ingest rate admission. Acknowledgments wait for the
  combined segment PUT and manifest CAS. Keyed requests never enter this process.

  Flushes are pipelined: each one encodes, uploads and registers its segment in
  its own linked process, so up to 16 segment uploads per tenant and signal
  overlap while the manifest owner folds their registrations into shared CAS
  commits. A batch flushes when its window expires or once it reaches a quarter
  of the reservation bounds, whichever comes first.

  A reserved caller hands its admission slot to the node's pending budget
  (`Pulso.IngestAdmission.handoff/1`) while it waits; see that module.
  """
  use Pulso.Runtime.GenServer

  alias Pulso.Runtime.GenServer
  alias Pulso.Runtime.Registry
  alias Pulso.Storage.S3
  alias Pulso.Storage.S3.AppendRegistry
  alias Pulso.Storage.S3.AppendSupervisor

  @max_callers 512
  @max_rows 400_000
  @max_bytes 32 * 1024 * 1024
  # One flush carries at most a quarter of the reservation, so a full buffer
  # always has several uploads in flight rather than one large serialized one.
  @flush_rows div(@max_rows, 4)
  @flush_bytes div(@max_bytes, 4)
  @max_flushes 16
  @idle_ms 30_000

  @doc "Read node-local buffer reservations without messaging owners or doing storage I/O."
  def stats do
    if Pulso.Runtime.whereis(AppendRegistry) do
      AppendRegistry
      |> Registry.select([{{:_, :_, :"$1"}, [], [:"$1"]}])
      |> Enum.reduce({0, 0, 0, 0}, fn table, totals -> add_stats(table, totals) end)
    else
      {0, 0, 0, 0}
    end
  rescue
    ArgumentError -> {0, 0, 0, 0}
  end

  defp add_stats(table, {buffers, callers, bytes, rows} = totals) do
    case :ets.lookup(table, :pending) do
      [{:pending, reserved_callers, reserved_bytes, reserved_rows}] ->
        {buffers + 1, callers + reserved_callers, bytes + reserved_bytes, rows + reserved_rows}

      _ ->
        totals
    end
  rescue
    # An idle or failed buffer can disappear during the scrape.
    ArgumentError -> totals
  end

  def append(signal, tenant, records, config, opts \\ []) do
    rows = length(records)
    bytes = :erlang.external_size(records)

    # `{:unbuffered, records}` hands the records back for the direct path, so
    # callers need not keep their own reference while a buffered append waits.
    if rows > @max_rows or bytes > @max_bytes do
      {:unbuffered, records}
    else
      # Copy before reservation: a heap/watchdog kill during detachment must
      # not strand credits for a message the owner never received.
      records = Pulso.JSON.detach(records)

      with {:ok, pid, admission} <- ensure_started(signal, tenant, config),
           :ok <- reserve(admission, bytes, rows) do
        call(pid, {:append, records, bytes, rows, opts}, bytes)
      else
        :unbuffered -> {:unbuffered, records}
        other -> other
      end
    end
  end

  defp reserve(table, bytes, rows) do
    case :ets.update_counter(table, :pending, [{2, 1}, {3, bytes}, {4, rows}]) do
      [callers, total_bytes, total_rows]
      when callers <= @max_callers and total_bytes <= @max_bytes and total_rows <= @max_rows ->
        :ok

      _ ->
        release(table, bytes, rows)
        :unbuffered
    end
  rescue
    ArgumentError -> :unbuffered
  end

  defp release(table, bytes, rows), do: :ets.update_counter(table, :pending, [{2, -1}, {3, -bytes}, {4, -rows}])

  # `GenServer.call/3`, split so the caller can drop its copy of the records
  # once the buffer has it: send, collect garbage, hand the admission slot to
  # the pending budget, then wait. The reply alias discards late replies.
  defp call(pid, request, bytes) do
    mref = Process.monitor(pid, alias: :demonitor)
    send(pid, {:"$gen_call", {self(), [:alias | mref]}, request})
    # Charge what the waiting caller still retains (its compacted heap and
    # referenced binaries) plus the buffer's copy, about twice its external size.
    Pulso.IngestAdmission.handoff(retained_bytes() + 2 * bytes)

    receive do
      {[:alias | ^mref], reply} ->
        Process.demonitor(mref, [:flush])
        reply

      {:DOWN, ^mref, _, _, reason} when reason in [:noproc, :normal] ->
        {:error, :owner_overloaded}

      {:DOWN, ^mref, _, _, reason} ->
        exit({reason, {GenServer, :call, [pid, :append, 15_000]}})
    after
      # A timed-out caller does NOT release its reservation: its message or
      # native publication can still be in progress. Only the owner releases it.
      15_000 ->
        Process.demonitor(mref, [:flush])
        {:error, :timeout}
    end
  end

  defp retained_bytes do
    :erlang.garbage_collect()
    [memory: memory, binary: binaries] = Process.info(self(), [:memory, :binary])
    Enum.reduce(binaries, memory, fn {_id, size, _refc}, total -> total + size end)
  end

  defp ensure_started(signal, tenant, config) do
    identity = :crypto.hash(:sha256, :erlang.term_to_binary(config, [:deterministic]))
    key = {identity, tenant, signal}

    case Registry.lookup(AppendRegistry, key) do
      [{pid, admission}] when is_reference(admission) ->
        {:ok, pid, admission}

      [{_pid, _starting}] ->
        :unbuffered

      [] ->
        opts = [key: key, signal: signal, tenant: tenant, config: config]
        spec = %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}, restart: :transient}

        case DynamicSupervisor.start_child(Pulso.Runtime.name(AppendSupervisor), Pulso.Runtime.child_spec(spec)) do
          {:ok, _pid} -> lookup(key)
          {:error, {:already_started, _pid}} -> lookup(key)
          {:error, _} -> {:error, :owner_overloaded}
        end
    end
  end

  defp lookup(key) do
    case Registry.lookup(AppendRegistry, key) do
      [{pid, admission}] when is_reference(admission) -> {:ok, pid, admission}
      _ -> :unbuffered
    end
  end

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: {:via, Registry, {AppendRegistry, opts[:key]}})

  @impl true
  def init(opts) do
    admission = :ets.new(__MODULE__, [:set, :public, write_concurrency: true])
    true = :ets.insert(admission, {:pending, 0, 0, 0})
    Registry.update_value(AppendRegistry, opts[:key], fn _ -> admission end)

    state = %{
      signal: opts[:signal],
      tenant: opts[:tenant],
      config: opts[:config],
      admission: admission,
      pending: [],
      pending_bytes: 0,
      pending_rows: 0,
      flushing: 0,
      timer: nil
    }

    {:ok, state, @idle_ms}
  end

  @impl true
  def handle_call({:append, records, bytes, rows, opts}, from, state) do
    state = %{
      state
      | pending: [{from, records, bytes, rows, opts} | state.pending],
        pending_bytes: state.pending_bytes + bytes,
        pending_rows: state.pending_rows + rows
    }

    state =
      cond do
        state.pending_bytes >= @flush_bytes or state.pending_rows >= @flush_rows -> flush(state)
        state.timer -> state
        true -> schedule(state)
      end

    {:noreply, state, @idle_ms}
  end

  defp schedule(state) do
    token = make_ref()
    ref = Process.send_after(self(), {:flush, token}, Map.fetch!(state.config, :ingest_flush_interval_ms))
    %{state | timer: {ref, token}}
  end

  @impl true
  def handle_info({:flush, token}, %{timer: {_ref, token}} = state),
    do: {:noreply, flush(%{state | timer: nil}), @idle_ms}

  def handle_info({:flush, _obsolete}, state), do: {:noreply, state, @idle_ms}

  def handle_info(:flushed, state) do
    state = %{state | flushing: state.flushing - 1}
    # A batch that became due while every upload slot was busy goes now.
    state = if state.pending != [] and state.timer == nil, do: flush(state), else: state
    {:noreply, state, @idle_ms}
  end

  def handle_info(:timeout, %{pending: [], flushing: 0} = state), do: {:stop, :normal, state}
  def handle_info(:timeout, state), do: {:noreply, state, @idle_ms}

  # At the upload limit the batch keeps growing (within the reservation) and
  # goes when an upload finishes. The obsolete timer, if any, is ignored.
  defp flush(%{pending: []} = state), do: state
  defp flush(%{flushing: flushing} = state) when flushing >= @max_flushes, do: %{state | timer: nil}

  defp flush(state) do
    if state.timer, do: Process.cancel_timer(elem(state.timer, 0))
    pending = Enum.reverse(state.pending)
    buffer = self()

    # Linked: a crashed upload takes the buffer and its callers down, exactly
    # as an inline flush would, instead of leaving callers without a reply.
    {:ok, _pid} =
      Pulso.Runtime.Task.start_link(fn ->
        publish(pending, state)
        send(buffer, :flushed)
      end)

    %{state | pending: [], pending_bytes: 0, pending_rows: 0, flushing: state.flushing + 1, timer: nil}
  end

  defp publish(pending, state) do
    records = Enum.flat_map(pending, fn {_from, records, _bytes, _rows, _opts} -> records end)
    result = S3.append_unbuffered(state.signal, state.tenant, records, [split_admission_errors: true], state.config)

    # An invalid input must not poison other valid requests sharing a flush.
    # Encoding errors occur before any upload. Never fall back after a storage
    # error: a lost response could have published the combined segment already.
    if encoding_error?(result) do
      pending
      |> Pulso.Runtime.Task.async_stream(&split_reply(&1, state),
        max_concurrency: 8,
        ordered: false,
        timeout: :infinity
      )
      |> Stream.run()
    else
      # After PUT/CAS begins, a moving floor can reject the combined segment.
      # Fresh siblings must retry, not receive a permanent drop; do not split an
      # ambiguous publication into new segments that could duplicate its data.
      result =
        case {result, pending} do
          {{:error, :retention_expired}, [_, _ | _]} -> {:error, :retention_floor_advanced}
          _ -> result
        end

      Enum.each(pending, &reply_pending(&1, result, state))
    end
  end

  defp split_reply({_from, records, _bytes, _rows, opts} = pending, state) do
    result =
      try do
        S3.append_unbuffered(state.signal, state.tenant, records, opts, state.config)
      rescue
        _error -> {:error, :storage_unavailable}
      catch
        :exit, _reason -> {:error, :storage_unavailable}
      end

    reply_pending(pending, result, state)
  end

  defp reply_pending({from, _records, bytes, rows, _opts}, result, state) do
    # Native work keeps its reservation until it finishes, even if its caller
    # timed out. Replies are sent from the bounded tasks as each write completes.
    release(state.admission, bytes, rows)
    GenServer.reply(from, result)
  end

  # Retention preflight failures precede PUT, so split valid siblings safely.
  # Once PUT/CAS starts, storage errors remain ambiguous and never split.
  defp encoding_error?({:error, :payload_too_large}), do: true

  defp encoding_error?({:error, {:admission_rejected, reason}}) when reason in [:retention_expired, :timestamp_too_new],
    do: true

  defp encoding_error?({:error, {kind, _}}) when kind in [:encode_failed, :attribute_key_collision], do: true
  defp encoding_error?(_), do: false
end
