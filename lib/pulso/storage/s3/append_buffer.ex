defmodule Pulso.Storage.S3.AppendBuffer do
  @moduledoc """
  Optional node-local coalescing for appends without an idempotency key.

  Reservations bound queued and executing input by caller count, estimated term
  bytes, and rows before a request enters the mailbox. Overflow uses the original
  unbuffered path; this is not ingest rate admission. Acknowledgments wait for the
  combined segment PUT and manifest CAS. Keyed requests never enter this process.
  """
  use Pulso.Runtime.GenServer

  alias Pulso.Runtime.GenServer
  alias Pulso.Runtime.Registry
  alias Pulso.Storage.S3
  alias Pulso.Storage.S3.AppendRegistry
  alias Pulso.Storage.S3.AppendSupervisor

  @max_callers 128
  @max_rows 100_000
  @max_bytes 10 * 1024 * 1024
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

    if rows > @max_rows or bytes > @max_bytes do
      :unbuffered
    else
      # Copy before reservation: a heap/watchdog kill during detachment must
      # not strand credits for a message the owner never received.
      records = Pulso.JSON.detach(records)

      with {:ok, pid, admission} <- ensure_started(signal, tenant, config),
           :ok <- reserve(admission, bytes, rows) do
        call(pid, {:append, records, bytes, rows, opts})
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

  defp call(pid, request) do
    GenServer.call(pid, request, 15_000)
  catch
    # A timed-out caller does NOT release its reservation: its message or
    # native publication can still be in progress. Only the owner releases it.
    :exit, {:timeout, {GenServer, :call, _}} -> {:error, :timeout}
    :exit, {:noproc, {GenServer, :call, _}} -> {:error, :owner_overloaded}
    :exit, {:normal, {GenServer, :call, _}} -> {:error, :owner_overloaded}
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
      timer: nil
    }

    {:ok, state, @idle_ms}
  end

  @impl true
  def handle_call({:append, records, bytes, rows, opts}, from, state) do
    pending = [{from, records, bytes, rows, opts} | state.pending]
    state = %{state | pending: pending}
    state = if state.timer, do: state, else: schedule(state)
    {:noreply, state, @idle_ms}
  end

  defp schedule(state) do
    token = make_ref()
    ref = Process.send_after(self(), {:flush, token}, Map.fetch!(state.config, :ingest_flush_interval_ms))
    %{state | timer: {ref, token}}
  end

  @impl true
  def handle_info({:flush, token}, %{timer: {_ref, token}} = state) do
    pending = Enum.reverse(state.pending)
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

    {:noreply, %{state | pending: [], timer: nil}, @idle_ms}
  end

  def handle_info({:flush, _obsolete}, state), do: {:noreply, state, @idle_ms}
  def handle_info(:timeout, %{pending: []} = state), do: {:stop, :normal, state}

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
