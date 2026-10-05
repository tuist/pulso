defmodule Pulso.Storage.S3.AppendBuffer do
  @moduledoc """
  Optional node-local coalescing for appends without an idempotency key.

  Reservations bound queued and executing input by caller count, estimated term
  bytes, and rows before a request enters the mailbox. Overflow uses the original
  unbuffered path; this is not ingest rate admission. Acknowledgments wait for the
  combined segment PUT and manifest CAS. Keyed requests never enter this process.
  """
  use GenServer

  alias Pulso.Storage.S3
  alias Pulso.Storage.S3.AppendRegistry
  alias Pulso.Storage.S3.AppendSupervisor

  @max_callers 128
  @max_rows 100_000
  @max_bytes 10 * 1024 * 1024
  @idle_ms 30_000

  @doc "Read node-local buffer reservations without messaging owners or doing storage I/O."
  def stats do
    if Process.whereis(AppendRegistry) do
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

  def append(signal, tenant, records, config) do
    rows = length(records)
    bytes = :erlang.external_size(records)

    if rows > @max_rows or bytes > @max_bytes do
      :unbuffered
    else
      with {:ok, pid, admission} <- ensure_started(signal, tenant, config),
           :ok <- reserve(admission, bytes, rows) do
        call(pid, {:append, records, bytes, rows})
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

        case DynamicSupervisor.start_child(AppendSupervisor, spec) do
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
  def handle_call({:append, records, bytes, rows}, from, state) do
    pending = [{from, records, bytes, rows} | state.pending]
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
    records = Enum.flat_map(pending, fn {_from, records, _bytes, _rows} -> records end)
    result = S3.append_unbuffered(state.signal, state.tenant, records, [], state.config)

    # An invalid input must not poison other valid requests sharing a flush.
    # Encoding errors occur before any upload. Never fall back after a storage
    # error: a lost response could have published the combined segment already.
    results =
      if encoding_error?(result) do
        Enum.map(pending, fn {_from, records, _bytes, _rows} ->
          S3.append_unbuffered(state.signal, state.tenant, records, [], state.config)
        end)
      else
        List.duplicate(result, length(pending))
      end

    Enum.zip(pending, results)
    |> Enum.each(fn {{from, _records, bytes, rows}, result} ->
      release(state.admission, bytes, rows)
      GenServer.reply(from, result)
    end)

    {:noreply, %{state | pending: [], timer: nil}, @idle_ms}
  end

  def handle_info({:flush, _obsolete}, state), do: {:noreply, state, @idle_ms}
  def handle_info(:timeout, %{pending: []} = state), do: {:stop, :normal, state}

  defp encoding_error?({:error, {kind, _}}) when kind in [:encode_failed, :attribute_key_collision], do: true
  defp encoding_error?(_), do: false
end
