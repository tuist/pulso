defmodule Pulso.IngestAdmission do
  @moduledoc """
  Node-local bound on concurrently processed ingest requests, shared fairly
  between signals.

  Per-request limits cap one request's body, records and heap, but not how
  many run at once: an exporter's retry backlog after a restart can otherwise
  decode many large batches in parallel and exhaust the node's memory. At most
  `:max_concurrent` (`PULSO_INGEST_MAX_CONCURRENT_REQUESTS`) requests hold a
  slot; further requests are refused with a retryable 429 instead, and
  exporters back off and retry, possibly on another instance.

  ## Fairness

  Slots are split by signal class (`:metrics`, `:logs`, `:traces`). Each class
  owns `:reserved_per_signal` slots (`PULSO_INGEST_RESERVED_REQUESTS_PER_SIGNAL`,
  default an eighth of the slots) that no other class can take; the rest form a
  shared pool any class may borrow from. A remote-write replay can therefore
  hold its reserve plus the whole shared pool, but never the reserves that keep
  logs and traces admitted. Every counter is updated atomically, so the global
  bound and every reserve are exact.

  ## Pending publication handoff

  A slot bounds decoding memory. Once a request's records are queued in a
  bounded ingest buffer, it only waits for the durable segment PUT and manifest
  CAS. `handoff/1` then moves the request from its slot into a node-wide
  pending budget charged by its estimated record bytes (`:max_pending_bytes`,
  `PULSO_INGEST_MAX_PENDING_BYTES`), freeing the slot for the next request.
  When the budget is full the request keeps its slot while it waits, exactly
  as without handoff, so node memory stays bounded by slots plus the budget.
  The acknowledgment is still sent only after durable publication.

  A slot or pending charge belongs to the process handling the request. HTTP/1.1
  connection processes handle one request at a time, so a request that raises
  before its response does not leak: the connection's next request replaces
  it, and everything is released when the process exits. This is a disposable
  per-instance optimization, never cluster state.
  """
  use Pulso.Runtime.GenServer

  alias Pulso.Runtime.GenServer

  @table __MODULE__
  @default_max 32
  @default_max_pending_bytes 64 * 1024 * 1024
  @signals [:metrics, :logs, :traces]

  def signals, do: @signals

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  defp table, do: Pulso.Runtime.table(@table)

  @doc """
  Take this process's slot for `signal`, or `{:error, :ingest_overloaded}`
  when neither the signal's reserve nor the shared pool has room.
  """
  @spec acquire(atom()) :: :ok | {:error, :ingest_overloaded}
  def acquire(signal \\ :metrics) when signal in @signals do
    pid = self()
    table = table()

    case :ets.lookup(table, {:held, pid}) do
      [{_, {:slot, _signal, _pool}}] ->
        :ok

      held ->
        # A request that raised after handoff leaves its pending charge behind.
        if held != [], do: release(pid)
        take(table, pid, signal)
    end
  rescue
    # Not started (some test runtimes and tools): never fail ingest.
    ArgumentError -> :ok
  end

  defp take(table, pid, signal) do
    %{reserved: reserved, shared: shared} = limits()

    pool =
      cond do
        claim(table, {:reserved, signal}, reserved) -> :reserved
        claim(table, :shared, shared) -> :shared
        true -> nil
      end

    if pool do
      :ets.insert(table, {{:held, pid}, {:slot, signal, pool}})
      :ets.update_counter(table, {:in_flight, signal}, {2, 1}, {{:in_flight, signal}, 0})
      count(table, signal, :admitted)
      GenServer.cast(__MODULE__, {:monitor, pid})
      :ok
    else
      count(table, signal, :rejected)
      {:error, :ingest_overloaded}
    end
  end

  defp claim(table, key, limit) do
    if :ets.update_counter(table, key, {2, 1}, {key, 0}) <= limit do
      true
    else
      :ets.update_counter(table, key, {2, -1})
      false
    end
  end

  @doc """
  Move this process from its slot to the pending-publication budget, charging
  `bytes`. Without a slot, or when the budget is full, nothing changes.
  """
  @spec handoff(non_neg_integer()) :: :ok
  def handoff(bytes) when is_integer(bytes) and bytes >= 0 do
    table = table()
    pid = self()

    with [{_, {:slot, signal, _pool}}] <- :ets.lookup(table, {:held, pid}),
         true <- claim_bytes(table, bytes) do
      free_slot(table, pid)
      :ets.insert(table, {{:held, pid}, {:pending, signal, bytes}})
      count(table, signal, :handed_off)
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  defp claim_bytes(table, bytes) do
    if :ets.update_counter(table, :pending_bytes, {2, bytes}, {:pending_bytes, 0}) <= max_pending_bytes() do
      true
    else
      :ets.update_counter(table, :pending_bytes, {2, -bytes})
      false
    end
  end

  @doc "Release this process's slot or pending charge, if it holds one."
  def release(pid \\ self()) do
    table = table()

    case :ets.take(table, {:held, pid}) do
      [{_, {:slot, signal, pool}}] ->
        :ets.update_counter(table, pool_key(pool, signal), {2, -1})
        :ets.update_counter(table, {:in_flight, signal}, {2, -1})

      [{_, {:pending, _signal, bytes}}] ->
        :ets.update_counter(table, :pending_bytes, {2, -bytes})

      [] ->
        :ok
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  defp free_slot(table, pid) do
    [{_, {:slot, signal, pool}}] = :ets.lookup(table, {:held, pid})
    :ets.update_counter(table, pool_key(pool, signal), {2, -1})
    :ets.update_counter(table, {:in_flight, signal}, {2, -1})
  end

  defp pool_key(:reserved, signal), do: {:reserved, signal}
  defp pool_key(:shared, _signal), do: :shared

  @doc "Requests currently holding a slot on this node, in total or for one signal."
  def in_flight, do: Enum.sum(Enum.map(@signals, &in_flight/1))

  def in_flight(signal) when signal in @signals, do: counter({:in_flight, signal})

  @doc "Estimated record bytes of requests waiting for publication without a slot."
  def pending_bytes, do: counter(:pending_bytes)

  defp counter(key) do
    case :ets.lookup(table(), key) do
      [{_, value}] -> value
      [] -> 0
    end
  rescue
    ArgumentError -> 0
  end

  @doc "Node-local admission counters and gauges for the metrics endpoint."
  def stats do
    for signal <- @signals, into: %{} do
      {signal,
       %{
         in_flight: in_flight(signal),
         admitted: counter({:outcome, signal, :admitted}),
         rejected: counter({:outcome, signal, :rejected}),
         handed_off: counter({:outcome, signal, :handed_off})
       }}
    end
  end

  defp count(table, signal, outcome) do
    key = {:outcome, signal, outcome}
    :ets.update_counter(table, key, {2, 1}, {key, 0})
  end

  @doc false
  def limits do
    config = Pulso.Runtime.get_env(:pulso, __MODULE__, [])
    max = Keyword.get(config, :max_concurrent, @default_max)
    # Reserves never exceed the bound: every class keeps at least its reserve.
    reserved = config |> Keyword.get(:reserved_per_signal, div(max, 8)) |> min(div(max, length(@signals)))
    %{max: max, reserved: reserved, shared: max - reserved * length(@signals)}
  end

  defp max_pending_bytes do
    Keyword.get(Pulso.Runtime.get_env(:pulso, __MODULE__, []), :max_pending_bytes, @default_max_pending_bytes)
  end

  @impl true
  def init(_opts) do
    :ets.new(table(), [:named_table, :public, :set, write_concurrency: true])
    {:ok, MapSet.new()}
  end

  @impl true
  def handle_cast({:monitor, pid}, monitored) do
    if MapSet.member?(monitored, pid) do
      {:noreply, monitored}
    else
      Process.monitor(pid)
      {:noreply, MapSet.put(monitored, pid)}
    end
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, monitored) do
    release(pid)
    {:noreply, MapSet.delete(monitored, pid)}
  end
end
