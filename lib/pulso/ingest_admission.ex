defmodule Pulso.IngestAdmission do
  @moduledoc """
  Node-local bound on concurrently processed ingest requests.

  Per-request limits cap one request's body, records and heap, but not how
  many run at once: an exporter's retry backlog after a restart can otherwise
  decode many large batches in parallel and exhaust the node's memory. Above
  `:max_concurrent` (`PULSO_INGEST_MAX_CONCURRENT_REQUESTS`), new requests are
  refused with a retryable 429 instead; exporters back off and retry, possibly
  on another instance. This is a disposable per-instance optimization, never
  cluster state.

  A slot belongs to the process handling the request. HTTP/1.1 connection
  processes handle one request at a time, so a request that raises before its
  response does not leak a slot: the connection's next request reuses it, and
  the slot is released when the process exits.
  """
  use Pulso.Runtime.GenServer

  alias Pulso.Runtime.GenServer

  @table __MODULE__
  @default_max 32

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  defp table, do: Pulso.Runtime.table(@table)

  @doc "Take this process's slot, or `{:error, :ingest_overloaded}` when the node is full."
  @spec acquire() :: :ok | {:error, :ingest_overloaded}
  def acquire do
    pid = self()

    if :ets.insert_new(table(), {{:held, pid}}) do
      if :ets.update_counter(table(), :in_flight, {2, 1}, {:in_flight, 0}) > max() do
        release()
        {:error, :ingest_overloaded}
      else
        GenServer.cast(__MODULE__, {:monitor, pid})
        :ok
      end
    else
      :ok
    end
  rescue
    # Not started (some test runtimes and tools): never fail ingest.
    ArgumentError -> :ok
  end

  @doc "Release this process's slot, if it holds one."
  def release(pid \\ self()) do
    if :ets.take(table(), {:held, pid}) != [], do: :ets.update_counter(table(), :in_flight, {2, -1})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc "Requests currently holding a slot on this node."
  def in_flight do
    case :ets.lookup(table(), :in_flight) do
      [{_, count}] -> count
      [] -> 0
    end
  rescue
    ArgumentError -> 0
  end

  defp max, do: Keyword.get(Pulso.Runtime.get_env(:pulso, __MODULE__, []), :max_concurrent, @default_max)

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
