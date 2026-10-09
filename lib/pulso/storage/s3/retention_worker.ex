defmodule Pulso.Storage.S3.RetentionWorker do
  @moduledoc "Bounded, opt-in retention maintenance; rendezvous is an optimization, never a lease."
  use Pulso.Runtime.GenServer

  alias Pulso.ObjectStore
  alias Pulso.Rendezvous
  alias Pulso.Runtime.GenServer
  alias Pulso.Runtime.ProcessGroup
  alias Pulso.Runtime.Task
  alias Pulso.Storage.S3.ManifestCache
  alias Pulso.Storage.S3.Retention
  alias Pulso.Storage.S3.RetentionScope
  alias Pulso.Storage.S3.RetentionSupervision
  alias Pulso.Storage.S3.RetentionTasks

  require Logger

  @scope RetentionScope

  def start_link(config), do: GenServer.start_link(__MODULE__, config, name: __MODULE__)
  def children(config), do: if(Retention.configured?(config), do: [{RetentionSupervision, config}], else: [])
  def group(config), do: {__MODULE__, config[:endpoint], config[:region], config[:bucket]}

  def local_owner?(tenant, signal, config) do
    members = ProcessGroup.get_members(@scope, group(config)) |> Enum.map(&node/1)
    Rendezvous.owner(["retention", tenant, signal], members) == node()
  end

  @impl true
  def init(config) do
    if config[:retention_mode] != "paused", do: :ok = ProcessGroup.join(@scope, group(config), self())
    schedule(config)
    {:ok, %{config: config, queue: [], discovery: nil, local_cursor: nil, action: 0}}
  end

  @impl true
  def handle_info(:retain, state) do
    state =
      if Task.Supervisor.children(RetentionTasks) == [] and state.config[:retention_mode] != "paused" do
        if self() not in ProcessGroup.get_local_members(@scope, group(state.config)),
          do: :ok = ProcessGroup.join(@scope, group(state.config), self())

        run_pass(state)
      else
        if state.config[:retention_mode] == "paused" and
             self() in ProcessGroup.get_local_members(@scope, group(state.config)),
           do: :ok = ProcessGroup.leave(@scope, group(state.config), self())

        state
      end

    schedule(state.config)
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp run_pass(state) do
    # Reserve room for durable discovery even under sustained local activity.
    local_slots = min(8, div(64 - length(state.queue), 2))

    {chosen, local_cursor} =
      if local_slots == 0,
        do: {[], state.local_cursor},
        else: ManifestCache.scopes_page(state.local_cursor, local_slots)

    queue = Enum.uniq(state.queue ++ chosen)
    # Never advance durable discovery past scopes that did not fit in the queue.
    slots = min(16, div(64 - length(queue), 2))

    discovery =
      if slots == 0,
        do: {:ok, [], state.discovery},
        else: run(fn -> ObjectStore.discover_tenants(state.config, state.discovery, slots) end, state.config)

    case discovery do
      {:ok, tenants, cursor} ->
        discovered = for tenant <- tenants, signal <- ["logs", "metrics"], do: {tenant, signal}
        queue = Enum.uniq(queue ++ discovered)
        state = %{state | discovery: cursor, local_cursor: local_cursor, queue: queue}
        maintain(state, 8, System.monotonic_time(:millisecond) + Map.get(state.config, :retention_timeout_ms, 30_000))

      error ->
        Logger.warning("retention discovery failed: #{inspect(error)}")
        state
    end
  end

  defp maintain(%{queue: []} = state, _remaining, _deadline), do: state
  defp maintain(state, 0, _deadline), do: state

  defp maintain(%{queue: [{tenant, signal} | rest]} = state, remaining, deadline) do
    config = state.config

    if System.monotonic_time(:millisecond) >= deadline do
      state
    else
      result = maintain_scope(tenant, signal, config)

      if actionable_error?(result),
        do: Logger.warning("retention tenant=#{tenant} signal=#{signal}: #{inspect(result)}")

      if result == {:error, :retention_timeout},
        do: state,
        else: maintain(%{state | queue: rest}, remaining - 1, deadline)
    end
  end

  defp maintain_scope(tenant, signal, config) do
    if local_owner?(tenant, signal, config) do
      inner =
        config
        |> Map.put(:retention_timeout_ms, max(1, div(Map.get(config, :retention_timeout_ms, 30_000) * 4, 5)))
        |> Map.put(:retention_migration_notify, self())
        |> Map.put(:retention_bounded_read, true)

      run(fn -> run_stages(tenant, signal, inner) end, config)
    else
      :ok
    end
  end

  # Every stage runs, in order, even when an earlier one fails.
  defp run_stages(tenant, signal, config) do
    results = [
      Retention.advance(tenant, signal, config),
      Retention.cleanup(tenant, signal, config),
      Retention.cleanup_retired(tenant, signal, config),
      Retention.sweep(tenant, signal, config)
    ]

    Enum.find(results, &actionable_error?/1) || :ok
  end

  defp actionable_error?({:error, reason}) when reason in [:not_found, :retention_migration_required], do: false
  defp actionable_error?({:error, _}), do: true
  defp actionable_error?(_), do: false

  defp run(fun, config) do
    task =
      Task.Supervisor.async_nolink(RetentionTasks, fn ->
        heap = if config[:retention_mode] == "enforce", do: 64_000_000, else: 4_000_000
        Process.flag(:max_heap_size, %{size: heap, kill: true, error_logger: false, include_shared_binaries: true})

        try do
          fun.()
        rescue
          error -> {:error, {:retention_exception, Exception.message(error)}}
        end
      end)

    case Task.yield(task, Map.get(config, :retention_timeout_ms, 30_000)) do
      {:ok, result} ->
        result

      {:exit, reason} ->
        {:error, {:retention_task_exit, reason}}

      nil ->
        receive do
          {:retention_migration, pid} when pid == task.pid ->
            timeout = Map.get(config, :retention_migration_timeout_ms, 600_000)

            case Task.yield(task, timeout) do
              {:ok, result} -> result
              {:exit, reason} -> {:error, {:retention_task_exit, reason}}
              nil -> withdraw(task, config)
            end
        after
          0 -> withdraw(task, config)
        end
    end
  rescue
    RuntimeError -> {:error, :retention_busy}
  end

  defp withdraw(task, config) do
    Process.demonitor(task.ref, [:flush])
    ProcessGroup.leave(@scope, group(config), self())
    {:error, :retention_timeout}
  end

  defp schedule(config) do
    interval = Map.get(config, :retention_interval_ms, 30_000)
    Process.send_after(self(), :retain, interval + :rand.uniform(max(1, div(interval, 10))))
  end
end
