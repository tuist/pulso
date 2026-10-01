defmodule Pulso.Storage.S3.CompactionWorker do
  @moduledoc """
  Opt-in background metrics maintenance on the rendezvous owner. Durable
  manifest discovery includes tenants never observed by this node. Membership
  disagreement can overlap work; conditional publication keeps those races safe.

  Enable only after every writer has been upgraded: `:compaction_enabled` defaults
  to false. Configure `:compaction_interval_ms` (60_000), `:compaction_options`,
  and `:compaction_cleanup_options` to tune limits. Scheduling has up to 10% jitter.
  Each tenant is isolated from errors and exceptions in the rest of the pass.
  """
  use GenServer

  alias Pulso.Storage.S3.CompactionDiscovery
  alias Pulso.Storage.S3.CompactionOwnership
  alias Pulso.Storage.S3.CompactionSupervision
  alias Pulso.Storage.S3.CompactionTasks
  alias Pulso.Storage.S3.MetricsCompactor

  require Logger

  def start_link(config), do: GenServer.start_link(__MODULE__, config, name: __MODULE__)

  @doc false
  def children(config) do
    if Map.get(config, :compaction_enabled, false), do: [{CompactionSupervision, config}], else: []
  end

  @impl true
  def init(config) do
    if Task.Supervisor.children(CompactionTasks) == [], do: :ok = CompactionOwnership.join(config)
    schedule(config)
    {:ok, config}
  end

  @impl true
  def handle_info(:compact, config) do
    # Timed-out work may still be inside a native call. Do not rejoin or admit
    # more work until the operation actually leaves the dedicated supervisor.
    config =
      if Task.Supervisor.children(CompactionTasks) == [] do
        :ok = CompactionOwnership.join_once(config)
        run_pass(config)
      else
        Logger.warning("metrics compaction remains withdrawn while an earlier task is still running")
        config
      end

    schedule(config)
    {:noreply, config}
  end

  @impl true
  def handle_info(_message, config), do: {:noreply, config}

  defp run_pass(config) do
    case run_operation(fn -> CompactionDiscovery.tenants(config) end, "*", "discovery", config) do
      {:ok, tenants} -> Enum.reduce_while(tenants, config, &maintain_tenant/2)
      _error -> config
    end
  end

  defp maintain_tenant(tenant, config) do
    if ready?(tenant, config) and CompactionOwnership.local_owner?(tenant, "metrics", config) do
      maintain_owned_tenant(tenant, config)
    else
      {:cont, config}
    end
  end

  defp maintain_owned_tenant(tenant, config) do
    backoff = Map.get(config, :compaction_backoff, %{})

    case compact_tenant(tenant, config) do
      {:error, :timeout} ->
        deadline = System.monotonic_time(:millisecond) + Map.get(config, :compaction_retry_ms, 300_000)
        {:halt, Map.put(config, :compaction_backoff, Map.put(backoff, tenant, deadline))}

      _result ->
        {:cont, Map.put(config, :compaction_backoff, Map.delete(backoff, tenant))}
    end
  end

  defp ready?(tenant, config) do
    deadline = config |> Map.get(:compaction_backoff, %{}) |> Map.get(tenant, 0)
    deadline == 0 or deadline <= System.monotonic_time(:millisecond)
  end

  defp compact_tenant(tenant, config) do
    opts = Map.get(config, :compaction_options, [])
    cleanup_opts = Map.get(config, :compaction_cleanup_options, [])
    result = run_operation(fn -> MetricsCompactor.compact(tenant, config, opts) end, tenant, "merge", config)
    cleanup_if_owner(result, tenant, config, cleanup_opts)
  end

  defp cleanup_if_owner({:error, :timeout} = result, _tenant, _config, _opts), do: result

  defp cleanup_if_owner(result, tenant, config, opts) do
    # Recheck after the merge: ownership may have moved while storage was busy.
    if CompactionOwnership.local_owner?(tenant, "metrics", config) do
      run_operation(fn -> MetricsCompactor.cleanup(tenant, config, opts) end, tenant, "cleanup", config)
    else
      result
    end
  end

  defp run_operation(operation, tenant, name, config) do
    task = Task.Supervisor.async_nolink(CompactionTasks, fn -> execute_operation(operation, tenant, name) end)

    case Task.yield(task, operation_timeout(config, name)) do
      {:ok, result} ->
        result

      {:exit, reason} ->
        report_failure(tenant, name, reason)

      nil ->
        # Keep the task supervised until it finishes. Killing a process inside
        # a dirty native call can signal termination before native work ends,
        # hiding it from admission checks. Late publication remains conditional.
        Process.demonitor(task.ref, [:flush])
        :pg.leave(CompactionOwnership.scope(), CompactionOwnership.group(config), self())
        report_failure(tenant, name, :timeout)
    end
  end

  defp operation_timeout(config, "discovery"), do: Map.get(config, :compaction_discovery_timeout_ms, 60_000)
  defp operation_timeout(config, _operation), do: Map.get(config, :compaction_timeout_ms, 30_000)

  defp report_failure(tenant, name, reason) do
    Logger.warning("metrics compaction #{name} failed tenant=#{tenant}: #{inspect(reason)}")
    {:error, reason}
  end

  defp execute_operation(operation, tenant, name) do
    result = operation.()
    report_result(result, tenant, name)
    result
  rescue
    exception ->
      Logger.warning("metrics compaction #{name} raised tenant=#{tenant}: #{Exception.message(exception)}")
      {:error, :operation_raised}
  catch
    kind, reason ->
      Logger.warning("metrics compaction #{name} exited tenant=#{tenant}: #{inspect({kind, reason})}")
      {:error, :operation_exited}
  end

  defp report_result({:error, :not_found}, tenant, operation) do
    Logger.debug("metrics compaction #{operation} missing object tenant=#{tenant}")
  end

  defp report_result({:ok, _}, _tenant, _operation), do: :ok

  defp report_result({:error, reason}, tenant, operation) do
    Logger.warning("metrics compaction #{operation} failed tenant=#{tenant}: #{inspect(reason)}")
  end

  defp schedule(config) do
    interval = Map.get(config, :compaction_interval_ms, 60_000)
    jitter = :rand.uniform(max(div(interval, 10), 1)) - 1
    Process.send_after(self(), :compact, interval + jitter)
  end
end
