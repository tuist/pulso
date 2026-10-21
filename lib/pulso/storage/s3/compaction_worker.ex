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
  use Pulso.Runtime.GenServer

  alias Pulso.Runtime.GenServer
  alias Pulso.Runtime.Task
  alias Pulso.Storage.S3.CompactionDiscovery
  alias Pulso.Storage.S3.CompactionOwnership
  alias Pulso.Storage.S3.CompactionSupervision
  alias Pulso.Storage.S3.CompactionTasks
  alias Pulso.Storage.S3.LogCompactor
  alias Pulso.Storage.S3.MetricsCompactor
  alias Pulso.Storage.S3.TraceCompactor

  require Logger

  def start_link(config), do: GenServer.start_link(__MODULE__, config, name: __MODULE__)

  @doc false
  def children(config) do
    if Map.get(config, :compaction_enabled, false) or CompactionOwnership.trace_maintenance_enabled?(config) or
         CompactionOwnership.log_maintenance_enabled?(config),
       do: [{CompactionSupervision, config}],
       else: []
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

  defp maintain_tenant(tenant, config), do: maintain_signals(CompactionOwnership.signals(config), tenant, config)
  defp maintain_signals([], _tenant, config), do: {:cont, config}

  defp maintain_signals([signal | rest], tenant, config) do
    case maintain_signal(tenant, signal, config) do
      {:cont, updated} -> maintain_signals(rest, tenant, updated)
      {:halt, updated} -> {:halt, updated}
    end
  end

  defp maintain_signal(tenant, signal, config) do
    if ready?(tenant, signal, config) and CompactionOwnership.local_owner?(tenant, signal, config),
      do: maintain_owned_tenant(tenant, signal, config),
      else: {:cont, config}
  end

  defp backoff_key(tenant, "metrics"), do: tenant
  defp backoff_key(tenant, "traces"), do: {tenant, "traces"}
  defp backoff_key(tenant, "logs"), do: {tenant, "logs"}

  defp maintain_owned_tenant(tenant, signal, config) do
    backoff = Map.get(config, :compaction_backoff, %{})
    key = backoff_key(tenant, signal)

    case compact_tenant(tenant, signal, config) do
      {:error, :timeout} ->
        deadline = System.monotonic_time(:millisecond) + Map.get(config, :compaction_retry_ms, 300_000)
        {:halt, Map.put(config, :compaction_backoff, Map.put(backoff, key, deadline))}

      _result ->
        {:cont, Map.put(config, :compaction_backoff, Map.delete(backoff, key))}
    end
  end

  defp ready?(tenant, signal, config) do
    deadline = config |> Map.get(:compaction_backoff, %{}) |> Map.get(backoff_key(tenant, signal), 0)
    deadline == 0 or deadline <= System.monotonic_time(:millisecond)
  end

  defp maintenance("metrics"), do: {MetricsCompactor, :compaction_options, :compaction_cleanup_options}
  defp maintenance("traces"), do: {TraceCompactor, :trace_compaction_options, :trace_compaction_cleanup_options}
  defp maintenance("logs"), do: {LogCompactor, :log_compaction_options, :log_compaction_cleanup_options}

  defp compact_tenant(tenant, signal, config) do
    {module, option_key, cleanup_key} = maintenance(signal)
    opts = Map.get(config, option_key, [])
    cleanup_opts = Map.get(config, cleanup_key, [])
    result = merge_while_progressing(module, tenant, config, opts, Map.get(config, :compaction_max_merges, 32))
    cleanup_if_owner(result, tenant, signal, config, module, cleanup_opts)
  end

  # One merge combines at most 32 segments, while frequent pushers (a log
  # shipper flushing every second) create more than that per interval. Keep
  # merging while each merge makes progress, within a per-pass budget, so
  # compaction can catch up instead of falling further behind.
  defp merge_while_progressing(module, tenant, config, opts, remaining) do
    result = run_operation(fn -> merge_operation(module, tenant, config, opts) end, tenant, "merge", config)

    case result do
      {:ok, %{merged: merged}} when merged > 0 and remaining > 1 ->
        if CompactionOwnership.local_owner?(tenant, signal_of(module), config),
          do: merge_while_progressing(module, tenant, config, opts, remaining - 1),
          else: result

      _ ->
        result
    end
  end

  defp signal_of(MetricsCompactor), do: "metrics"
  defp signal_of(TraceCompactor), do: "traces"
  defp signal_of(LogCompactor), do: "logs"

  defp merge_operation(TraceCompactor, tenant, config, opts), do: TraceCompactor.maintain(tenant, config, opts)
  defp merge_operation(module, tenant, config, opts), do: module.compact(tenant, config, opts)

  defp cleanup_if_owner({:error, :timeout} = result, _tenant, _signal, _config, _module, _opts), do: result

  defp cleanup_if_owner(result, tenant, signal, config, module, opts) do
    # Recheck after the merge: ownership may have moved while storage was busy.
    if CompactionOwnership.local_owner?(tenant, signal, config) do
      run_operation(fn -> module.cleanup(tenant, config, opts) end, tenant, "cleanup", config)
    else
      result
    end
  end

  defp run_operation(operation, tenant, name, config) do
    Pulso.SelfMetrics.track(:compaction, metric_dimension(name), fn ->
      do_run_operation(operation, tenant, name, config)
    end)
  end

  defp metric_dimension("merge"), do: :worker_merge
  defp metric_dimension("cleanup"), do: :worker_cleanup
  defp metric_dimension("discovery"), do: :worker_discovery

  defp do_run_operation(operation, tenant, name, config) do
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
        CompactionOwnership.leave(config)
        :telemetry.execute([:pulso, :compaction, :timeout], %{}, %{operation: name})
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
