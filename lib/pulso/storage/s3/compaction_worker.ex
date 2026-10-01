defmodule Pulso.Storage.S3.CompactionWorker do
  @moduledoc """
  Opt-in background maintenance for metrics tenants observed locally by ingest
  or queries. No recursive object listing is performed for tenant discovery.
  Cold tenants can be maintained explicitly with MetricsCompactor.compact/3.

  Enable only after every writer has been upgraded: `:compaction_enabled` defaults
  to false. Configure `:compaction_interval_ms` (60_000), `:compaction_options`,
  and `:compaction_cleanup_options` to tune limits. Scheduling has up to 10% jitter.
  Each tenant is isolated from errors and exceptions in the rest of the pass.
  """
  use GenServer

  alias Pulso.Storage.S3.ManifestCache
  alias Pulso.Storage.S3.MetricsCompactor

  require Logger

  def start_link(config), do: GenServer.start_link(__MODULE__, config, name: __MODULE__)

  @doc false
  def children(config) do
    if Map.get(config, :compaction_enabled, false), do: [{__MODULE__, config}], else: []
  end

  @impl true
  def init(config) do
    schedule(config)
    {:ok, config}
  end

  @impl true
  def handle_info(:compact, config) do
    ManifestCache.tenants("metrics") |> Enum.each(&compact_tenant(&1, config))
    schedule(config)
    {:noreply, config}
  end

  defp compact_tenant(tenant, config) do
    opts = Map.get(config, :compaction_options, [])
    cleanup_opts = Map.get(config, :compaction_cleanup_options, [])
    run_operation(fn -> MetricsCompactor.compact(tenant, config, opts) end, tenant, "merge")
    run_operation(fn -> MetricsCompactor.cleanup(tenant, config, cleanup_opts) end, tenant, "cleanup")
  end

  defp run_operation(operation, tenant, name) do
    report_result(operation.(), tenant, name)
  rescue
    exception -> Logger.warning("metrics compaction #{name} raised tenant=#{tenant}: #{Exception.message(exception)}")
  catch
    kind, reason -> Logger.warning("metrics compaction #{name} exited tenant=#{tenant}: #{inspect({kind, reason})}")
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
