defmodule Pulso.Alerting.Worker do
  @moduledoc "Opt-in disposable native evaluator. Membership reduces work; rule-head CAS enforces correctness."
  use GenServer

  alias Pulso.Alerting.{Canonical, Evaluator, Repository}
  alias Pulso.Alerting.Membership
  alias Pulso.Alerting.Notifier
  alias Pulso.Alerting.Tasks
  alias Pulso.Storage.S3

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    config = Keyword.get(opts, :store_config, Application.get_env(:pulso, S3, [])) |> Map.new()
    group = {:alerting, Canonical.hash(Enum.map([:bucket, :endpoint, :region], &Map.get(config, &1)))}
    :ok = :pg.join(Membership, group, self())
    send(self(), :tick)
    {:ok, %{opts: opts, group: group, task: nil, last_pass: nil}}
  end

  @impl true
  def handle_info(:tick, %{task: nil} = state) do
    nodes = :pg.get_members(Membership, state.group) |> Enum.map(&node/1) |> Enum.uniq()

    task =
      Task.Supervisor.async_nolink(Tasks, fn ->
        Process.flag(:max_heap_size, %{size: 8_000_000, kill: true, error_logger: false, include_shared_binaries: true})
        run_once(state.opts, nodes)
      end)

    {:noreply, %{state | task: task.ref}}
  end

  def handle_info({ref, result}, %{task: ref} = state) do
    Process.demonitor(ref, [:flush])
    schedule(state.opts)
    {:noreply, %{state | task: nil, last_pass: result}}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{task: ref} = state) do
    schedule(state.opts)
    {:noreply, %{state | task: nil, last_pass: {:error, :pass_failed}}}
  end

  def handle_info(_, state), do: {:noreply, state}

  def run_once(opts, nodes) do
    store = Keyword.get(opts, :object_store, Pulso.ObjectStore)
    config = Keyword.get(opts, :store_config, Application.get_env(:pulso, S3, [])) |> Map.new()

    repo_opts = [
      store: store,
      store_config: config,
      evaluation_enabled: Keyword.get(opts, :evaluation_enabled, true),
      notifications_enabled: Keyword.get(opts, :notifications_enabled, false)
    ]

    with {:ok, prefixes} <- store.list_prefixes(config, "tenants/") do
      total = Enum.reduce(prefixes, 0, &evaluate_tenant(&1, &2, nodes, repo_opts))

      :telemetry.execute([:pulso, :alerting, :pass], %{rules: total}, %{})
      {:ok, total}
    end
  end

  defp evaluate_tenant(prefix, total, nodes, opts) do
    tenant = prefix |> String.trim_trailing("/") |> String.split("/") |> List.last()

    case Repository.list(tenant, opts) do
      {:ok, ids} -> Enum.reduce(ids, total, &evaluate_owned(tenant, &1, &2, nodes, opts))
      _ -> total
    end
  end

  defp evaluate_owned(tenant, id, count, nodes, opts) do
    if Pulso.Rendezvous.owner(["alerting", tenant, id], nodes) == node() do
      actor = %{
        tenant: tenant,
        id: "pulso-evaluator",
        type: "service",
        capabilities: ~w(alert:read alert:evaluate alert:import)
      }

      if Keyword.get(opts, :evaluation_enabled, true), do: Evaluator.evaluate(actor, id, opts)
      if Keyword.get(opts, :notifications_enabled, false), do: Notifier.run_once(tenant, id, opts)
      count + 1
    else
      count
    end
  end

  defp schedule(opts), do: Process.send_after(self(), :tick, Keyword.get(opts, :poll_interval_ms, 5000))
end
