defmodule Pulso.Application do
  # See https://elixir.hexdocs.pm/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  alias Pulso.Alerting.Subscriptions
  alias Pulso.Auth.Issuer
  alias Pulso.Auth.JWKS
  alias Pulso.Auth.Membership
  alias Pulso.Codec.NIF
  alias Pulso.Health.StorageMonitor
  alias Pulso.Loki.Tail
  alias Pulso.PromQL.QuerySlots
  alias Pulso.PromQL.TaskSupervisor
  alias Pulso.Storage.Memory
  alias Pulso.Storage.S3
  alias Pulso.Storage.S3.CompactionWorker
  alias Pulso.Storage.S3.ManifestSupervision
  alias Pulso.Storage.S3.RetentionWorker
  alias Pulso.Tenants.Purger

  @impl true
  def start(_type, _args) do
    :ok = NIF.ensure_log_metadata_api!()
    :ok = NIF.ensure_metric_metadata_api!()
    :ok = NIF.ensure_trace_metadata_api!()

    children =
      [
        Pulso.SelfMetrics,
        Pulso.IngestAdmission,
        Pulso.Tenants,
        Pulso.Usage,
        Pulso.Metrics,
        PulsoWeb.Telemetry,
        {Task.Supervisor,
         name: TaskSupervisor,
         max_children: Keyword.get(Application.get_env(:pulso, Pulso.QueryRunner, []), :max_global, 4)},
        {Registry, keys: :unique, name: QuerySlots},
        Subscriptions,
        {DNSCluster, query: Application.get_env(:pulso, :dns_cluster_query) || :ignore},
        {Phoenix.PubSub, name: Pulso.PubSub},
        Tail
      ] ++
        storage_children() ++
        maintenance(Pulso.Alerting.Supervisor.children()) ++
        JWKS.children() ++ issuer_children() ++ [PulsoWeb.Endpoint] ++ otlp_grpc_children()

    # See https://elixir.hexdocs.pm/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Pulso.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    PulsoWeb.Endpoint.config_change(changed, removed)
    :ok
  end

  # Background workers run only on instances with the maintenance role.
  defp maintenance(children), do: if(Pulso.Roles.enabled?(:maintenance), do: children, else: [])

  # Issuer keys and status are cached before the endpoint accepts traffic. The
  # membership scope lets instances divide tenant quotas among themselves.
  defp issuer_children do
    case Issuer.children() do
      [] -> []
      cache -> [%{id: Membership, start: {:pg, :start_link, [Membership]}} | cache]
    end
  end

  # The OTLP/gRPC listener starts after the endpoint, only when configured.
  defp otlp_grpc_children do
    config = Application.get_env(:pulso, PulsoWeb.OTLPGRPC, [])

    if Keyword.get(config, :enabled, false) and Pulso.Roles.enabled?(:ingest),
      do: [{PulsoWeb.OTLPGRPC, config |> Keyword.delete(:enabled) |> Keyword.put(:name, PulsoWeb.OTLPGRPC)}],
      else: []
  end

  # `Memory` only runs when it is the configured adapter — in `mix test` no
  # adapter is set, so `Pulso.Storage.adapter/0` falls back to it. In dev and
  # prod the S3 adapter is configured and Memory would just be dead weight.
  #
  # The manifest layer (registry + dynamic supervisor + ETS cache) is only
  # started when the S3 adapter is active; other adapters have no manifest.
  defp storage_children do
    adapter =
      case Application.get_env(:pulso, Pulso.Storage) do
        nil -> nil
        env -> Keyword.get(env, :adapter)
      end

    case adapter do
      nil ->
        [Memory]

      Memory ->
        [Memory]

      S3 ->
        :ok = Pulso.ObjectStore.ensure_retention_api!()
        config = :pulso |> Application.fetch_env!(S3) |> Map.new()

        [ManifestSupervision, {StorageMonitor, config: config}] ++
          maintenance(
            CompactionWorker.children(config) ++
              RetentionWorker.children(config) ++
              Pulso.Errors.Supervisor.children() ++
              Pulso.Reliability.Supervisor.children() ++
              Purger.children()
          )

      _ ->
        []
    end
  end
end
