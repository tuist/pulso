defmodule Pulso.Application do
  # See https://elixir.hexdocs.pm/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  alias Pulso.Storage.Memory
  alias Pulso.Storage.S3
  alias Pulso.Storage.S3.ManifestSupervision

  @impl true
  def start(_type, _args) do
    children =
      [
        PulsoWeb.Telemetry,
        {DNSCluster, query: Application.get_env(:pulso, :dns_cluster_query) || :ignore},
        {Phoenix.PubSub, name: Pulso.PubSub}
      ] ++ storage_children() ++ [PulsoWeb.Endpoint]

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
      nil -> [Memory]
      Memory -> [Memory]
      S3 -> [ManifestSupervision]
      _ -> []
    end
  end
end
