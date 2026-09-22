defmodule Pulso.Application do
  # See https://elixir.hexdocs.pm/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  alias Pulso.Storage.Memory

  @impl true
  def start(_type, _args) do
    children = [
      PulsoWeb.Telemetry,
      {DNSCluster, query: Application.get_env(:pulso, :dns_cluster_query) || :ignore},
      {Phoenix.PubSub, name: Pulso.PubSub},
      Memory,
      PulsoWeb.Endpoint
    ]

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
end
