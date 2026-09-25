defmodule Pulso.Storage.S3.ManifestSupervision do
  @moduledoc """
  Supervision tree fragment for the manifest layer.

  Runs three children in this order:

    1. `Pulso.Storage.S3.ManifestRegistry` — via-name lookup for owners.
    2. `Pulso.Storage.S3.ManifestSupervisor` — `DynamicSupervisor`
       parenting the per-tenant owner processes.
    3. `Pulso.Storage.S3.ManifestCache` — the ETS-backed hot-path
       cache. Started last so any prior owner-recovery attempt hits an
       empty table cleanly.

  Wired into `Pulso.Application` only when the S3 adapter is active.
  """

  use Supervisor

  alias Pulso.Storage.S3.ManifestCache
  alias Pulso.Storage.S3.ManifestRegistry
  alias Pulso.Storage.S3.ManifestSupervisor

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl Supervisor
  def init(_opts) do
    children = [
      {Registry, keys: :unique, name: ManifestRegistry},
      {DynamicSupervisor, strategy: :one_for_one, name: ManifestSupervisor},
      ManifestCache
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
