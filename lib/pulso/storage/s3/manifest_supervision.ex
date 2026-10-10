defmodule Pulso.Storage.S3.ManifestSupervision do
  @moduledoc """
  Supervision tree fragment for the manifest layer.

  Runs seven children in this order:

    1. `Pulso.Storage.S3.ManifestRegistry` — via-name lookup for owners.
    2. `Pulso.Storage.S3.ManifestSupervisor` — `DynamicSupervisor`
       parenting the per-tenant owner processes.
    3. `Pulso.Storage.S3.ManifestCache` — the ETS-backed hot-path
       cache. Started last so any prior owner-recovery attempt hits an
       empty table cleanly.
    4. `Pulso.Storage.S3.AppendRegistry` — optional unkeyed buffer lookup.
    5. `Pulso.Storage.S3.AppendSupervisor` — bounded input buffers. Buffers
       are created only when ingest coalescing is enabled.
    6. `Pulso.Storage.S3.MetadataCache` — bounded, disposable immutable page cache.
    7. `Pulso.Storage.S3.RetentionAdmission` — node-wide monitored DELETE slots.

  Wired into `Pulso.Application` only when the S3 adapter is active.
  """

  use Pulso.Runtime.Supervision

  alias Pulso.Runtime.Registry
  alias Pulso.Runtime.Supervision, as: Supervisor
  alias Pulso.Storage.S3.AppendRegistry
  alias Pulso.Storage.S3.AppendSupervisor
  alias Pulso.Storage.S3.ManifestCache
  alias Pulso.Storage.S3.ManifestRegistry
  alias Pulso.Storage.S3.ManifestSupervisor
  alias Pulso.Storage.S3.MetadataCache
  alias Pulso.Storage.S3.RetentionAdmission

  @spec start_link(keyword()) :: Elixir.Supervisor.on_start()
  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      {Registry, keys: :unique, name: ManifestRegistry},
      {DynamicSupervisor, strategy: :one_for_one, name: ManifestSupervisor},
      ManifestCache,
      {Registry, keys: :unique, name: AppendRegistry},
      {DynamicSupervisor, strategy: :one_for_one, name: AppendSupervisor},
      MetadataCache,
      RetentionAdmission
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
