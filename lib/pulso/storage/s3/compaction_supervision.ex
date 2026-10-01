defmodule Pulso.Storage.S3.CompactionSupervision do
  @moduledoc """
  Restarts the worker when its process-group scope restarts, restoring eligibility.
  """
  use Supervisor

  alias Pulso.Storage.S3.CompactionOwnership
  alias Pulso.Storage.S3.CompactionTasks
  alias Pulso.Storage.S3.CompactionWorker

  def start_link(config), do: Supervisor.start_link(__MODULE__, config, name: __MODULE__)

  @impl true
  def init(config) do
    children = [
      %{id: CompactionOwnership, start: {:pg, :start_link, [CompactionOwnership.scope()]}},
      {Task.Supervisor, name: CompactionTasks, max_children: 4},
      {CompactionWorker, config}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
