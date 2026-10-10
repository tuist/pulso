defmodule Pulso.Alerting.Supervisor do
  @moduledoc false
  use Pulso.Runtime.Supervision

  alias Pulso.Alerting.Membership
  alias Pulso.Alerting.Tasks
  alias Pulso.Alerting.Worker
  alias Pulso.Runtime
  alias Pulso.Runtime.Supervision, as: Supervisor
  alias Pulso.Runtime.Task

  def children do
    opts = Runtime.get_env(:pulso, Pulso.Alerting, [])
    enabled = Keyword.get(opts, :evaluation_enabled, false) or Keyword.get(opts, :notifications_enabled, false)
    if enabled, do: [{__MODULE__, opts}], else: []
  end

  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    Supervisor.init(
      [
        %{id: Membership, start: {:pg, :start_link, [Membership]}},
        {Task.Supervisor, name: Tasks, max_children: 1},
        {Worker, opts}
      ],
      strategy: :one_for_all
    )
  end
end
