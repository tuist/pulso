defmodule Pulso.Errors.Supervisor do
  @moduledoc false
  use Pulso.Runtime.Supervision

  alias Pulso.Errors.Projection
  alias Pulso.Errors.Scope
  alias Pulso.Errors.Worker
  alias Pulso.Runtime
  alias Pulso.Runtime.Supervision, as: Supervisor

  def start_link(opts), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  def children do
    opts = Runtime.get_env(:pulso, Projection, [])
    if opts[:enabled], do: [{__MODULE__, opts}], else: []
  end

  @impl true
  def init(opts) do
    Supervisor.init(
      [%{id: Scope, start: {:pg, :start_link, [Scope]}}, {Worker, opts}],
      strategy: :rest_for_one
    )
  end
end
