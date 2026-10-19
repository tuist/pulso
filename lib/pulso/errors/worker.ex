defmodule Pulso.Errors.Worker do
  @moduledoc false
  use Pulso.Runtime.GenServer

  alias Pulso.Errors.Projection
  alias Pulso.Errors.Scope
  alias Pulso.Rendezvous
  alias Pulso.Runtime
  alias Pulso.Runtime.GenServer
  alias Pulso.Runtime.ProcessGroup
  alias Pulso.Storage.S3

  require Logger

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    config = Map.new(Runtime.get_env(:pulso, S3, []))
    group = {__MODULE__, config[:endpoint], config[:region], config[:bucket]}
    :ok = ProcessGroup.join(Scope, group, self())
    send(self(), :project)
    {:ok, %{opts: opts, group: group, cursor: 0}}
  end

  @impl true
  def handle_info(:project, state) do
    projects =
      Runtime.get_env(:pulso, Pulso.Sentry, [])
      |> Keyword.get(:projects, %{})
      |> Enum.map(fn {id, config} -> {config["tenant"], id} end)
      |> Enum.sort()

    members = ProcessGroup.get_members(Scope, state.group) |> Enum.map(&node/1)

    case Enum.at(projects, rem(state.cursor, max(1, length(projects)))) do
      {tenant, project} ->
        if Rendezvous.owner(["error-projection", tenant, project], members) == node() do
          project(tenant, project)
        end

      nil ->
        :ok
    end

    Process.send_after(self(), :project, Keyword.get(state.opts, :interval_ms, 5000))
    {:noreply, %{state | cursor: state.cursor + 1}}
  end

  defp project(tenant, project) do
    case Projection.step(tenant, project) do
      {:ok, _} -> :ok
      {:error, :not_found} -> :ok
      {:error, reason} -> Logger.warning("error projection tenant=#{tenant} project=#{project}: #{inspect(reason)}")
    end
  end
end
