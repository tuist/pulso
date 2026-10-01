defmodule Pulso.Storage.S3.CompactionOwnership do
  @moduledoc """
  Expected compaction owners among live worker processes, scoped to a store.

  Erlang process groups propagate eligibility over the existing connected-node
  mesh. Disabled nodes do not join; worker death removes eligibility even if the
  node stays connected. Views are eventually consistent, not exclusive leases.
  Conditional manifest writes remain mandatory during transitions and partitions.
  """

  alias Pulso.Rendezvous

  @scope __MODULE__

  def scope, do: @scope

  def group(config) do
    {__MODULE__, Map.get(config, :endpoint), Map.get(config, :region), Map.get(config, :bucket)}
  end

  def join(config), do: :pg.join(@scope, group(config), self())

  def join_once(config) do
    if self() in :pg.get_local_members(@scope, group(config)), do: :ok, else: join(config)
  end

  def members(config) do
    @scope
    |> :pg.get_members(group(config))
    |> Enum.map(&node/1)
    |> Enum.uniq()
  end

  @doc "Select the highest-scoring node, independent of membership order. Empty views have no owner."
  def owner(tenant, signal, members) do
    Rendezvous.owner(["compaction", tenant, signal], members)
  end

  def local_owner?(tenant, signal, config) do
    owner(tenant, signal, members(config)) == node()
  end
end
