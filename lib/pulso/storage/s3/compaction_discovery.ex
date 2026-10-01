defmodule Pulso.Storage.S3.CompactionDiscovery do
  @moduledoc """
  Reconstruct compaction tenant discovery from durable tenant prefixes, including idle
  tenants and tenants first ingested on another node. No peer cache is required.

  Delimiter listing returns only immediate tenant directories, not segment keys.
  Owners load metrics manifests; missing manifests never adopt orphan segments.
  Memory grows with tenant count, independently of the segment backlog.
  """

  alias Pulso.ObjectStore
  alias Pulso.Storage.S3

  def tenants(config) do
    with {:ok, keys} <- ObjectStore.list_prefixes(config, "tenants/") do
      tenants =
        keys
        |> Enum.flat_map(&tenant/1)
        |> Enum.uniq()
        |> Enum.sort()

      {:ok, tenants}
    end
  end

  defp tenant(key) do
    case String.split(String.trim_trailing(key, "/"), "/") do
      ["tenants", tenant] ->
        if S3.validate_tenant(tenant) == :ok, do: [tenant], else: []

      _ ->
        []
    end
  end
end
