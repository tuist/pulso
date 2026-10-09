defmodule Pulso.Storage.S3.ManifestCache do
  @moduledoc """
  Lock-free in-memory cache of manifests, keyed by `(tenant, signal)`.

  The query path is the hot path — every `Pulso.Storage.S3.query/2` call
  looks up its manifest before deciding which segments to fetch. This
  cache sits on a public ETS table with `read_concurrency: true`, so
  concurrent queries never contend with each other or with the
  ManifestOwner that owns writes.

  Writes come only from `Pulso.Storage.S3.ManifestOwner`. Only one owner
  process exists per `(tenant, signal)` (a Registry singleton), so there
  is no need for `write_concurrency: true` — every write serializes
  through the owner already, and enabling `write_concurrency` would pay
  for a striping that helps nothing here.

  The entry shape carries the manifest struct, the ETag it was fetched
  with (used on the next conditional GET to short-circuit an unchanged
  read), and the monotonic timestamp of the last refresh (used by the
  owner to decide whether a refresh is due).
  """

  use Pulso.Runtime.GenServer

  alias Pulso.Runtime.GenServer
  alias Pulso.Storage.S3.Manifest
  alias Pulso.Storage.S3.Retention

  @table __MODULE__

  @type entry :: %{
          manifest: Manifest.t(),
          etag: String.t(),
          refreshed_at_mono: integer()
        }

  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # Every table operation goes through the current runtime's instance name.
  defp table, do: Pulso.Runtime.table(@table)

  @doc """
  Look up the cached manifest for one `(tenant, signal)`. Returns
  `nil` when nothing is cached yet — the caller is responsible for
  populating it.

  This is the hot path. A single `:ets.lookup/2` under
  `read_concurrency: true` — no message passing, no locks under load.
  """
  @spec get(String.t(), String.t()) :: entry() | nil
  def get(tenant, signal) when is_binary(tenant) and is_binary(signal) do
    case :ets.lookup(table(), {tenant, signal}) do
      [{_, entry}] -> entry
      [] -> nil
    end
  end

  @doc """
  Store a manifest entry. Called only by `ManifestOwner` after a
  successful load or CAS.
  """
  @spec put(String.t(), String.t(), Manifest.t(), String.t()) :: :ok
  def put(tenant, signal, manifest, etag) when is_binary(tenant) and is_binary(signal) and is_binary(etag) do
    entry = %{
      manifest: manifest,
      etag: etag,
      refreshed_at_mono: System.monotonic_time(:millisecond)
    }

    entry =
      if manifest.version == 3 do
        previous = :ets.select(table(), [{{{tenant, signal}, %{etag: etag, statistics: :"$1"}}, [], [:"$1"]}])

        statistics =
          case previous do
            [statistics] -> statistics
            [] -> Retention.metadata_stats(manifest)
          end

        Map.put(entry, :statistics, statistics)
      else
        entry
      end

    :ets.insert(table(), {{tenant, signal}, entry})
    :ok
  end

  @doc "Small node-local summaries for fixed-cardinality retention gauges, without storage I/O."
  def retention_statistics do
    :ets.select(table(), [{{:_, %{statistics: :"$1"}}, [], [:"$1"]}])
  rescue
    ArgumentError -> []
  end

  @doc "Tenants observed locally through ingest or queries, without listing segment objects."
  def tenants(signal) when is_binary(signal) do
    :ets.select(table(), [{{{:"$1", signal}, :_}, [], [:"$1"]}]) |> Enum.sort()
  end

  @doc "Bounded local scope discovery; the opaque ETS continuation contains no root values."
  def scopes_page(cursor, limit) when limit in 1..16 do
    scope_scan(cursor, limit, [])
  rescue
    ArgumentError -> {[], nil}
  end

  defp scope_scan(cursor, 0, acc), do: {Enum.reverse(acc), cursor}

  defp scope_scan(cursor, remaining, acc) do
    # The continuation embeds its chunk size. Keep it at one so callers may
    # shrink their queue allowance without receiving an oversized next chunk.
    result = if cursor == nil, do: :ets.select(table(), [{{:"$1", :_}, [], [:"$1"]}], 1), else: :ets.select(cursor)

    case result do
      :"$end_of_table" -> {Enum.reverse(acc), nil}
      {scopes, :"$end_of_table"} -> {Enum.reverse(scopes ++ acc), nil}
      {[scope], next} -> scope_scan(next, remaining - 1, [scope | acc])
    end
  end

  @doc "Drop a cached entry — used on tenant deletion or forced cache flush."
  @spec drop(String.t(), String.t()) :: :ok
  def drop(tenant, signal) when is_binary(tenant) and is_binary(signal) do
    :ets.delete(table(), {tenant, signal})
    :ok
  end

  @doc false
  @spec reset() :: :ok
  def reset do
    if :ets.info(table()) != :undefined do
      :ets.delete_all_objects(table())
    end

    :ok
  end

  @impl true
  def init(_opts) do
    ensure_table()
    {:ok, %{}}
  end

  @doc false
  def ensure_table do
    case :ets.info(table()) do
      :undefined ->
        :ets.new(table(), [
          :named_table,
          :set,
          :public,
          {:read_concurrency, true}
        ])

      _ ->
        table()
    end
  end
end
