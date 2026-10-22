defmodule Pulso.Storage.S3.ManifestCache do
  @moduledoc """
  Lock-free in-memory cache of manifests, keyed by `(tenant, signal)`.

  The query path is the hot path — every `Pulso.Storage.S3.query/2` call
  looks up its manifest before deciding which segments to fetch. This
  cache sits on a public ETS table with `read_concurrency: true`, so
  concurrent queries never contend with each other or with the
  ManifestOwner that owns writes.

  Refreshes replace only their starting ETag. Publications also compare their
  starting ETag, invalidating on an intervening snapshot rather than guessing
  an order for opaque tags. Delayed reads and maintenance responses must not
  overwrite a newer acknowledged publication. Both paths remain lock-free.

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
    {entry, _token} = snapshot(tenant, signal)
    entry
  end

  @doc false
  def snapshot(tenant, signal) do
    case :ets.lookup(table(), {tenant, signal}) do
      [{_, %{invalidated?: true, token: token}}] -> {nil, token}
      [{_, entry}] -> {entry, entry.token}
      [] -> {nil, nil}
    end
  end

  @doc "Install a read only if its exact cache generation is still current."
  def put_snapshot(tenant, signal, manifest, etag, token) do
    object = {{tenant, signal}, entry(tenant, signal, manifest, etag)}

    if token == nil do
      :ets.insert_new(table(), object)
    else
      :ets.select_replace(table(), [{{{tenant, signal}, %{token: token}}, [], [{:const, object}]}])
    end

    :ok
  end

  @doc """
  Seed a manifest entry unconditionally. Production refresh/publication paths
  use the conditional APIs below; this is useful for owned fixtures.
  """
  @spec put(String.t(), String.t(), Manifest.t(), String.t()) :: :ok
  def put(tenant, signal, manifest, etag) when is_binary(tenant) and is_binary(signal) and is_binary(etag) do
    :ets.insert(table(), {{tenant, signal}, entry(tenant, signal, manifest, etag)})
    :ok
  end

  @doc "Cache a reader snapshot only if no other publication replaced its starting ETag."
  def put_if_current(tenant, signal, manifest, etag, expected_etag) do
    object = {{tenant, signal}, entry(tenant, signal, manifest, etag)}

    if is_nil(expected_etag) do
      :ets.insert_new(table(), object)
    else
      :ets.select_replace(table(), [{{{tenant, signal}, %{etag: expected_etag}}, [], [{:const, object}]}])
    end

    :ok
  end

  @doc "Install against the predecessor ETag and pre-I/O publication_base generation, or invalidate."
  def publish(tenant, signal, manifest, etag, expected_etag, base) do
    object = {{tenant, signal}, entry(tenant, signal, manifest, etag)}

    replaced =
      case base do
        {:warm, token} ->
          :ets.select_replace(table(), [
            {{{tenant, signal}, %{etag: expected_etag, token: token}}, [], [{:const, object}]}
          ])

        {:cold, _token} ->
          0
      end

    # Cold publications may install only the exact empty generation observed
    # BEFORE their CAS, never an absence created by another writer afterwards.
    if replaced == 0 and not install_cold(object, base), do: drop(tenant, signal)
    :ok
  end

  @doc false
  def publication_base(tenant, signal) do
    case snapshot(tenant, signal) do
      {nil, token} -> {:cold, token}
      {_entry, token} -> {:warm, token}
    end
  end

  defp install_cold(object, {:cold, nil}), do: :ets.insert_new(table(), object)

  defp install_cold({key, _entry} = object, {:cold, token}) do
    :ets.select_replace(table(), [{{key, %{invalidated?: true, token: token}}, [], [{:const, object}]}]) == 1
  end

  defp install_cold(_object, {:warm, _token}), do: false

  defp entry(tenant, signal, manifest, etag) do
    entry = %{
      manifest: manifest,
      etag: etag,
      token: make_ref(),
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

    entry
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
    # Keep a generation fence, not a manifest. Deleting the key would allow
    # a cold reader or delayed publisher from before the ACK to reinsert it.
    :ets.insert(table(), {{tenant, signal}, %{invalidated?: true, token: make_ref()}})
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
