defmodule Pulso.ObjectStore do
  @moduledoc """
  Ergonomic wrapper around the `Pulso.ObjectStore.NIF` object store client.

  Every call takes an explicit `StoreConfig` map so the NIF has no ambient
  state.

  Beyond the plain `put/3`, `get/2`, `delete/2`, `list/2` operations this
  module also exposes the conditional variants Pulso's manifest CAS
  relies on: `put_if_none_match/3` for first-time creation,
  `put_if_match/4` for compare-and-swap updates, and `get_if_none_match/3`
  for cache-friendly conditional reads. Every write returns the new
  object's ETag so a caller does not have to issue a follow-up GET just
  to prime a CAS cache.
  """

  alias Pulso.ObjectStore.NIF

  @type config :: %{
          required(:bucket) => String.t(),
          required(:region) => String.t(),
          required(:access_key_id) => String.t(),
          required(:secret_access_key) => String.t(),
          required(:allow_http) => boolean(),
          optional(:endpoint) => String.t() | nil
        }

  @type etag :: String.t()

  @spec put(config(), String.t(), binary()) :: {:ok, etag()} | {:error, term()}
  def put(config, key, data) when is_map(config) and is_binary(key) and is_binary(data) do
    observe("put", key, byte_size(data), fn -> normalize(NIF.put(normalize_config(config), key, data)) end)
  end

  # Creates the object only if no object exists at that key (`If-None-Match: *`).
  # Returns `{:error, :already_exists}` if the key is already taken — the
  # loser in a first-create race retries by falling through to `put_if_match/4`
  # with the current ETag.
  @spec put_if_none_match(config(), String.t(), binary()) ::
          {:ok, etag()} | {:error, :already_exists | term()}
  def put_if_none_match(config, key, data) when is_map(config) and is_binary(key) and is_binary(data) do
    observe("put_if_none_match", key, byte_size(data), fn ->
      normalize(NIF.put_if_none_match(normalize_config(config), key, data))
    end)
  end

  # Updates the object only if its current ETag matches (`If-Match: <etag>`).
  # Returns `{:error, :precondition_failed}` if the object changed since
  # the caller last read it — the standard CAS retry path.
  @spec put_if_match(config(), String.t(), binary(), etag()) ::
          {:ok, etag()} | {:error, :precondition_failed | :not_found | term()}
  def put_if_match(config, key, data, etag)
      when is_map(config) and is_binary(key) and is_binary(data) and is_binary(etag) do
    observe("put_if_match", key, byte_size(data), fn ->
      normalize(NIF.put_if_match(normalize_config(config), key, data, etag))
    end)
  end

  @spec get(config(), String.t()) :: {:ok, binary()} | {:error, term()}
  def get(config, key) when is_map(config) and is_binary(key) do
    observe("get", key, 0, fn ->
      case NIF.get(normalize_config(config), key) do
        {:ok, data} -> {:ok, data}
        other -> normalize(other)
      end
    end)
  end

  # Conditional GET. `etag` may be `nil` (or `""`) to force a full read.
  #
  # - Object unchanged since `etag` was read → `:not_modified`. No body
  #   crosses the wire; parsing is skipped entirely.
  # - Object changed, or no ETag was supplied → `{:ok, new_etag, body}`.
  # - Missing key → `{:error, :not_found}`.
  @spec get_if_none_match(config(), String.t(), etag() | nil) ::
          {:ok, etag(), binary()} | :not_modified | {:error, :not_found | term()}
  def get_if_none_match(config, key, etag) when is_map(config) and is_binary(key) do
    etag_string = etag || ""

    observe("get_if_none_match", key, 0, fn ->
      case NIF.get_if_none_match(normalize_config(config), key, etag_string) do
        {:ok, new_etag, data} -> {:ok, new_etag, data}
        other -> normalize(other)
      end
    end)
  end

  @spec delete(config(), String.t()) :: :ok | {:error, term()}
  def delete(config, key) when is_map(config) and is_binary(key) do
    observe("delete", key, 0, fn -> normalize(NIF.delete(normalize_config(config), key)) end)
  end

  @spec list(config(), String.t()) :: {:ok, [String.t()]} | {:error, term()}
  def list(config, prefix \\ "") when is_map(config) and is_binary(prefix) do
    observe("list", prefix, 0, fn ->
      case NIF.list(normalize_config(config), prefix) do
        {:ok, keys} -> {:ok, keys}
        other -> normalize(other)
      end
    end)
  end

  @doc "List immediate directory prefixes, following provider pagination without materializing descendant object keys."
  @spec list_prefixes(config(), String.t()) :: {:ok, [String.t()]} | {:error, term()}
  def list_prefixes(config, prefix) when is_map(config) and is_binary(prefix) do
    observe("list_prefixes", prefix, 0, fn -> normalize(NIF.list_prefixes(normalize_config(config), prefix)) end)
  end

  defp observe(operation, key, write_bytes, fun) do
    purpose =
      cond do
        String.ends_with?(key, "/manifest.json") -> "manifest"
        String.ends_with?(key, ".parquet") -> "segment"
        true -> "other"
      end

    Pulso.Metrics.measure(
      :object,
      operation,
      fun,
      fn
        {:ok, _etag, body} when is_binary(body) -> %{read: byte_size(body)}
        {:ok, body} when operation == "get" and is_binary(body) -> %{read: byte_size(body)}
        {:ok, _} when operation in ["put", "put_if_match", "put_if_none_match"] -> %{write: write_bytes}
        _ -> %{}
      end,
      purpose
    )
  end

  # 304 responses reach us as a NIF error carrying the `:not_modified`
  # atom (that is what `Error::Term(Box::new(atoms::not_modified()))`
  # translates to on the BEAM side). We map it to a plain return value
  # so callers do not have to know the NIF error shape.
  defp normalize(:not_modified), do: :not_modified
  defp normalize(:ok), do: :ok
  defp normalize({:error, :not_modified}), do: :not_modified
  defp normalize({:error, reason}), do: {:error, reason}
  defp normalize(other), do: other

  defp normalize_config(config) do
    %{
      bucket: fetch!(config, :bucket),
      endpoint: Map.get(config, :endpoint),
      region: fetch!(config, :region),
      access_key_id: fetch!(config, :access_key_id),
      secret_access_key: fetch!(config, :secret_access_key),
      allow_http: Map.get(config, :allow_http, false)
    }
  end

  defp fetch!(config, key) do
    case Map.fetch(config, key) do
      {:ok, value} -> value
      :error -> raise ArgumentError, "Pulso.ObjectStore config is missing #{inspect(key)}"
    end
  end
end
