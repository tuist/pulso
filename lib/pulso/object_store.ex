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
    normalize(NIF.put(normalize_config(config), key, data))
  end

  # Creates the object only if no object exists at that key (`If-None-Match: *`).
  # Returns `{:error, :already_exists}` if the key is already taken — the
  # loser in a first-create race retries by falling through to `put_if_match/4`
  # with the current ETag.
  @spec put_if_none_match(config(), String.t(), binary()) ::
          {:ok, etag()} | {:error, :already_exists | term()}
  def put_if_none_match(config, key, data) when is_map(config) and is_binary(key) and is_binary(data) do
    normalize(NIF.put_if_none_match(normalize_config(config), key, data))
  end

  # Updates the object only if its current ETag matches (`If-Match: <etag>`).
  # Returns `{:error, :precondition_failed}` if the object changed since
  # the caller last read it — the standard CAS retry path.
  @spec put_if_match(config(), String.t(), binary(), etag()) ::
          {:ok, etag()} | {:error, :precondition_failed | :not_found | term()}
  def put_if_match(config, key, data, etag)
      when is_map(config) and is_binary(key) and is_binary(data) and is_binary(etag) do
    normalize(NIF.put_if_match(normalize_config(config), key, data, etag))
  end

  @spec get(config(), String.t()) :: {:ok, binary()} | {:error, term()}
  def get(config, key) when is_map(config) and is_binary(key) do
    case NIF.get(normalize_config(config), key) do
      {:ok, data} -> {:ok, data}
      other -> normalize(other)
    end
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

    case NIF.get_if_none_match(normalize_config(config), key, etag_string) do
      {:ok, new_etag, data} -> {:ok, new_etag, data}
      other -> normalize(other)
    end
  end

  @spec delete(config(), String.t()) :: :ok | {:error, term()}
  def delete(config, key) when is_map(config) and is_binary(key) do
    normalize(NIF.delete(normalize_config(config), key))
  end

  @spec list(config(), String.t()) :: {:ok, [String.t()]} | {:error, term()}
  def list(config, prefix \\ "") when is_map(config) and is_binary(prefix) do
    case NIF.list(normalize_config(config), prefix) do
      {:ok, keys} -> {:ok, keys}
      other -> normalize(other)
    end
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
