defmodule Pulso.ObjectStore do
  @moduledoc """
  Ergonomic wrapper around the `Pulso.ObjectStore.NIF` object store client.

  Every call takes an explicit `StoreConfig` map so the NIF has no ambient
  state. In step 3 (segment writer + manifest CAS) this will be the only
  path Elixir uses to reach S3.
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

  @spec put(config(), String.t(), binary()) :: :ok | {:error, term()}
  def put(config, key, data) when is_map(config) and is_binary(key) and is_binary(data) do
    normalize(NIF.put(normalize_config(config), key, data))
  end

  @spec get(config(), String.t()) :: {:ok, binary()} | {:error, term()}
  def get(config, key) when is_map(config) and is_binary(key) do
    case NIF.get(normalize_config(config), key) do
      {:ok, data} -> {:ok, data}
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

  defp normalize(:ok), do: :ok
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
