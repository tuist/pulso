defmodule Pulso.Storage.S3.MetricMetadataStore do
  @moduledoc """
  The per-tenant metric metadata object, `tenants/<tenant>/v4/metadata/metrics.json`
  (see `Pulso.MetricMetadata`). Writers merge with conditional PUTs and retry
  on conflicts, so concurrent instances never lose each other's variants;
  unchanged sets cost one GET and no write. Reads always GET the object, so
  every instance serves the latest published set.
  """

  alias Pulso.MetricMetadata
  alias Pulso.ObjectStore

  @attempts 16

  def key(tenant), do: "tenants/#{tenant}/v4/metadata/metrics.json"

  @spec put(map(), String.t(), [MetricMetadata.entry()], integer()) :: :ok | {:error, term()}
  def put(config, tenant, entries, today), do: put(config, tenant, entries, today, @attempts)

  defp put(_config, _tenant, _entries, _today, 0), do: {:error, :metric_metadata_conflict}

  defp put(config, tenant, entries, today, attempts) do
    with {:ok, etag, set} <- load(config, tenant) do
      if MetricMetadata.changed?(set, entries, today),
        do: write(config, tenant, etag, MetricMetadata.merge(set, entries, today), {entries, today, attempts}),
        else: :ok
    end
  end

  defp write(config, tenant, etag, merged, {entries, today, attempts}) do
    body = MetricMetadata.encode(merged)

    result =
      if etag,
        do: ObjectStore.put_if_match(config, key(tenant), body, etag),
        else: ObjectStore.put_if_none_match(config, key(tenant), body)

    case result do
      {:ok, _etag} ->
        :ok

      {:error, reason} when reason in [:precondition_failed, :already_exists, :not_found] ->
        backoff(attempts)
        put(config, tenant, entries, today, attempts - 1)

      {:error, _} = error ->
        error
    end
  end

  # Jittered, growing waits so concurrent writers stop colliding.
  defp backoff(attempts) do
    round = @attempts - attempts + 1
    Process.sleep(:rand.uniform(20 * round))
  end

  @spec get(map(), String.t()) :: {:ok, MetricMetadata.set()} | {:error, term()}
  def get(config, tenant) do
    with {:ok, _etag, set} <- load(config, tenant), do: {:ok, set}
  end

  defp load(config, tenant) do
    case ObjectStore.get_bounded(config, key(tenant), nil, MetricMetadata.max_bytes()) do
      {:ok, etag, body} ->
        with {:ok, set} <- MetricMetadata.decode(body), do: {:ok, etag, set}

      {:error, :not_found} ->
        {:ok, nil, %{}}

      {:error, _} = error ->
        error
    end
  end
end
