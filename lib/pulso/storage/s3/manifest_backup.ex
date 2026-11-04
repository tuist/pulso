defmodule Pulso.Storage.S3.ManifestBackup do
  @moduledoc """
  Pulso-side manifest backups, for object stores without bucket versioning.

  Once compaction or hour finalization has run, a prefix's manifest is the
  only record of which objects are live. `backup/4` writes a self-contained
  flat copy of the current manifest (format 4 pages included) to
  `tenants/<t>/v4/signal=<s>/backups/<unix_ms>.json` and keeps the newest
  `:keep` (48) copies.

  Maintenance holds deletions back so the newest backup stays restorable:
  cleanup only deletes objects retired before that backup was taken
  (`hold_ms/2`). An object retired later is still active in the backup and
  is kept until a newer backup records its retirement.

  `restore/4` rebuilds a lost manifest from the newest backup and adopts the
  ingested segments written after it (keys a backup does not list). Outputs
  of compaction and finalization written after the backup are not adopted;
  their sources were not deleted, so no acknowledged data is lost or
  duplicated. Prefixes with enforced retention (format 3) are not backed up.
  """

  alias Pulso.ObjectStore
  alias Pulso.Storage.S3.JournalManifest
  alias Pulso.Storage.S3.Manifest
  alias Pulso.Storage.S3.Manifest.Segment
  alias Pulso.Storage.S3.ManifestOwner
  alias Pulso.Storage.S3.Retention

  @width 20
  @materialize_bytes 512 * 1_048_576

  def prefix(tenant, signal), do: "tenants/#{tenant}/v4/signal=#{signal}/backups/"

  @doc """
  Back up the current manifest. Returns `{:ok, %{key, taken_ms, segments}}`,
  or `{:ok, :retention_managed}` for format-3 prefixes, which are not backed up.
  """
  def backup(tenant, signal, config, opts \\ []) do
    with {:ok, root, _etag} <- Retention.load(tenant, signal, config) do
      case flatten(root, tenant, signal, config) do
        {:ok, flat} -> write(tenant, signal, config, flat, Keyword.get(opts, :keep, 48))
        :retention_managed -> {:ok, :retention_managed}
        error -> error
      end
    end
  end

  defp flatten(%{version: 3}, _tenant, _signal, _config), do: :retention_managed

  defp flatten(%{version: 4} = root, tenant, signal, config),
    do: JournalManifest.materialize(root, tenant, signal, config, @materialize_bytes)

  defp flatten(root, _tenant, _signal, _config), do: {:ok, %{root | version: max(root.version, 2)}}

  defp write(tenant, signal, config, flat, keep) do
    now = System.system_time(:millisecond)
    key = prefix(tenant, signal) <> pad(now) <> ".json"
    body = flat |> Manifest.encode() |> IO.iodata_to_binary()

    with {:ok, _etag} <- ObjectStore.put(config, key, body) do
      prune(tenant, signal, config, keep)
      {:ok, %{key: key, taken_ms: now, segments: length(flat.segments)}}
    end
  end

  defp prune(tenant, signal, config, keep) do
    with {:ok, keys} <- list(tenant, signal, config) do
      keys |> Enum.drop(-keep) |> Enum.each(&ObjectStore.delete(config, &1))
    end
  end

  # Backup keys, oldest first.
  defp list(tenant, signal, config) do
    with {:ok, keys} <- ObjectStore.list(config, prefix(tenant, signal)) do
      {:ok, keys |> Enum.filter(&match?({:ok, _}, taken(&1))) |> Enum.sort()}
    end
  end

  defp taken(key) do
    case Integer.parse(Path.basename(key, ".json")) do
      {ms, ""} -> {:ok, ms}
      _ -> :error
    end
  end

  @doc "When the newest backup was taken, or nil without one."
  def latest(tenant, signal, config) do
    with {:ok, keys} <- list(tenant, signal, config) do
      case List.last(keys) do
        nil -> {:ok, nil}
        key -> taken(key)
      end
    end
  end

  @doc """
  The `now` cleanup may use so that it only deletes objects retired before
  the backup taken at `taken_ms` (strictly earlier, since a retirement in the
  same millisecond may follow it): retirement deadlines are retirement time
  plus `grace_ms`. Without a backup nothing is deleted.
  """
  def hold_ms(nil, _grace_ms), do: 0
  def hold_ms(taken_ms, grace_ms), do: min(System.system_time(:millisecond), taken_ms + grace_ms - 1)

  @doc """
  Rebuild a missing manifest from the newest backup. Refuses to overwrite an
  existing manifest unless `force: true`. Returns `{:ok, %{backup, segments, adopted}}`.
  """
  def restore(tenant, signal, config, opts \\ []) do
    with :ok <- check_missing(tenant, signal, config, opts),
         {:ok, keys} <- list(tenant, signal, config),
         {:ok, key} <- newest(keys),
         {:ok, body} <- ObjectStore.get(config, key),
         {:ok, manifest} <- Manifest.decode(body),
         {:ok, adopted} <- adoptable(tenant, signal, config, manifest) do
      restored = Manifest.merge(%{manifest | version: max(manifest.version, 2)}, adopted)
      payload = restored |> Manifest.encode() |> IO.iodata_to_binary()
      manifest_key = Manifest.manifest_key(tenant, signal)

      result =
        if Keyword.get(opts, :force, false),
          do: ObjectStore.put(config, manifest_key, payload),
          else: ObjectStore.put_if_none_match(config, manifest_key, payload)

      with {:ok, _etag} <- result,
           do: {:ok, %{backup: key, segments: length(restored.segments), adopted: length(adopted)}}
    end
  end

  defp newest([]), do: {:error, :no_manifest_backup}
  defp newest(keys), do: {:ok, List.last(keys)}

  defp check_missing(tenant, signal, config, opts) do
    case Retention.load(tenant, signal, config) do
      {:ok, _root, _etag} -> if Keyword.get(opts, :force, false), do: :ok, else: {:error, :manifest_exists}
      {:error, reason} when reason in [:not_found, :managed_manifest_missing] -> :ok
      _error -> if Keyword.get(opts, :force, false), do: :ok, else: {:error, :manifest_exists}
    end
  end

  # Ingested segments the backup does not know: written after it was taken.
  defp adoptable(tenant, signal, config, manifest) do
    known = MapSet.new(Enum.map(manifest.segments, & &1.key) ++ Map.keys(manifest.retired))
    scope = "tenants/#{tenant}/v4/signal=#{signal}/"

    with {:ok, keys} <- ObjectStore.list(config, scope) do
      adopted =
        for key <- keys,
            ingested?(key, scope),
            not MapSet.member?(known, key),
            {:ok, segment} <- [ManifestOwner.segment_from_key(key)],
            do: segment

      {:ok, adopted}
    end
  end

  defp ingested?(key, scope) do
    base = Path.basename(key)

    String.starts_with?(key, scope <> "date=") and String.ends_with?(base, ".parquet") and
      Segment.key_tier(key) == :raw and
      (String.contains?(base, "-rand-") or String.contains?(base, "-idem-"))
  end

  defp pad(ms), do: ms |> Integer.to_string() |> String.pad_leading(@width, "0")
end
