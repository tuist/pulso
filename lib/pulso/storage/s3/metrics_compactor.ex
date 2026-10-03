defmodule Pulso.Storage.S3.MetricsCompactor do
  @moduledoc """
  Bounded metrics compaction and restart-safe cleanup.

  `compact/3` merges at most one hour's small segments per call. Readers and
  ingesters keep running while the native codec decodes and re-encodes samples.
  The replacement is uploaded before a conditional manifest update. Conflicts
  reload the manifest and preserve concurrent appends; overlapping compactions
  fail rather than publish the same samples twice.

  Retired keys and wall-clock deletion deadlines live in the manifest. Cleanup
  is repeatable across nodes and restarts. Tombstones are retained permanently
  to preserve ingest idempotency. The default grace period is one hour; queries
  overtaken by cleanup restart against the current manifest.

  Options: `:max_segments` (32), `:max_input_bytes` (8 MiB), `:max_rows`
  (100_000), `:small_segment_bytes` (1 MiB), `:grace_ms` (3_600_000).
  Unknown segment sizes/counts are skipped rather than bypassing the bounds.
  """

  alias Pulso.ObjectStore
  alias Pulso.Storage.S3
  alias Pulso.Storage.S3.Manifest
  alias Pulso.Storage.S3.Manifest.Segment

  def compact(tenant, config, opts \\ []) do
    monitor(:compact, fn -> do_compact(tenant, config, opts) end)
  end

  defp do_compact(tenant, config, opts) do
    with :ok <- S3.validate_tenant(tenant),
         :ok <- validate_options(opts),
         {:ok, manifest, _etag} <- load(tenant, config) do
      case select(manifest, opts) do
        [] -> {:ok, %{segments_before: length(manifest.segments), merged: 0}}
        sources -> merge_and_publish(tenant, config, manifest, sources, opts)
      end
    end
  end

  @doc false
  def select(manifest, opts) do
    max_segments = Keyword.get(opts, :max_segments, 32)
    max_bytes = Keyword.get(opts, :max_input_bytes, 8 * 1024 * 1024)
    max_rows = Keyword.get(opts, :max_rows, 100_000)
    small_bytes = Keyword.get(opts, :small_segment_bytes, 1024 * 1024)

    manifest.segments
    |> Enum.filter(fn s ->
      is_integer(s.byte_size) and s.byte_size > 0 and s.byte_size <= small_bytes and
        is_integer(s.row_count) and s.row_count > 0
    end)
    |> Enum.group_by(&Path.dirname(&1.key))
    |> Enum.sort_by(fn {hour, _} -> hour end)
    |> Enum.find_value([], fn {_hour, segments} ->
      segments
      |> Enum.sort_by(&{&1.row_count, &1.byte_size, &1.key})
      |> select_group(max_segments, max_bytes, max_rows, 32)
    end)
  end

  # Try a bounded number of starting points: a full-sized replacement at the
  # front must not prevent smaller original segments from being merged.
  defp select_group([], _max_segments, _max_bytes, _max_rows, _attempts), do: nil
  defp select_group(_segments, _max_segments, _max_bytes, _max_rows, 0), do: nil

  defp select_group([_first | rest] = segments, max_segments, max_bytes, max_rows, attempts) do
    {selected, _, _} = Enum.reduce(segments, {[], 0, 0}, &select_segment(&1, &2, max_segments, max_bytes, max_rows))

    if length(selected) >= 2 do
      Enum.reverse(selected)
    else
      select_group(rest, max_segments, max_bytes, max_rows, attempts - 1)
    end
  end

  defp select_segment(segment, {selected, bytes, rows} = acc, max_segments, max_bytes, max_rows) do
    if length(selected) < max_segments and bytes + segment.byte_size <= max_bytes and
         rows + segment.row_count <= max_rows do
      {[segment | selected], bytes + segment.byte_size, rows + segment.row_count}
    else
      acc
    end
  end

  defp merge_and_publish(tenant, config, manifest, sources, opts) do
    with :ok <- validate_sources(tenant, sources),
         {:ok, records} <- read_sources(config, sources),
         {:ok, payload, min_ts, max_ts} <- S3.encode_segment(:metrics, records) do
      # A distinct suffix prevents prefix migration from adopting an unpublished
      # replacement after a crash. A manifest must exist before compaction starts.
      suffix = Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
      original_key = S3.object_key(tenant, "metrics", min_ts, max_ts, suffix, nil)
      key = Path.join(Path.dirname(original_key), String.replace(Path.basename(original_key), "-rand-", "-compact-"))

      replacement =
        key
        |> Segment.build(min_ts, max_ts, length(records), byte_size(payload))
        |> Segment.summarize_metrics(records)

      source_keys = Enum.map(sources, & &1.key)
      grace = Keyword.get(opts, :grace_ms, 3_600_000)

      with {:ok, _etag} <- ObjectStore.put(config, key, payload),
           :ok <- publish_replacement(tenant, config, source_keys, replacement, grace) do
        {:ok, %{segments_before: length(manifest.segments), merged: length(sources), replacement: key}}
      end

      # Ambiguous publications retain their upload: the manifest may reference it.
      # Definitive conflicts are reclaimed by publish_replacement/5.
    end
  end

  defp publish_replacement(tenant, config, sources, replacement, grace) do
    case publish(tenant, config, sources, replacement, grace, 5) do
      {:error, reason} = error when reason in [:compaction_conflict, :cas_retries_exhausted] ->
        # A fresh manifest proved this unique key was never published. Unlike
        # network failures, this definitive loss is safe to clean up immediately.
        case ObjectStore.delete(config, replacement.key) do
          :ok -> error
          {:error, :not_found} -> error
          {:error, reason} -> {:error, {:replacement_cleanup_failed, replacement.key, reason}}
        end

      result ->
        result
    end
  end

  defp validate_sources(tenant, sources) do
    if Enum.all?(sources, &valid_metric_key?(tenant, &1.key)), do: :ok, else: {:error, :invalid_segment_key}
  end

  defp valid_metric_key?(tenant, key) do
    String.starts_with?(key, "tenants/#{tenant}/v4/signal=metrics/") and
      String.ends_with?(key, ".parquet") and
      Enum.all?(String.split(key, "/"), &(&1 not in [".", ".."]))
  end

  defp read_sources(config, sources) do
    Enum.reduce_while(sources, {:ok, []}, fn source, {:ok, batches} ->
      with {:ok, blob} <- ObjectStore.get(config, source.key),
           true <- byte_size(blob) == source.byte_size,
           {:ok, records} <- S3.decode_segment(:metrics, blob, nil, nil, []),
           true <- length(records) == source.row_count do
        {:cont, {:ok, [records | batches]}}
      else
        false -> {:halt, {:error, :segment_summary_mismatch}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, batches} -> {:ok, batches |> Enum.reverse() |> List.flatten()}
      error -> error
    end
  end

  defp publish(tenant, config, sources, replacement, _grace, 0) do
    # The last conditional response can also have been lost after success.
    # Resolve it before declaring exhaustion or reclaiming this unique upload.
    with {:ok, current, _etag} <- load(tenant, config) do
      if published?(current, sources, replacement.key), do: :ok, else: {:error, :cas_retries_exhausted}
    end
  end

  defp publish(tenant, config, sources, replacement, grace, attempts) do
    with {:ok, current, etag} <- load(tenant, config) do
      if published?(current, sources, replacement.key) do
        :ok
      else
        publish_current(current, etag, tenant, config, sources, replacement, grace, attempts)
      end
    end
  end

  defp published?(manifest, _sources, key) do
    Enum.any?(manifest.segments, &(&1.key == key)) or Map.has_key?(manifest.retired, key)
  end

  defp publish_current(current, etag, tenant, config, sources, replacement, grace, attempts) do
    with {:ok, updated} <- Manifest.replace(current, sources, replacement, System.system_time(:millisecond) + grace) do
      payload = updated |> Manifest.encode() |> IO.iodata_to_binary()

      case ObjectStore.put_if_match(config, Manifest.manifest_key(tenant, "metrics"), payload, etag) do
        {:ok, _etag} ->
          :ok

        {:error, :precondition_failed} ->
          Process.sleep(10 * (6 - attempts))
          publish(tenant, config, sources, replacement, grace, attempts - 1)

        error ->
          error
      end
    end
  end

  @doc "Delete up to max_deletions (128) expired objects; persist progress and retry failed keys on later passes."
  def cleanup(tenant, config, opts \\ []) do
    monitor(:cleanup, fn -> do_cleanup(tenant, config, opts) end)
  end

  defp monitor(operation, fun) do
    Pulso.SelfMetrics.track(:compaction, operation, fn ->
      result = fun.()
      Pulso.SelfMetrics.compaction(operation, result)
      result
    end)
  end

  defp do_cleanup(tenant, config, opts) do
    maximum = Keyword.get(opts, :max_deletions, 128)

    with :ok <- S3.validate_tenant(tenant),
         :ok <- validate_cleanup_limit(maximum),
         {:ok, manifest, _etag} <- load(tenant, config) do
      manifest
      |> deletion_candidates(tenant, maximum)
      |> delete_candidates(tenant, config)
    end
  end

  defp validate_cleanup_limit(maximum) when is_integer(maximum) and maximum > 0, do: :ok
  defp validate_cleanup_limit(_), do: {:error, :invalid_cleanup_options}

  defp deletion_candidates(manifest, tenant, maximum) do
    now = System.system_time(:millisecond)
    active = MapSet.new(manifest.segments, & &1.key)

    eligible =
      manifest.retired
      |> Enum.filter(fn {key, retirement} ->
        not retirement.deleted? and retirement.delete_after <= now and valid_metric_key?(tenant, key) and
          not MapSet.member?(active, key)
      end)
      |> Enum.sort_by(&elem(&1, 0))

    {before, after_cursor} =
      Enum.split_while(eligible, fn {key, _} -> manifest.cleanup_cursor != nil and key <= manifest.cleanup_cursor end)

    Enum.take(after_cursor ++ before, maximum)
  end

  defp delete_candidates([], _tenant, _config), do: {:ok, 0}

  defp delete_candidates(candidates, tenant, config) do
    {deleted, errors} = Enum.reduce(candidates, {[], []}, &delete_retired(&1, &2, config))
    {cursor, _} = List.last(candidates)

    with :ok <- publish_deletions(tenant, config, deleted, cursor, 5) do
      if errors == [], do: {:ok, length(deleted)}, else: {:error, {:cleanup_failed, Enum.reverse(errors)}}
    end
  end

  defp delete_retired({key, _} = candidate, {deleted, errors}, config) do
    case ObjectStore.delete(config, key) do
      :ok -> {[candidate | deleted], errors}
      {:error, :not_found} -> {[candidate | deleted], errors}
      {:error, reason} -> {deleted, [{key, reason} | errors]}
    end
  end

  defp publish_deletions(_tenant, _config, _deleted, _cursor, 0), do: {:error, :cas_retries_exhausted}

  defp publish_deletions(tenant, config, deleted, cursor, attempts) do
    with {:ok, current, etag} <- load(tenant, config) do
      updated = Manifest.mark_deleted(current, deleted, cursor)

      if updated == current do
        :ok
      else
        write_deletions(updated, etag, tenant, config, deleted, cursor, attempts)
      end
    end
  end

  defp write_deletions(updated, etag, tenant, config, deleted, cursor, attempts) do
    payload = updated |> Manifest.encode() |> IO.iodata_to_binary()

    case ObjectStore.put_if_match(config, Manifest.manifest_key(tenant, "metrics"), payload, etag) do
      {:ok, _etag} -> :ok
      {:error, :precondition_failed} -> publish_deletions(tenant, config, deleted, cursor, attempts - 1)
      error -> error
    end
  end

  defp load(tenant, config) do
    with {:ok, etag, body} <- ObjectStore.get_if_none_match(config, Manifest.manifest_key(tenant, "metrics"), nil),
         {:ok, manifest} <- Manifest.decode(body) do
      {:ok, manifest, etag}
    end
  end

  defp validate_options(opts) do
    valid? =
      Enum.all?([:max_segments, :max_input_bytes, :max_rows, :small_segment_bytes], fn key ->
        value = Keyword.get(opts, key, 32)
        is_integer(value) and value > 0
      end)

    grace = Keyword.get(opts, :grace_ms, 3_600_000)
    if valid? and is_integer(grace) and grace >= 0, do: :ok, else: {:error, :invalid_compaction_options}
  end
end
