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
  alias Pulso.Storage.S3.ManifestOwner
  alias Pulso.Storage.S3.PagedManifest
  alias Pulso.Storage.S3.Retention

  def compact(tenant, config, opts \\ []) do
    monitor(:compact, fn -> do_compact(tenant, config, opts) end)
  end

  defp do_compact(tenant, config, opts) do
    with :ok <- S3.validate_tenant(tenant),
         :ok <- validate_options(opts),
         {:ok, manifest, _etag} <- load(tenant, config),
         :ok <- migration_gate(manifest, config),
         :ok <- validate_retained_grace(manifest, config, opts),
         {:ok, candidates} <- compaction_candidates(manifest, tenant, config) do
      compact_selected(select(candidates, opts), candidates, manifest, tenant, config, opts)
    end
  end

  defp compact_selected([], candidates, manifest, tenant, config, _opts) do
    with :ok <- advance_empty_cursor(manifest, candidates, tenant, config),
         do: {:ok, %{segments_before: length(manifest.segments), merged: 0}}
  end

  defp compact_selected(sources, _candidates, manifest, tenant, config, opts),
    do: merge_and_publish(tenant, config, manifest, sources, opts)

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
    |> Enum.group_by(fn s ->
      if manifest.version == 3, do: PagedManifest.bucket_start(manifest.paging, s.max_ts), else: Path.dirname(s.key)
    end)
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
    with {:ok, current, _etag} <- load(tenant, config),
         {:ok, published} <- published?(current, sources, replacement.key, tenant, config) do
      if published, do: :ok, else: {:error, :cas_retries_exhausted}
    end
  end

  defp publish(tenant, config, sources, replacement, grace, attempts) do
    with {:ok, current, etag} <- load(tenant, config),
         {:ok, published} <- published?(current, sources, replacement.key, tenant, config) do
      if published do
        :ok
      else
        publish_current(current, etag, tenant, config, sources, replacement, grace, attempts)
      end
    end
  end

  defp validate_retained_grace(%{version: 3}, config, opts) do
    if opts[:grace_ms] <= Retention.effective_grace(config), do: :ok, else: {:error, :unsupported_compaction_grace}
  end

  defp validate_retained_grace(_manifest, _config, _opts), do: :ok

  defp migration_gate(%{version: 3}, _config), do: :ok

  defp migration_gate(_root, config) do
    if config[:retention_mode] == "enforce" and Retention.days(config, "metrics") > 0,
      do: {:error, :retention_migration_required},
      else: :ok
  end

  defp advance_empty_cursor(%{version: 3}, candidates, tenant, config),
    do: rotate_cursor(candidates.paging["compaction_candidate"], tenant, config)

  defp advance_empty_cursor(_root, _candidates, _tenant, _config), do: :ok

  defp rotate_cursor(nil, _tenant, _config), do: :ok

  defp rotate_cursor(start, tenant, config) do
    case Retention.transact(
           tenant,
           "metrics",
           config,
           &{%{&1 | paging: Map.put(&1.paging, "compaction_bucket", start)}, :ok}
         ) do
      {:ok, :ok} -> :ok
      error -> error
    end
  end

  defp compaction_candidates(%{version: 3} = root, tenant, config),
    do: PagedManifest.candidates(root, tenant, "metrics", config)

  defp compaction_candidates(root, _tenant, _config), do: {:ok, root}

  defp published?(%{version: 3} = root, _sources, key, tenant, config) do
    result =
      PagedManifest.with_budget(config, fn ->
        {:ok, s} = ManifestOwner.segment_from_key(key)
        # After a bucket is fenced, even an unpublished replacement is an
        # expiration-owned orphan. Never delete a possibly published key early.
        s.max_ts < PagedManifest.floor(root) or PagedManifest.lookup(root, s, tenant, "metrics", config) != :absent
      end)

    if result == {:error, :retention_expired}, do: {:ok, false}, else: result
  end

  defp published?(manifest, _sources, key, _tenant, _config) do
    {:ok, Enum.any?(manifest.segments, &(&1.key == key)) or Map.has_key?(manifest.retired, key)}
  end

  defp prepare_replacement(%{version: 3} = root, sources, replacement, grace, tenant, config) do
    segments =
      Enum.map(sources, fn key ->
        {:ok, s} = ManifestOwner.segment_from_key(key)
        s
      end)

    PagedManifest.replace(
      root,
      segments,
      replacement,
      System.system_time(:millisecond) + grace,
      tenant,
      "metrics",
      config
    )
  end

  defp prepare_replacement(root, sources, replacement, grace, _tenant, _config),
    do: Manifest.replace(root, sources, replacement, System.system_time(:millisecond) + grace)

  defp publish_current(current, etag, tenant, config, sources, replacement, grace, attempts) do
    with {:ok, updated} <- prepare_replacement(current, sources, replacement, grace, tenant, config) do
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
    Pulso.Metrics.measure(
      :compaction,
      Atom.to_string(operation),
      fn ->
        Pulso.SelfMetrics.track(:compaction, operation, fn ->
          result = fun.()
          Pulso.SelfMetrics.compaction(operation, result)
          result
        end)
      end,
      fn
        {:ok, %{merged: count}} -> %{segments: count}
        {:ok, count} when is_integer(count) -> %{segments: count}
        _ -> %{}
      end
    )
  end

  defp do_cleanup(tenant, config, opts) do
    maximum = Keyword.get(opts, :max_deletions, 128)

    with :ok <- S3.validate_tenant(tenant),
         :ok <- validate_cleanup_limit(maximum),
         {:ok, manifest, _etag} <- load(tenant, config),
         :ok <- migration_gate(manifest, config) do
      if manifest.version == 3 do
        Retention.cleanup_retired(tenant, "metrics", config, opts)
      else
        manifest |> deletion_candidates(tenant, maximum) |> delete_candidates(tenant, config)
      end
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
    Retention.load(tenant, "metrics", config)
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
