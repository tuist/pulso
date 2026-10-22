defmodule Pulso.Storage.S3.SignalCompactor do
  @moduledoc """
  Bounded signal compaction and restart-safe cleanup.

  `compact/3` merges at most one hour's small segments per call. Readers and
  ingesters keep running while native codecs merge data.
  The replacement is uploaded before a conditional manifest update. Conflicts
  reload the manifest and preserve concurrent appends; overlapping compactions
  fail rather than publish the same samples twice.

  Retired keys and wall-clock deletion deadlines live in the manifest. Cleanup
  is repeatable across nodes and restarts. Tombstones are retained permanently
  to preserve ingest idempotency. The default grace period is one hour; queries
  overtaken by cleanup restart against the current manifest.

  Segments whose newest record is younger than `:settle_ms` are left alone;
  log compaction sets it so live tail followers read every live segment
  before it is merged (`Pulso.Loki.TailFollower`).

  Options: `:max_segments` (32), `:max_input_bytes` (8 MiB), `:max_rows`
  (100_000), `:small_segment_bytes` (1 MiB), `:settle_ms` (0), `:grace_ms` (3_600_000).
  Unknown segment sizes/counts are skipped rather than bypassing the bounds.
  """

  alias Pulso.Codec.NIF
  alias Pulso.ObjectStore
  alias Pulso.Storage.S3
  alias Pulso.Storage.S3.Manifest
  alias Pulso.Storage.S3.Manifest.Segment
  alias Pulso.Storage.S3.ManifestOwner
  alias Pulso.Storage.S3.PagedManifest
  alias Pulso.Storage.S3.Retention
  alias Pulso.Storage.S3.TraceSummary

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

  defp compact_selected(sources, candidates, manifest, tenant, config, opts) do
    result = merge_and_publish(tenant, config, manifest, sources, opts)
    # Rotation is only an optimization. Never mask the original failure if a
    # competing writer prevents progress; publication remains independently CAS-fenced.
    if match?({:error, _}, result), do: advance_empty_cursor(manifest, candidates, tenant, config)
    result
  end

  @doc false
  def select(manifest, opts) do
    max_segments = Keyword.get(opts, :max_segments, 32)
    max_bytes = Keyword.get(opts, :max_input_bytes, 8 * 1024 * 1024)
    max_rows = Keyword.get(opts, :max_rows, 100_000)
    small_bytes = Keyword.get(opts, :small_segment_bytes, 1024 * 1024)
    settle_ns = Keyword.get(opts, :settle_ms, 0) * 1_000_000
    settled_before = System.os_time(:nanosecond) - settle_ns

    manifest.segments
    |> Enum.filter(fn s ->
      is_integer(s.byte_size) and s.byte_size > 0 and s.byte_size <= small_bytes and
        is_integer(s.row_count) and s.row_count > 0 and
        (settle_ns == 0 or (is_integer(s.max_ts) and s.max_ts <= settled_before))
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
    with :ok <- validate_sources(tenant, sources, config),
         {:ok, records} <- read_sources(config, sources),
         {:ok, payload, min_ts, max_ts, row_count, summary} <- encode_replacement(config, records, opts) do
      # A distinct suffix prevents prefix migration from adopting an unpublished
      # replacement after a crash. A manifest must exist before compaction starts.
      suffix = Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
      original_key = S3.object_key(tenant, signal_string(config), min_ts, max_ts, suffix, nil)
      key = Path.join(Path.dirname(original_key), String.replace(Path.basename(original_key), "-rand-", "-compact-"))

      replacement =
        key
        |> Segment.build(min_ts, max_ts, row_count, byte_size(payload))
        |> summarize_replacement(config, records, summary)

      source_keys = Enum.map(sources, & &1.key)
      grace = Keyword.get(opts, :grace_ms, 3_600_000)

      with {:ok, _etag} <- ObjectStore.put(config, key, payload),
           :ok <- publish_replacement(tenant, config, source_keys, replacement, grace) do
        {:ok, %{segments_before: length(manifest.segments), merged: length(sources), replacement: key}}
      end

      # Ambiguous publications retain their upload: the manifest may reference it.
      # Definitive conflicts are reclaimed by publish_replacement/5.
    else
      {:error, reason} = error when reason in [:payload_too_large, :query_sample_limit] ->
        if length(sources) > 2 do
          # Encoding failed before any upload. Shrink the source set, never
          # truncate rows. A compressed-byte estimate cannot bound metadata.
          merge_and_publish(tenant, config, manifest, Enum.take(sources, max(2, div(length(sources), 2))), opts)
        else
          error
        end

      error ->
        error
    end
  end

  defp publish_replacement(tenant, config, sources, replacement, grace) do
    case publish(tenant, config, sources, replacement, grace, 5) do
      {:error, {:unpublished_replacement, reason}} ->
        discard_replacement(config, replacement, {:error, reason})

      {:error, reason} = error when reason in [:compaction_conflict, :cas_retries_exhausted] ->
        # A fresh manifest proved this unique key was never published. Unlike
        # network failures, this definitive loss is safe to clean up immediately.
        discard_replacement(config, replacement, error)

      result ->
        result
    end
  end

  defp discard_replacement(config, replacement, error) do
    case ObjectStore.delete(config, replacement.key) do
      :ok -> error
      {:error, :not_found} -> error
      {:error, reason} -> {:error, {:replacement_cleanup_failed, replacement.key, reason}}
    end
  end

  defp signal(config), do: Map.get(config, :compaction_signal, :metrics)
  defp signal_string(config), do: Atom.to_string(signal(config))

  defp validate_sources(tenant, sources, config) do
    if signal(config) in [:metrics, :logs, :traces] and Enum.all?(sources, &valid_segment_key?(tenant, &1.key, config)),
      do: :ok,
      else: {:error, :invalid_segment_key}
  end

  defp valid_segment_key?(tenant, key, config) do
    String.starts_with?(key, "tenants/#{tenant}/v4/signal=#{signal_string(config)}/") and
      String.ends_with?(key, ".parquet") and
      Enum.all?(String.split(key, "/"), &(&1 not in [".", ".."]))
  end

  defp read_sources(config, sources) do
    Enum.reduce_while(sources, {:ok, []}, fn source, {:ok, batches} ->
      with {:ok, blob} <- ObjectStore.get(config, source.key),
           true <- byte_size(blob) == source.byte_size,
           {:ok, records} <- read_records(config, blob, source.row_count) do
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

  defp read_records(%{compaction_signal: :traces}, blob, count) do
    case NIF.trace_segment_parquet_row_count(blob) do
      {:ok, ^count} -> {:ok, [blob]}
      _ -> {:error, :segment_summary_mismatch}
    end
  end

  defp read_records(%{compaction_signal: :logs}, blob, count) do
    case NIF.log_segment_parquet_row_count(blob) do
      {:ok, ^count} -> {:ok, [blob]}
      _ -> {:error, :segment_summary_mismatch}
    end
  end

  defp read_records(config, blob, count) do
    with {:ok, records} <- S3.decode_segment(signal(config), blob, nil, nil, []) do
      if length(records) == count, do: {:ok, records}, else: {:error, :segment_summary_mismatch}
    end
  end

  defp encode_replacement(%{compaction_signal: :traces}, blobs, opts) do
    case NIF.compact_trace_segments_v2(blobs, min(100_000, Keyword.get(opts, :max_rows, 100_000))) do
      {:ok, payload, min, max, count, {services, ids, proof}} ->
        {:ok, payload, min, max, count, TraceSummary.from_values(services, ids, proof)}

      {:error, _} = error ->
        error

      _ ->
        {:error, {:encode_failed, :parquet_encoder_rejected_input}}
    end
  end

  defp encode_replacement(%{compaction_signal: :logs}, blobs, opts) do
    case NIF.compact_log_segments(blobs, min(100_000, Keyword.get(opts, :max_rows, 100_000))) do
      {:ok, payload, min, max, count, services} -> {:ok, payload, min, max, count, services}
      {:error, _} = error -> error
      _ -> {:error, {:encode_failed, :parquet_encoder_rejected_input}}
    end
  end

  defp encode_replacement(_config, records, _opts) do
    with {:ok, payload, min, max} <- S3.encode_segment(:metrics, records),
         do: {:ok, payload, min, max, length(records), nil}
  end

  defp summarize_replacement(segment, %{compaction_signal: :traces}, _records, summary),
    do: %{segment | trace_summary: summary}

  defp summarize_replacement(segment, %{compaction_signal: :logs}, _records, services),
    do: %{segment | log_services: services}

  defp summarize_replacement(segment, _config, records, _summary), do: Segment.summarize_metrics(segment, records)

  defp publish(tenant, config, _sources, replacement, _grace, 0) do
    # The last conditional response can also have been lost after success.
    # Resolve it before declaring exhaustion or reclaiming this unique upload.
    with {:ok, current, _etag} <- load(tenant, config),
         {:ok, published} <- published?(current, replacement.key, tenant, config) do
      if published, do: :ok, else: {:error, :cas_retries_exhausted}
    end
  end

  defp publish(tenant, config, sources, replacement, grace, attempts) do
    with {:ok, current, etag} <- load(tenant, config),
         {:ok, published} <- published?(current, replacement.key, tenant, config) do
      if published do
        :ok
      else
        publish_current(current, etag, tenant, config, sources, replacement, grace, attempts)
      end
    end
  end

  defp published?(%{version: 3} = root, key, tenant, config) do
    result =
      PagedManifest.with_budget(config, fn ->
        {:ok, segment} = ManifestOwner.segment_from_key(key)
        # Once wholly expired, even an unpublished upload belongs to retention.
        # Do not erase a possibly committed replacement ahead of its grace.
        segment.max_ts < PagedManifest.floor(root) or
          PagedManifest.lookup(root, segment, tenant, signal_string(config), config) != :absent
      end)

    if result == {:error, :retention_expired}, do: {:ok, false}, else: result
  end

  defp published?(manifest, key, _tenant, _config),
    do: {:ok, Enum.any?(manifest.segments, &(&1.key == key)) or Map.has_key?(manifest.retired, key)}

  defp validate_retained_grace(%{version: 3}, config, opts) do
    if Keyword.get(opts, :grace_ms, 3_600_000) <= Retention.effective_grace(config, signal_string(config)),
      do: :ok,
      else: {:error, :unsupported_compaction_grace}
  end

  defp validate_retained_grace(_manifest, _config, _opts), do: :ok
  defp migration_gate(%{version: 3}, _config), do: :ok

  defp migration_gate(_root, config) do
    if Retention.enforcing?(config, signal_string(config)),
      do: {:error, :retention_migration_required},
      else: :ok
  end

  defp advance_empty_cursor(%{version: 3}, candidates, tenant, config) do
    case candidates.paging["compaction_candidate"] do
      nil ->
        :ok

      start ->
        case Retention.transact(
               tenant,
               signal_string(config),
               config,
               &{%{&1 | paging: Map.put(&1.paging, "compaction_bucket", start)}, :ok}
             ) do
          {:ok, :ok} -> :ok
          error -> error
        end
    end
  end

  defp advance_empty_cursor(_root, _candidates, _tenant, _config), do: :ok

  defp compaction_candidates(%{version: 3} = root, tenant, config),
    do: PagedManifest.candidates(root, tenant, signal_string(config), config)

  defp compaction_candidates(root, _tenant, _config), do: {:ok, root}

  defp prepare_replacement(%{version: 3} = root, sources, replacement, grace, tenant, config) do
    segments =
      Enum.map(sources, fn key ->
        {:ok, segment} = ManifestOwner.segment_from_key(key)
        segment
      end)

    PagedManifest.replace(
      root,
      segments,
      replacement,
      System.system_time(:millisecond) + grace,
      tenant,
      signal_string(config),
      config
    )
  end

  defp prepare_replacement(root, sources, replacement, grace, _tenant, _config),
    do: Manifest.replace(root, sources, replacement, System.system_time(:millisecond) + grace)

  defp publish_current(current, etag, tenant, config, sources, replacement, grace, attempts) do
    case prepare_replacement(current, sources, replacement, grace, tenant, config) do
      {:ok, updated} ->
        payload = updated |> Manifest.encode() |> IO.iodata_to_binary()

        case ObjectStore.put_if_match(config, Manifest.manifest_key(tenant, signal_string(config)), payload, etag) do
          {:ok, _etag} ->
            :ok

          {:error, :precondition_failed} ->
            Process.sleep(10 * (6 - attempts))
            publish(tenant, config, sources, replacement, grace, attempts - 1)

          error ->
            error
        end

      # This branch precedes the conditional PUT. publish/7 has just loaded
      # authoritative state and proved the unique key absent (active/retired or
      # expired keys return success before here), including earlier lost replies.
      {:error, reason} ->
        {:error, {:unpublished_replacement, reason}}
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
        Retention.cleanup_retired(tenant, signal_string(config), config, opts)
      else
        manifest |> deletion_candidates(tenant, maximum, config) |> delete_candidates(tenant, config)
      end
    end
  end

  defp validate_cleanup_limit(maximum) when is_integer(maximum) and maximum > 0, do: :ok
  defp validate_cleanup_limit(_), do: {:error, :invalid_cleanup_options}

  defp deletion_candidates(manifest, tenant, maximum, config) do
    now = System.system_time(:millisecond)
    active = MapSet.new(manifest.segments, & &1.key)

    eligible =
      manifest.retired
      |> Enum.filter(fn {key, retirement} ->
        not retirement.deleted? and retirement.delete_after <= now and valid_segment_key?(tenant, key, config) and
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

    case ObjectStore.put_if_match(config, Manifest.manifest_key(tenant, signal_string(config)), payload, etag) do
      {:ok, _etag} -> :ok
      {:error, :precondition_failed} -> publish_deletions(tenant, config, deleted, cursor, attempts - 1)
      error -> error
    end
  end

  defp load(tenant, config), do: Retention.load(tenant, signal_string(config), config)

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
