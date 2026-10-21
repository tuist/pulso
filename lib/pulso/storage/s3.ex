defmodule Pulso.Storage.S3 do
  @moduledoc """
  S3-backed signal storage.

  By default, each `append/4` writes one Parquet object under a tenant- and
  signal-scoped, versioned, time-partitioned prefix:

      tenants/<tenant>/v4/signal=<s>/date=<YYYY-MM-DD>/hour=<HH>/<min_ts>-<max_ts>-<suffix>.parquet

  Tenant isolation is enforced by key construction — tenant names are
  validated against a conservative charset, so a batch cannot land
  outside its own prefix. `<min_ts>` and `<max_ts>` are zero-padded
  20-decimal-digit representations of the smallest and largest
  caller-supplied `timestamp_ns` in the batch (records with `nil` map to
  `0`, which sorts first). The `date=` and `hour=` partition components
  are derived from `min_ts` as UTC, so a batch spanning an hour boundary
  files under the hour of its earliest sample — segments are small
  enough (`~1s` flush cadence, `~10 MiB` size cap) that cross-boundary
  spill is negligible, and the chronological `[min_ts, max_ts]` tail
  still gives the manifest a tight prunable range.

  The trailing `<suffix>` depends on whether the caller supplied an
  `idempotency_key`:

    * **With `idempotency_key`** — the suffix is a deterministic hash of
      `tenant || idempotency_key || caller_content_hash`. Two `append`
      calls with the same key AND the same caller-supplied content
      resolve to the same object; a lost-response retry does not
      duplicate. Uploads use conditional creation, and failed creates verify
      existing exact bytes by streaming before manifest registration. A byte
      mismatch fails permanently rather than overwriting or guessing equality.
    * **Without `idempotency_key`** — the suffix is random, so distinct
      calls always produce distinct objects and no content hash is
      computed.

  ## Signal dispatch

  Every call carries an explicit `signal` (`:logs` | `:metrics` | `:traces`). The
  adapter translates to the corresponding Rust NIF entry point
  (`encode_log_segment_parquet`, `encode_metric_segment_parquet`, or
  `encode_trace_segment_parquet`) and back. Logs and traces sort by
  `(service, timestamp_ns)`, metrics by `(series_id, timestamp_ns)`.
  Traces additionally retain trace/span IDs as sort tiebreakers. See `Pulso.Codec.NIF`.

  ## Optional unkeyed coalescing

  With `ingest_flush_interval_ms` enabled, concurrent unkeyed requests can
  share a segment. Node-local input reservations bound the buffer at 128
  callers, 100,000 rows, and 10 MiB of estimated term bytes. Overflow and
  oversized batches use the unbuffered path. Keyed requests retain their
  original fingerprint/retry semantics and are never combined. Every caller
  still waits for segment PUT and manifest CAS before acknowledgment.

  ## Manifest coordination

  Every accepted append is registered in the per-`(tenant, signal)`
  manifest via `Pulso.Storage.S3.ManifestOwner`. The owner batches
  concurrent registrations into a single S3 CAS per flush window so
  ingest throughput is decoupled from S3 CAS latency. `query/3` reads
  the manifest from the ETS-backed cache
  (`Pulso.Storage.S3.ManifestCache`), prunes by time bounds, and only
  fetches segments that could contribute records.

  Only after both the segment PUT and the manifest CAS succeed does
  `append/4` return `:ok`. That is the load-bearing durability point.

  ## Key format stability

  Every object lives under a versioned prefix (`tenants/<t>/v4/…`).
  Within one schema version, the exact suffix format is deliberately
  not a public API. Any change to the fingerprint, the delimiter, the
  sort-key width, the number of bounds segments, or the partitioning
  scheme bumps the schema version — new writes go to `v5/`, old objects
  stay at `v4/`, and a compaction job migrates at its own pace.
  """

  @behaviour Pulso.Storage

  alias Pulso.Codec.NIF
  alias Pulso.ObjectStore
  alias Pulso.Record.Log
  alias Pulso.Record.MetricSample
  alias Pulso.Record.Span
  alias Pulso.Record.SpanKey
  alias Pulso.Runtime
  alias Pulso.Storage.S3.AppendBuffer
  alias Pulso.Storage.S3.KeyedPublication
  alias Pulso.Storage.S3.Manifest
  alias Pulso.Storage.S3.Manifest.Segment
  alias Pulso.Storage.S3.ManifestCache
  alias Pulso.Storage.S3.ManifestOwner
  alias Pulso.Storage.S3.MetadataCache
  alias Pulso.Storage.S3.MetricFooterReader
  alias Pulso.Storage.S3.MetricMetadataStore
  alias Pulso.Storage.S3.PagedManifest
  alias Pulso.Storage.S3.Retention
  alias Pulso.Storage.S3.TraceSummary
  alias Pulso.Storage.SortOrder
  alias Pulso.Traces.TempoDiscovery

  @tenant_regex ~r/\A[A-Za-z0-9_.\-]{1,128}\z/
  # 20 decimal digits fits a u64 nanosecond timestamp (max ~1.84e19). Zero-padding
  # keeps S3's UTF-8 list order chronological by write time (`sort_ns`).
  @sort_key_width 20
  # 16 hex chars = 64 bits from SHA-256. Collision probability is negligible
  # for the volumes any single tenant will produce in a step-2 adapter.
  @content_hash_width 16
  # Schema version segment. Baked into every object key so a future change
  # to the key format can coexist with older objects rather than orphan them.
  @schema_version "v4"

  @impl Pulso.Storage
  def append(signal, tenant, records, opts \\ [])

  def append(signal, tenant, [], _opts) when is_atom(signal) and is_binary(tenant) do
    # Validate even on empty so an adversarial tenant name is rejected on the
    # first attempt, not only once a real record survives decoding.
    validate_tenant(tenant)
  end

  def append(signal, tenant, records, opts) when is_atom(signal) and is_binary(tenant) and is_list(records) do
    config = Map.new(Runtime.get_env(:pulso, __MODULE__) || %{})
    keyed? = opts[:idempotency_key] not in [nil, ""]

    if not keyed? and Map.get(config, :ingest_flush_interval_ms, 0) > 0 do
      append_buffered(signal, tenant, records, opts, config)
    else
      append_unbuffered(signal, tenant, records, opts)
    end
  end

  defp append_buffered(signal, tenant, records, opts, config) do
    with :ok <- validate_tenant(tenant),
         :ok <- check_retention(signal, tenant, records, config) do
      case AppendBuffer.append(signal, tenant, records, config, opts) do
        :unbuffered -> append_unbuffered(signal, tenant, records, opts, config)
        result -> result
      end
    end
  end

  @doc false
  def append_unbuffered(signal, tenant, records, opts, config \\ nil) do
    idempotency_key = Keyword.get(opts, :idempotency_key)

    with :ok <- validate_tenant(tenant),
         {:ok, payload, min_ts, max_ts, pruning_safe?} <-
           encode_for_append(signal, records, idempotency_key not in [nil, ""]) do
      config = config || config!()

      key =
        object_key(
          tenant,
          signal_string(signal),
          min_ts,
          max_ts,
          fingerprint(signal, records, idempotency_key),
          idempotency_key
        )

      with :ok <- append_admission(signal, tenant, records, config, opts),
           :ok <- put_segment(config, key, payload, idempotency_key not in [nil, ""], opts) do
        segment = Segment.build(key, min_ts, max_ts, length(records), byte_size(payload))
        segment = summarize_segment(signal, segment, records, pruning_safe?)
        ManifestOwner.register_segments(tenant, signal_string(signal), [segment], config)
      end
    end
  end

  defp put_segment(config, key, payload, true, opts), do: KeyedPublication.put(config, key, payload, opts)

  defp put_segment(config, key, payload, false, _opts) do
    with {:ok, _etag} <- ObjectStore.put(config, key, payload), do: :ok
  end

  defp append_admission(signal, tenant, records, config, opts) do
    result = check_retention(signal, tenant, records, config)

    case result do
      {:error, reason} when reason in [:retention_expired, :timestamp_too_new] ->
        if opts[:split_admission_errors], do: {:error, {:admission_rejected, reason}}, else: result

      _ ->
        result
    end
  end

  @impl Pulso.Storage
  def query_traces_json(tenant, opts, fields) do
    query(:traces, tenant, Keyword.put(opts, :native_json_fields, fields))
  end

  @impl true
  def get_trace_json(tenant, trace_id, opts) do
    opts =
      opts
      |> Keyword.take([:max_scan_segments, :max_scan_bytes, :max_scan_rows, :deadline_ms])
      |> Keyword.put(:trace_id, trace_id)
      |> Keyword.put(:limit, 5001)
      |> Keyword.put(:assemble_trace, trace_id)

    query_traces_json(tenant, opts, nil)
  end

  @impl Pulso.Storage
  def tempo_windowed_discovery_json(tenant, scope, key, query, start, finish, opts),
    do:
      tempo_discovery_json(
        tenant,
        scope,
        key,
        opts |> Keyword.put(:discovery_query, query) |> Keyword.put(:discovery_window, {start, finish})
      )

  @impl true
  def tempo_unscoped_tag_values_json(tenant, key, opts), do: tempo_discovery_json(tenant, "none", key, opts)

  @impl true
  def tempo_unscoped_tag_values_json(tenant, key, query, opts),
    do: tempo_discovery_json(tenant, "none", key, Keyword.put(opts, :discovery_query, query))

  @impl true
  def tempo_windowed_unscoped_tag_values_json(tenant, key, query, start, finish, opts),
    do: tempo_windowed_discovery_json(tenant, "none", key, query, start, finish, opts)

  @impl true
  def tempo_trace_duration_tag_values_json(tenant, query, opts),
    do: tempo_discovery_json(tenant, "intrinsic", "trace:duration", Keyword.put(opts, :discovery_query, query))

  @impl true
  def tempo_windowed_trace_duration_tag_values_json(tenant, query, start, finish, opts),
    do: tempo_windowed_discovery_json(tenant, "intrinsic", "trace:duration", query, start, finish, opts)

  @impl true
  def tempo_root_tag_values_json(tenant, key, query, opts),
    do: tempo_discovery_json(tenant, "intrinsic", key, Keyword.put(opts, :discovery_query, query))

  @impl true
  def tempo_windowed_root_tag_values_json(tenant, key, query, start, finish, opts),
    do: tempo_windowed_discovery_json(tenant, "intrinsic", key, query, start, finish, opts)

  @impl true
  def tempo_event_time_tag_values_json(tenant, key, query, opts),
    do: tempo_discovery_json(tenant, "intrinsic", key, Keyword.put(opts, :discovery_query, query))

  @impl true
  def tempo_windowed_event_time_tag_values_json(tenant, key, query, start, finish, opts),
    do: tempo_windowed_discovery_json(tenant, "intrinsic", key, query, start, finish, opts)

  @impl true
  def tempo_intrinsic_tag_values_json(tenant, key, query, opts),
    do: tempo_discovery_json(tenant, "intrinsic", key, Keyword.put(opts, :discovery_query, query))

  @impl true
  def tempo_default_tag_names_json(tenant, query, opts),
    do: tempo_discovery_json(tenant, "none", "", Keyword.put(opts, :discovery_query, query))

  @impl true
  def tempo_filtered_tag_names_json(tenant, scope, query, opts),
    do: tempo_discovery_json(tenant, scope, "", Keyword.put(opts, :discovery_query, query))

  @impl Pulso.Storage
  def tempo_filtered_discovery_json(tenant, scope, key, query, opts),
    do: tempo_discovery_json(tenant, scope, key, Keyword.put(opts, :discovery_query, query))

  @impl Pulso.Storage
  def tempo_discovery_json(tenant, scope, key, opts) do
    opts =
      opts
      |> Keyword.drop([:start_ts, :end_ts, :limit])
      |> Keyword.put(:max_scan_rows, min(opts[:max_scan_rows] || 1_000_000, 1_000_000))

    with :ok <- TempoDiscovery.validate(scope, key),
         :ok <- TempoDiscovery.validate_window(opts[:discovery_window]),
         {:ok, predicates} <- TempoDiscovery.filter(scope, key, opts[:discovery_query]),
         do: query(:traces, tenant, discovery_opts(scope, key, predicates, opts))
  end

  defp discovery_opts("intrinsic", key, predicates, opts)
       when key in ~w(rootName trace:rootName rootServiceName trace:rootService trace:duration) do
    opts
    |> Keyword.take([:max_scan_segments, :max_scan_bytes, :max_scan_rows, :deadline_ms, :discovery_window])
    |> Keyword.put(:max_scan_bytes, min(opts[:max_scan_bytes] || 33_554_432, 33_554_432))
    |> Keyword.put(:max_scan_segments, min(opts[:max_scan_segments] || 1024, 1024))
    |> Keyword.put(:tempo_root_discovery, {key, predicates})
  end

  defp discovery_opts(scope, key, predicates, opts), do: Keyword.put(opts, :tempo_discovery, {scope, key, predicates})

  @impl Pulso.Storage
  def tempo_expression_search_json(tenant, program, opts), do: tempo_search_json(tenant, {:expression, program}, opts)

  @impl Pulso.Storage
  def tempo_search_json(tenant, predicates, opts) do
    opts = opts |> Keyword.drop([:start_ts, :end_ts])
    opts = Keyword.put(opts, :max_scan_rows, min(opts[:max_scan_rows] || 1_000_000, 1_000_000))
    opts = Keyword.put(opts, :max_scan_bytes, min(opts[:max_scan_bytes] || 33_554_432, 33_554_432))
    opts = Keyword.put(opts, :max_scan_segments, min(opts[:max_scan_segments] || 1024, 1024))
    query(:traces, tenant, Keyword.put(opts, :tempo_search, predicates))
  end

  @impl Pulso.Storage
  def search_traces_json(tenant, predicates, opts) do
    opts = Keyword.put(opts, :max_scan_rows, min(opts[:max_scan_rows] || 1_000_000, 1_000_000))
    query(:traces, tenant, Keyword.put(opts, :trace_search, predicates))
  end

  @impl Pulso.Storage
  def metric_discovery_json(tenant, mode, key, selectors, opts) do
    # A single selector can reuse proven manifest pruning. OR selectors must
    # never be narrowed to one branch; they share one conservative source scan.
    matchers =
      case selectors do
        [one] -> one
        _ -> []
      end

    opts =
      opts
      |> Keyword.drop([:limit, :matchers])
      |> Keyword.put(:start_ts, opts[:start_ts] || -9_223_372_036_854_775_808)
      |> Keyword.put(:end_ts, opts[:end_ts] || 9_223_372_036_854_775_807)

    opts =
      Enum.reduce([max_scan_segments: 1024, max_scan_bytes: 134_217_728, max_scan_rows: 1_000_000], opts, fn {name, cap},
                                                                                                             acc ->
        Keyword.put(acc, name, min(Keyword.get(acc, name, cap), cap))
      end)

    query(:metrics, tenant, [metric_discovery: {mode, key, selectors}, matchers: matchers] ++ opts)
  end

  @impl Pulso.Storage
  def log_index_stats(tenant, opts) do
    with :ok <- validate_tenant(tenant),
         :ok <- check_deadline(opts[:deadline_ms]) do
      config = config!()

      timeout =
        if opts[:deadline_ms],
          do: max(0, opts[:deadline_ms] - System.monotonic_time(:millisecond)),
          else: Map.get(config, :retention_timeout_ms, 30_000)

      config
      |> Map.put(:retention_timeout_ms, timeout)
      |> PagedManifest.with_budget(fn -> index_stats(tenant, opts, config) end)
      |> budget_result(opts)
    end
  end

  # Manifest metadata only: every retained segment overlapping the range
  # whose service summary can match, counted whole. No segment is read.
  defp index_stats(tenant, opts, config) do
    with {:ok, entry} <- ManifestOwner.ensure_loaded(tenant, "logs", config, opts) do
      start_ts = retained_start(entry.manifest, opts[:start_ts])

      with {:ok, segments} <- query_segments(entry.manifest, :logs, start_ts, opts[:end_ts], opts, tenant, config) do
        {:ok,
         %{
           segments: length(segments),
           entries: Enum.sum_by(segments, &(&1.row_count || 0)),
           bytes: Enum.sum_by(segments, &(&1.byte_size || 0))
         }}
      end
    end
  end

  @impl Pulso.Storage
  def log_metric_scan(tenant, opts, scan) do
    query(:logs, tenant, Keyword.put(Keyword.delete(opts, :limit), :log_metric, scan))
  end

  @impl Pulso.Storage
  def put_metric_metadata(tenant, entries, today), do: MetricMetadataStore.put(config!(), tenant, entries, today)

  @impl Pulso.Storage
  def metric_metadata(tenant), do: MetricMetadataStore.get(config!(), tenant)

  @impl Pulso.Storage
  def trace_metrics_scan(tenant, opts, metrics) do
    query(:traces, tenant, Keyword.put(Keyword.delete(opts, :limit), :trace_metrics, metrics))
  end

  @impl Pulso.Storage
  def fold_logs(tenant, opts, acc, fun) when is_function(fun, 2) do
    query(:logs, tenant, Keyword.put(Keyword.delete(opts, :limit), :log_fold, {fun, acc}))
  end

  # Live tail follows log segments published within this window; older late
  # arrivals are left to queries. Excess unseen originals fail explicitly:
  # a partial cursor could silently lose unread sources to compaction.
  @follow_window_ns 3_600_000_000_000
  @follow_max_segments 64

  @impl Pulso.Storage
  def follow_logs(tenant, seen) do
    with :ok <- validate_tenant(tenant) do
      config = config!()
      floor = System.os_time(:nanosecond) - @follow_window_ns

      config
      |> PagedManifest.with_budget(fn -> follow_segments(tenant, floor, seen, config) end)
      |> budget_result([])
    end
  end

  defp follow_segments(tenant, floor, seen, config) do
    case ManifestOwner.ensure_loaded(tenant, "logs", config, require_fresh_manifest: true) do
      {:ok, entry} ->
        with {:ok, segments} <- query_segments(entry.manifest, :logs, floor, nil, [], tenant, config),
             do: read_unseen(segments, seen, config)

      {:error, :signal_authority_missing} ->
        {:ok, [], seen || MapSet.new()}

      {:error, _} = error ->
        error
    end
  end

  # The first poll only records what already exists; history is the query
  # path's job. Keys leaving the window are forgotten with it.
  defp read_unseen(segments, nil, _config), do: {:ok, [], MapSet.new(segments, & &1.key)}

  defp read_unseen(segments, seen, config) do
    current = MapSet.new(segments, & &1.key)

    unseen =
      segments
      |> Enum.reject(&(MapSet.member?(seen, &1.key) or compaction_output?(&1.key)))
      |> Enum.sort_by(&{&1.max_ts || 0, &1.key})
      |> Enum.take(@follow_max_segments + 1)

    if length(unseen) > @follow_max_segments do
      {:error, :tail_lag_limit}
    else
      with {:ok, records} <- scan_segments(:logs, config, unseen, nil, nil, [], nil) do
        {:ok, records, seen |> MapSet.intersection(current) |> MapSet.union(MapSet.new(unseen, & &1.key))}
      end
    end
  end

  # Do not replay replacements. Settle is event-time based, not proof that a
  # follower observed originals; late publication/poll failure can still miss rows.
  defp compaction_output?(key), do: key |> Path.basename() |> String.contains?("-compact-")

  @impl Pulso.Storage
  def log_discovery_json(tenant, mode, key, opts) do
    opts =
      opts
      |> Keyword.delete(:limit)
      |> Keyword.put(:start_ts, opts[:start_ts] || -9_223_372_036_854_775_808)
      |> Keyword.put(:end_ts, opts[:end_ts] || 9_223_372_036_854_775_807)

    opts =
      Enum.reduce([max_scan_segments: 1024, max_scan_bytes: 134_217_728], opts, fn {name, cap}, acc ->
        Keyword.put(acc, name, min(Keyword.get(acc, name, cap), cap))
      end)

    query(:logs, tenant, [log_discovery: {mode, key}] ++ opts)
  end

  @impl true
  def query(signal, tenant, opts) when is_atom(signal) and is_binary(tenant) and is_list(opts) do
    # An already expired deadline is a timeout, not a metadata budget overrun.
    with :ok <- validate_tenant(tenant),
         :ok <- check_deadline(opts[:deadline_ms]) do
      config = config!()

      timeout =
        if opts[:deadline_ms],
          do: max(0, opts[:deadline_ms] - System.monotonic_time(:millisecond)),
          else: Map.get(config, :retention_timeout_ms, 30_000)

      budget = Map.put(config, :retention_timeout_ms, timeout)

      budget
      |> PagedManifest.with_budget(fn -> do_query(signal, tenant, opts, config) end)
      |> budget_result(opts)
    end
  end

  # The metadata budget shares the caller's deadline, so exhausting it after
  # that deadline is a timeout rather than an oversized query.
  defp budget_result({:ok, {:error, :metadata_scan_limit}}, opts), do: metadata_limit(opts)
  defp budget_result({:ok, result}, _opts), do: result
  defp budget_result({:error, :metadata_scan_limit}, opts), do: metadata_limit(opts)
  defp budget_result(error, _opts), do: error

  defp metadata_limit(opts) do
    with :ok <- check_deadline(opts[:deadline_ms]), do: {:error, :query_scan_limit}
  end

  defp do_query(signal, tenant, opts, config) do
    start_ts = Keyword.get(opts, :start_ts)
    end_ts = Keyword.get(opts, :end_ts)
    limit = Keyword.get(opts, :limit)
    native? = opts[:log_metric] != nil or opts[:trace_metrics] != nil

    with :ok <- check_deadline(opts[:deadline_ms]),
         {:ok, entry} <- ManifestOwner.ensure_loaded(tenant, signal_string(signal), config, opts),
         :ok <- check_deadline(opts[:deadline_ms]) do
      start_ts = retained_start(entry.manifest, start_ts)

      with {:ok, segments} <- query_segments(entry.manifest, signal, start_ts, end_ts, opts, tenant, config),
           :ok <- check_scan_budget(signal, segments, opts) do
        scan_segments(signal, config, segments, start_ts, end_ts, opts, limit)
      end
      |> case do
        {:ok, records} ->
          {:ok, finalize_records(records, signal, limit)}

        # A native range scan has already absorbed the partial pass; rescanning
        # into it would count rows twice. The caller re-evaluates instead.
        {:error, reason} when native? and reason == :manifest_page_missing ->
          {:error, :native_fallback}

        {:error, {:segment_missing, _}} when native? ->
          {:error, :native_fallback}

        {:error, :manifest_page_missing} ->
          retry_scan(signal, tenant, config, start_ts, end_ts, opts, limit)

        {:error, {:segment_missing, consumed}} ->
          # Cleanup can overtake a cached snapshot or an in-flight query.
          # Restart the whole scan against a fresh manifest, never mix generations.
          retry_scan(signal, tenant, config, start_ts, end_ts, remaining_budget(opts, consumed), limit)

        {:error, _} = err ->
          err
      end
    end
  end

  defp finalize_records(json, _signal, _limit) when is_binary(json), do: json
  defp finalize_records({:log_fold, acc}, _signal, _limit), do: acc
  defp finalize_records(records, signal, limit), do: records |> SortOrder.sort(signal) |> take_limit(limit)

  @doc false
  def check_retention(signal, tenant, records, config) do
    name = signal_string(signal)

    cached = ManifestCache.get(tenant, name)

    cond do
      # Managed roots check eligibility and capacity; enforcing nodes also gate
      # legacy roots behind conversion.
      (cached != nil and cached.manifest.version == 3) or Retention.enforcing?(config, name) ->
        do_check_retention(signal, tenant, records, config)

      cached != nil ->
        :ok

      # A cold scope probes the marker so a managed prefix is checked before
      # the segment upload. The probe is advisory: on error the append
      # proceeds, and the owner's marker check and root CAS still refuse to
      # publish into a managed prefix it has not loaded.
      true ->
        case cached_marker(config, tenant, name) do
          {:ok, true} -> do_check_retention(signal, tenant, records, config)
          _ -> :ok
        end
    end
  end

  @doc false
  def retained_traces(tenant, spans) do
    config = config!()
    cached = ManifestCache.get(tenant, "traces")

    managed? =
      (cached != nil and cached.manifest.version == 3) or Retention.enforcing?(config, "traces") or
        (cached == nil and cached_marker(config, tenant, "traces") == {:ok, true})

    if managed? do
      with {:ok, entry} <- ManifestOwner.ensure_loaded(tenant, "traces", config),
           {:ok, root} <- ensure_policy(entry.manifest, tenant, "traces", config) do
        filter_retained_spans(root, spans, config)
      end
    else
      {:ok, spans, 0}
    end
  end

  defp filter_retained_spans(%Manifest{version: 3} = root, spans, config) do
    floor = PagedManifest.floor(root)
    latest = System.system_time(:nanosecond) + Map.get(config, :retention_future_skew_ms, 600_000) * 1_000_000
    {kept, expired} = Enum.split_with(spans, &(&1.timestamp_ns >= floor and &1.timestamp_ns <= latest))
    {:ok, kept, length(expired)}
  end

  defp filter_retained_spans(_root, spans, _config), do: {:ok, spans, 0}

  defp cached_marker(config, tenant, signal) do
    ref = %{"k" => "marker-cache:" <> PagedManifest.scope(tenant, signal), "b" => 1}
    now = System.monotonic_time(:millisecond)

    case MetadataCache.get(ref) do
      {:ok, {checked, exists}} when now - checked < 1000 ->
        {:ok, exists}

      _ ->
        case PagedManifest.managed?(config, tenant, signal) do
          {:ok, exists} = result ->
            MetadataCache.put(ref, {now, exists})
            result

          error ->
            error
        end
    end
  end

  defp do_check_retention(signal, tenant, records, config) do
    name = signal_string(signal)

    with {:ok, entry} <- ManifestOwner.ensure_loaded(tenant, name, config),
         {:ok, root} <- ensure_policy(entry.manifest, tenant, name, config),
         do: check_admission(root, records, config)
  end

  defp check_admission(%Manifest{version: 3} = root, records, config) do
    timestamps = Enum.map(records, &(&1.timestamp_ns || 0))
    segment = Segment.build("", Enum.min(timestamps), Enum.max(timestamps), length(records))

    with :ok <- PagedManifest.eligible(root, segment, System.system_time(:nanosecond), config),
         {:ok, :ok} <- PagedManifest.check_capacity(root, segment, config),
         do: :ok
  end

  defp check_admission(_root, _records, _config), do: :ok

  defp ensure_policy(%Manifest{version: 3} = root, _tenant, _signal, _config), do: {:ok, root}

  defp ensure_policy(root, tenant, signal, config) do
    cond do
      not Retention.enforcing?(config, signal) -> {:ok, root}
      root.segments == [] and map_size(root.retired) == 0 -> convert_empty(tenant, signal, config)
      true -> {:error, :retention_migration_required}
    end
  end

  defp convert_empty(tenant, signal, config) do
    with {:ok, _} <- Retention.advance(tenant, signal, Map.put(config, :retention_empty_conversion_only, true)),
         {:ok, managed, _} <- Retention.load(tenant, signal, config),
         do: {:ok, managed}
  end

  defp retained_start(%Manifest{version: 3} = root, nil), do: PagedManifest.floor(root)
  defp retained_start(%Manifest{version: 3} = root, start), do: max(start, PagedManifest.floor(root))
  defp retained_start(_root, start), do: start

  defp query_segments(%Manifest{version: 3} = root, signal, start_ts, end_ts, opts, tenant, config) do
    opts = opts |> Keyword.put(:start_ts, start_ts) |> Keyword.put(:end_ts, end_ts)
    PagedManifest.query(root, tenant, signal_string(signal), opts, config)
  end

  defp query_segments(manifest, signal, start_ts, end_ts, opts, _tenant, _config) do
    segments = Manifest.prune_by_time(manifest, start_ts, end_ts)

    filtered =
      case signal do
        :metrics -> Enum.filter(segments, &Segment.matches_metrics?(&1, Keyword.get(opts, :matchers, [])))
        :logs -> Enum.filter(segments, &Segment.matches_log_service?(&1, opts))
        :traces -> Enum.filter(segments, &TraceSummary.matches?(&1.trace_summary, opts))
      end

    {:ok, filtered}
  end

  defp remaining_budget(opts, consumed) do
    Enum.reduce(consumed, opts, fn {key, used}, acc ->
      case Keyword.get(acc, key) do
        nil -> acc
        maximum -> Keyword.put(acc, key, maximum - used)
      end
    end)
  end

  defp retry_scan(signal, tenant, config, start_ts, end_ts, opts, limit) do
    with :ok <- check_deadline(opts[:deadline_ms]),
         {:ok, manifest, _etag} <- Retention.load(tenant, signal_string(signal), config),
         start_ts = retained_start(manifest, start_ts),
         {:ok, segments} <- query_segments(manifest, signal, start_ts, end_ts, opts, tenant, config),
         :ok <- check_deadline(opts[:deadline_ms]),
         :ok <- check_scan_budget(signal, segments, opts),
         {:ok, records} <- scan_segments(signal, config, segments, start_ts, end_ts, opts, limit) do
      {:ok, finalize_records(records, signal, limit)}
    else
      {:error, {:segment_missing, _}} -> {:error, :not_found}
      {:error, _} = error -> error
    end
  end

  # Limited traces may stop at a strict timestamp boundary. Actual visited
  # objects are still charged for bytes/rows/segments in scan_segments/7.
  defp check_scan_budget(:traces, segments, opts) do
    service = TraceSummary.service_filter(opts[:tempo_search])

    cond do
      is_binary(service) ->
        if length(segments) > TraceSummary.plan_catalog_limit(),
          do: {:error, :query_scan_limit},
          else: check_scan_budget(candidate_segments(segments, service), opts)

      limited_span_query?(opts) ->
        :ok

      true ->
        check_scan_budget(segments, opts)
    end
  end

  defp check_scan_budget(_signal, segments, opts), do: check_scan_budget(segments, opts)

  defp limited_span_query?(opts),
    do:
      is_integer(opts[:limit]) and opts[:limit] in 1..5000 and
        Enum.all?([:trace_search, :tempo_search, :tempo_discovery, :tempo_root_discovery], &is_nil(opts[&1]))

  defp check_scan_budget(segments, opts) do
    bytes = Enum.sum(Enum.map(segments, &(&1.byte_size || 0)))
    rows = Enum.sum(Enum.map(segments, &(&1.row_count || 0)))

    if exceeds_budget?(length(segments), opts[:max_scan_segments]) or
         exceeds_budget?(bytes, opts[:max_scan_bytes]) or exceeds_budget?(rows, opts[:max_scan_rows]),
       do: {:error, :query_scan_limit},
       else: :ok
  end

  defp exceeds_budget?(_, nil), do: false
  defp exceeds_budget?(value, max), do: value > max
  defp check_deadline(nil), do: :ok

  defp check_deadline(deadline) do
    if System.monotonic_time(:millisecond) >= deadline, do: {:error, :query_timeout}, else: :ok
  end

  defp summarize_segment(:metrics, segment, records, _proof), do: Segment.summarize_metrics(segment, records)
  defp summarize_segment(:logs, segment, records, _proof), do: Segment.summarize_logs(segment, records)

  defp summarize_segment(:traces, segment, records, proof) do
    %{segment | trace_summary: TraceSummary.build(records, proof)}
  end

  # -- helpers -----------------------------------------------------------------

  defp signal_string(:logs), do: "logs"
  defp signal_string(:metrics), do: "metrics"
  defp signal_string(:traces), do: "traces"

  # The content fingerprint only matters when an idempotency key makes the
  # object key deterministic. Without one the key is random anyway, so
  # skip hashing the batch.
  defp fingerprint(signal, records, idempotency_key)
       when is_binary(idempotency_key) and byte_size(idempotency_key) > 0 do
    {:ok, hash} = caller_content_hash(signal, records)
    hash
  end

  defp fingerprint(_signal, _records, _idempotency_key), do: rand_hex()

  # Encode for fresh/random object keys only: metric output may add footer
  # postings. Deterministic keyed writers must use append/4, which preserves
  # legacy v2 bytes. Do not PUT this helper's output under an existing key.
  @doc false
  @spec encode_segment(Pulso.Storage.signal(), [Pulso.Storage.signal_record()]) ::
          {:ok, binary(), integer(), integer()} | {:error, term()}
  def encode_segment(:logs, records) do
    with {:ok, normalized} <- normalize_logs(records) do
      case NIF.encode_log_segment_parquet_v2(normalized) do
        {:ok, payload, min_ts, max_ts, _count} ->
          {:ok, payload, min_ts, max_ts}

        {:error, _} = error ->
          error

        :fallback ->
          {:error, {:encode_failed, :parquet_encoder_rejected_input}}
      end
    end
  end

  def encode_segment(:traces, records) do
    with {:ok, payload, min_ts, max_ts, _proof} <- encode_for_append(:traces, records),
         do: {:ok, payload, min_ts, max_ts}
  end

  def encode_segment(:metrics, records), do: encode_metrics(records, true)

  defp encode_metrics(records, indexed?) do
    result =
      if indexed?,
        do: NIF.encode_metric_segment_parquet_indexed_v1(records),
        else: NIF.encode_metric_segment_parquet_v2(records)

    case result do
      {:ok, payload, min_ts, max_ts, _count} ->
        {:ok, payload, min_ts, max_ts}

      {:error, _} = error ->
        error

      :fallback ->
        {:error, {:encode_failed, :parquet_encoder_rejected_input}}
    end
  end

  defp encode_for_append(signal, records, keyed? \\ false)

  defp encode_for_append(:traces, records, _keyed?) do
    case NIF.encode_trace_segment_parquet_v2(Enum.map(records, &Span.to_map/1)) do
      {:ok, payload, min_ts, max_ts, _count, proof} -> {:ok, payload, min_ts, max_ts, proof}
      {:error, _} = error -> error
      :fallback -> {:error, {:encode_failed, :parquet_encoder_rejected_input}}
    end
  end

  # Keep keyed bytes compatible with pre-index objects. Conditional creation
  # now prevents overwrites, but a different footer would become a permanent
  # exact-byte conflict. Fresh compaction keys can safely acquire postings.
  defp encode_for_append(:metrics, records, true) do
    with {:ok, payload, min_ts, max_ts} <- encode_metrics(records, false),
         do: {:ok, payload, min_ts, max_ts, nil}
  end

  defp encode_for_append(signal, records, _keyed?) do
    with {:ok, payload, min_ts, max_ts} <- encode_segment(signal, records),
         do: {:ok, payload, min_ts, max_ts, nil}
  end

  @doc false
  def validate_tenant(tenant) do
    if tenant not in [".", ".."] and Regex.match?(@tenant_regex, tenant) do
      :ok
    else
      {:error, {:invalid_tenant, tenant}}
    end
  end

  defp normalize_logs(records) do
    Enum.reduce_while(records, {:ok, []}, fn
      %Log{} = record, {:ok, acc} ->
        with {:ok, attrs} <- sanitize_map(record.attributes || %{}),
             {:ok, resource} <- sanitize_map(record.resource || %{}) do
          normalized = %{record | attributes: attrs, resource: resource}
          {:cont, {:ok, [normalized | acc]}}
        else
          err -> {:halt, err}
        end
    end)
    |> case do
      {:ok, records} -> {:ok, Enum.reverse(records)}
      err -> err
    end
  end

  # Coerce every attribute/resource map key to a string, recursively.
  @doc false
  @spec sanitize_map(map()) :: {:ok, map()} | {:error, {:attribute_key_collision, [String.t()]}}
  def sanitize_map(map) when is_map(map) do
    Enum.reduce_while(map, {:ok, %{}}, &insert_sanitized/2)
  end

  defp insert_sanitized({k, v}, {:ok, acc}) do
    string_key = stringify_key(k)

    if Map.has_key?(acc, string_key) do
      {:halt, {:error, {:attribute_key_collision, [string_key]}}}
    else
      put_sanitized(acc, string_key, v)
    end
  end

  defp put_sanitized(acc, key, value) do
    case sanitize_value(value) do
      {:ok, sanitized} -> {:cont, {:ok, Map.put(acc, key, sanitized)}}
      err -> {:halt, err}
    end
  end

  defp sanitize_value(v) when is_map(v), do: sanitize_map(v)
  defp sanitize_value(v) when is_list(v), do: sanitize_list(v)
  defp sanitize_value(v), do: {:ok, v}

  defp sanitize_list(list) do
    list
    |> Enum.reduce_while({:ok, []}, &prepend_sanitized/2)
    |> case do
      {:ok, sanitized} -> {:ok, Enum.reverse(sanitized)}
      err -> err
    end
  end

  defp prepend_sanitized(value, {:ok, acc}) do
    case sanitize_value(value) do
      {:ok, sanitized} -> {:cont, {:ok, [sanitized | acc]}}
      err -> {:halt, err}
    end
  end

  defp stringify_key(k) when is_binary(k), do: k
  defp stringify_key(k) when is_atom(k), do: Atom.to_string(k)
  defp stringify_key(k) when is_integer(k), do: Integer.to_string(k)
  defp stringify_key(k), do: inspect(k)

  # Scan segments newest-first, decode each, keep only records inside the
  # requested filter, and short-circuit once we know the running accumulator
  # already dominates every remaining segment. The safety condition:
  #
  #   have >= limit records AND
  #   kth-largest.timestamp_ns > next_segment.max_ts
  #
  # means no future segment can produce a record higher than what we have,
  # so it is safe to stop. Segments whose max_ts is unknown never satisfy
  # the condition, so they always get fetched — that is the price of not
  # knowing their bounds.
  #
  # `opts` is passed through to the per-signal decoder so each signal's
  # pushdowns (service + matchers + line_filters for logs, matchers for
  # metrics) stay typed at the signal boundary rather than fanning into
  # positional args here.
  defp scan_segments(signal, config, segments, start_ts, end_ts, opts, limit) do
    with {:ok, native} <- start_native_query(signal, start_ts, end_ts, opts) do
      ctx = %{
        signal: signal,
        config: config,
        segments: segments,
        start_ts: start_ts,
        end_ts: end_ts,
        opts: opts,
        limit: limit,
        search?:
          not is_nil(opts[:trace_search]) or not is_nil(opts[:tempo_search]) or not is_nil(opts[:tempo_discovery]) or
            not is_nil(opts[:tempo_root_discovery]),
        metric_discovery?: not is_nil(opts[:metric_discovery]),
        log_discovery?: not is_nil(opts[:log_discovery]),
        log_fold: opts[:log_fold],
        log_metric: opts[:log_metric],
        trace_metrics: opts[:trace_metrics],
        discovery?: not is_nil(opts[:tempo_discovery]),
        tempo?: not is_nil(opts[:tempo_search]) or not is_nil(opts[:tempo_root_discovery]),
        native: native
      }

      run_scan(ctx)
      |> finish_scan(native, limit, scan_mode(opts))
    end
  end

  # Decode a segment a bounded chunk at a time: a compacted segment can hold
  # 100k rows, more than one query process may hold as terms at once.
  @fold_chunk_records 4096
  defp fold_segment(blob, ctx, fun, acc, count) do
    service = Keyword.get(ctx.opts, :service)
    matchers = Keyword.get(ctx.opts, :matchers, [])
    line_filters = Keyword.get(ctx.opts, :line_filters, [])
    :ok = validate_decode_args(blob, ctx.start_ts, ctx.end_ts, service, matchers, line_filters)

    case NIF.log_segment_reader_new(blob, ctx.start_ts, ctx.end_ts, service, matchers, line_filters) do
      {:ok, reader} -> fold_chunks(reader, fun, acc, count)
      :fallback -> {:error, {:decode_failed, :parquet_decoder_rejected_input}}
    end
  end

  defp fold_chunks(reader, fun, acc, count) do
    case NIF.log_segment_reader_next(reader, @fold_chunk_records) do
      {:ok, records} -> fold_chunks(reader, fun, fun.(records, acc), count + length(records))
      :done -> {:ok, acc, count}
      :fallback -> {:error, {:decode_failed, :parquet_decoder_rejected_input}}
    end
  end

  defp fold_acc({_fun, acc}), do: acc
  defp fold_acc(nil), do: nil

  # Segments are visited newest first. A fold may stop once the next segment
  # cannot contribute, judged from that segment's newest timestamp.
  defp fold_done?(%{opts: opts, segments: segments}, acc, index) do
    case opts[:log_fold_stop] do
      nil -> false
      stop -> stop.(acc, segments |> Enum.at(index + 1) |> then(&(&1 && &1.max_ts)))
    end
  end

  defp candidate_segments(segments, service),
    do: Enum.filter(segments, &TraceSummary.candidate_source?(&1.trace_summary, service))

  defp run_scan(ctx) do
    fold =
      cond do
        ctx.opts[:log_metric] -> {0, 0}
        ctx.opts[:trace_metrics] -> 0
        true -> fold_acc(ctx.opts[:log_fold])
      end

    state = %{batches: [], count: 0, top: [], bytes: 0, rows: 0, fold: fold}

    case TraceSummary.service_filter(ctx.opts[:tempo_search]) do
      nil ->
        scan_sources(ctx.segments, ctx, state)

      target ->
        first = candidate_segments(ctx.segments, target)

        with {:ok, state} <- scan_sources(first, ctx, state),
             {:ok, ids} <- native_result(NIF.tempo_search_plan_seal(ctx.native)),
             do: scan_context(ctx, first, state, ids)
    end
  end

  # Object GET latency dominates scans on real object stores. With
  # `query_prefetch` above 1 (opt-in), the next segments are fetched while
  # earlier ones are processed, in order; a halted scan stops outstanding
  # fetches. Budgets are still charged as segments are visited, so up to
  # `query_prefetch - 1` extra reads can happen past an exhausted budget or an
  # early stop. The default reads strictly one segment at a time.
  defp scan_sources(sources, ctx, state) do
    items = Enum.with_index(sources)
    concurrency = Map.get(ctx.config, :query_prefetch, 1)

    if concurrency > 1 and length(items) > 1 and prefetchable?(ctx) do
      items
      |> Pulso.Runtime.Task.async_stream(fn {segment, _} -> prefetch(ctx, segment) end,
        max_concurrency: concurrency,
        ordered: true,
        timeout: :infinity
      )
      |> Stream.zip(items)
      |> Enum.reduce_while({:ok, state}, fn {{:ok, fetched}, item}, acc -> visit_segment(item, acc, ctx, fetched) end)
    else
      Enum.reduce_while(items, {:ok, state}, &visit_segment(&1, &2, ctx))
    end
  end

  # Footer-first metric reads issue their own ranged requests.
  defp prefetchable?(%{signal: :metrics, metric_discovery?: false, config: %{metric_footer_pruning_enabled: true}}),
    do: false

  defp prefetchable?(_ctx), do: true

  defp prefetch(ctx, segment) do
    case ctx.opts[:max_scan_bytes] do
      limit when is_integer(limit) -> read_bounded_segment(ctx.config, segment.key, limit)
      _ -> ObjectStore.get(ctx.config, segment.key)
    end
  end

  defp scan_context(_ctx, _first, state, <<>>), do: {:ok, state}

  defp scan_context(ctx, first, state, ids) do
    # Only bounded IDs/hashes cross BEAM, never spans. Native owns the candidate
    # intersection, bloom work budget and complete-context aggregation.
    hashes = for <<id::binary-size(32) <- ids>>, into: <<>>, do: :binary.part(:crypto.hash(:sha256, id), 0, 8)

    with {:ok, true} <- native_result(NIF.tempo_search_plan_hashes(ctx.native, hashes)) do
      fetched = MapSet.new(first, & &1.key)

      ctx.segments
      |> Enum.reject(&MapSet.member?(fetched, &1.key))
      |> Enum.reduce_while({:ok, {state, length(first)}}, &visit_context_segment(&1, &2, ctx))
      |> case do
        {:ok, {state, _visited}} -> {:ok, state}
        {:error, _} = error -> error
      end
    end
  end

  defp visit_context_segment(segment, {:ok, {state, index}} = acc, ctx) do
    {hashes, bits} = TraceSummary.context_filter(segment.trace_summary)

    case native_result(NIF.tempo_search_plan_may_match(ctx.native, hashes, bits)) do
      {:ok, false} ->
        {:cont, acc}

      {:ok, true} ->
        case visit_segment({segment, index}, {:ok, state}, ctx) do
          {:cont, {:ok, state}} -> {:cont, {:ok, {state, index + 1}}}
          {:halt, error} -> {:halt, error}
        end

      {:error, _} = error ->
        {:halt, error}
    end
  end

  @log_modes [:log_discovery, :log_fold, :log_metric]
  defp scan_mode(opts) do
    cond do
      opts[:metric_discovery] -> :metric_discovery
      opts[:trace_metrics] -> :trace_metrics
      mode = Enum.find(@log_modes, &opts[&1]) -> mode
      opts[:tempo_root_discovery] -> context_discovery_mode(opts[:tempo_root_discovery])
      opts[:tempo_discovery] -> :tempo_discovery
      opts[:tempo_search] -> :tempo_search
      opts[:trace_search] -> :search
      true -> opts[:assemble_trace]
    end
  end

  defp context_discovery_mode({"trace:duration", _}), do: :tempo_trace_duration_discovery
  defp context_discovery_mode({_root_key, _}), do: :tempo_root_discovery

  defp start_native_query(:traces, start, finish, opts) do
    case opts[:tempo_root_discovery] do
      nil ->
        start_native_trace_query(start, finish, opts)

      {key, predicates} ->
        start_context_discovery(start, key, predicates, opts[:discovery_window] || {0, 0})
    end
  end

  defp start_native_query(:metrics, start, finish, opts) do
    case opts[:metric_discovery] do
      {mode, key, selectors} ->
        case NIF.metric_discovery_new(mode, key, start, finish, selectors) do
          :fallback -> {:error, :invalid_discovery_selector}
          result -> result
        end

      nil ->
        {:ok, nil}
    end
  end

  defp start_native_query(:logs, start, finish, opts) do
    case opts[:log_discovery] do
      {mode, key} ->
        case NIF.log_discovery_new(mode, key, start, finish, opts[:service], opts[:matchers] || []) do
          :fallback -> {:error, :invalid_discovery_selector}
          result -> result
        end

      nil ->
        {:ok, nil}
    end
  end

  defp start_native_query(_signal, _start, _finish, _opts), do: {:ok, nil}

  defp start_context_discovery(floor, "trace:duration", predicates, {start, finish}),
    do: native_result(NIF.tempo_trace_duration_discovery_new(floor || 0, predicates, start, finish))

  defp start_context_discovery(floor, key, predicates, {start, finish}),
    do: native_result(NIF.tempo_root_discovery_new(floor || 0, key, predicates, start, finish))

  defp start_native_trace_query(start, finish, opts) do
    cond do
      opts[:trace_metrics] ->
        {:ok, nil}

      opts[:tempo_discovery] ->
        {scope, key, predicates} = opts[:tempo_discovery]
        start_discovery(start || 0, scope, key, predicates, opts[:discovery_window])

      opts[:tempo_search] ->
        start_tempo_search(start, opts)

      opts[:trace_search] ->
        native_result(
          NIF.trace_search_new(
            start,
            finish,
            opts[:trace_search],
            opts[:limit] || 100,
            opts[:max_trace_groups] || 65_536
          )
        )

      true ->
        start_native_span_query(start, finish, opts)
    end
  end

  defp start_discovery(floor, scope, key, predicates, {start, finish}),
    do: native_result(NIF.tempo_windowed_discovery_new(floor, scope, key, predicates, start, finish))

  defp start_discovery(floor, scope, key, predicates, nil), do: start_discovery(floor, scope, key, predicates)

  defp start_discovery(floor, scope, key, []), do: native_result(NIF.tempo_discovery_new(floor, scope, key))

  defp start_discovery(floor, scope, key, predicates),
    do: native_result(NIF.tempo_filtered_discovery_new(floor, scope, key, predicates))

  defp start_tempo_search(start, opts) do
    {constructor, predicates} =
      case opts[:tempo_search] do
        {:expression, program} -> {&NIF.tempo_expression_search_new/6, program}
        predicates -> {&NIF.tempo_search_new/6, predicates}
      end

    native_result(
      constructor.(start || 0, opts[:trace_start], opts[:trace_end], predicates, opts[:limit] || 100, opts[:spss] || 3)
    )
  end

  defp start_native_span_query(start, finish, opts) do
    if Keyword.has_key?(opts, :native_json_fields) do
      maximum = opts[:max_records] || 9_223_372_036_854_775_807

      result =
        if opts[:assemble_trace] do
          NIF.trace_query_new_trace(opts[:assemble_trace], start, maximum)
        else
          filters = Map.put_new(trace_filters(opts), "limit", 5001)
          NIF.trace_query_new(start, finish, filters, opts[:native_json_fields], maximum)
        end

      native_result(result)
    else
      {:ok, nil}
    end
  end

  defp native_result(:fallback), do: {:error, {:decode_failed, :parquet_decoder_rejected_input}}
  defp native_result(result), do: result

  defp finish_scan({:ok, %{fold: acc}}, nil, _limit, :log_fold), do: {:ok, {:log_fold, acc}}
  defp finish_scan({:ok, %{fold: acc}}, nil, _limit, :log_metric), do: {:ok, {:log_fold, acc}}
  defp finish_scan({:ok, %{fold: rows}}, nil, _limit, :trace_metrics), do: {:ok, {:log_fold, rows}}

  defp finish_scan({:ok, %{batches: batches}}, nil, _limit, _trace_id),
    do: {:ok, batches |> Enum.reverse() |> List.flatten()}

  defp finish_scan({:ok, _state}, native, _limit, :metric_discovery),
    do: native_result(NIF.metric_discovery_finish(native))

  defp finish_scan({:ok, _state}, native, _limit, :log_discovery), do: native_result(NIF.log_discovery_finish(native))

  defp finish_scan({:ok, _state}, native, _limit, :tempo_trace_duration_discovery),
    do: native_result(NIF.tempo_trace_duration_discovery_finish(native))

  defp finish_scan({:ok, _state}, native, _limit, :tempo_root_discovery),
    do: native_result(NIF.tempo_root_discovery_finish(native))

  defp finish_scan({:ok, _state}, native, _limit, :tempo_discovery),
    do: native_result(NIF.tempo_discovery_finish(native))

  defp finish_scan({:ok, _state}, native, _limit, :tempo_search), do: native_result(NIF.tempo_search_finish(native))

  defp finish_scan({:ok, _state}, native, _limit, :search), do: native_result(NIF.trace_search_finish(native))

  defp finish_scan({:ok, _state}, native, limit, trace_id) do
    maximum = if limit in [nil, 5001], do: 5000, else: 9_223_372_036_854_775_807

    result =
      if is_nil(trace_id),
        do: NIF.trace_query_finish(native, maximum),
        else: NIF.trace_query_finish_trace(native, maximum, trace_id)

    native_result(result)
  end

  defp finish_scan({:error, _} = error, native, _limit, :metric_discovery) do
    _ = NIF.metric_discovery_abort(native)
    error
  end

  defp finish_scan({:error, _} = error, native, _limit, :log_discovery) do
    _ = NIF.log_discovery_abort(native)
    error
  end

  defp finish_scan({:error, _} = error, native, _limit, mode)
       when mode in [:tempo_search, :tempo_root_discovery, :tempo_trace_duration_discovery] do
    _ = NIF.tempo_search_abort(native)
    error
  end

  defp finish_scan({:error, _} = error, _native, _limit, _trace_id), do: error

  defp visit_segment({segment, index}, {:ok, state}, ctx, fetched \\ nil) do
    with :ok <- check_visited_budget(index + 1, state.rows + (segment.row_count || 0), ctx),
         :ok <- check_read_bytes(state.bytes + (segment.byte_size || 0), ctx.opts),
         :ok <- check_deadline(ctx.opts[:deadline_ms]),
         {:ok, blob} <- fetched || read_segment(ctx, segment.key, state),
         :ok <- check_deadline(ctx.opts[:deadline_ms]),
         bytes = state.bytes + scan_bytes(blob),
         :ok <- check_read_bytes(bytes, ctx.opts),
         {:ok, row_count} <- scan_row_count(ctx, segment, blob),
         rows = state.rows + row_count,
         :ok <- check_visited_budget(index + 1, rows, ctx) do
      continue_or_halt(%{state | bytes: bytes, rows: rows}, blob, index, ctx)
    else
      {:error, {:footer_source_missing, cost, rows}} ->
        {:halt,
         {:error,
          {:segment_missing,
           %{
             max_scan_bytes: state.bytes + cost,
             max_scan_segments: index + 1,
             max_scan_rows: state.rows + rows
           }}}}

      {:error, :not_found} ->
        consumed = %{
          max_scan_bytes: state.bytes,
          max_scan_segments: index + 1,
          max_scan_rows: state.rows
        }

        {:halt, {:error, {:segment_missing, consumed}}}

      {:error, _} = err ->
        {:halt, err}
    end
  end

  defp scan_bytes(%MetricFooterReader{charge: cost}), do: cost
  defp scan_bytes(blob), do: byte_size(blob)

  defp read_segment(
         %{signal: :metrics, metric_discovery?: false, config: %{metric_footer_pruning_enabled: true}} = ctx,
         key,
         state
       ) do
    opts =
      Enum.reduce([max_scan_bytes: state.bytes, max_scan_rows: state.rows], ctx.opts, fn {key, used}, opts ->
        if is_integer(opts[key]), do: Keyword.put(opts, key, opts[key] - used), else: opts
      end)

    MetricFooterReader.read(ctx.config, key, ctx.start_ts, ctx.end_ts, ctx.opts[:matchers] || [], opts)
  end

  defp read_segment(ctx, key, %{bytes: consumed}) do
    case ctx.opts[:max_scan_bytes] do
      limit when is_integer(limit) ->
        remaining = limit - consumed
        if remaining > 0, do: read_bounded_segment(ctx.config, key, remaining), else: {:error, :query_scan_limit}

      _ ->
        ObjectStore.get(ctx.config, key)
    end
  end

  defp read_bounded_segment(config, key, remaining) do
    case ObjectStore.get_bounded(config, key, nil, remaining) do
      {:ok, _etag, body} -> {:ok, body}
      {:error, :response_too_large} -> {:error, :query_scan_limit}
      error -> error
    end
  end

  defp check_read_bytes(bytes, opts) do
    if exceeds_budget?(bytes, opts[:max_scan_bytes]), do: {:error, :query_scan_limit}, else: :ok
  end

  defp check_visited_budget(count, rows, ctx) do
    if exceeds_budget?(count, ctx.opts[:max_scan_segments]) or exceeds_budget?(rows, ctx.opts[:max_scan_rows]),
      do: {:error, :query_scan_limit},
      else: :ok
  end

  defp scan_row_count(_ctx, _segment, %MetricFooterReader{rows: rows}), do: {:ok, rows}

  defp scan_row_count(%{signal: :traces} = ctx, segment, blob) do
    # Whole-trace assembly and grouping cannot trust an undercounted manifest.
    # Charge the actual footer before expanding/accumulating native rows.
    if ctx.search? or not is_nil(ctx.opts[:assemble_trace]),
      do: scanned_row_count(:traces, %{segment | row_count: nil}, blob),
      else: scanned_row_count(:traces, segment, blob)
  end

  defp scan_row_count(%{metric_discovery?: true}, _segment, blob),
    do: native_result(NIF.metric_segment_parquet_row_count(blob))

  defp scan_row_count(ctx, segment, blob), do: scanned_row_count(ctx.signal, segment, blob)

  defp scanned_row_count(:traces, %{row_count: count}, blob) when count in [nil, 0] do
    # Legacy/LIST entries lack counts. Recover from the footer so conservative
    # scanning still charges rows instead of treating an unknown count as zero.
    case NIF.trace_segment_parquet_row_count(blob) do
      {:ok, count} -> {:ok, count}
      _ -> {:error, {:decode_failed, :parquet_decoder_rejected_input}}
    end
  end

  defp scanned_row_count(_signal, segment, _blob), do: {:ok, segment.row_count || 0}

  defp continue_or_halt(state, blob, _index, %{native: native, metric_discovery?: true}) do
    case native_result(NIF.metric_discovery_add(native, blob)) do
      {:ok, _rows} -> {:cont, {:ok, state}}
      {:error, _} = error -> {:halt, error}
    end
  end

  defp continue_or_halt(state, blob, _index, %{native: native, log_discovery?: true}) do
    case native_result(NIF.log_discovery_add(native, blob)) do
      {:ok, _rows} -> {:cont, {:ok, state}}
      {:error, _} = error -> {:halt, error}
    end
  end

  defp continue_or_halt(state, blob, _index, %{native: native, discovery?: true}) do
    case native_result(NIF.tempo_discovery_add(native, blob)) do
      {:ok, _rows} -> {:cont, {:ok, state}}
      {:error, _} = error -> {:halt, error}
    end
  end

  defp continue_or_halt(state, blob, _index, %{native: native, tempo?: true}) do
    case native_result(NIF.tempo_search_add(native, blob)) do
      {:ok, _rows} -> {:cont, {:ok, state}}
      {:error, _} = error -> {:halt, error}
    end
  end

  defp continue_or_halt(state, blob, _index, %{native: native, search?: true}) do
    # A trace can acquire an earlier start or its first matching span in ANY
    # remaining object. Span kth floors/early exits would lose whole traces.
    case native_result(NIF.trace_search_add(native, blob)) do
      {:ok, _rows} -> {:cont, {:ok, state}}
      {:error, _} = error -> {:halt, error}
    end
  end

  defp continue_or_halt(state, blob, index, %{native: native} = ctx) when not is_nil(native) do
    case native_result(NIF.trace_query_add(native, blob)) do
      {:ok, {count, kth, _rows}} ->
        next = Enum.at(ctx.segments, index + 1)
        done? = is_nil(next) or (is_integer(kth) and is_integer(next.max_ts) and kth > next.max_ts)
        state = %{state | count: count}
        if done?, do: {:halt, {:ok, state}}, else: {:cont, {:ok, state}}

      {:error, _} = error ->
        {:halt, error}
    end
  end

  # TraceQL metrics: the native accumulator reduces each segment's spans.
  defp continue_or_halt(state, blob, _index, %{trace_metrics: metrics} = ctx) when not is_nil(metrics) do
    case NIF.trace_metrics_add(metrics, blob, ctx.start_ts || 0) do
      {:ok, rows} -> {:cont, {:ok, %{state | fold: state.fold + rows}}}
      {:error, _} = error -> {:halt, error}
      :fallback -> {:halt, {:error, {:decode_failed, :invalid_trace_metadata}}}
    end
  end

  defp continue_or_halt(state, blob, _index, %{log_metric: scan} = ctx) when not is_nil(scan) do
    service = Keyword.get(ctx.opts, :service)
    matchers = Keyword.get(ctx.opts, :matchers, [])
    line_filters = Keyword.get(ctx.opts, :line_filters, [])

    case NIF.log_metric_scan_add(scan, blob, ctx.start_ts, ctx.end_ts, service, matchers, line_filters) do
      {:ok, {selected, kept}} ->
        {total_selected, total_kept} = state.fold
        {:cont, {:ok, %{state | fold: {total_selected + selected, total_kept + kept}}}}

      {:error, _} = error ->
        {:halt, error}

      :fallback ->
        {:halt, {:error, :native_fallback}}
    end
  end

  # Fold each decoded segment into the caller's accumulator and release it,
  # so memory follows one segment rather than the whole result.
  defp continue_or_halt(state, blob, index, %{log_fold: {fun, _initial}} = ctx) do
    case fold_segment(blob, ctx, fun, state.fold, 0) do
      {:ok, acc, count} ->
        state = %{state | fold: acc, count: state.count + count}
        if fold_done?(ctx, acc, index), do: {:halt, {:ok, state}}, else: {:cont, {:ok, state}}

      {:error, _} = error ->
        {:halt, error}
    end
  end

  defp continue_or_halt(state, %MetricFooterReader{body: nil}, index, ctx) do
    if can_short_circuit?(state, ctx.limit, ctx.segments, index), do: {:halt, {:ok, state}}, else: {:cont, {:ok, state}}
  end

  defp continue_or_halt(state, %MetricFooterReader{body: body}, index, ctx),
    do: continue_or_halt(state, body, index, ctx)

  defp continue_or_halt(state, blob, index, ctx) do
    # Dirty NIF term construction cannot reclaim garbage left by the preceding
    # merge. Compact it before expanding another bounded trace candidate set.
    if ctx.signal == :traces, do: :erlang.garbage_collect()

    opts =
      case Keyword.get(ctx.opts, :max_records) do
        nil -> ctx.opts
        max -> Keyword.put(ctx.opts, :max_records, max - state.count)
      end

    with {:ok, batch} <- decode_segment(ctx.signal, blob, trace_start_floor(state, ctx), ctx.end_ts, opts),
         {:ok, batches} <- retain_batches(ctx.signal, [batch | state.batches], ctx.limit) do
      new_state = %{
        bytes: state.bytes,
        rows: state.rows,
        batches: batches,
        count: state.count + length(batch),
        top: top_timestamps(state.top, batch, ctx.limit)
      }

      if can_short_circuit?(new_state, ctx.limit, ctx.segments, index),
        do: {:halt, {:ok, new_state}},
        else: {:cont, {:ok, new_state}}
    else
      {:error, _} = err -> {:halt, err}
    end
  end

  defp trace_start_floor(state, %{signal: :traces, limit: limit, start_ts: start_ts})
       when is_integer(limit) and limit > 0 and state.count >= limit do
    # Anything strictly older than global kth cannot improve the result. Keep
    # the boundary inclusive: ID/metadata ties there still need comparison.
    case Enum.at(state.top, limit - 1) do
      timestamp when is_integer(timestamp) -> max(start_ts || timestamp, timestamp)
      _ -> start_ts
    end
  end

  defp trace_start_floor(_state, ctx), do: ctx.start_ts

  defp retain_batches(:traces, [batch | previous], limit) when is_integer(limit) do
    # Native survivors and the running top-k are already sorted. Merge without
    # re-sorting or allocating a full metadata key for every retained span.
    top = SortOrder.merge_spans(batch, List.flatten(previous), limit)
    bytes = Enum.reduce(top, 0, fn span, sum -> sum + SpanKey.size(span) end)
    if bytes <= 16 * 1024 * 1024, do: {:ok, [top]}, else: {:error, :query_resource_limit}
  end

  defp retain_batches(_signal, batches, _limit), do: {:ok, batches}

  defp can_short_circuit?(_state, nil, _segments, _index), do: false

  defp can_short_circuit?(state, limit, segments, index) when state.count >= limit do
    case Enum.at(segments, index + 1) do
      nil ->
        true

      %{max_ts: nil} ->
        false

      %{max_ts: next_max} ->
        kth = Enum.at(state.top, limit - 1)
        kth != nil and kth > next_max
    end
  end

  defp can_short_circuit?(_state, _limit, _segments, _index), do: false

  defp top_timestamps(top, _batch, nil), do: top

  defp top_timestamps(top, batch, limit) do
    batch_ts = for r <- batch, (ts = record_ts(r)) != nil, do: ts
    merge_desc(top, Enum.sort(batch_ts, :desc), limit)
  end

  defp record_ts(%Log{timestamp_ns: ts}), do: ts
  defp record_ts(%MetricSample{timestamp_ns: ts}), do: ts
  defp record_ts(%Span{timestamp_ns: ts}), do: ts

  defp merge_desc(_a, _b, 0), do: []
  defp merge_desc([], b, k), do: Enum.take(b, k)
  defp merge_desc(a, [], k), do: Enum.take(a, k)
  defp merge_desc([x | xs], [y | _] = b, k) when x >= y, do: [x | merge_desc(xs, b, k - 1)]
  defp merge_desc(a, [y | ys], k), do: [y | merge_desc(a, ys, k - 1)]

  defp trace_filters(opts) do
    opts
    |> Keyword.take([
      :service,
      :trace_id,
      :span_id,
      :parent_span_id,
      :name,
      :kind,
      :status_code,
      :attributes,
      :resource,
      :attributes_regex,
      :resource_regex,
      :root_only,
      :min_duration_ns,
      :max_duration_ns,
      :limit
    ])
    |> Map.new(fn {k, v} -> {Atom.to_string(k), v} end)
  end

  @doc false
  @spec decode_segment(Pulso.Storage.signal(), binary(), term(), term(), keyword()) ::
          {:ok, [Pulso.Storage.signal_record()]} | {:error, term()}
  def decode_segment(:logs, blob, start_ts, end_ts, opts) when is_binary(blob) and is_list(opts) do
    service = Keyword.get(opts, :service)
    matchers = Keyword.get(opts, :matchers, [])
    line_filters = Keyword.get(opts, :line_filters, [])

    :ok = validate_decode_args(blob, start_ts, end_ts, service, matchers, line_filters)

    case NIF.decode_log_segment_parquet_v2(blob, start_ts, end_ts, service, matchers, line_filters) do
      {:ok, records} -> {:ok, records}
      :fallback -> {:error, {:decode_failed, :parquet_decoder_rejected_input}}
    end
  end

  def decode_segment(:traces, blob, start_ts, end_ts, opts) when is_binary(blob) and is_list(opts) do
    max = Keyword.get(opts, :max_records, 9_223_372_036_854_775_807)

    case NIF.decode_trace_segment_parquet(blob, start_ts, end_ts, trace_filters(opts), max) do
      {:ok, records} -> {:ok, Enum.map(records, &Span.from_map/1)}
      {:error, _} = error -> error
      :fallback -> {:error, {:decode_failed, :parquet_decoder_rejected_input}}
    end
  end

  def decode_segment(:metrics, blob, start_ts, end_ts, opts) when is_binary(blob) and is_list(opts) do
    case decode_metrics(blob, start_ts, end_ts, Keyword.get(opts, :matchers, []), opts) do
      {:ok, records} -> {:ok, records}
      {:error, _} = error -> error
      :fallback -> {:error, {:decode_failed, :parquet_decoder_rejected_input}}
    end
  end

  defp decode_metrics(blob, start_ts, end_ts, matchers, opts) do
    case {Keyword.get(opts, :exemplars, false), Keyword.get(opts, :max_records)} do
      {true, _max} ->
        NIF.decode_metric_exemplars_parquet(blob, start_ts, end_ts, matchers)

      {false, nil} ->
        NIF.decode_metric_segment_parquet_v2(blob, start_ts, end_ts, matchers)

      {false, max} when is_integer(max) and max >= 0 ->
        NIF.decode_metric_segment_parquet_bounded_v2(blob, start_ts, end_ts, matchers, max)

      _ ->
        {:error, :invalid_query_limit}
    end
  end

  defp validate_decode_args(blob, start, finish, service, matchers, line_filters)
       when is_binary(blob) and (is_nil(start) or is_integer(start)) and (is_nil(finish) or is_integer(finish)) and
              (is_nil(service) or is_binary(service)) and is_list(matchers) and is_list(line_filters) do
    :ok
  end

  defp prefix(tenant, signal) when is_binary(tenant) and is_binary(signal),
    do: "tenants/#{tenant}/#{@schema_version}/signal=#{signal}/"

  # Returns `date=<YYYY-MM-DD>/hour=<HH>/` for a nanosecond-epoch timestamp,
  # UTC. Nil `min_ts` maps to `0`, which becomes `date=1970-01-01/hour=00/` —
  # fine for correctness (segments with unknown bounds have always been a
  # degenerate case) and still prunable by the manifest's own bounds.
  defp time_partition(min_ns) when is_integer(min_ns) and min_ns >= 0 do
    seconds = div(min_ns, 1_000_000_000)
    dt = DateTime.from_unix!(seconds, :second)
    date = Date.to_iso8601(dt)
    hour = dt.hour |> Integer.to_string() |> String.pad_leading(2, "0")
    "date=#{date}/hour=#{hour}/"
  end

  defp time_partition(_), do: "date=1970-01-01/hour=00/"

  # `caller_hash` is a 16-hex fingerprint of the pre-normalization records.
  @doc false
  @spec object_key(
          String.t(),
          String.t(),
          non_neg_integer(),
          non_neg_integer(),
          String.t(),
          String.t() | nil
        ) :: String.t()
  def object_key(tenant, signal, min_ts, max_ts, caller_hash, idempotency_key)
      when is_binary(tenant) and is_binary(signal) and is_binary(caller_hash) do
    suffix =
      case idempotency_key do
        <<key::binary>> when byte_size(key) > 0 ->
          "idem-" <> stable_hash(tenant <> "\0" <> key <> "\0" <> caller_hash)

        _ ->
          "rand-" <> caller_hash <> "-" <> rand_hex()
      end

    "#{prefix(tenant, signal)}#{time_partition(min_ts)}#{zero_pad(min_ts)}-#{zero_pad(max_ts)}-#{suffix}.parquet"
  end

  # Fingerprint of the raw caller records.
  @doc false
  @spec caller_content_hash(Pulso.Storage.signal(), [Pulso.Storage.signal_record()]) ::
          {:ok, String.t()}
  def caller_content_hash(:logs, records) when is_list(records) do
    canonical =
      Enum.map(records, fn %Log{} = r ->
        %{
          timestamp_ns: r.timestamp_ns,
          observed_timestamp_ns: r.observed_timestamp_ns,
          severity_number: r.severity_number,
          severity_text: r.severity_text,
          service: r.service,
          body: normalize_hash_value(r.body),
          trace_id: r.trace_id,
          span_id: r.span_id,
          attributes: r.attributes,
          resource: r.resource
        }
        |> then(fn fields ->
          # Keep legacy/Loki fingerprints unchanged when no OTLP envelope exists.
          with_log_metadata(fields, r.otlp)
        end)
      end)

    digest_hex(canonical)
  end

  def caller_content_hash(:metrics, records) when is_list(records) do
    canonical =
      Enum.map(records, fn %MetricSample{} = s ->
        %{
          timestamp_ns: s.timestamp_ns,
          value: s.value,
          labels: s.labels
        }
        |> with_log_metadata(s.otlp)
        |> with_rich_sample(s)
      end)

    digest_hex(canonical)
  end

  def caller_content_hash(:traces, records) when is_list(records), do: digest_hex(Enum.map(records, &Span.to_map/1))

  defp with_log_metadata(fields, nil), do: fields
  defp with_log_metadata(fields, otlp), do: Map.put(fields, :otlp, otlp)

  # Histograms and exemplars join the hash only when present, so the keys of
  # plain samples stay what they were.
  defp with_rich_sample(canonical, %MetricSample{histogram: nil, exemplars: nil}), do: canonical

  defp with_rich_sample(canonical, %MetricSample{histogram: histogram, exemplars: exemplars}),
    do: Map.merge(canonical, %{histogram: histogram, exemplars: exemplars})

  defp digest_hex(canonical) do
    digest =
      :sha256
      |> :crypto.hash(:erlang.term_to_binary(canonical, [:deterministic]))
      |> Base.encode16(case: :lower)
      |> binary_part(0, @content_hash_width)

    {:ok, digest}
  end

  defp normalize_hash_value(v), do: v

  defp zero_pad(ns) when is_integer(ns) and ns >= 0 do
    ns
    |> Integer.to_string()
    |> String.pad_leading(@sort_key_width, "0")
  end

  defp stable_hash(bin) do
    :sha256
    |> :crypto.hash(bin)
    |> Base.encode16(case: :lower)
    |> binary_part(0, @content_hash_width)
  end

  defp rand_hex do
    :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
  end

  defp take_limit(records, nil), do: records
  defp take_limit(records, limit) when is_integer(limit) and limit > 0, do: Enum.take(records, limit)

  defp config! do
    case Runtime.get_env(:pulso, __MODULE__) do
      nil ->
        raise "Pulso.Storage.S3 is not configured. Set `config :pulso, Pulso.Storage.S3, bucket: ..., endpoint: ..., region: ..., access_key_id: ..., secret_access_key: ..., allow_http: ...`"

      config ->
        Map.new(config)
    end
  end
end
