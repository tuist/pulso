defmodule Pulso.Storage.S3 do
  @moduledoc """
  S3-backed signal storage.

  Each `append/4` writes one Parquet object under a tenant- and
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
      duplicate.
    * **Without `idempotency_key`** — the suffix is random, so distinct
      calls always produce distinct objects and no content hash is
      computed.

  ## Signal dispatch

  Every call carries an explicit `signal` (`:logs` | `:metrics`). The
  adapter translates to the corresponding Rust NIF entry point
  (`encode_log_segment_parquet` / `encode_metric_segment_parquet`) and
  back. Logs sort by `(service, timestamp_ns)`, metrics by
  `(series_id, timestamp_ns)`. See `Pulso.Codec.NIF`.

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
  alias Pulso.Storage.S3.Manifest
  alias Pulso.Storage.S3.Manifest.Segment
  alias Pulso.Storage.S3.ManifestOwner
  alias Pulso.Storage.SortOrder

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
    idempotency_key = Keyword.get(opts, :idempotency_key)

    with :ok <- validate_tenant(tenant),
         {:ok, payload, min_ts, max_ts} <- encode_segment(signal, records) do
      config = config!()

      key =
        object_key(
          tenant,
          signal_string(signal),
          min_ts,
          max_ts,
          fingerprint(signal, records, idempotency_key),
          idempotency_key
        )

      with {:ok, _etag} <- ObjectStore.put(config, key, payload) do
        segment = Segment.build(key, min_ts, max_ts, length(records), byte_size(payload))
        segment = summarize_segment(signal, segment, records)
        ManifestOwner.register_segments(tenant, signal_string(signal), [segment], config)
      end
    end
  end

  @impl Pulso.Storage
  def query(signal, tenant, opts) when is_atom(signal) and is_binary(tenant) and is_list(opts) do
    start_ts = Keyword.get(opts, :start_ts)
    end_ts = Keyword.get(opts, :end_ts)
    limit = Keyword.get(opts, :limit)

    with :ok <- validate_tenant(tenant),
         config = config!(),
         {:ok, entry} <- ManifestOwner.ensure_loaded(tenant, signal_string(signal), config) do
      segments = query_segments(entry.manifest, signal, start_ts, end_ts, opts)

      with :ok <- check_scan_budget(segments, opts) do
        scan_segments(signal, config, segments, start_ts, end_ts, opts, limit)
      end
      |> case do
        {:ok, records} ->
          sorted = records |> SortOrder.sort(signal) |> take_limit(limit)
          {:ok, sorted}

        {:error, {:segment_missing, consumed}} ->
          # Cleanup can overtake a cached snapshot or an in-flight query.
          # Restart the whole scan against a fresh manifest, never mix generations.
          retry_scan(signal, tenant, config, start_ts, end_ts, remaining_budget(opts, consumed), limit)

        {:error, _} = err ->
          err
      end
    end
  end

  defp query_segments(manifest, signal, start_ts, end_ts, opts) do
    segments = Manifest.prune_by_time(manifest, start_ts, end_ts)

    if signal == :metrics,
      do: Enum.filter(segments, &Segment.matches_metric_name?(&1, Keyword.get(opts, :matchers, []))),
      else: segments
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
         {:ok, _etag, body} <-
           ObjectStore.get_if_none_match(config, Manifest.manifest_key(tenant, signal_string(signal)), nil),
         {:ok, manifest} <- Manifest.decode(body),
         segments = query_segments(manifest, signal, start_ts, end_ts, opts),
         :ok <- check_deadline(opts[:deadline_ms]),
         :ok <- check_scan_budget(segments, opts),
         {:ok, records} <- scan_segments(signal, config, segments, start_ts, end_ts, opts, limit) do
      {:ok, records |> SortOrder.sort(signal) |> take_limit(limit)}
    else
      {:error, {:segment_missing, _}} -> {:error, :not_found}
      {:error, _} = error -> error
    end
  end

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

  defp summarize_segment(:metrics, segment, records), do: Segment.summarize_metrics(segment, records)
  defp summarize_segment(_signal, segment, _records), do: segment

  # -- helpers -----------------------------------------------------------------

  defp signal_string(:logs), do: "logs"
  defp signal_string(:metrics), do: "metrics"

  # The content fingerprint only matters when an idempotency key makes the
  # object key deterministic. Without one the key is random anyway, so
  # skip hashing the batch.
  defp fingerprint(signal, records, idempotency_key)
       when is_binary(idempotency_key) and byte_size(idempotency_key) > 0 do
    {:ok, hash} = caller_content_hash(signal, records)
    hash
  end

  defp fingerprint(_signal, _records, _idempotency_key), do: rand_hex()

  @doc false
  @spec encode_segment(Pulso.Storage.signal(), [Pulso.Storage.signal_record()]) ::
          {:ok, binary(), integer(), integer()} | {:error, term()}
  def encode_segment(:logs, records) do
    with {:ok, normalized} <- normalize_logs(records) do
      case NIF.encode_log_segment_parquet(normalized) do
        {:ok, payload, min_ts, max_ts, _count} ->
          {:ok, payload, min_ts, max_ts}

        :fallback ->
          {:error, {:encode_failed, :parquet_encoder_rejected_input}}
      end
    end
  end

  def encode_segment(:metrics, records) do
    case NIF.encode_metric_segment_parquet(records) do
      {:ok, payload, min_ts, max_ts, _count} ->
        {:ok, payload, min_ts, max_ts}

      :fallback ->
        {:error, {:encode_failed, :parquet_encoder_rejected_input}}
    end
  end

  @doc false
  def validate_tenant(tenant) do
    if Regex.match?(@tenant_regex, tenant) do
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
    ctx = %{
      signal: signal,
      config: config,
      segments: segments,
      start_ts: start_ts,
      end_ts: end_ts,
      opts: opts,
      limit: limit
    }

    segments
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, %{batches: [], count: 0, top: [], bytes: 0}}, &visit_segment(&1, &2, ctx))
    |> case do
      {:ok, %{batches: batches}} -> {:ok, batches |> Enum.reverse() |> List.flatten()}
      {:error, _} = err -> err
    end
  end

  defp visit_segment({segment, index}, {:ok, state}, ctx) do
    with :ok <- check_deadline(ctx.opts[:deadline_ms]),
         {:ok, blob} <- ObjectStore.get(ctx.config, segment.key),
         :ok <- check_deadline(ctx.opts[:deadline_ms]) do
      bytes = state.bytes + byte_size(blob)

      if exceeds_budget?(bytes, ctx.opts[:max_scan_bytes]),
        do: {:halt, {:error, :query_scan_limit}},
        else: continue_or_halt(%{state | bytes: bytes}, blob, index, ctx)
    else
      {:error, :not_found} when ctx.signal == :logs ->
        {:cont, {:ok, state}}

      {:error, :not_found} ->
        consumed = %{
          max_scan_bytes: state.bytes,
          max_scan_segments: index + 1,
          max_scan_rows: ctx.segments |> Enum.take(index) |> Enum.map(&(&1.row_count || 0)) |> Enum.sum()
        }

        {:halt, {:error, {:segment_missing, consumed}}}

      {:error, _} = err ->
        {:halt, err}
    end
  end

  defp continue_or_halt(state, blob, index, ctx) do
    opts =
      case Keyword.get(ctx.opts, :max_records) do
        nil -> ctx.opts
        max -> Keyword.put(ctx.opts, :max_records, max - state.count)
      end

    case decode_segment(ctx.signal, blob, ctx.start_ts, ctx.end_ts, opts) do
      {:ok, batch} ->
        new_state = %{
          bytes: state.bytes,
          batches: [batch | state.batches],
          count: state.count + length(batch),
          top: top_timestamps(state.top, batch, ctx.limit)
        }

        if can_short_circuit?(new_state, ctx.limit, ctx.segments, index),
          do: {:halt, {:ok, new_state}},
          else: {:cont, {:ok, new_state}}

      {:error, _} = err ->
        {:halt, err}
    end
  end

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

  defp merge_desc(_a, _b, 0), do: []
  defp merge_desc([], b, k), do: Enum.take(b, k)
  defp merge_desc(a, [], k), do: Enum.take(a, k)
  defp merge_desc([x | xs], [y | _] = b, k) when x >= y, do: [x | merge_desc(xs, b, k - 1)]
  defp merge_desc(a, [y | ys], k), do: [y | merge_desc(a, ys, k - 1)]

  @doc false
  @spec decode_segment(Pulso.Storage.signal(), binary(), term(), term(), keyword()) ::
          {:ok, [Pulso.Storage.signal_record()]} | {:error, term()}
  def decode_segment(:logs, blob, start_ts, end_ts, opts) when is_binary(blob) and is_list(opts) do
    service = Keyword.get(opts, :service)
    matchers = Keyword.get(opts, :matchers, [])
    line_filters = Keyword.get(opts, :line_filters, [])

    :ok = validate_decode_args(blob, start_ts, end_ts, service, matchers, line_filters)

    case NIF.decode_log_segment_parquet(blob, start_ts, end_ts, service, matchers, line_filters) do
      {:ok, records} -> {:ok, records}
      :fallback -> {:error, {:decode_failed, :parquet_decoder_rejected_input}}
    end
  end

  def decode_segment(:metrics, blob, start_ts, end_ts, opts) when is_binary(blob) and is_list(opts) do
    matchers = Keyword.get(opts, :matchers, [])

    decoded =
      case Keyword.get(opts, :max_records) do
        nil ->
          NIF.decode_metric_segment_parquet(blob, start_ts, end_ts, matchers)

        max when is_integer(max) and max >= 0 ->
          NIF.decode_metric_segment_parquet_bounded(blob, start_ts, end_ts, matchers, max)

        _ ->
          {:error, :invalid_query_limit}
      end

    case decoded do
      {:ok, records} -> {:ok, records}
      {:error, _} = error -> error
      :fallback -> {:error, {:decode_failed, :parquet_decoder_rejected_input}}
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
      end)

    digest_hex(canonical)
  end

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
    case Application.get_env(:pulso, __MODULE__) do
      nil ->
        raise "Pulso.Storage.S3 is not configured. Set `config :pulso, Pulso.Storage.S3, bucket: ..., endpoint: ..., region: ..., access_key_id: ..., secret_access_key: ..., allow_http: ...`"

      config ->
        Map.new(config)
    end
  end
end
