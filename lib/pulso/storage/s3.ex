defmodule Pulso.Storage.S3 do
  @moduledoc """
  S3-backed log storage.

  Each `append/3` writes one Parquet object under a tenant-scoped,
  versioned prefix: `tenants/<tenant>/v3/logs/<min_ts>-<max_ts>-<suffix>.parquet`.
  Tenant isolation is enforced by key construction — tenant names are
  validated against a conservative charset, so a batch cannot land
  outside its own prefix. The `<min_ts>` and `<max_ts>` segments are
  zero-padded 20-decimal-digit representations of the smallest and
  largest caller-supplied `timestamp_ns` in the batch (records with
  `nil` map to `0`, which sorts first).

  The trailing `<suffix>` depends on whether the caller supplied an
  `idempotency_key`:

    * **With `idempotency_key`** — the suffix is a deterministic hash of
      `tenant || idempotency_key || caller_content_hash`. Two `append`
      calls with the same key AND the same caller-supplied content
      resolve to the same object; a lost-response retry does not
      duplicate. Two calls with the same key and different content
      produce different objects, surfacing a client bug rather than
      silently overwriting.
    * **Without `idempotency_key`** — the suffix is random, so distinct
      calls always produce distinct objects and no content hash is
      computed. Retries in this mode duplicate — callers who need dedup
      must opt in.

  ## Manifest coordination

  Every accepted append is registered in the per-`(tenant, signal)`
  manifest via `Pulso.Storage.S3.ManifestOwner`. The owner batches
  concurrent registrations into a single S3 CAS per flush window so
  ingest throughput is decoupled from S3 CAS latency. `query/2` reads
  the manifest from the ETS-backed cache
  (`Pulso.Storage.S3.ManifestCache`), prunes by time bounds, and only
  fetches segments that could contribute records — no per-query LIST.

  Only after both the segment PUT and the manifest CAS succeed does
  `append/3` return `:ok`. That is the load-bearing durability point.

  ## Encoding and decoding

  Segments are Apache Parquet files, encoded and decoded in Rust
  (`Pulso.Codec.NIF.encode_log_segment_parquet` / `decode_log_segment_parquet`).
  Rows are sorted by `(service, timestamp_ns)` on write, so dictionary
  encoding on `service` and delta encoding on `timestamp_ns` compress
  tightly, and the row-group `timestamp_ns` min/max stats let a query
  skip whole segments outside its time range without reading a single
  column page. `attributes` and `resource` are stored as JSON strings
  in dedicated Utf8 columns — real nested column projection lands with
  sidecar indexes.

  Every input map is first stringified through `sanitize_map/1` so a
  caller passing atom or integer attribute keys sees the same result the
  NDJSON path produced. There is no Elixir Parquet reference; when the
  NIF returns `:fallback` the call errors out rather than falling back.

  ## Key format stability

  Every object lives under a versioned prefix (`tenants/<tenant>/v3/…`).
  Within one schema version, the exact suffix format is deliberately
  not a public API. The idempotency suffix is derived from
  `:erlang.term_to_binary(_, [:deterministic])`, which is stable within
  an OTP release but is not guaranteed across a major OTP upgrade. Any
  change to the fingerprint, the delimiter, the sort-key width, or the
  number of bounds segments bumps the schema version — new writes go
  to `v4/`, old objects stay at `v3/`, and a compaction job migrates
  at its own pace.
  """

  @behaviour Pulso.Storage

  alias Pulso.Codec.NIF
  alias Pulso.ObjectStore
  alias Pulso.Record.Log
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
  @signal "logs"

  @impl Pulso.Storage
  def append(tenant, records, opts \\ [])

  def append(tenant, [], _opts) when is_binary(tenant) do
    # Validate even on empty so an adversarial tenant name is rejected on the
    # first attempt, not only once a real record survives OTLP decoding.
    validate_tenant(tenant)
  end

  def append(tenant, records, opts) when is_binary(tenant) and is_list(records) do
    idempotency_key = Keyword.get(opts, :idempotency_key)

    # Validation and encoding come first so a bad tenant name or an
    # unencodable record is rejected before `config!/0` is even evaluated.
    # That keeps the "reject adversarial tenant names before touching the
    # object store" property from surviving as an accident of test setup.
    #
    # The key's `[min_ts, max_ts]` and fingerprint come from the
    # caller-supplied records, so a legitimate retry produces the same key
    # in full and the second PUT overwrites the first as intended.
    with :ok <- validate_tenant(tenant),
         {:ok, payload, min_ts, max_ts} <- encode_segment(records) do
      config = config!()
      key = object_key(tenant, min_ts, max_ts, fingerprint(records, idempotency_key), idempotency_key)

      with {:ok, _etag} <- ObjectStore.put(config, key, payload) do
        segment = Segment.build(key, min_ts, max_ts, length(records), byte_size(payload))
        ManifestOwner.register_segments(tenant, @signal, [segment], config)
      end
    end
  end

  @impl Pulso.Storage
  def query(tenant, opts) when is_binary(tenant) and is_list(opts) do
    start_ts = Keyword.get(opts, :start_ts)
    end_ts = Keyword.get(opts, :end_ts)
    service = Keyword.get(opts, :service)
    limit = Keyword.get(opts, :limit)
    matchers = Keyword.get(opts, :matchers, [])
    line_filters = Keyword.get(opts, :line_filters, [])

    with :ok <- validate_tenant(tenant),
         config = config!(),
         {:ok, entry} <- ManifestOwner.ensure_loaded(tenant, @signal, config) do
      # Manifest segments are already sorted by max_ts desc (invariant
      # maintained by `Manifest.merge/2` and `Manifest.decode/1`). Prune
      # by time and hand the survivors to the scan loop as-is — no
      # per-query sort.
      segments = Manifest.prune_by_time(entry.manifest, start_ts, end_ts)

      case scan_segments(config, segments, start_ts, end_ts, service, matchers, line_filters, limit) do
        {:ok, records} ->
          sorted = records |> SortOrder.sort() |> take_limit(limit)
          {:ok, sorted}

        {:error, _} = err ->
          err
      end
    end
  end

  # -- helpers -----------------------------------------------------------------

  # The content fingerprint only matters when an idempotency key makes the
  # object key deterministic. Without one the key is random anyway, so
  # skip hashing the batch (a full deterministic `term_to_binary` of every
  # record).
  defp fingerprint(records, idempotency_key) when is_binary(idempotency_key) and byte_size(idempotency_key) > 0 do
    {:ok, hash} = caller_content_hash(records)
    hash
  end

  defp fingerprint(_records, _idempotency_key), do: rand_hex()

  @doc false
  @spec encode_segment([Log.t()]) :: {:ok, binary(), integer(), integer()} | {:error, term()}
  def encode_segment(records) do
    with {:ok, normalized} <- normalize(records) do
      case NIF.encode_log_segment_parquet(normalized) do
        {:ok, payload, min_ts, max_ts, _count} ->
          {:ok, payload, min_ts, max_ts}

        :fallback ->
          {:error, {:encode_failed, :parquet_encoder_rejected_input}}
      end
    end
  end

  defp validate_tenant(tenant) do
    if Regex.match?(@tenant_regex, tenant) do
      :ok
    else
      {:error, {:invalid_tenant, tenant}}
    end
  end

  defp normalize(records) do
    Enum.reduce_while(records, {:ok, []}, fn
      %Log{} = record, {:ok, acc} ->
        with {:ok, attrs} <- sanitize_map(record.attributes || %{}),
             {:ok, resource} <- sanitize_map(record.resource || %{}) do
          # Deliberately no wall-clock backfill here. Injecting `now` for a
          # nil timestamp would make retries under the same idempotency key
          # overwrite the first stored record with a later timestamp, so an
          # already-acknowledged log would disappear from its original time
          # range and re-emerge in a later one.
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

  # Coerce every attribute/resource map key to a string, recursively. OTLP
  # decoding already produces string keys, but a caller building `%Log{}`
  # directly (or a future backend surface) could hand us atoms or integers.
  # If two logical keys coerce to the same string (`%{1 => a, "1" => b}`) we
  # refuse — silently dropping either value would surprise a reader looking
  # at either the original struct or the JSON-encoded record.
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
  defp scan_segments(config, segments, start_ts, end_ts, service, matchers, line_filters, limit) do
    ctx = %{
      config: config,
      segments: segments,
      start_ts: start_ts,
      end_ts: end_ts,
      service: service,
      matchers: matchers,
      line_filters: line_filters,
      limit: limit
    }

    segments
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, %{batches: [], count: 0, top: []}}, &visit_segment(&1, &2, ctx))
    |> case do
      {:ok, %{batches: batches}} -> {:ok, batches |> Enum.reverse() |> List.flatten()}
      {:error, _} = err -> err
    end
  end

  defp visit_segment({segment, index}, {:ok, state}, ctx) do
    case ObjectStore.get(ctx.config, segment.key) do
      {:ok, blob} ->
        continue_or_halt(state, blob, index, ctx)

      {:error, :not_found} ->
        {:cont, {:ok, state}}

      {:error, _} = err ->
        {:halt, err}
    end
  end

  defp continue_or_halt(state, blob, index, ctx) do
    case decode_segment(blob, ctx.start_ts, ctx.end_ts, ctx.service, ctx.matchers, ctx.line_filters) do
      {:ok, batch} ->
        new_state = %{
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
      # No more segments to scan.
      nil ->
        true

      # An unknown-bounds segment could contain anything.
      %{max_ts: nil} ->
        false

      %{max_ts: next_max} ->
        # Kth-largest timestamp in what we have so far. If it strictly
        # exceeds the next segment's max_ts, no record we have not yet
        # fetched can dominate it.
        kth = Enum.at(state.top, limit - 1)
        kth != nil and kth > next_max
    end
  end

  defp can_short_circuit?(_state, _limit, _segments, _index), do: false

  # The `limit` largest non-nil timestamps seen so far, descending. Kept
  # incrementally so the short-circuit check costs one batch sort and a
  # bounded merge per segment instead of re-sorting everything fetched.
  defp top_timestamps(top, _batch, nil), do: top

  defp top_timestamps(top, batch, limit) do
    batch_ts =
      for %Log{timestamp_ns: ts} <- batch, ts != nil do
        ts
      end

    merge_desc(top, Enum.sort(batch_ts, :desc), limit)
  end

  defp merge_desc(_a, _b, 0), do: []
  defp merge_desc([], b, k), do: Enum.take(b, k)
  defp merge_desc(a, [], k), do: Enum.take(a, k)
  defp merge_desc([x | xs], [y | _] = b, k) when x >= y, do: [x | merge_desc(xs, b, k - 1)]
  defp merge_desc(a, [y | ys], k), do: [y | merge_desc(a, ys, k - 1)]

  @doc false
  @spec decode_segment(binary(), term(), term(), term(), [Pulso.Storage.matcher()], [Pulso.Storage.line_filter()]) ::
          {:ok, [Log.t()]} | {:error, term()}
  def decode_segment(blob, start_ts, end_ts, service, matchers \\ [], line_filters \\ []) do
    :ok = validate_decode_args(blob, start_ts, end_ts, service, matchers, line_filters)

    case NIF.decode_log_segment_parquet(blob, start_ts, end_ts, service, matchers, line_filters) do
      {:ok, records} -> {:ok, records}
      :fallback -> {:error, {:decode_failed, :parquet_decoder_rejected_input}}
    end
  end

  defp validate_decode_args(blob, start_ts, end_ts, service, matchers, line_filters)
       when is_binary(blob) and (is_nil(start_ts) or is_integer(start_ts)) and (is_nil(end_ts) or is_integer(end_ts)) and
              (is_nil(service) or is_binary(service)) and is_list(matchers) and is_list(line_filters) do
    :ok
  end

  # Schema version segment. Baked into every object key so a future change
  # to the key format can coexist with older objects rather than orphan
  # them.
  @schema_version "v3"

  defp prefix(tenant), do: "tenants/#{tenant}/#{@schema_version}/logs/"

  # `caller_hash` is a 16-hex fingerprint of the pre-normalization records
  # from `caller_content_hash/1`. The pre-normalization form matters:
  # `normalize/1` fills in a fresh wall-clock `observed_timestamp_ns` on
  # every call, so hashing after normalization would make identical retries
  # produce different keys even under an idempotency key.
  @doc false
  @spec object_key(String.t(), non_neg_integer(), non_neg_integer(), String.t(), String.t() | nil) ::
          String.t()
  def object_key(tenant, min_ts, max_ts, caller_hash, idempotency_key)
      when is_binary(tenant) and is_binary(caller_hash) do
    suffix =
      case idempotency_key do
        <<key::binary>> when byte_size(key) > 0 ->
          # Mixes tenant, key, and caller_hash. A client that accidentally
          # reuses an idempotency key with different content produces a
          # different object (no silent overwrite). Same content + same key
          # collapses onto one object, which is the point of idempotency.
          "idem-" <> stable_hash(tenant <> "\0" <> key <> "\0" <> caller_hash)

        _ ->
          # No idempotency key: the caller accepts duplicates on retry, and
          # `caller_hash` is random rather than a content hash. A random
          # suffix guarantees distinct writes even when payload and
          # timestamp collide.
          "rand-" <> caller_hash <> "-" <> rand_hex()
      end

    "#{prefix(tenant)}#{zero_pad(min_ts)}-#{zero_pad(max_ts)}-#{suffix}.parquet"
  end

  # Fingerprint of the raw caller records. Two properties hold:
  #
  # 1. Every field the caller controls (including `observed_timestamp_ns`
  #    when they set it) is included — a caller who legitimately changes
  #    that field on a "retry" is signalling a distinct write, and gets a
  #    distinct object.
  # 2. The encoding is canonical across runtime, GC, and Jason versions.
  #    Two identical records always hash to the same byte sequence, in
  #    this process and in any future release. That is what makes cross-
  #    version idempotency safe.
  #
  # `:erlang.term_to_binary/2` with `:deterministic` gives us the canonical
  # form for free within an OTP release: map keys are sorted, atoms and
  # integers are encoded canonically, and the same term always produces
  # the same bytes.
  @doc false
  @spec caller_content_hash([Log.t()]) :: {:ok, String.t()}
  def caller_content_hash(records) when is_list(records) do
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

    digest =
      :sha256
      |> :crypto.hash(:erlang.term_to_binary(canonical, [:deterministic]))
      |> Base.encode16(case: :lower)
      |> binary_part(0, @content_hash_width)

    {:ok, digest}
  end

  # `body` can arrive as a non-string term via OTLP AnyValue (int, bool,
  # list). `:erlang.term_to_binary` handles all of these fine; nothing to
  # normalize. This hook exists so a future callsite that wants a stable
  # representation can add one without changing the fingerprint contract.
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
