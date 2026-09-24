defmodule Pulso.Storage.S3 do
  @moduledoc """
  S3-backed log storage.

  Each `append/3` writes one NDJSON object under a tenant-scoped, versioned
  prefix: `tenants/<tenant>/v2/logs/<min_ts>-<max_ts>-<suffix>.ndjson`.
  Tenant isolation is enforced by key construction: tenant names are
  validated against a conservative charset so a batch cannot land outside
  its own prefix. The `<min_ts>` and `<max_ts>` segments are zero-padded
  20-decimal-digit representations of the smallest and largest
  caller-supplied `timestamp_ns` in the batch (records with `nil` map to
  `0`, which sorts first). Putting the segment's time bounds directly in
  the key removes the need for a separate manifest and lets `query/2`
  skip objects that fall outside the requested time range at LIST time.

  The trailing `<suffix>` depends on whether the caller supplied an
  `idempotency_key`:

    * **With `idempotency_key`** — the suffix is a deterministic hash of
      `tenant || idempotency_key || caller_content_hash`. Two `append`
      calls with the same key AND the same caller-supplied content resolve
      to the same object; a lost-response retry does not duplicate. Two
      calls with the same key and different content produce different
      objects, surfacing a client bug rather than silently overwriting an
      earlier write.
    * **Without `idempotency_key`** — the suffix mixes the content hash
      with random bytes so distinct calls always produce distinct objects.
      Retries in this mode duplicate — callers who need dedup must opt in.

  `query/2` lists the tenant prefix, parses `[min_ts, max_ts]` from each
  key, prunes segments that cannot overlap the requested time range, and
  fetches only the survivors. Results are ordered by
  `Pulso.Storage.SortOrder` (shared with `Pulso.Storage.Memory`). Under
  `limit`, segments are iterated newest-first (by `max_ts` desc) and the
  scan short-circuits once the accumulator's k-th largest `timestamp_ns`
  strictly exceeds the next segment's `max_ts`. A `NotFound` for a listed
  key that has since been deleted is skipped rather than aborting.

  Known limits, still deferred:

    * **No columnar layout.** Records go on the wire as NDJSON, not Parquet.
      A columnar segment format lives in the Rust NIF's future.
    * **No conditional writes (CAS).** Retry idempotency relies on
      deterministic keys plus content-addressed suffixes rather than
      compare-and-swap. Two concurrent PUTs on the same key are last-write-
      wins; a caller who accidentally reuses an idempotency key with
      different content gets a distinct object, so nothing is lost, but
      neither request sees the other's write.

  ## Key format stability

  Every object lives under a versioned prefix (`tenants/<tenant>/v2/…`).
  Within one schema version, the exact suffix format is deliberately not
  a public API. The idempotency suffix is derived from
  `:erlang.term_to_binary(_, [:deterministic])`, which is stable within an
  OTP release but is not guaranteed across a major OTP upgrade. Any change
  to the fingerprint, the delimiter, the sort-key width, or the number of
  bounds segments bumps the schema version — new writes go to `v3/`, old
  objects stay at `v2/`, and a compaction job migrates at its own pace.
  The reader can be taught to look at both during the migration window.
  """

  @behaviour Pulso.Storage

  alias Pulso.ObjectStore
  alias Pulso.Record.Log
  alias Pulso.Storage.SortOrder

  @tenant_regex ~r/\A[A-Za-z0-9_.\-]{1,128}\z/
  # 20 decimal digits fits a u64 nanosecond timestamp (max ~1.84e19). Zero-padding
  # keeps S3's UTF-8 list order chronological by write time (`sort_ns`).
  @sort_key_width 20
  # 16 hex chars = 64 bits from SHA-256. Collision probability is negligible
  # for the volumes any single tenant will produce in a step-2 adapter.
  @content_hash_width 16

  @impl Pulso.Storage
  def append(tenant, records, opts \\ [])

  def append(tenant, [], _opts) when is_binary(tenant) do
    # Validate even on empty so an adversarial tenant name is rejected on the
    # first attempt, not only once a real record survives OTLP decoding.
    validate_tenant(tenant)
  end

  def append(tenant, records, opts) when is_binary(tenant) and is_list(records) do
    idempotency_key = Keyword.get(opts, :idempotency_key)

    with :ok <- validate_tenant(tenant),
         # The fingerprint, the sort-key prefix, AND the max-ts key segment
         # are derived from the caller-supplied records BEFORE `normalize/1`
         # fills any wall-clock timestamps. A legitimate retry produces the
         # same key in full — every part is stable, so the second PUT
         # overwrites the first as intended.
         {:ok, caller_hash} = caller_content_hash(records),
         {min_ts, max_ts} = caller_ts_bounds(records),
         {:ok, normalized} <- normalize(records),
         {:ok, payload} <- encode(normalized) do
      key = object_key(tenant, min_ts, max_ts, caller_hash, idempotency_key)
      ObjectStore.put(config!(), key, payload)
    end
  end

  @impl Pulso.Storage
  def query(tenant, opts) when is_binary(tenant) and is_list(opts) do
    start_ts = Keyword.get(opts, :start_ts)
    end_ts = Keyword.get(opts, :end_ts)
    service = Keyword.get(opts, :service)
    limit = Keyword.get(opts, :limit)

    with :ok <- validate_tenant(tenant),
         config = config!(),
         {:ok, keys} <- ObjectStore.list(config, prefix(tenant)) do
      # Parse [min_ts, max_ts] from every key. A key we cannot parse (e.g.
      # written under a hypothetical schema/format we do not recognize) is
      # kept without bounds so it is always fetched — the safe default.
      # Then prune by time bounds and iterate newest max_ts first so a
      # limited query can stop as soon as the k-th largest timestamp in
      # the accumulator is guaranteed to beat every remaining segment.
      segments =
        keys
        |> Enum.map(&parse_segment/1)
        |> prune_by_time(start_ts, end_ts)
        |> Enum.sort_by(&segment_max_ts_for_sort/1, :desc)

      case scan_segments(config, segments, start_ts, end_ts, service, limit) do
        {:ok, records} ->
          sorted = records |> SortOrder.sort() |> take_limit(limit)
          {:ok, sorted}

        {:error, _} = err ->
          err
      end
    end
  end

  # -- helpers -----------------------------------------------------------------

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
          # range and re-emerge in a later one. Step 3's conditional PUT
          # (write-only-if-absent) will allow first-write-wins backfill.
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

  # Caller-supplied `[min_ts, max_ts]` for the object key. Runs on records
  # BEFORE normalization so a retry with identical caller input produces
  # the same bounds. A record with `nil` timestamp contributes 0, which
  # parks the segment at the head of the tenant listing and makes it
  # always visible to a query with no time bounds — good enough for the
  # fallback case and, importantly, deterministic across retries.
  defp caller_ts_bounds(records) do
    tss = Enum.map(records, fn %Log{timestamp_ns: ts} -> ts || 0 end)
    {Enum.min(tss), Enum.max(tss)}
  end

  defp encode(records) do
    encoded =
      Enum.reduce_while(records, {:ok, []}, fn record, {:ok, acc} ->
        # `JSON.encode_to_iodata!/1` skips the per-record binary allocation
        # that `JSON.encode!/1` would produce; we keep the intermediate as
        # iodata and only collapse to a single binary at the very end
        # (right before the NIF boundary), which is the one copy we
        # cannot avoid anyway.
        try do
          line = JSON.encode_to_iodata!(Map.from_struct(record))
          {:cont, {:ok, [[line, "\n"] | acc]}}
        rescue
          e -> {:halt, {:error, {:encode_failed, e}}}
        end
      end)

    with {:ok, lines} <- encoded do
      {:ok, lines |> Enum.reverse() |> IO.iodata_to_binary()}
    end
  end

  # A parsed segment listing. Bounds are nil when the key does not match
  # the current schema's `<min_ts>-<max_ts>-` shape, which is the safe
  # default: we still fetch the object, we just cannot prune it or use it
  # for early-exit.
  @doc false
  @spec parse_segment(String.t()) :: %{
          key: String.t(),
          min_ts: non_neg_integer() | nil,
          max_ts: non_neg_integer() | nil
        }
  def parse_segment(key) when is_binary(key), do: do_parse_segment(key)

  defp do_parse_segment(key) do
    prefix_stripped = String.split(key, "/logs/", parts: 2)

    case prefix_stripped do
      [_, rest] ->
        parse_bounds(key, rest)

      _ ->
        %{key: key, min_ts: nil, max_ts: nil}
    end
  end

  defp parse_bounds(key, rest) do
    case rest do
      <<min_str::binary-size(@sort_key_width), "-", max_str::binary-size(@sort_key_width), "-", _rest::binary>> ->
        with {min_ts, ""} <- Integer.parse(min_str),
             {max_ts, ""} <- Integer.parse(max_str) do
          %{key: key, min_ts: min_ts, max_ts: max_ts}
        else
          _ -> %{key: key, min_ts: nil, max_ts: nil}
        end

      _ ->
        %{key: key, min_ts: nil, max_ts: nil}
    end
  end

  # A segment overlaps the requested time range iff its own [min, max]
  # intersects [start_ts, end_ts]. Segments whose bounds are unknown are
  # kept (safe default). Segments explicitly outside the range are
  # dropped, which is where the time-bounded query pays back.
  @doc false
  @spec prune_by_time([map()], non_neg_integer() | nil, non_neg_integer() | nil) :: [map()]
  def prune_by_time(segments, nil, nil), do: segments

  def prune_by_time(segments, start_ts, end_ts) do
    Enum.filter(segments, &segment_intersects?(&1, start_ts, end_ts))
  end

  defp segment_intersects?(%{min_ts: nil}, _start_ts, _end_ts), do: true
  defp segment_intersects?(%{max_ts: nil}, _start_ts, _end_ts), do: true

  defp segment_intersects?(%{min_ts: min_ts, max_ts: max_ts}, start_ts, end_ts) do
    (start_ts == nil or max_ts >= start_ts) and (end_ts == nil or min_ts <= end_ts)
  end

  # `sort_by` sees `nil` as greater than every integer under Elixir's term
  # order — unknown-bounds segments float to the top, which is exactly the
  # safe default (always fetch first, never rely on their bounds for
  # early-exit).
  defp segment_max_ts_for_sort(%{max_ts: nil}), do: nil
  defp segment_max_ts_for_sort(%{max_ts: max_ts}), do: max_ts

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
  defp scan_segments(config, segments, start_ts, end_ts, service, limit) do
    initial = %{batches: [], count: 0}

    segments
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, initial}, fn {segment, index}, {:ok, state} ->
      case ObjectStore.get(config, segment.key) do
        {:ok, blob} ->
          batch =
            blob
            |> decode()
            |> filter_by_time(start_ts, end_ts)
            |> filter_by_service(service)

          new_state = %{
            batches: [batch | state.batches],
            count: state.count + length(batch)
          }

          if can_short_circuit?(new_state, limit, segments, index) do
            {:halt, {:ok, new_state}}
          else
            {:cont, {:ok, new_state}}
          end

        {:error, :not_found} ->
          {:cont, {:ok, state}}

        {:error, _} = err ->
          {:halt, err}
      end
    end)
    |> case do
      {:ok, %{batches: batches}} -> {:ok, batches |> Enum.reverse() |> List.flatten()}
      {:error, _} = err -> err
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
        kth = kth_largest_ts(state.batches, limit)
        kth != nil and kth > next_max
    end
  end

  defp can_short_circuit?(_state, _limit, _segments, _index), do: false

  defp kth_largest_ts(batches, k) do
    batches
    |> List.flatten()
    |> Enum.map(& &1.timestamp_ns)
    |> Enum.reject(&is_nil/1)
    |> Enum.sort(:desc)
    |> Enum.at(k - 1)
  end

  defp decode(blob) do
    blob
    |> String.split("\n", trim: true)
    |> Enum.map(&decode_line/1)
  end

  defp decode_line(line) do
    map = JSON.decode!(line)

    %Log{
      timestamp_ns: Map.fetch!(map, "timestamp_ns"),
      observed_timestamp_ns: map["observed_timestamp_ns"],
      severity_number: map["severity_number"],
      severity_text: map["severity_text"],
      service: map["service"],
      body: map["body"],
      trace_id: map["trace_id"],
      span_id: map["span_id"],
      attributes: map["attributes"] || %{},
      resource: map["resource"] || %{}
    }
  end

  # Schema version segment. Baked into every object key so a future change
  # to the key format can coexist with older objects rather than orphan
  # them. v1 used `<sort_ns>-<suffix>.ndjson`. v2 adds the segment's
  # `max_ts` so `query/2` can prune by time bounds at LIST time.
  @schema_version "v2"

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
          # No idempotency key: the caller accepts duplicates on retry. A
          # random suffix guarantees distinct writes even when payload and
          # timestamp collide.
          "rand-" <> caller_hash <> "-" <> rand_hex()
      end

    "#{prefix(tenant)}#{zero_pad(min_ts)}-#{zero_pad(max_ts)}-#{suffix}.ndjson"
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
  # the same bytes. That is stronger than a JSON encoder (map keys emit
  # in `Map.to_list/1` order, which is not canonical and can shift when a
  # small map promotes to a hash map). It is NOT guaranteed across major
  # OTP upgrades — see the module docstring's "Key format stability"
  # section for how a version bump migrates old objects when that happens.
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

  defp filter_by_time(records, nil, nil), do: records

  defp filter_by_time(records, start_ts, end_ts) do
    Enum.filter(records, fn %Log{timestamp_ns: ts} ->
      # A record with a nil timestamp has no place inside a time-bounded
      # range. Elixir's term ordering puts atoms greater than numbers, so
      # `nil >= 5` is true — without the `is_integer` guard, nil-ts
      # records would leak through every time filter.
      is_integer(ts) and
        (start_ts == nil or ts >= start_ts) and
        (end_ts == nil or ts <= end_ts)
    end)
  end

  defp filter_by_service(records, nil), do: records
  defp filter_by_service(records, service), do: Enum.filter(records, &(&1.service == service))

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
