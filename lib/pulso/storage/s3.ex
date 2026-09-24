defmodule Pulso.Storage.S3 do
  @moduledoc """
  S3-backed log storage. Step 2 adapter.

  Each `append/3` writes one NDJSON object under a tenant-scoped prefix
  (`tenants/<tenant>/logs/<sort_ns>-<suffix>.ndjson`). Tenant isolation is
  enforced by key construction: tenant names are validated against a
  conservative charset so a batch cannot land outside its own prefix.

  The key suffix depends on whether the caller supplied an
  `idempotency_key`:

    * **With `idempotency_key`** — the suffix is a deterministic hash of
      `tenant || idempotency_key`. Two `append` calls with the same key
      resolve to the same object, so a lost-response retry does not
      duplicate. Two producers that pick the same idempotency key are
      explicitly claiming "these are the same write" — semantics that
      match Stripe's `Idempotency-Key` and RFC 9457.
    * **Without `idempotency_key`** — the suffix mixes a truncated content
      hash with random bytes. Distinct calls always produce distinct
      objects (no accidental collapse of two identical-content batches).
      Retries in this mode duplicate — callers who need dedup must opt in.

  `query/2` lists the tenant prefix, downloads every object, decodes NDJSON,
  filters, and sorts. Order is shared with `Pulso.Storage.Memory` via
  `Pulso.Storage.SortOrder`. A `NotFound` for a key that was listed but
  disappeared before the fetch (concurrent retention, compaction, another
  process deleting) is skipped rather than aborting the query.

  Known limits, deferred to step 3 (segments + manifest):

    * **Unbounded query work when `limit` is set.** Without per-batch time
      metadata this adapter cannot safely skip objects: a batch written
      recently may contain an old-timestamp record, so scanning every
      object is required to preserve the sort semantics. Step 3's segment
      manifest will carry min/max `timestamp_ns` per segment and let the
      query short-circuit.
    * **No columnar layout.** Records go on the wire as NDJSON, not Parquet.

  ## Key format stability

  Every object lives under a versioned prefix (`tenants/<tenant>/v1/…`).
  Within one schema version, the exact suffix format is deliberately not
  a public API. It is derived from `:erlang.term_to_binary(_,
  [:deterministic])`, which is stable within an OTP release but is not
  guaranteed to survive a major OTP upgrade (per the erts release notes,
  the algorithm can change intentionally). Any change to the fingerprint,
  the delimiter, or the sort-key width bumps the schema version — new
  writes go to `v2/`, old objects stay at `v1/`, and a compaction job
  migrates at its own pace. The reader can be taught to look at both
  during the migration window.
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
         # Both the fingerprint AND the sort-key prefix are derived from the
         # caller-provided records BEFORE `normalize/1` fills any wall-clock
         # timestamps. A legitimate retry then produces the same object key
         # in full — the sort_ns prefix and the idempotency suffix are both
         # stable, so the second PUT overwrites the first as intended.
         {:ok, caller_hash} = caller_content_hash(records),
         sort_ns = caller_sort_ns(records),
         {:ok, normalized} <- normalize(records),
         {:ok, payload} <- encode(normalized) do
      key = object_key(tenant, sort_ns, caller_hash, idempotency_key)
      ObjectStore.put(config!(), key, payload)
    end
  end

  @impl Pulso.Storage
  def query(tenant, opts) when is_binary(tenant) and is_list(opts) do
    with :ok <- validate_tenant(tenant),
         config = config!(),
         {:ok, keys} <- ObjectStore.list(config, prefix(tenant)),
         {:ok, records} <- fetch_records(config, keys) do
      filtered =
        records
        |> filter_by_time(Keyword.get(opts, :start_ts), Keyword.get(opts, :end_ts))
        |> filter_by_service(Keyword.get(opts, :service))
        |> SortOrder.sort()
        |> take_limit(Keyword.get(opts, :limit))

      {:ok, filtered}
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
    now = System.system_time(:nanosecond)

    Enum.reduce_while(records, {:ok, []}, fn
      %Log{} = record, {:ok, acc} ->
        with {:ok, attrs} <- sanitize_map(record.attributes || %{}),
             {:ok, resource} <- sanitize_map(record.resource || %{}) do
          observed_ts = record.observed_timestamp_ns || now

          # OTLP allows both timestamps to be absent (or explicitly zero,
          # which the decoder folds to nil). We backfill: prefer the
          # observed timestamp, then wall clock. This runs AFTER the
          # caller_content_hash, so retries with identical raw records
          # still hash to the same fingerprint.
          normalized = %{
            record
            | timestamp_ns: record.timestamp_ns || observed_ts,
              observed_timestamp_ns: observed_ts,
              attributes: attrs,
              resource: resource
          }

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
    Enum.reduce_while(map, {:ok, %{}}, fn {k, v}, {:ok, acc} ->
      string_key = stringify_key(k)

      cond do
        Map.has_key?(acc, string_key) ->
          {:halt, {:error, {:attribute_key_collision, [string_key]}}}

        is_map(v) ->
          case sanitize_map(v) do
            {:ok, sanitized} -> {:cont, {:ok, Map.put(acc, string_key, sanitized)}}
            err -> {:halt, err}
          end

        is_list(v) ->
          case sanitize_list(v) do
            {:ok, sanitized} -> {:cont, {:ok, Map.put(acc, string_key, sanitized)}}
            err -> {:halt, err}
          end

        true ->
          {:cont, {:ok, Map.put(acc, string_key, v)}}
      end
    end)
  end

  defp sanitize_list(list) do
    Enum.reduce_while(list, {:ok, []}, fn
      v, {:ok, acc} when is_map(v) ->
        case sanitize_map(v) do
          {:ok, sanitized} -> {:cont, {:ok, [sanitized | acc]}}
          err -> {:halt, err}
        end

      v, {:ok, acc} when is_list(v) ->
        case sanitize_list(v) do
          {:ok, sanitized} -> {:cont, {:ok, [sanitized | acc]}}
          err -> {:halt, err}
        end

      v, {:ok, acc} ->
        {:cont, {:ok, [v | acc]}}
    end)
    |> case do
      {:ok, sanitized} -> {:ok, Enum.reverse(sanitized)}
      err -> err
    end
  end

  defp stringify_key(k) when is_binary(k), do: k
  defp stringify_key(k) when is_atom(k), do: Atom.to_string(k)
  defp stringify_key(k) when is_integer(k), do: Integer.to_string(k)
  defp stringify_key(k), do: inspect(k)

  # Pick the smallest caller-supplied `timestamp_ns` for the object key
  # prefix. Runs on records BEFORE normalization so a retry with identical
  # caller input produces the same sort_ns. A record with no timestamp
  # contributes 0, which parks the object at the head of the tenant
  # listing — good enough for the fallback case and, importantly,
  # deterministic across retries.
  defp caller_sort_ns(records) do
    records
    |> Enum.map(fn %Log{timestamp_ns: ts} -> ts || 0 end)
    |> Enum.min()
  end

  defp encode(records) do
    encoded =
      Enum.reduce_while(records, {:ok, []}, fn record, {:ok, acc} ->
        case Jason.encode(Map.from_struct(record)) do
          {:ok, line} -> {:cont, {:ok, [[line, "\n"] | acc]}}
          {:error, reason} -> {:halt, {:error, {:encode_failed, reason}}}
        end
      end)

    with {:ok, lines} <- encoded do
      {:ok, lines |> Enum.reverse() |> IO.iodata_to_binary()}
    end
  end

  defp fetch_records(config, keys) do
    # Accumulate batches as a list of lists then flatten once, so a large
    # tenant does not pay O(n^2) list concatenation. A `:not_found` for a
    # key that vanished after `list` is treated as "raced with a delete" and
    # skipped; anything else halts the query so an outage is not hidden.
    Enum.reduce_while(keys, {:ok, []}, fn key, {:ok, batches} ->
      case ObjectStore.get(config, key) do
        {:ok, blob} -> {:cont, {:ok, [decode(blob) | batches]}}
        {:error, :not_found} -> {:cont, {:ok, batches}}
        {:error, _} = err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, batches} -> {:ok, batches |> Enum.reverse() |> List.flatten()}
      {:error, _} = err -> err
    end
  end

  defp decode(blob) do
    blob
    |> String.split("\n", trim: true)
    |> Enum.map(&decode_line/1)
  end

  defp decode_line(line) do
    map = Jason.decode!(line)

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
  # to the key format (a new fingerprint algorithm, a different sort key
  # width) can coexist with v1 objects rather than orphan them. Bump the
  # version, keep readers that recognize both, and let a compaction job
  # migrate the old prefix at leisure.
  @schema_version "v1"

  defp prefix(tenant), do: "tenants/#{tenant}/#{@schema_version}/logs/"

  # `caller_hash` is a 16-hex fingerprint of the pre-normalization records
  # from `caller_content_hash/1`. The pre-normalization form matters:
  # `normalize/1` fills in a fresh wall-clock `observed_timestamp_ns` on
  # every call, so hashing after normalization would make identical retries
  # produce different keys even under an idempotency key.
  @doc false
  @spec object_key(String.t(), non_neg_integer(), String.t(), String.t() | nil) :: String.t()
  def object_key(tenant, sort_ns, caller_hash, idempotency_key) when is_binary(tenant) and is_binary(caller_hash) do
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

    "#{prefix(tenant)}#{zero_pad(sort_ns)}-#{suffix}.ndjson"
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
  # the same bytes. That is stronger than `Jason.encode/1` (map keys emit
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
      (start_ts == nil or ts >= start_ts) and (end_ts == nil or ts <= end_ts)
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
