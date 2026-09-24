defmodule Pulso.Storage.S3 do
  @moduledoc """
  S3-backed log storage. Step 2 adapter.

  Each `append/2` writes one NDJSON object under a tenant-scoped prefix
  (`tenants/<tenant>/logs/<sort_ns>-<content_hash>.ndjson`). Tenant isolation
  is enforced by key construction: tenant names are validated against a
  conservative charset so a batch cannot land outside its own prefix. The
  key's suffix is a truncated SHA-256 of the encoded payload, which makes
  identical retries land on the same object — a client that PUTs the same
  batch twice under a lost-response retry does not create a duplicate.

  `query/2` lists the tenant prefix, downloads every object, decodes NDJSON,
  filters, and sorts. Order matches `Pulso.Storage.Memory`: `timestamp_ns`
  descending, then `observed_timestamp_ns` descending, then `trace_id`, then
  `body`, so equal-timestamp ties resolve identically across adapters.
  A `NotFound` for a key that was listed but disappeared before the fetch
  (concurrent retention, compaction, another process deleting) is skipped
  rather than aborting the query.

  Known limits, deferred to step 3 (segments + manifest):

    * **Unbounded query work when `limit` is set.** Without per-batch time
      metadata this adapter cannot safely skip objects: a batch written
      recently may contain an old-timestamp record, so scanning every
      object is required to preserve the sort semantics. Step 3's segment
      manifest will carry min/max `timestamp_ns` per segment and let the
      query short-circuit.
    * **No columnar layout.** Records go on the wire as NDJSON, not Parquet.
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
  def append(tenant, []) when is_binary(tenant) do
    # Validate even on empty so an adversarial tenant name is rejected on the
    # first attempt, not only once a real record survives OTLP decoding.
    validate_tenant(tenant)
  end

  def append(tenant, records) when is_binary(tenant) and is_list(records) do
    with :ok <- validate_tenant(tenant),
         normalized = normalize(records),
         {:ok, payload} <- encode(normalized) do
      key = object_key(tenant, batch_sort_ns(normalized), payload)
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

    for %Log{} = record <- records do
      %{
        record
        | observed_timestamp_ns: record.observed_timestamp_ns || now,
          attributes: sanitize_map(record.attributes || %{}),
          resource: sanitize_map(record.resource || %{})
      }
    end
  end

  # Force every attribute/resource map key to be a string and drop nils. OTLP
  # decoding already produces string keys, but a caller building `%Log{}`
  # directly (or a future backend surface) could hand us atoms or integers.
  # Encoding those with Jason coerces them to strings, so two logical keys
  # can silently collapse on the round trip. Doing the coercion here makes
  # the write path deterministic and the decode path lossless.
  @doc false
  @spec sanitize_map(map()) :: map()
  def sanitize_map(map) when is_map(map) do
    Map.new(map, fn {k, v} -> {stringify_key(k), sanitize_value(v)} end)
  end

  defp stringify_key(k) when is_binary(k), do: k
  defp stringify_key(k) when is_atom(k), do: Atom.to_string(k)
  defp stringify_key(k) when is_integer(k), do: Integer.to_string(k)
  defp stringify_key(k), do: inspect(k)

  defp sanitize_value(v) when is_map(v), do: sanitize_map(v)
  defp sanitize_value(v) when is_list(v), do: Enum.map(v, &sanitize_value/1)
  defp sanitize_value(v), do: v

  defp batch_sort_ns(records) do
    # Pick the smallest observed_timestamp_ns so identical retries hash into
    # the same object key. Every record is normalized, so this is never nil.
    records
    |> Enum.map(& &1.observed_timestamp_ns)
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

  defp prefix(tenant), do: "tenants/#{tenant}/logs/"

  @doc false
  @spec object_key(String.t(), non_neg_integer(), binary()) :: String.t()
  def object_key(tenant, sort_ns, payload) when is_binary(tenant) and is_binary(payload) do
    "#{prefix(tenant)}#{zero_pad(sort_ns)}-#{content_hash(payload)}.ndjson"
  end

  defp zero_pad(ns) when is_integer(ns) and ns >= 0 do
    ns
    |> Integer.to_string()
    |> String.pad_leading(@sort_key_width, "0")
  end

  defp content_hash(payload) do
    :sha256
    |> :crypto.hash(payload)
    |> Base.encode16(case: :lower)
    |> binary_part(0, @content_hash_width)
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
