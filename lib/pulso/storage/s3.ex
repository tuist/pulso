defmodule Pulso.Storage.S3 do
  @moduledoc """
  S3-backed log storage. Step 2 adapter.

  Each `append/2` writes one NDJSON object under a tenant-scoped prefix
  (`tenants/<tenant>/logs/<sort_key>.ndjson`). Tenant isolation is enforced by
  key construction: tenant names are validated against a conservative charset
  so a batch cannot land outside its own prefix. `query/2` lists the tenant's
  prefix, downloads every object, decodes NDJSON, filters and sorts in Elixir.

  Known limits of this adapter, deferred to step 3 (segments + manifest CAS):

    * **Not idempotent.** An `append` that succeeds but whose response is lost
      will be duplicated by a retry — each call generates a fresh random
      object key. Step 3 introduces a manifest with conditional writes.
    * **Unbounded query work.** Every query lists and downloads every object
      under the tenant prefix before applying `limit`. Fine for a small tenant
      or a smoke test; not fine at scale.
    * **No cross-tenant authentication.** Tenant validation here only prevents
      key-escape; it does not verify that the caller is *allowed* to read or
      write the tenant they named. Auth belongs at the ingest boundary.
    * **No columnar layout.** Records go on the wire as NDJSON, not Parquet.
  """

  @behaviour Pulso.Storage

  alias Pulso.ObjectStore
  alias Pulso.Record.Log

  @tenant_regex ~r/\A[A-Za-z0-9_.\-]{1,128}\z/
  # 20 decimal digits fits a u64 nanosecond timestamp (max ~1.84e19). Zero-padding
  # this way makes the S3 list order roughly chronological, which lets query
  # short-circuit once the sort/limit is satisfied in a later step.
  @sort_key_width 20

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
      key = object_key(tenant, batch_sort_ns(normalized))
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
        |> Enum.sort_by(& &1.timestamp_ns, :desc)
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
      %{record | observed_timestamp_ns: record.observed_timestamp_ns || now}
    end
  end

  defp batch_sort_ns(records) do
    # Pick the smallest observed_timestamp_ns so the first key in a listing is
    # the earliest batch. Every record is normalized, so this is never nil.
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
    # tenant does not pay O(n^2) list concatenation. Any get error halts the
    # query — silently skipping would hide backend outages. Distinguishing a
    # since-deleted key from a real failure requires typed errors from the
    # NIF (step 3, once compaction can delete out from under a reader).
    Enum.reduce_while(keys, {:ok, []}, fn key, {:ok, batches} ->
      case ObjectStore.get(config, key) do
        {:ok, blob} -> {:cont, {:ok, [decode(blob) | batches]}}
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

  defp object_key(tenant, sort_ns) do
    "#{prefix(tenant)}#{zero_pad(sort_ns)}-#{rand_suffix()}.ndjson"
  end

  defp zero_pad(ns) when is_integer(ns) and ns >= 0 do
    ns
    |> Integer.to_string()
    |> String.pad_leading(@sort_key_width, "0")
  end

  defp rand_suffix do
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
