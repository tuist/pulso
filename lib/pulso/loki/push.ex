defmodule Pulso.Loki.Push do
  @moduledoc """
  Decode a Loki `POST /loki/api/v1/push` body into a flat list of
  `Pulso.Record.Log`.

  Two wire formats land here — the JSON push and the Snappy-compressed
  protobuf push (Alloy's default). The Snappy decompression and
  protobuf parse happen in the controller; this module owns the shared
  mapping into `Pulso.Record.Log` so both paths agree on how labels,
  timestamps, and structured metadata land in the record.

  ### JSON shape

      {
        "streams": [
          {
            "stream": { "service_name": "api", "level": "info" },
            "values": [
              [ "1700000000000000000", "hello" ],
              [ "1700000000000000001", "with meta", {"trace_id": "abc", "user_id": "u1"} ]
            ]
          }
        ]
      }

  ### Protobuf shape

  See `Pulso.Loki.PushProto`. Labels arrive as a Prometheus-style
  `{k="v",...}` string (parsed by `Pulso.Loki.LabelString`);
  timestamps are `google.protobuf.Timestamp`-shaped (`{seconds, nanos}`);
  structured metadata is a repeated `LabelPair`.

  ## Mapping to Pulso.Record.Log

    * Stream labels populate `resource` — they identify the emitter and
      are constant across the stream, the same role resource attributes
      play in OTLP.
    * `service_name` (Grafana's OTel-aligned convention) or, as a
      fallback, `service`, is lifted to `Log.service`. It stays in
      `resource` too so a `resource.service_name` query still sees it.
    * `level` (or `detected_level`) is lifted to `Log.severity_text`.
    * Structured metadata is per-record — it populates `attributes`.
    * `trace_id` / `span_id` from structured metadata are lifted to the
      dedicated struct fields and removed from `attributes`, so a
      caller querying by trace id sees them in one canonical place
      regardless of which ingest path a record came in on.

  ## Return shape

  `{records, rejected}`, mirroring `Pulso.OTLP.Logs.decode/1`. `rejected`
  counts value tuples the decoder could not interpret (missing timestamp,
  non-string line, malformed shape). A whole-stream drop (e.g. `stream`
  is not a map, or a protobuf `labels` string is malformed) is counted
  as one reject per entry in that stream — the sender should still know
  that many records did not land.
  """

  alias Pulso.Loki.LabelString
  alias Pulso.Loki.PushProto
  alias Pulso.Record.Log

  @doc """
  Decode a parsed JSON payload.

  Returns `{records, rejected}`. A payload that is not shaped like a
  Loki push request returns `{[], 0}` — there is nothing to count,
  because nothing was parseable in the first place.
  """
  @spec decode(map()) :: {[Log.t()], non_neg_integer()}
  def decode(%{"streams" => streams}) when is_list(streams) do
    streams
    |> Enum.reduce({[], 0}, fn stream, {records, rejected} ->
      {s_records, s_rejected} = decode_stream(stream)
      {[s_records | records], rejected + s_rejected}
    end)
    |> then(fn {records, rejected} ->
      {records |> Enum.reverse() |> List.flatten(), rejected}
    end)
  end

  def decode(_), do: {[], 0}

  @doc """
  Decode an already-parsed `Pulso.Loki.PushProto.PushRequest` (i.e. the
  Snappy layer stripped, the protobuf parsed) into the same
  `{records, rejected}` shape as `decode/1`.

  Records with a malformed timestamp count as one reject each. A whole
  stream whose Prometheus-style `labels` string cannot be parsed counts
  as `length(entries)` rejects — the labels are the only source of
  emitter identity, and a record with no attribution has no place in
  a per-tenant, per-service store.
  """
  @spec decode_proto(PushProto.PushRequest.t()) :: {[Log.t()], non_neg_integer()}
  def decode_proto(%PushProto.PushRequest{streams: streams}) when is_list(streams) do
    streams
    |> Enum.reduce({[], 0}, fn stream, {records, rejected} ->
      {s_records, s_rejected} = decode_proto_stream(stream)
      {[s_records | records], rejected + s_rejected}
    end)
    |> then(fn {records, rejected} ->
      {records |> Enum.reverse() |> List.flatten(), rejected}
    end)
  end

  def decode_proto(_), do: {[], 0}

  defp decode_proto_stream(%PushProto.Stream{labels: labels_str, entries: entries})
       when is_binary(labels_str) and is_list(entries) do
    case LabelString.parse(labels_str) do
      {:ok, labels} -> decode_proto_entries(entries, labels)
      :error -> {[], length(entries)}
    end
  end

  defp decode_proto_stream(_), do: {[], 0}

  defp decode_proto_entries(entries, labels) do
    {service, severity_text, resource} = split_labels(labels)

    entries
    |> Enum.reduce({[], 0}, &collect_proto_entry(&1, &2, service, severity_text, resource))
    |> then(fn {records, rejected} -> {Enum.reverse(records), rejected} end)
  end

  defp collect_proto_entry(entry, {rs, rj}, service, severity_text, resource) do
    case decode_proto_entry(entry, service, severity_text, resource) do
      {:ok, record} -> {[record | rs], rj}
      :error -> {rs, rj + 1}
    end
  end

  defp decode_proto_entry(
         %PushProto.Entry{timestamp: ts, line: line, structured_metadata: meta_pairs},
         service,
         severity_text,
         resource
       )
       when is_binary(line) and is_list(meta_pairs) do
    with {:ok, ts_ns} <- proto_ts(ts) do
      meta = pairs_to_map(meta_pairs)
      {trace_id, span_id, attributes} = split_metadata(meta)

      {:ok,
       %Log{
         timestamp_ns: ts_ns,
         observed_timestamp_ns: nil,
         severity_number: nil,
         severity_text: severity_text,
         service: service,
         body: line,
         trace_id: trace_id,
         span_id: span_id,
         attributes: attributes,
         resource: resource
       }}
    end
  end

  defp decode_proto_entry(_, _, _, _), do: :error

  # google.protobuf.Timestamp is (int64 seconds, int32 nanos). Both are
  # required to be non-negative for a time-partitioned store — a negative
  # instant or a missing message is a reject rather than a silent 1970.
  defp proto_ts(%PushProto.Timestamp{seconds: s, nanos: n})
       when is_integer(s) and s >= 0 and is_integer(n) and n >= 0 and n < 1_000_000_000 do
    {:ok, s * 1_000_000_000 + n}
  end

  defp proto_ts(_), do: :error

  defp pairs_to_map(pairs) do
    Enum.reduce(pairs, %{}, fn
      %PushProto.LabelPair{name: name, value: value}, acc
      when is_binary(name) and is_binary(value) ->
        Map.put(acc, name, value)

      _, acc ->
        acc
    end)
  end

  defp decode_stream(%{"stream" => labels, "values" => values}) when is_map(labels) and is_list(values) do
    {service, severity_text, resource} = split_labels(labels)

    Enum.reduce(values, {[], 0}, fn value, {rs, rj} ->
      case decode_value(value, service, severity_text, resource) do
        {:ok, record} -> {[record | rs], rj}
        :error -> {rs, rj + 1}
      end
    end)
    |> then(fn {records, rejected} -> {Enum.reverse(records), rejected} end)
  end

  # Missing/malformed `stream` map: every value in this stream is a
  # reject, because we can't attribute records without labels.
  defp decode_stream(%{"values" => values}) when is_list(values), do: {[], length(values)}
  defp decode_stream(_), do: {[], 0}

  # Split stream labels into (service, severity_text, resource). The
  # resource map keeps *all* labels — including the lifted service and
  # level — so structured queries that filter on `resource.level` still
  # work, and so a caller looking at the stored record can see the raw
  # label set as sent.
  defp split_labels(labels) do
    service = labels["service_name"] || labels["service"]

    severity_text =
      case labels["level"] || labels["detected_level"] do
        nil -> nil
        v when is_binary(v) -> v
        v -> to_string(v)
      end

    {service, severity_text, labels}
  end

  defp decode_value([ts, line], service, severity_text, resource) when is_binary(line) do
    build_record(ts, line, %{}, service, severity_text, resource)
  end

  defp decode_value([ts, line, meta], service, severity_text, resource) when is_binary(line) and is_map(meta) do
    build_record(ts, line, meta, service, severity_text, resource)
  end

  defp decode_value(_, _, _, _), do: :error

  defp build_record(ts, line, meta, service, severity_text, resource) do
    with {:ok, ts_ns} <- parse_ts(ts) do
      {trace_id, span_id, attributes} = split_metadata(meta)

      {:ok,
       %Log{
         timestamp_ns: ts_ns,
         observed_timestamp_ns: nil,
         severity_number: nil,
         severity_text: severity_text,
         service: service,
         body: line,
         trace_id: trace_id,
         span_id: span_id,
         attributes: attributes,
         resource: resource
       }}
    end
  end

  # Loki's spec form is a decimal-string ns integer. Numeric forms show
  # up in a handful of client libraries, so accept them too. Anything
  # else is a reject; a record with no timestamp has no place in a
  # time-partitioned store.
  defp parse_ts(ts) when is_integer(ts) and ts >= 0, do: {:ok, ts}

  defp parse_ts(ts) when is_binary(ts) do
    case Integer.parse(ts) do
      {int, ""} when int >= 0 -> {:ok, int}
      _ -> :error
    end
  end

  defp parse_ts(_), do: :error

  # Structured metadata is a flat string map per the Loki spec. We lift
  # `trace_id` and `span_id` into their dedicated struct fields and
  # remove them from `attributes` so a caller has exactly one canonical
  # place to look. Every other key stays in `attributes`.
  defp split_metadata(meta) when is_map(meta) do
    trace_id = nil_if_blank(meta["trace_id"])
    span_id = nil_if_blank(meta["span_id"])
    attributes = meta |> Map.drop(["trace_id", "span_id"])
    {trace_id, span_id, attributes}
  end

  defp nil_if_blank(nil), do: nil
  defp nil_if_blank(""), do: nil
  defp nil_if_blank(v), do: v
end
