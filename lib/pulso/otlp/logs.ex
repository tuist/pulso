defmodule Pulso.OTLP.Logs do
  @moduledoc """
  Decode an OTLP/HTTP JSON `ExportLogsServiceRequest` into a flat list of
  `Pulso.Record.Log`. Follows the JSON encoding described in the OTLP spec:
  camelCase field names, `AnyValue` wrappers around attribute values, base64
  for trace and span ids, decimal-string int64 timestamps.

  Only the fields Pulso currently uses are lifted; the rest are dropped. That
  is intentional for step 1 — the internal record shape is what queriers and
  the Rust hot path will build on, and adding fields there is cheap.
  """

  alias Pulso.Record.Log

  @doc """
  Decode a parsed JSON payload.

  Returns `{records, rejected}`:

    * `records` — the flat list of `Pulso.Record.Log` that survived decoding.
    * `rejected` — the count of `LogRecord` entries that could not be
      decoded (missing or malformed `timeUnixNano`, wrong shape, etc.).
      The OTLP receiver surfaces this to the sender via
      `ExportLogsPartialSuccess.rejected_log_records` per the OTLP spec so
      the sender knows some records did not make it into storage.
  """
  @spec decode(map()) :: {[Log.t()], non_neg_integer()}
  def decode(%{"resourceLogs" => resource_logs}) when is_list(resource_logs) do
    resource_logs
    |> Enum.reduce({[], 0}, fn rl, {records, rejected} ->
      {rl_records, rl_rejected} = decode_resource_logs(rl)
      {[rl_records | records], rejected + rl_rejected}
    end)
    |> then(fn {records, rejected} -> {records |> Enum.reverse() |> List.flatten(), rejected} end)
  end

  # A top-level shape that is not an ExportLogsServiceRequest is not a valid
  # OTLP body at all — treat every record as rejected (well, zero counted,
  # since we can't count what we couldn't parse) and return empty.
  def decode(_), do: {[], 0}

  defp decode_resource_logs(%{"scopeLogs" => scope_logs} = resource_logs) when is_list(scope_logs) do
    resource_attrs = attributes(resource_logs["resource"])
    service = resource_attrs["service.name"]

    scope_logs
    |> Enum.reduce({[], 0}, fn sl, acc -> decode_scope_logs(sl, resource_attrs, service, acc) end)
    |> then(fn {records, rejected} -> {records |> Enum.reverse() |> List.flatten(), rejected} end)
  end

  defp decode_resource_logs(_), do: {[], 0}

  defp decode_scope_logs(scope_logs, resource_attrs, service, {records, rejected}) do
    raw = scope_logs["logRecords"] || []

    {sl_records, sl_rejected} =
      Enum.reduce(raw, {[], 0}, fn record, acc ->
        decode_and_collect(record, resource_attrs, service, acc)
      end)

    {[Enum.reverse(sl_records) | records], rejected + sl_rejected}
  end

  defp decode_and_collect(record, resource_attrs, service, {rs, rj}) do
    case decode_log_record(record, resource_attrs, service) do
      {:ok, r} -> {[r | rs], rj}
      :error -> {rs, rj + 1}
    end
  end

  # Per the OTLP logs data model, both `time_unix_nano` and
  # `observed_time_unix_nano` MAY be absent, and a value of 0 explicitly
  # means "unknown". The decoder preserves the caller's intent verbatim
  # (nil for absent or 0). The storage adapter fills a stored timestamp
  # later so that (a) the caller_content_hash is stable across retries
  # even when both timestamps are absent, and (b) records with only an
  # observed time are not lost or misplaced at the Unix epoch.
  defp decode_log_record(%{} = record, resource_attrs, service) do
    {:ok,
     %Log{
       timestamp_ns: nano_or_nil(record["timeUnixNano"]),
       observed_timestamp_ns: nano_or_nil(record["observedTimeUnixNano"]),
       severity_number: record["severityNumber"],
       severity_text: record["severityText"],
       service: service,
       body: any_value(record["body"]),
       trace_id: nil_if_empty(record["traceId"]),
       span_id: nil_if_empty(record["spanId"]),
       attributes: attributes(record),
       resource: resource_attrs
     }}
  end

  defp decode_log_record(_, _, _), do: :error

  # OTLP: a `*_unix_nano` value of 0 signals "unknown", identical in
  # meaning to the field being absent. Fold both into nil so downstream
  # code has one shape to reason about.
  defp nano_or_nil(nil), do: nil
  defp nano_or_nil(0), do: nil
  defp nano_or_nil(value) when is_integer(value), do: value

  defp nano_or_nil(value) when is_binary(value) do
    case Integer.parse(value) do
      {0, ""} -> nil
      {int, ""} -> int
      _ -> nil
    end
  end

  defp nano_or_nil(_), do: nil

  defp attributes(%{"attributes" => kvs}) when is_list(kvs) do
    Map.new(kvs, fn
      %{"key" => key, "value" => value} -> {key, any_value(value)}
      _ -> {nil, nil}
    end)
    |> Map.delete(nil)
  end

  defp attributes(_), do: %{}

  defp any_value(nil), do: nil
  defp any_value(%{"stringValue" => v}), do: v
  defp any_value(%{"boolValue" => v}), do: v
  defp any_value(%{"doubleValue" => v}), do: v
  defp any_value(%{"bytesValue" => v}), do: v

  defp any_value(%{"intValue" => v}) when is_binary(v) do
    case Integer.parse(v) do
      {int, ""} -> int
      _ -> v
    end
  end

  defp any_value(%{"intValue" => v}), do: v

  defp any_value(%{"arrayValue" => %{"values" => values}}) when is_list(values) do
    Enum.map(values, &any_value/1)
  end

  defp any_value(%{"kvlistValue" => %{"values" => kvs}}) when is_list(kvs) do
    Map.new(kvs, fn
      %{"key" => key, "value" => value} -> {key, any_value(value)}
      _ -> {nil, nil}
    end)
    |> Map.delete(nil)
  end

  defp any_value(other), do: other

  defp nil_if_empty(nil), do: nil
  defp nil_if_empty(""), do: nil
  defp nil_if_empty(value), do: value
end
