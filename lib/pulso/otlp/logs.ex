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
  Decode a parsed JSON payload. Returns the flat list of log records; malformed
  entries are skipped rather than aborting the whole batch.
  """
  @spec decode(map()) :: [Log.t()]
  def decode(%{"resourceLogs" => resource_logs}) when is_list(resource_logs) do
    Enum.flat_map(resource_logs, &decode_resource_logs/1)
  end

  def decode(_), do: []

  defp decode_resource_logs(%{"scopeLogs" => scope_logs} = resource_logs) when is_list(scope_logs) do
    resource_attrs = attributes(resource_logs["resource"])
    service = resource_attrs["service.name"]

    Enum.flat_map(scope_logs, fn scope_logs ->
      records = scope_logs["logRecords"] || []
      Enum.flat_map(records, &decode_log_record(&1, resource_attrs, service))
    end)
  end

  defp decode_resource_logs(_), do: []

  defp decode_log_record(%{} = record, resource_attrs, service) do
    case timestamp(record["timeUnixNano"]) do
      {:ok, ts} ->
        [
          %Log{
            timestamp_ns: ts,
            observed_timestamp_ns: nano(record["observedTimeUnixNano"]),
            severity_number: record["severityNumber"],
            severity_text: record["severityText"],
            service: service,
            body: any_value(record["body"]),
            trace_id: nil_if_empty(record["traceId"]),
            span_id: nil_if_empty(record["spanId"]),
            attributes: attributes(record),
            resource: resource_attrs
          }
        ]

      _ ->
        []
    end
  end

  defp decode_log_record(_, _, _), do: []

  defp timestamp(nil), do: :error

  defp timestamp(value) do
    case nano(value) do
      nil -> :error
      ns -> {:ok, ns}
    end
  end

  defp nano(nil), do: nil
  defp nano(value) when is_integer(value), do: value

  defp nano(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} -> int
      _ -> nil
    end
  end

  defp nano(_), do: nil

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
