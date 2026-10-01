defmodule Pulso.LogQL.Entry do
  @moduledoc """
  A single log line as it moves through the LogQL pipeline.

    * `timestamp_ns` — original ingest timestamp (never mutated).
    * `line` — the *current* text of the log entry. `line_format`,
      `decolorize`, and `unpack` rewrite it in place. Downstream parsers
      that key off the line (`json`, `logfmt`, `regexp`, `pattern`) read
      whatever the previous stages left here.
    * `labels` — the merged label bag. Starts as `resource ∪ attributes`
      plus synthetic promoted fields (`service`, `service_name`,
      `level`, `detected_level`) derived from the typed struct fields,
      so `{service_name="api"}` and `{service="api"}` both match
      regardless of which ingest path produced the record.
  """

  alias Pulso.Record.Log

  @enforce_keys [:timestamp_ns, :line, :labels]
  defstruct [:timestamp_ns, :line, :labels]

  @type t :: %__MODULE__{
          timestamp_ns: non_neg_integer() | nil,
          line: String.t(),
          labels: %{optional(String.t()) => String.t()}
        }

  @spec from_record(Log.t()) :: t()
  def from_record(%Log{} = record) do
    labels =
      (record.resource || %{})
      |> Map.merge(record.attributes || %{})
      |> stringify_values()
      |> maybe_put("service", record.service)
      |> maybe_put("service_name", record.service)
      |> maybe_put("level", record.severity_text)
      |> maybe_put("detected_level", record.severity_text)

    %__MODULE__{
      timestamp_ns: record.timestamp_ns,
      line: to_line(record.body),
      labels: labels
    }
  end

  # LogQL matchers compare against strings. Non-string label values coming
  # from OTLP attributes (numbers, booleans) get stringified once at entry
  # time so every downstream matcher can rely on the invariant.
  defp stringify_values(map) do
    Map.new(map, fn {k, v} -> {k, stringify(v)} end)
  end

  defp stringify(v) when is_binary(v), do: v
  defp stringify(v) when is_integer(v), do: Integer.to_string(v)
  defp stringify(v) when is_float(v), do: Float.to_string(v)
  defp stringify(true), do: "true"
  defp stringify(false), do: "false"
  defp stringify(nil), do: ""
  defp stringify(v), do: inspect(v)

  defp maybe_put(map, _key, nil), do: map

  defp maybe_put(map, key, value) do
    if Map.has_key?(map, key), do: map, else: Map.put(map, key, value)
  end

  defp to_line(nil), do: ""
  defp to_line(body) when is_binary(body), do: body
  defp to_line(body), do: Pulso.JSON.encode!(body)
end
