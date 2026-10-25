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
    * `base_labels` — immutable resource/promoted stream-name provenance.
      Structured attributes stay overridable by parser output. Manual entries
      without provenance treat all initial labels as stream labels; use
      `from_record/1` to distinguish structured attributes.
    * `label_categories` — optional current name-to-origin map for negotiated
      categorized HTTP output. Mutations update it alongside `labels`, even
      when the new value equals the old one. Never inferred from final values.
  """

  alias Pulso.Record.Log

  @enforce_keys [:timestamp_ns, :line, :labels]
  defstruct [:timestamp_ns, :line, :labels, :base_labels, :label_categories]

  @type category :: :stream | :structured_metadata | :parsed
  @type t :: %__MODULE__{
          timestamp_ns: non_neg_integer() | nil,
          line: String.t(),
          labels: %{optional(String.t()) => String.t()},
          base_labels: map() | nil,
          label_categories: %{optional(String.t()) => category()} | nil
        }

  @spec from_record(Log.t(), boolean()) :: t()
  def from_record(%Log{} = record, categorize_labels? \\ false) do
    # Resource + promoted fields define Pulso's stream labels, just as detected
    # fields do. Structured attributes remain overridable by parser output.
    base_labels =
      (record.resource || %{})
      |> maybe_put("service", record.service)
      |> maybe_put("service_name", record.service)
      |> maybe_put("level", record.severity_text)
      |> maybe_put("detected_level", record.severity_text)

    attributes = record.attributes || %{}
    labels = base_labels |> Map.merge(attributes) |> stringify_values()

    categories =
      if categorize_labels? do
        base_labels
        |> Map.new(fn {name, _} -> {name, :stream} end)
        |> Map.merge(Map.new(attributes, fn {name, _} -> {name, :structured_metadata} end))
      end

    %__MODULE__{
      timestamp_ns: record.timestamp_ns,
      line: to_line(record.body),
      labels: labels,
      base_labels: base_labels,
      label_categories: categories
    }
  end

  @doc false
  def put_label(entry, name, value, category) do
    categories = if entry.label_categories, do: Map.put(entry.label_categories, name, category)
    %{entry | labels: Map.put(entry.labels, name, value), label_categories: categories}
  end

  @doc false
  def delete_label(entry, name) do
    categories = if entry.label_categories, do: Map.delete(entry.label_categories, name)
    %{entry | labels: Map.delete(entry.labels, name), label_categories: categories}
  end

  @doc false
  def keep_labels(entry, labels) do
    categories = if entry.label_categories, do: Map.take(entry.label_categories, Map.keys(labels))
    %{entry | labels: labels, label_categories: categories}
  end

  @doc false
  # Called only on selected entries, after the unchanged global top-k. Empty
  # per-entry categories still encode as {}, never as an omitted third value.
  def categorized_labels(%__MODULE__{label_categories: categories, labels: labels}) when is_map(categories) do
    Enum.reduce(labels, {%{}, %{}}, fn {name, value}, {stream, entry} ->
      case Map.fetch!(categories, name) do
        :stream -> {Map.put(stream, name, value), entry}
        :structured_metadata -> {stream, put_category(entry, "structuredMetadata", name, value)}
        :parsed -> {stream, put_category(entry, "parsed", name, value)}
      end
    end)
  end

  defp put_category(categories, category, name, value),
    do: Map.update(categories, category, %{name => value}, &Map.put(&1, name, value))

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
