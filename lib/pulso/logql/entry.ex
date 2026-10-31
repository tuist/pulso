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
    * `base_labels` — resource/promoted stream names still protected from
      parser writes. Drop/rename releases a name. Manual entries without
      provenance treat all initial labels as stream labels.
    * `metadata_labels` — current structured-metadata owners, including the
      one-suffix names of metadata that collided with stream labels.
    * `extracted_labels` — names written by earlier parsed stages, including
      names subsequently dropped. Ordinary parsers keep the first extraction;
      explicit expression parsers have their own overwrite rules.
    * `label_categories` — optional current name-to-origin map for negotiated
      categorized HTTP output. Mutations update it alongside `labels`, even
      when the new value equals the old one. Never inferred from final values.
  """

  alias Pulso.Record.Log

  @enforce_keys [:timestamp_ns, :line, :labels]
  defstruct [
    :timestamp_ns,
    :line,
    :labels,
    :base_labels,
    :label_categories,
    metadata_labels: %{},
    extracted_labels: %{}
  ]

  @type category :: :stream | :structured_metadata | :parsed
  @type t :: %__MODULE__{
          timestamp_ns: non_neg_integer() | nil,
          line: String.t(),
          labels: %{optional(String.t()) => String.t()},
          base_labels: map() | nil,
          label_categories: %{optional(String.t()) => category()} | nil,
          metadata_labels: map(),
          extracted_labels: %{optional(String.t()) => true}
        }

  @spec from_record(Log.t(), boolean()) :: t()
  def from_record(%Log{} = record, categorize_labels? \\ false) do
    # Stream labels keep their names. Structured metadata is added in label
    # order, with one suffix on collision and no replacement of an occupied
    # suffixed name, matching Loki's LabelsBuilder.Add.
    base_labels =
      (record.resource || %{})
      |> maybe_put("service", record.service)
      |> maybe_put("service_name", record.service)
      |> maybe_put("level", record.severity_text)
      |> maybe_put("detected_level", record.severity_text)

    {labels, metadata} =
      Enum.reduce(Enum.sort(record.attributes || %{}), {stringify_values(base_labels), %{}}, fn {name, value},
                                                                                                {labels, metadata} ->
        target = if Map.has_key?(base_labels, name), do: name <> "_extracted", else: name

        if target != name and Map.has_key?(labels, target) do
          {labels, metadata}
        else
          value = stringify(value)
          {Map.put(labels, target, value), Map.put(metadata, target, value)}
        end
      end)

    categories =
      if categorize_labels? do
        base_labels
        |> Map.new(fn {name, _} -> {name, :stream} end)
        |> Map.merge(Map.new(metadata, fn {name, _} -> {name, :structured_metadata} end))
      end

    %__MODULE__{
      timestamp_ns: record.timestamp_ns,
      line: to_line(record.body),
      labels: labels,
      base_labels: base_labels,
      metadata_labels: metadata,
      label_categories: categories
    }
  end

  @doc false
  def extraction_name(entry, name) do
    if Map.has_key?(entry.base_labels, name) or Map.has_key?(entry.metadata_labels, name),
      do: name <> "_extracted",
      else: name
  end

  @doc false
  def extracted?(entry, name), do: Map.has_key?(entry.extracted_labels, name)

  @doc false
  def put_label(entry, name, value, category) do
    categories = if entry.label_categories, do: Map.put(entry.label_categories, name, category)

    metadata =
      if category == :structured_metadata,
        do: Map.put(entry.metadata_labels, name, value),
        else: Map.delete(entry.metadata_labels, name)

    extracted = if category == :parsed, do: Map.put(entry.extracted_labels, name, true), else: entry.extracted_labels

    %{
      entry
      | labels: Map.put(entry.labels, name, value),
        label_categories: categories,
        metadata_labels: metadata,
        extracted_labels: extracted
    }
  end

  @doc false
  def delete_label(entry, name) do
    categories = if entry.label_categories, do: Map.delete(entry.label_categories, name)
    base = if entry.base_labels, do: Map.delete(entry.base_labels, name)

    %{
      entry
      | labels: Map.delete(entry.labels, name),
        label_categories: categories,
        base_labels: base,
        metadata_labels: Map.delete(entry.metadata_labels, name)
    }
  end

  @doc false
  def keep_labels(entry, labels) do
    names = Map.keys(labels)
    categories = if entry.label_categories, do: Map.take(entry.label_categories, names)
    base = if entry.base_labels, do: Map.take(entry.base_labels, names)

    %{
      entry
      | labels: labels,
        label_categories: categories,
        base_labels: base,
        metadata_labels: Map.take(entry.metadata_labels, names)
    }
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
