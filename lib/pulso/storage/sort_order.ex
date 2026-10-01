defmodule Pulso.Storage.SortOrder do
  @moduledoc """
  Canonical sort order for a query result, per signal. Every storage
  adapter must apply this so a client that switches adapters — or fans a
  query out to more than one — cannot observe the tiebreaker changing.

  ## Logs

  Primary key: `timestamp_ns` descending (newest first).
  Tiebreakers, in order: `observed_timestamp_ns` desc, `trace_id`,
  `span_id`, `body`. `nil` sorts last within its position.

  ## Metrics

  Primary key: `timestamp_ns` descending (newest sample first).
  Tiebreakers, in order: `series_id` desc, then a canonical stringified
  label-set. `nil` sorts last within its position. This matches what a
  PromQL evaluator expects at the output boundary: samples bucketed by
  time, with a deterministic series order within each timestamp.
  """

  alias Pulso.Record.Log
  alias Pulso.Record.MetricSample

  @spec sort([Pulso.Storage.signal_record()], Pulso.Storage.signal()) :: [Pulso.Storage.signal_record()]
  def sort(records, :logs), do: Enum.sort_by(records, &log_sort_key/1, &compare_desc/2)
  def sort(records, :metrics), do: Enum.sort_by(records, &metric_sort_key/1, &compare_desc/2)

  @doc "Logs-signal convenience; retained for callers that have not been taught signal dispatch."
  @spec sort([Log.t()]) :: [Log.t()]
  def sort(records) when is_list(records), do: sort(records, :logs)

  # Each string-shaped tiebreaker becomes `{presence_flag, value}` so a nil
  # sorts *after* every real string in descending order, and — crucially —
  # never collapses with an empty string.
  defp log_sort_key(%Log{} = r) do
    {
      r.timestamp_ns || 0,
      r.observed_timestamp_ns || 0,
      presence_pair(r.trace_id),
      presence_pair(r.span_id),
      presence_pair(r.body)
    }
  end

  defp metric_sort_key(%MetricSample{} = s) do
    {
      s.timestamp_ns || 0,
      s.series_id || 0,
      canonical_labels(s.labels)
    }
  end

  # Deterministic string form of a label map, used only as a tiebreaker.
  # Sorting the keys makes it stable across the two Elixir map
  # implementations (small-flat vs big-hash).
  defp canonical_labels(labels) when is_map(labels) do
    labels
    |> Enum.sort()
    |> Enum.map_join("\0", fn {k, v} -> "#{k}=#{v}" end)
  end

  defp canonical_labels(_), do: ""

  # `body` is `String.t() | nil` by the internal Log spec, but the OTLP
  # decoder can produce integers, booleans, or lists when an incoming
  # record uses those OTLP `AnyValue` shapes. Coerce any non-string,
  # non-nil term to a string so it still tiebreaks deterministically.
  defp presence_pair(nil), do: {0, ""}
  defp presence_pair(value) when is_binary(value), do: {1, value}
  defp presence_pair(value), do: {1, inspect(value)}

  defp compare_desc(a, b), do: a >= b
end
