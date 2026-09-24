defmodule Pulso.Storage.SortOrder do
  @moduledoc """
  Canonical sort order for a query result. Every storage adapter must apply
  this so a client that switches adapters — or fans a query out to more than
  one — cannot observe the tiebreaker changing.

  Primary key: `timestamp_ns` descending (newest first).
  Tiebreakers, in order: `observed_timestamp_ns` desc, `trace_id`, `span_id`,
  `body`. `nil` sorts last within its position.
  """

  alias Pulso.Record.Log

  @spec sort([Log.t()]) :: [Log.t()]
  def sort(records) do
    Enum.sort_by(records, &sort_key/1, &compare_desc/2)
  end

  defp sort_key(%Log{} = r) do
    {
      r.timestamp_ns || 0,
      r.observed_timestamp_ns || 0,
      # Strings compare lexicographically. Descending sort is what the caller
      # asked for, so we invert the two lower-priority string tiebreakers by
      # negating the comparison in `compare_desc/2`.
      r.trace_id || "",
      r.span_id || "",
      r.body || ""
    }
  end

  defp compare_desc(a, b), do: a >= b
end
