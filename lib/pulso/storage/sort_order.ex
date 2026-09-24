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

  # Each string-shaped tiebreaker becomes `{presence_flag, value}` so a nil
  # sorts *after* every real string in descending order, and — crucially —
  # never collapses with an empty string. `1` for present, `0` for nil:
  # descending order places `1 > 0` first, so real strings win the tie and
  # a nil-vs-"" comparison sees a real difference in the first element of
  # the pair.
  defp sort_key(%Log{} = r) do
    {
      r.timestamp_ns || 0,
      r.observed_timestamp_ns || 0,
      presence_pair(r.trace_id),
      presence_pair(r.span_id),
      presence_pair(r.body)
    }
  end

  defp presence_pair(nil), do: {0, ""}
  defp presence_pair(value) when is_binary(value), do: {1, value}

  defp compare_desc(a, b), do: a >= b
end
