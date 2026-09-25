defmodule Pulso.Storage.S3.Manifest.Segment do
  @moduledoc """
  One row inside a manifest — the identity and summary metadata of a
  segment that has been PUT to S3.

  `key` is the FULL S3 object key (`tenants/<tenant>/v2/logs/<tail>`),
  not the tail alone. The wire form uses only the tail, and
  `from_wire/1` requires the containing tenant + signal to reconstitute
  the full key. Keeping the full key in memory means the query path can
  hand it straight to `ObjectStore.get/2` without a per-segment format
  operation.
  """

  defstruct [:key, :min_ts, :max_ts, :row_count, :byte_size]

  @type t :: %__MODULE__{
          key: String.t(),
          min_ts: non_neg_integer() | nil,
          max_ts: non_neg_integer() | nil,
          row_count: non_neg_integer() | nil,
          byte_size: non_neg_integer() | nil
        }

  @doc """
  Build a segment record for a segment the ingester just wrote.

  `tail` is the object key with the leading `tenants/<tenant>/v2/<signal>/`
  stripped. The struct holds the full key so the query path never has to
  re-glue the prefix.
  """
  @spec build(String.t(), non_neg_integer(), non_neg_integer(), non_neg_integer()) :: t()
  def build(key, min_ts, max_ts, row_count)
      when is_binary(key) and is_integer(min_ts) and is_integer(max_ts) and is_integer(row_count) do
    %__MODULE__{
      key: key,
      min_ts: min_ts,
      max_ts: max_ts,
      row_count: row_count,
      byte_size: nil
    }
  end

  @spec build(
          String.t(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer() | nil
        ) :: t()
  def build(key, min_ts, max_ts, row_count, byte_size)
      when is_binary(key) and is_integer(min_ts) and is_integer(max_ts) and is_integer(row_count) do
    %__MODULE__{
      key: key,
      min_ts: min_ts,
      max_ts: max_ts,
      row_count: row_count,
      byte_size: byte_size
    }
  end

  @doc """
  Reduce a segment to its compact wire form: short keys, the segment's
  key relative to its containing tenant/signal prefix.
  """
  @spec to_wire(t()) :: map()
  def to_wire(%__MODULE__{} = segment) do
    base = %{
      "k" => segment.key,
      "mn" => segment.min_ts,
      "mx" => segment.max_ts,
      "r" => segment.row_count
    }

    if segment.byte_size, do: Map.put(base, "b", segment.byte_size), else: base
  end

  @doc """
  Parse one wire-form segment. Returns `{:error, :invalid_segment}` on
  a shape that the writer would never produce, so a garbled manifest
  fails loudly rather than silently mispruning a query.
  """
  @spec from_wire(map()) :: {:ok, t()} | {:error, :invalid_segment}
  def from_wire(%{"k" => key, "mn" => min_ts, "mx" => max_ts} = wire)
      when is_binary(key) and is_integer(min_ts) and is_integer(max_ts) do
    row_count = valid_non_neg(wire["r"])
    byte_size = valid_non_neg(wire["b"])

    {:ok,
     %__MODULE__{
       key: key,
       min_ts: min_ts,
       max_ts: max_ts,
       row_count: row_count,
       byte_size: byte_size
     }}
  end

  def from_wire(_), do: {:error, :invalid_segment}

  @doc """
  Does this segment's `[min_ts, max_ts]` intersect `[start_ts, end_ts]`?
  A nil bound on either side is treated as unbounded on that side.
  Segments whose own bounds are nil are kept — the query cannot safely
  skip them without knowing their real range.
  """
  @spec intersects?(t(), non_neg_integer() | nil, non_neg_integer() | nil) :: boolean()
  def intersects?(%__MODULE__{min_ts: nil}, _start_ts, _end_ts), do: true
  def intersects?(%__MODULE__{max_ts: nil}, _start_ts, _end_ts), do: true

  def intersects?(%__MODULE__{min_ts: min_ts, max_ts: max_ts}, start_ts, end_ts) do
    (start_ts == nil or max_ts >= start_ts) and (end_ts == nil or min_ts <= end_ts)
  end

  defp valid_non_neg(v) when is_integer(v) and v >= 0, do: v
  defp valid_non_neg(_), do: nil
end
