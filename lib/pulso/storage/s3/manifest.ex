defmodule Pulso.Storage.S3.Manifest do
  @moduledoc """
  Per-tenant, per-signal manifest data model.

  A manifest lists the segments that currently exist for one
  `(tenant, signal)` — each with its full time bounds and a small
  summary. Query time uses those bounds to prune segments that cannot
  match, so the vast majority of segment reads never happen.

  The wire form is JSON per `docs/architecture.md`. Every field name is
  short (`v`, `s`, `k`, `mn`, `mx`, `r`, `b`) so a manifest with thousands
  of segments stays small on the wire, and every decode is a single
  linear pass over the received bytes.

  ## In-memory invariants

    * `segments` is kept sorted by `max_ts` **descending**. The query
      pruner walks it in order and stops as soon as the accumulated
      k-th largest timestamp beats every remaining segment's `max_ts`.
      Pre-sorting on load means each query pays O(n) filter, not
      O(n log n) sort.
    * A segment's `key` on the wire is the tail after the tenant/signal
      prefix (e.g. `00000...20-00000...30-idem-<hash>.ndjson`), never
      the full path. Two reasons: it halves manifest bytes, and it makes
      it impossible for a hand-crafted manifest to reference an object
      outside its own tenant prefix.

  ## Compaction

    * `merge/2` takes an existing manifest and a batch of newly-written
      segments and returns a new manifest with them merged in. The merge
      is a single pass through both lists (both sorted by `max_ts` desc)
      producing a sorted output — O(n) rather than O(n log n).
  """

  alias Pulso.Storage.S3.Manifest.Segment

  @schema_version 1

  # `tenant` and `signal` are not serialized — they are derivable from
  # the manifest object's own key. Keeping them off the wire keeps the
  # payload smaller and forecloses a class of tenant-mixing bugs.
  defstruct version: @schema_version, segments: []

  @type t :: %__MODULE__{
          version: non_neg_integer(),
          segments: [Segment.t()]
        }

  @doc "Path of the manifest object for one `(tenant, signal)`."
  @spec manifest_key(String.t(), String.t()) :: String.t()
  def manifest_key(tenant, signal \\ "logs") when is_binary(tenant) and is_binary(signal) do
    "tenants/#{tenant}/v2/#{signal}/manifest.json"
  end

  @doc "An empty manifest — the shape a first-time `put_if_none_match` uploads."
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc """
  Encode a manifest to iodata for transport.

  Callers collapse to a binary with `IO.iodata_to_binary/1` only at the
  NIF boundary — the intermediate steps stay as iodata to avoid extra
  per-segment binary allocations.
  """
  @spec encode(t()) :: iodata()
  def encode(%__MODULE__{version: version, segments: segments}) do
    JSON.encode_to_iodata!(%{
      "v" => version,
      "s" => Enum.map(segments, &Segment.to_wire/1)
    })
  end

  @doc """
  Decode a manifest from its wire form.

  Guarantees the returned struct maintains the sorted invariant even
  if the wire form was out of order (belt and braces — a well-behaved
  writer produces sorted output, and a hostile one still gets sorted
  before we trust the invariant).
  """
  @spec decode(binary()) :: {:ok, t()} | {:error, term()}
  def decode(binary) when is_binary(binary) do
    with {:ok, %{"v" => version, "s" => segments}} <- safe_decode(binary),
         {:ok, parsed} <- decode_segments(segments) do
      {:ok,
       %__MODULE__{
         version: version,
         segments: sort_by_max_ts_desc(parsed)
       }}
    else
      {:ok, _malformed} -> {:error, :invalid_manifest}
      {:error, _} = err -> err
    end
  end

  defp safe_decode(binary) do
    {:ok, JSON.decode!(binary)}
  rescue
    e -> {:error, {:decode_failed, e}}
  end

  defp decode_segments(list) when is_list(list) do
    Enum.reduce_while(list, {:ok, []}, fn wire, {:ok, acc} ->
      case Segment.from_wire(wire) do
        {:ok, segment} -> {:cont, {:ok, [segment | acc]}}
        {:error, _} = err -> {:halt, err}
      end
    end)
  end

  defp decode_segments(_), do: {:error, :invalid_manifest}

  @doc """
  Merge a batch of newly-written segments into an existing manifest.

  Both inputs are sorted by `max_ts` desc; the output preserves that
  invariant with an O(n) merge. Duplicate keys (a retry that already
  landed in the manifest) collapse to a single entry, preferring the
  incoming segment — its bounds and row_count are what the caller has
  in hand, and any drift from the previous entry would be silent
  otherwise.
  """
  @spec merge(t(), [Segment.t()]) :: t()
  def merge(%__MODULE__{} = manifest, []), do: manifest

  def merge(%__MODULE__{segments: existing} = manifest, new_segments) when is_list(new_segments) do
    # Dedup the incoming batch by key BEFORE the merge. Same-key
    # duplicates arise on the retry-idempotency path: two `register_segments`
    # calls carrying the same idempotency key produce two `%Segment{}`
    # values with identical S3 keys. Without this dedup they would BOTH
    # land in the merged manifest — one S3 object referenced twice — so
    # a later `query/2` would fetch that object twice and return each of
    # its records twice. `Enum.uniq_by/2` keeps the first occurrence,
    # which is fine here because the duplicates are byte-identical.
    deduped = Enum.uniq_by(new_segments, & &1.key)
    new_sorted = sort_by_max_ts_desc(deduped)
    merged = merge_sorted(new_sorted, existing, [], MapSet.new(Enum.map(new_sorted, & &1.key)))
    %{manifest | segments: merged}
  end

  # `new` overrides `existing` on key collision — MapSet holds the keys
  # in `new`, so any element in `existing` with a matching key is skipped
  # rather than appended a second time.
  defp merge_sorted([], rest, acc, new_keys) do
    remaining = Enum.reject(rest, &MapSet.member?(new_keys, &1.key))
    Enum.reverse(acc, remaining)
  end

  defp merge_sorted(new, [], acc, _new_keys) do
    Enum.reverse(acc, new)
  end

  defp merge_sorted([n | rest_n] = new, [e | rest_e], acc, new_keys) do
    cond do
      MapSet.member?(new_keys, e.key) ->
        # `e` is superseded by the corresponding entry in `new`. Drop it
        # and keep merging.
        merge_sorted(new, rest_e, acc, new_keys)

      n.max_ts >= e.max_ts ->
        merge_sorted(rest_n, [e | rest_e], [n | acc], new_keys)

      true ->
        merge_sorted(new, rest_e, [e | acc], new_keys)
    end
  end

  @doc """
  Prune segments to those overlapping `[start_ts, end_ts]`.

  A nil bound is unbounded on that side. Segments with `nil` min/max
  (never produced by this codebase but possible in a hand-crafted or
  migrated manifest) are kept — the safe default.
  """
  @spec prune_by_time(t(), non_neg_integer() | nil, non_neg_integer() | nil) :: [Segment.t()]
  def prune_by_time(%__MODULE__{segments: segments}, nil, nil), do: segments

  def prune_by_time(%__MODULE__{segments: segments}, start_ts, end_ts) do
    Enum.filter(segments, &Segment.intersects?(&1, start_ts, end_ts))
  end

  defp sort_by_max_ts_desc(segments) do
    Enum.sort_by(segments, &segment_max_ts_for_sort/1, :desc)
  end

  # nil floats to the head — the safe default. Elixir's term order already
  # puts atoms greater than integers so a bare `Enum.sort_by(&(&1.max_ts))`
  # would do the same, but pinning it in a helper keeps the ordering
  # invariant explicit at the point it matters.
  defp segment_max_ts_for_sort(%{max_ts: nil}), do: :infinity
  defp segment_max_ts_for_sort(%{max_ts: ts}), do: ts
end
