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
      prefix (e.g. `00000...20-00000...30-idem-<hash>.parquet`), never
      the full path. Two reasons: it halves manifest bytes, and it makes
      it impossible for a hand-crafted manifest to reference an object
      outside its own tenant prefix.

  ## Compaction

    * `merge/2` takes an existing manifest and a batch of newly-written
      segments and returns a new manifest with them merged in. The merge
      is a single pass through both lists (both sorted by `max_ts` desc)
      producing a sorted output — O(n) rather than O(n log n).
  """

  alias Pulso.Storage.S3.Manifest.Retirement
  alias Pulso.Storage.S3.Manifest.Segment
  alias Pulso.Storage.S3.PagedManifest

  @schema_version 1
  @name_dictionary_bytes 65_536

  # `tenant` and `signal` are not serialized — they are derivable from
  # the manifest object's own key. Keeping them off the wire keeps the
  # payload smaller and forecloses a class of tenant-mixing bugs.
  defstruct version: @schema_version, segments: [], retired: %{}, cleanup_cursor: nil, paging: nil

  @type t :: %__MODULE__{
          version: non_neg_integer(),
          segments: [Segment.t()],
          retired: %{String.t() => Retirement.t()},
          cleanup_cursor: String.t() | nil,
          paging: map() | nil
        }

  @doc "Path of the manifest object for one `(tenant, signal)`."
  @spec manifest_key(String.t(), String.t()) :: String.t()
  def manifest_key(tenant, signal \\ "logs") when is_binary(tenant) and is_binary(signal) do
    "tenants/#{tenant}/v4/signal=#{signal}/manifest.json"
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
  def encode(%__MODULE__{version: version, segments: segments, retired: retired, cleanup_cursor: cursor} = manifest) do
    {wire_segments, names, labels} = encode_segments(segments)

    wire = %{
      "v" => version,
      "s" => wire_segments,
      "retired" => Map.new(retired, fn {key, retirement} -> {key, Retirement.to_wire(retirement)} end),
      "cleanup_cursor" => cursor
    }

    wire = if version == 3, do: Map.put(wire, "p", manifest.paging), else: wire
    wire = if names == [], do: wire, else: Map.put(wire, "names", names)
    wire = if labels == [], do: wire, else: Map.put(wire, "labels", labels)
    json = Pulso.JSON.encode_to_iodata!(wire)

    if version == 3 do
      # Verify exact payload bytes, independent of OTP term encoding or JSON
      # map iteration order across reader/writer upgrades.
      ["{\"root_sha\":\"", root_digest(json), "\",\"payload\":", json, "}"]
    else
      json
    end
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
    with {:ok, payload, protected?} <- unwrap_root(binary),
         {:ok, %{"v" => version, "s" => segments} = wire} <- safe_decode(payload),
         :ok <- validate_version(version),
         :ok <- validate_fields(version, wire),
         {:ok, parsed} <- decode_segments(segments, name_dictionary(wire["names"]), name_dictionary(wire["labels"])),
         {:ok, retired} <- decode_retired(Map.get(wire, "retired", %{})),
         {:ok, cursor} <- decode_cursor(Map.get(wire, "cleanup_cursor")),
         :ok <- validate_paging(version, wire["p"]),
         :ok <- validate_root_bounds(version, parsed, byte_size(binary)),
         :ok <- validate_root_digest(version, protected?) do
      {:ok,
       %__MODULE__{
         version: version,
         segments: sort_by_max_ts_desc(parsed),
         retired: retired,
         cleanup_cursor: cursor,
         paging: wire["p"]
       }}
    else
      {:ok, _malformed} -> {:error, :invalid_manifest}
      {:error, _} = err -> err
    end
  end

  # Tombstones remain after deletion: a delayed idempotent ingest retry must
  # never reintroduce records that already live in a replacement segment.
  defp validate_version(version) when version in [1, 2, 3], do: :ok
  defp validate_version(_), do: {:error, :unsupported_manifest_version}

  defp validate_paging(3, paging), do: PagedManifest.validate(paging)
  defp validate_paging(_, nil), do: :ok
  defp validate_paging(_, _), do: {:error, :invalid_manifest}

  defp validate_fields(1, _wire), do: :ok
  defp validate_fields(2, %{"retired" => _}), do: :ok
  defp validate_fields(3, %{"retired" => retired, "p" => _}) when is_map(retired) and map_size(retired) == 0, do: :ok
  defp validate_fields(_, _wire), do: {:error, :invalid_manifest}

  defp root_digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)

  defp unwrap_root(<<"{\"root_sha\":\"", sha::binary-size(64), "\",\"payload\":", rest::binary>>) do
    if byte_size(rest) > 0 and :binary.last(rest) == ?} do
      payload = binary_part(rest, 0, byte_size(rest) - 1)
      if root_digest(payload) == sha, do: {:ok, payload, true}, else: {:error, :invalid_manifest}
    else
      {:error, :invalid_manifest}
    end
  end

  defp unwrap_root(bytes), do: {:ok, bytes, false}
  defp validate_root_digest(3, true), do: :ok
  defp validate_root_digest(3, _), do: {:error, :invalid_manifest}
  defp validate_root_digest(_, false), do: :ok
  defp validate_root_digest(_, _), do: {:error, :invalid_manifest}

  defp validate_root_bounds(3, segments, bytes) do
    tail_bytes = segments |> Enum.map(&Segment.to_wire/1) |> Pulso.JSON.encode_to_iodata!() |> IO.iodata_length()

    if bytes <= 524_288 and length(segments) <= 256 and tail_bytes <= 65_536,
      do: :ok,
      else: {:error, :retention_capacity}
  end

  defp validate_root_bounds(_, _, _), do: :ok

  defp decode_cursor(cursor) when is_nil(cursor) or is_binary(cursor), do: {:ok, cursor}
  defp decode_cursor(_), do: {:error, :invalid_manifest}

  defp decode_retired(retired) when is_map(retired) do
    Enum.reduce_while(retired, {:ok, %{}}, fn {key, wire}, {:ok, acc} ->
      case Retirement.from_wire(wire) do
        {:ok, retirement} when is_binary(key) -> {:cont, {:ok, Map.put(acc, key, retirement)}}
        _ -> {:halt, {:error, :invalid_manifest}}
      end
    end)
  end

  defp decode_retired(_), do: {:error, :invalid_manifest}

  @doc "Mark successful deletions only if no ingest retry changed their revision."
  def mark_deleted(manifest, deleted, cursor) do
    retired =
      Enum.reduce(deleted, manifest.retired, fn {key, observed}, acc ->
        if Map.get(acc, key) == observed, do: Map.put(acc, key, %{observed | deleted?: true}), else: acc
      end)

    %{manifest | retired: retired, cleanup_cursor: cursor}
  end

  @doc "Replace active sources atomically, retaining durable deletion deadlines."
  @spec replace(t(), [String.t()], Segment.t(), non_neg_integer()) :: {:ok, t()} | {:error, term()}
  def replace(manifest, source_keys, replacement, delete_after) do
    sources = MapSet.new(source_keys)
    active = MapSet.new(manifest.segments, & &1.key)

    if MapSet.size(sources) >= 2 and MapSet.subset?(sources, active) and
         not MapSet.member?(active, replacement.key) and
         not Map.has_key?(manifest.retired, replacement.key) do
      retired = Enum.reduce(source_keys, manifest.retired, &Map.put(&2, &1, Retirement.new(delete_after)))
      remaining = Enum.reject(manifest.segments, &MapSet.member?(sources, &1.key))
      {:ok, merge(%{manifest | version: 2, segments: remaining, retired: retired}, [replacement])}
    else
      {:error, :compaction_conflict}
    end
  end

  defp safe_decode(binary) do
    {:ok, Pulso.JSON.decode!(binary)}
  rescue
    e -> {:error, {:decode_failed, e}}
  end

  defp decode_segments(list, dictionary, labels) when is_list(list) do
    Enum.reduce_while(list, {:ok, []}, fn wire, {:ok, acc} ->
      wire = wire |> expand_names(dictionary) |> expand_labels(labels)

      case Segment.from_wire(wire) do
        {:ok, segment} -> {:cont, {:ok, [segment | acc]}}
        {:error, _} = err -> {:halt, err}
      end
    end)
  end

  defp decode_segments(_, _, _), do: {:error, :invalid_manifest}

  # Exact name sets repeat across ingest batches. One dictionary entry per set
  # keeps that repetition off the hot manifest, with a hard byte budget for
  # distinct sets. Omitted sets remain unknown, so budget exhaustion is safe.
  defp encode_segments(segments) do
    state = %{ids: %{}, names: [], bytes: 2, segments: [], label_ids: %{}, labels: [], label_bytes: 2}
    state = Enum.reduce(segments, state, &encode_segment/2)
    {Enum.reverse(state.segments), Enum.reverse(state.names), Enum.reverse(state.labels)}
  end

  defp encode_segment(segment, state) do
    wire = Segment.to_wire(segment) |> Map.drop(["n", "l"])
    {name_id, state} = dictionary_id(segment.metric_names, state)
    {label_id, state} = label_dictionary_id(segment.metric_labels, state)
    wire = if is_nil(name_id), do: wire, else: Map.put(wire, "ni", name_id)
    wire = if is_nil(label_id), do: wire, else: Map.put(wire, "li", label_id)
    %{state | segments: [wire | state.segments]}
  end

  defp dictionary_id(nil, state), do: {nil, state}

  defp dictionary_id(names, state) do
    case Map.fetch(state.ids, names) do
      {:ok, id} -> {id, state}
      :error -> add_name_set(names, state)
    end
  end

  defp add_name_set(names, state) do
    bytes = names |> Pulso.JSON.encode_to_iodata!() |> IO.iodata_length()

    if state.bytes + bytes + 1 <= @name_dictionary_bytes do
      id = map_size(state.ids)
      {id, %{state | ids: Map.put(state.ids, names, id), names: [names | state.names], bytes: state.bytes + bytes + 1}}
    else
      {nil, state}
    end
  end

  defp label_dictionary_id(nil, state), do: {nil, state}

  defp label_dictionary_id(labels, state) do
    case Map.fetch(state.label_ids, labels) do
      {:ok, id} ->
        {id, state}

      :error ->
        bytes = IO.iodata_length(Pulso.JSON.encode_to_iodata!(labels))

        if state.label_bytes + bytes + 1 <= @name_dictionary_bytes do
          id = map_size(state.label_ids)

          {id,
           %{
             state
             | label_ids: Map.put(state.label_ids, labels, id),
               labels: [labels | state.labels],
               label_bytes: state.label_bytes + bytes + 1
           }}
        else
          {nil, state}
        end
    end
  end

  defp expand_labels(%{"li" => id} = wire, dictionary) when is_integer(id) and id >= 0 and id < tuple_size(dictionary),
    do: Map.put(wire, "l", elem(dictionary, id))

  defp expand_labels(%{"li" => _} = wire, _), do: Map.put(wire, "l", nil)
  defp expand_labels(wire, _), do: wire

  defp name_dictionary(names) when is_list(names), do: List.to_tuple(names)
  defp name_dictionary(_), do: {}

  defp expand_names(%{"ni" => id} = wire, dictionary) when is_integer(id) and id >= 0 and id < tuple_size(dictionary),
    do: Map.put(wire, "n", elem(dictionary, id))

  defp expand_names(%{"ni" => _} = wire, _), do: Map.put(wire, "n", nil)
  defp expand_names(wire, _), do: wire

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
    retired = Enum.reduce(Enum.uniq_by(new_segments, & &1.key), manifest.retired, &retry_retired/2)
    deduped = new_segments |> Enum.reject(&Map.has_key?(manifest.retired, &1.key)) |> Enum.uniq_by(& &1.key)
    new_sorted = sort_by_max_ts_desc(deduped)
    merged = merge_sorted(new_sorted, existing, [], MapSet.new(Enum.map(new_sorted, & &1.key)))
    %{manifest | segments: merged, retired: retired}
  end

  defp retry_retired(segment, retired) do
    case Map.fetch(retired, segment.key) do
      {:ok, retirement} -> Map.put(retired, segment.key, Retirement.retried(retirement))
      :error -> retired
    end
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
