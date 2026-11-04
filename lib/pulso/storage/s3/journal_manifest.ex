defmodule Pulso.Storage.S3.JournalManifest do
  @moduledoc """
  Format-4 manifests: a bounded root for prefixes without retention.

  A flat manifest lists every segment ever published and is rewritten whole
  on every publication, so ingest cost and node memory grow with history.
  Format 4 keeps the conditionally written root bounded instead:

    * an inline tail of recently published segments and retirements. Once it
      exceeds 512 entries or 128 KiB it is spilled into immutable leaf pages;
    * references to those pages, oldest first, at most 15 per tree height:
      every 16 references of one height are packed into an immutable index
      page one height up, so the root holds O(log n) references;
    * superseded pages awaiting deletion, inline or in immutable gc pages.

  Pages are written before the root that references them and are never
  modified; each carries a random nonce, so a page key is never reused. A
  reference records its subtree's active time range, the range of `max_ts`
  over every entry (for key lookup), the earliest pending retirement deadline
  (for cleanup) and, for trace leaves, a pruning summary.

  Appends only touch the tail, so their cost does not depend on history.
  Compaction and its cleanup rewrite the path from a leaf to the root and
  record the superseded pages, which cleanup deletes after a reader grace.
  Leaf pages are flat (format-2) manifest documents.
  """

  alias Pulso.ObjectStore
  alias Pulso.Storage.S3.Manifest
  alias Pulso.Storage.S3.Manifest.Retirement
  alias Pulso.Storage.S3.Manifest.Segment
  alias Pulso.Storage.S3.ManifestOwner
  alias Pulso.Storage.S3.MetadataCache
  alias Pulso.Storage.S3.Retention
  alias Pulso.Storage.S3.TraceSummary

  @version 4
  @fanout 16
  @max_height 12
  @max_refs (@fanout - 1) * (@max_height + 1)
  @leaf_entries 512
  @leaf_bytes 131_072
  @leaf_read_entries 2048
  @page_limit 1_048_576
  @root_limit 1_048_576
  @root_entries 4096
  @ref_bytes 4096
  @gc_inline 256
  @gc_refs 512
  @gc_page_keys 4096
  @default_grace_ms 3_600_000
  @memo {__MODULE__, :pages}

  def version, do: @version
  def root_limit, do: @root_limit
  def root_entries, do: @root_entries

  def nonce, do: Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
  def scope(tenant, signal), do: "tenants/#{tenant}/v4/signal=#{signal}/"
  def prefix(tenant, signal), do: scope(tenant, signal) <> "journal/"

  @doc "Whether new and flat roots of `signal` are written in this format."
  def enabled?(config, signal),
    do: Map.get(config, :manifest_format, "paged") == "paged" and not Retention.enforcing?(config, signal)

  @doc "An empty format-4 root."
  def new do
    %Manifest{
      version: @version,
      segments: [],
      retired: %{},
      paging: %{"nonce" => nonce(), "refs" => [], "g" => [], "gr" => [], "cc" => 0}
    }
  end

  @doc "Active segments referenced by a root, including its pages."
  def referenced(%Manifest{version: @version} = root),
    do: length(root.segments) + Enum.sum_by(root.paging["refs"], & &1["n"])

  def referenced(%Manifest{} = root), do: length(root.segments)

  @doc "Whether retired objects or superseded pages may still need deleting."
  def cleanup_pending?(%Manifest{version: @version, paging: p} = root) do
    map_size(root.retired) > 0 or p["g"] != [] or p["gr"] != [] or Enum.any?(p["refs"], &is_integer(&1["rd"]))
  end

  def cleanup_pending?(%Manifest{} = root), do: map_size(root.retired) > 0

  # ---- validation ------------------------------------------------------------

  def validate(%{"nonce" => n, "refs" => refs, "g" => g, "gr" => gr, "cc" => cc} = p) do
    valid =
      hex?(n, 32) and non_negative?(cc) and valid_cursor?(p["rc"]) and
        bounded_list?(refs, @max_refs, &valid_ref?/1) and heights_ordered?(refs) and
        bounded_list?(g, @gc_inline, &valid_gc_entry?/1) and bounded_list?(gr, @gc_refs, &valid_gc_ref?/1)

    if valid, do: :ok, else: {:error, :invalid_manifest}
  end

  def validate(_), do: {:error, :invalid_manifest}

  defp valid_cursor?(cursor), do: cursor == nil or (is_binary(cursor) and byte_size(cursor) <= 1024)
  defp bounded_list?(list, limit, valid?), do: is_list(list) and length(list) <= limit and Enum.all?(list, valid?)

  defp valid_ref?(%{"k" => k, "b" => b, "sha" => sha, "h" => h} = ref) do
    is_integer(h) and h in 0..@max_height and valid_key?(k, sha, kind(h)) and is_integer(b) and b in 1..@page_limit and
      valid_counts?(ref) and valid_key_range?(ref) and valid_range?(ref) and valid_summary?(ref)
  end

  defp valid_ref?(_), do: false

  defp valid_counts?(%{"n" => n, "e" => e, "l" => l}),
    do: non_negative?(n) and is_integer(e) and e >= n and e > 0 and is_integer(l) and l >= 1

  defp valid_counts?(_), do: false

  defp valid_key_range?(%{"kn" => kn, "kx" => kx}), do: is_integer(kn) and is_integer(kx) and kn <= kx
  defp valid_key_range?(_), do: false

  defp valid_summary?(ref), do: (ref["rd"] == nil or non_negative?(ref["rd"])) and (ref["q"] == nil or is_map(ref["q"]))

  defp valid_range?(%{"n" => 0} = ref), do: ref["mn"] == nil and ref["mx"] == nil
  defp valid_range?(ref), do: is_integer(ref["mn"]) and is_integer(ref["mx"]) and ref["mn"] <= ref["mx"]

  defp valid_key?(key, sha, kind) do
    hex?(sha, 64) and is_binary(key) and byte_size(key) <= 1024 and
      String.ends_with?(key, "/journal/" <> sha <> "-" <> kind <> ".json")
  end

  defp kind(0), do: "leaf"
  defp kind(_), do: "index"

  defp heights_ordered?(refs) do
    refs |> Enum.map(& &1["h"]) |> Enum.chunk_every(2, 1, :discard) |> Enum.all?(fn [a, b] -> a >= b end)
  end

  defp valid_gc_entry?([key, deadline]), do: is_binary(key) and byte_size(key) <= 1024 and non_negative?(deadline)
  defp valid_gc_entry?(_), do: false

  defp valid_gc_ref?(%{"k" => k, "b" => b, "sha" => sha, "d" => d, "c" => c}),
    do: valid_key?(k, sha, "gc") and is_integer(b) and b in 1..@page_limit and non_negative?(d) and non_negative?(c)

  defp valid_gc_ref?(_), do: false

  defp hex?(value, size), do: is_binary(value) and byte_size(value) == size and Regex.match?(~r/\A[0-9a-f]+\z/, value)
  defp non_negative?(value), do: is_integer(value) and value >= 0

  @doc "Scope every key a decoded root references to its own tenant and signal."
  def validate_context(%Manifest{version: @version} = root, tenant, signal),
    do: with_ops(fn -> do_validate_context(root, tenant, signal) end)

  defp do_validate_context(root, tenant, signal) do
    Enum.each(root.segments, &validate_segment!(&1, tenant, signal))
    Enum.each(Map.keys(root.retired), &validate_retired_key!(&1, tenant, signal))
    prefix = prefix(tenant, signal)
    keys = Enum.map(root.paging["refs"] ++ root.paging["gr"], & &1["k"]) ++ Enum.map(root.paging["g"], &hd/1)
    if not Enum.all?(keys, &page_key?(&1, prefix)), do: fail(:invalid_manifest)
    :ok
  end

  defp page_key?(key, prefix) do
    String.starts_with?(key, prefix) and
      not String.contains?(binary_part(key, byte_size(prefix), byte_size(key) - byte_size(prefix)), "/")
  end

  @doc "Refuse to publish a root readers would reject."
  def bounded(%Manifest{version: @version} = root), do: with_ops(fn -> do_bounded(root) end)

  defp do_bounded(root) do
    if IO.iodata_length(Manifest.encode(root)) > @root_limit or length(root.segments) > @root_entries,
      do: fail(:manifest_capacity)

    root
  end

  # ---- conversion ----------------------------------------------------------

  @doc """
  Convert a flat root: every segment and retirement moves into leaf pages,
  written concurrently, packed bottom-up. The flat root stays authoritative
  until the caller's conditional write replaces it.
  """
  def convert(%Manifest{version: version} = flat, tenant, signal, config) when version in [1, 2],
    do: with_ops(fn -> do_convert(flat, tenant, signal, config) end)

  defp do_convert(flat, tenant, signal, config) do
    Enum.each(flat.segments, &validate_segment!(&1, tenant, signal))
    Enum.each(Map.keys(flat.retired), &validate_retired_key!(&1, tenant, signal))
    root = new()

    # A small history stays inline; sealing spills it once it outgrows the tail.
    if length(flat.segments) + map_size(flat.retired) <= @leaf_entries do
      %{root | segments: flat.segments, retired: flat.retired}
    else
      leaves = flat |> items() |> chunk() |> put_leaves(tenant, signal, config)
      %{root | paging: %{root.paging | "refs" => build(leaves, tenant, signal, config)}}
    end
  end

  @doc "Read every page into one flat format-2 manifest, bounded by `max_bytes` of pages."
  def materialize(%Manifest{version: @version} = root, tenant, signal, config, max_bytes),
    do: with_ops(fn -> do_materialize(root, tenant, signal, config, max_bytes) end)

  defp do_materialize(root, tenant, signal, config, max_bytes) do
    leaves = collect_leaves(root.paging["refs"], fn _ -> true end, tenant, signal, config, nil)
    if Enum.sum_by(leaves, & &1["b"]) > max_bytes, do: fail(:retention_capacity)

    pages = read_leaves(leaves, tenant, signal, config, nil, & &1)

    %Manifest{
      version: 2,
      segments: Enum.sort_by(root.segments ++ Enum.flat_map(pages, & &1.segments), & &1.max_ts, :desc),
      retired: Enum.reduce(pages, root.retired, &Map.merge(&2, &1.retired))
    }
  end

  # ---- publication ---------------------------------------------------------

  @doc """
  Add newly uploaded segments to the tail. Keyed (deterministic) keys are
  looked up in the pages first: an active copy is already published and a
  retired one only rearms its cleanup, as for flat roots.
  """
  def register(%Manifest{version: @version} = root, segments, tenant, signal, config),
    do: with_ops(fn -> do_register(root, segments, tenant, signal, config) end)

  defp do_register(root, segments, tenant, signal, config) do
    segments = Enum.uniq_by(segments, & &1.key)
    Enum.each(segments, &validate_segment!(&1, tenant, signal))
    tail = MapSet.new(root.segments, & &1.key)

    {root, fresh} =
      Enum.reduce(segments, {root, []}, fn segment, {root, fresh} ->
        if MapSet.member?(tail, segment.key) or Map.has_key?(root.retired, segment.key) or random_key?(segment.key),
          do: {root, [segment | fresh]},
          else: register_paged(root, fresh, segment, tenant, signal, config)
      end)

    root |> Manifest.merge(Enum.reverse(fresh)) |> touch()
  end

  # A deterministic key may already be published in a page: keep it active,
  # or revive a retired one.
  defp register_paged(root, fresh, segment, tenant, signal, config) do
    case find(root.paging["refs"], segment.key, segment.max_ts, tenant, signal, config) do
      :absent -> {root, [segment | fresh]}
      :active -> {root, fresh}
      {:retired, _} -> {revive(root, segment, tenant, signal, config), fresh}
    end
  end

  @doc """
  Encode a root for publication, spilling its tail into a leaf page first when
  it has outgrown the tail budget. Returns the root actually encoded.
  """
  def seal(%Manifest{version: @version} = root, tenant, signal, config, cache \\ nil),
    do: with_ops(fn -> do_seal(root, tenant, signal, config, cache) end)

  defp do_seal(root, tenant, signal, config, cache) do
    {payload, cache} = encode(root, cache)

    {root, payload, cache} =
      if spill?(root, payload) do
        root = root |> spill(tenant, signal, config) |> touch()
        {payload, cache} = encode(root, cache)
        {root, payload, cache}
      else
        {root, payload, cache}
      end

    if byte_size(payload) > @root_limit, do: fail(:manifest_capacity)
    {root, payload, cache}
  end

  defp encode(root, nil), do: {root |> Manifest.encode() |> IO.iodata_to_binary(), nil}

  defp encode(root, cache) do
    {iodata, cache} = Manifest.encode(root, cache)
    {IO.iodata_to_binary(iodata), cache}
  end

  defp spill?(root, payload) do
    entries = length(root.segments) + map_size(root.retired)

    entries > @leaf_entries or
      (entries > 0 and byte_size(payload) > @leaf_bytes and
         byte_size(payload) - IO.iodata_length(Manifest.encode(%{root | segments: [], retired: %{}})) > @leaf_bytes)
  end

  defp spill(root, tenant, signal, config) do
    leaves = root |> items() |> chunk() |> put_leaves(tenant, signal, config)
    refs = Enum.reduce(leaves, root.paging["refs"], &push(&2, &1, tenant, signal, config))
    %{root | segments: [], retired: %{}, paging: %{root.paging | "refs" => refs}}
  end

  defp touch(root), do: %{root | paging: %{root.paging | "nonce" => nonce()}}

  # Deterministic keys can be retried after publication; random ones cannot.
  defp random_key?(key), do: key |> Path.basename() |> String.contains?("-rand-")

  defp revive(root, segment, tenant, signal, config) do
    update = fn leaf ->
      case Map.fetch(leaf.retired, segment.key) do
        {:ok, retirement} -> %{leaf | retired: Map.put(leaf.retired, segment.key, Retirement.retried(retirement))}
        :error -> leaf
      end
    end

    rewrite_root(root, [segment.max_ts], update, tenant, signal, config)
  end

  # ---- lookup and query ----------------------------------------------------

  @doc "`:active`, `{:retired, retirement}` or `:absent` for a segment's key."
  def lookup(%Manifest{version: @version} = root, %Segment{key: key, max_ts: max_ts}, tenant, signal, config),
    do: with_ops(fn -> do_lookup(root, %Segment{key: key, max_ts: max_ts}, tenant, signal, config) end)

  defp do_lookup(root, %Segment{key: key, max_ts: max_ts}, tenant, signal, config) do
    cond do
      Enum.any?(root.segments, &(&1.key == key)) -> :active
      Map.has_key?(root.retired, key) -> {:retired, root.retired[key]}
      true -> find(root.paging["refs"], key, max_ts, tenant, signal, config)
    end
  end

  defp find(refs, key, max_ts, tenant, signal, config) do
    Enum.find_value(refs, :absent, fn ref ->
      if covers?(ref, max_ts), do: find_in(ref, key, max_ts, tenant, signal, config)
    end)
  end

  defp find_in(%{"h" => 0} = ref, key, _max_ts, tenant, signal, config) do
    leaf = read_leaf(ref, tenant, signal, config, deadline())

    cond do
      Enum.any?(leaf.segments, &(&1.key == key)) -> :active
      Map.has_key?(leaf.retired, key) -> {:retired, leaf.retired[key]}
      true -> nil
    end
  end

  defp find_in(ref, key, max_ts, tenant, signal, config) do
    case find(read_index(ref, tenant, signal, config, deadline()), key, max_ts, tenant, signal, config) do
      :absent -> nil
      found -> found
    end
  end

  defp covers?(ref, timestamp), do: is_integer(timestamp) and timestamp >= ref["kn"] and timestamp <= ref["kx"]

  @doc """
  Active segments that can match a query, newest `max_ts` first. Reads only
  pages whose ranges and summaries can match, eight at a time, and fails as
  soon as more candidates than the query may scan are found.
  """
  def query(%Manifest{version: @version} = root, tenant, signal, opts, config),
    do: with_ops(fn -> do_query(root, tenant, signal, opts, config) end)

  defp do_query(root, tenant, signal, opts, config) do
    start_ts = opts[:start_ts]
    end_ts = opts[:end_ts]
    maximum = query_maximum(signal, opts)
    match = fn s -> Segment.intersects?(s, start_ts, end_ts) and matches?(s, signal, opts) end
    deadline = deadline()

    select = fn ref ->
      ref["n"] > 0 and intersects?(ref, start_ts, end_ts) and (ref["h"] > 0 or leaf_matches?(ref, signal, opts))
    end

    leaves = collect_leaves(root.paging["refs"], select, tenant, signal, config, deadline)
    found = Enum.filter(root.segments, match)

    read = &stream_leaves(&1, tenant, signal, config, deadline, fn leaf -> Enum.filter(leaf.segments, match) end)

    case opts[:log_window] do
      {order, n} when signal == "logs" and is_integer(n) -> window(leaves, found, {order, n}, read)
      _ -> all_candidates(leaves, found, maximum, read)
    end
  end

  defp all_candidates(leaves, found, maximum, read) do
    if maximum != nil and length(found) > maximum, do: too_many(maximum, length(found))

    leaves
    |> read.()
    |> Enum.reduce({found, length(found)}, fn found_in_leaf, {acc, count} ->
      count = count + length(found_in_leaf)
      if maximum != nil and count > maximum, do: too_many(maximum, count)
      {found_in_leaf ++ acc, count}
    end)
    |> elem(0)
    |> Enum.sort_by(& &1.max_ts, :desc)
  end

  defp too_many(maximum, count) do
    Pulso.QueryLimit.exceeded(:max_scan_segments, maximum, count, :candidates)
    fail(:query_scan_limit)
  end

  # The first `n` candidates in a limited log query's scan order: newest
  # `max_ts` first (backward) or oldest `min_ts` first (forward). Leaves are
  # read in the order of their bound on that key, eight at a time, and reading
  # stops once `n` candidates are held and no unread leaf can order before
  # the last of them. Candidates beyond `n` are never visited by the scan.
  defp window(leaves, found, {order, n}, read) do
    {bound, rank} =
      case order do
        :backward -> {&(-&1["mx"]), &{-&1.max_ts, &1.key}}
        :forward -> {& &1["mn"], &{&1.min_ts, &1.key}}
      end

    leaves
    |> Enum.sort_by(bound)
    |> Enum.chunk_every(8)
    |> Enum.reduce_while(found |> Enum.sort_by(rank) |> Enum.take(n), fn chunk, held ->
      chunk = if length(held) < n, do: chunk, else: Enum.filter(chunk, &(bound.(&1) <= elem(rank.(List.last(held)), 0)))

      if chunk == [] do
        {:halt, held}
      else
        {:cont, (held ++ Enum.concat(read.(chunk))) |> Enum.sort_by(rank) |> Enum.take(n)}
      end
    end)
  end

  defp limited_trace_scan?("traces", opts),
    do:
      is_nil(opts[:trace_search]) and is_nil(opts[:tempo_search]) and is_nil(opts[:tempo_discovery]) and
        is_integer(opts[:limit]) and opts[:limit] in 1..5000

  defp limited_trace_scan?(_signal, _opts), do: false

  # The same candidate ceilings the scan applies afterwards, so a query that
  # cannot succeed stops reading pages early.
  defp query_maximum(signal, opts) do
    cond do
      signal == "traces" and is_binary(TraceSummary.service_filter(opts[:tempo_search])) ->
        TraceSummary.plan_catalog_limit()

      limited_trace_scan?(signal, opts) ->
        nil

      true ->
        opts[:max_scan_segments]
    end
  end

  defp matches?(s, "metrics", opts), do: Segment.matches_metrics?(s, opts[:metrics_filter] || opts[:matchers] || [])
  defp matches?(s, "logs", opts), do: Segment.matches_log_service?(s, opts)
  defp matches?(s, "traces", opts), do: TraceSummary.matches?(s.trace_summary, opts)
  defp leaf_matches?(ref, "traces", opts), do: TraceSummary.page_matches?(ref["q"], opts)
  defp leaf_matches?(_ref, _signal, _opts), do: true

  defp intersects?(ref, start, finish),
    do: (start == nil or ref["mx"] >= start) and (finish == nil or ref["mn"] <= finish)

  # ---- compaction ------------------------------------------------------------

  @doc """
  A bounded candidate set: the tail, the four newest leaves and four older
  leaves from a rotating cursor, so compaction also reaches history. The
  returned root is for selection only and must never be published.
  """
  def candidates(%Manifest{version: @version} = root, tenant, signal, config),
    do: with_ops(fn -> do_candidates(root, tenant, signal, config) end)

  defp do_candidates(root, tenant, signal, config) do
    refs = root.paging["refs"]
    total = Enum.sum_by(refs, & &1["l"])
    cursor = if root.paging["cc"] < total, do: root.paging["cc"], else: 0
    recent = Enum.to_list(max(0, total - 4)..(total - 1)//1)
    sweep = Enum.to_list(cursor..min(total - 1, cursor + 3)//1)
    wanted = MapSet.new(recent ++ sweep)
    leaves = leaves_at(refs, 0, wanted, tenant, signal, config)
    next = if cursor + 4 >= total, do: 0, else: cursor + 4

    %{
      root
      | segments: root.segments ++ Enum.flat_map(leaves, & &1.segments),
        paging: Map.put(root.paging, "compaction_candidate", next)
    }
  end

  defp leaves_at(refs, offset, wanted, tenant, signal, config) do
    {leaves, _} =
      Enum.flat_map_reduce(refs, offset, fn ref, start ->
        finish = start + ref["l"]

        leaves =
          cond do
            not Enum.any?(wanted, &(&1 >= start and &1 < finish)) ->
              []

            ref["h"] == 0 ->
              [read_leaf(ref, tenant, signal, config, deadline())]

            true ->
              leaves_at(read_index(ref, tenant, signal, config, deadline()), start, wanted, tenant, signal, config)
          end

        {leaves, finish}
      end)

    leaves
  end

  @doc "Advance the compaction cursor recorded by `candidates/4`."
  def advance_cursor(%Manifest{version: @version} = root, next), do: %{root | paging: %{root.paging | "cc" => next}}

  @doc """
  Retire active sources and add their replacement in one root transition.
  Fails with `:compaction_conflict` unless every source is still active and
  the replacement is unknown.
  """
  def replace(%Manifest{version: @version} = root, source_keys, replacement, deadline, tenant, signal, config),
    do: with_ops(fn -> do_replace(root, source_keys, replacement, deadline, tenant, signal, config) end)

  defp do_replace(root, source_keys, replacement, deadline, tenant, signal, config) do
    keys = Enum.uniq(source_keys)
    validate_segment!(replacement, tenant, signal)
    if length(keys) < 2, do: fail(:compaction_conflict)

    if lookup!(root, replacement.key, replacement.max_ts, tenant, signal, config) != :absent,
      do: fail(:compaction_conflict)

    tail = MapSet.new(root.segments, & &1.key)
    {inline, paged} = Enum.split_with(keys, &MapSet.member?(tail, &1))

    Enum.each(paged, fn key ->
      if find(root.paging["refs"], key, key_max_ts!(key), tenant, signal, config) != :active,
        do: fail(:compaction_conflict)
    end)

    inline_set = MapSet.new(inline)

    root = %{
      root
      | segments: Enum.reject(root.segments, &MapSet.member?(inline_set, &1.key)),
        retired: Enum.reduce(inline, root.retired, &Map.put(&2, &1, Retirement.new(deadline)))
    }

    root = retire_paged(root, paged, deadline, tenant, signal, config)
    root |> Manifest.merge([replacement]) |> touch()
  end

  defp lookup!(root, key, max_ts, tenant, signal, config) do
    cond do
      Enum.any?(root.segments, &(&1.key == key)) -> :active
      Map.has_key?(root.retired, key) -> {:retired, root.retired[key]}
      true -> find(root.paging["refs"], key, max_ts, tenant, signal, config)
    end
  end

  defp retire_paged(root, [], _deadline, _tenant, _signal, _config), do: root

  defp retire_paged(root, keys, deadline, tenant, signal, config) do
    targets = MapSet.new(keys)

    update = fn leaf ->
      case Enum.split_with(leaf.segments, &MapSet.member?(targets, &1.key)) do
        {[], _} ->
          leaf

        {gone, kept} ->
          %{
            leaf
            | segments: kept,
              retired: Enum.reduce(gone, leaf.retired, &Map.put(&2, &1.key, Retirement.new(deadline)))
          }
      end
    end

    rewrite_root(root, Enum.map(keys, &key_max_ts!/1), update, tenant, signal, config, deadline)
  end

  # ---- cleanup -------------------------------------------------------------

  @doc """
  Due work for one cleanup pass: up to `maximum` retired segments, due inline
  superseded pages, and up to two due gc pages with the keys they list.
  Retired segments rotate in key order after the last pass's cursor, among
  the first `4 * maximum` due entries found, so a key that cannot be deleted
  does not block the others.
  """
  def cleanup_plan(%Manifest{version: @version} = root, now, maximum, tenant, signal, config),
    do: with_ops(fn -> do_cleanup_plan(root, now, maximum, tenant, signal, config) end)

  defp do_cleanup_plan(root, now, maximum, tenant, signal, config) do
    window = 4 * maximum
    inline = for {key, r} <- root.retired, not r.deleted? and r.delete_after <= now, do: {key, r}
    inline = inline |> Enum.sort() |> Enum.take(window)
    paged = due_retired(root.paging["refs"], now, window - length(inline), tenant, signal, config)
    cursor = root.paging["rc"]
    due = Enum.sort(inline ++ paged)
    {before, after_cursor} = Enum.split_while(due, fn {key, _} -> cursor != nil and key <= cursor end)
    pages = for [key, deadline] <- root.paging["g"], deadline <= now, do: key

    gc_pages =
      root.paging["gr"]
      |> Enum.filter(&(&1["d"] <= now))
      |> Enum.take(2)
      |> Enum.map(&{&1, gc_keys(&1, tenant, signal, config)})

    %{retired: Enum.take(after_cursor ++ before, maximum), pages: Enum.take(pages, maximum), gc_pages: gc_pages}
  end

  # Cleanup deletes a gc page only after every page it lists.
  defp gc_keys(ref, tenant, signal, config) do
    read_gc(ref, tenant, signal, config)
  catch
    {:journal_manifest, :manifest_page_missing} -> []
  end

  defp due_retired(_refs, _now, remaining, _tenant, _signal, _config) when remaining <= 0, do: []

  defp due_retired(refs, now, remaining, tenant, signal, config) do
    {found, _} =
      Enum.flat_map_reduce(refs, remaining, fn ref, left ->
        if left > 0 and is_integer(ref["rd"]) and ref["rd"] <= now do
          found = due_in(ref, now, left, tenant, signal, config)
          {found, left - length(found)}
        else
          {[], left}
        end
      end)

    found
  end

  defp due_in(%{"h" => 0} = ref, now, left, tenant, signal, config) do
    leaf = read_leaf(ref, tenant, signal, config, deadline())

    leaf.retired
    |> Enum.filter(fn {_key, r} -> not r.deleted? and r.delete_after <= now end)
    |> Enum.sort()
    |> Enum.take(left)
  end

  defp due_in(ref, now, left, tenant, signal, config),
    do: due_retired(read_index(ref, tenant, signal, config, deadline()), now, left, tenant, signal, config)

  @doc """
  Record a cleanup pass against the current root: retirements whose revision
  is unchanged become deleted, deleted superseded pages are forgotten. Pages
  rewritten for this are themselves superseded.
  """
  def apply_cleanup(%Manifest{version: @version} = root, deleted, pages, gc_refs, cursor, tenant, signal, config),
    do: with_ops(fn -> do_apply_cleanup(root, deleted, pages, gc_refs, cursor, tenant, signal, config) end)

  defp do_apply_cleanup(root, deleted, pages, gc_refs, cursor, tenant, signal, config) do
    root = if cursor, do: %{root | paging: Map.put(root.paging, "rc", cursor)}, else: root
    observed = Map.new(deleted)
    mark = fn retired -> Map.new(retired, &mark_deleted(&1, observed)) end
    root = %{root | retired: mark.(root.retired)}
    paged = Enum.reject(deleted, fn {key, _} -> Map.has_key?(root.retired, key) end)

    root =
      if paged == [],
        do: root,
        else:
          rewrite_root(
            root,
            Enum.map(paged, fn {key, _} -> key_max_ts!(key) end),
            &%{&1 | retired: mark.(&1.retired)},
            tenant,
            signal,
            config
          )

    pages = MapSet.new(pages)
    gc_refs = MapSet.new(gc_refs)
    p = root.paging
    g = Enum.reject(p["g"], fn [key, _] -> MapSet.member?(pages, key) end)
    gr = Enum.reject(p["gr"], &MapSet.member?(gc_refs, &1["k"]))
    %{root | paging: %{p | "g" => g, "gr" => gr}}
  end

  defp mark_deleted({key, retirement}, observed) do
    if Map.get(observed, key) == retirement, do: {key, %{retirement | deleted?: true}}, else: {key, retirement}
  end

  # ---- tree writes -----------------------------------------------------------

  # Rewrite every leaf that can hold one of `timestamps`, copying the path to
  # the root; unchanged subtrees keep their references.
  defp rewrite_root(root, timestamps, update, tenant, signal, config, at_least \\ 0) do
    pred = fn ref -> Enum.any?(timestamps, &covers?(ref, &1)) end
    {refs, superseded} = rewrite(root.paging["refs"], pred, update, tenant, signal, config)
    root = %{root | paging: %{root.paging | "refs" => refs}}
    grace = Map.get(config, :manifest_page_grace_ms, @default_grace_ms)
    supersede(root, superseded, max(at_least, System.system_time(:millisecond) + grace), tenant, signal, config)
  end

  defp rewrite(refs, pred, update, tenant, signal, config) do
    Enum.map_reduce(refs, [], fn ref, superseded ->
      if pred.(ref),
        do: rewrite_ref(ref, superseded, pred, update, tenant, signal, config),
        else: {ref, superseded}
    end)
  end

  defp rewrite_ref(%{"h" => 0} = ref, superseded, _pred, update, tenant, signal, config) do
    leaf = read_leaf(ref, tenant, signal, config, deadline())

    case update.(leaf) do
      ^leaf -> {ref, superseded}
      updated -> {leaf_ref(updated, tenant, signal, config), [ref["k"] | superseded]}
    end
  end

  defp rewrite_ref(ref, superseded, pred, update, tenant, signal, config) do
    children = read_index(ref, tenant, signal, config, deadline())

    case rewrite(children, pred, update, tenant, signal, config) do
      {^children, []} -> {ref, superseded}
      {updated, below} -> {index_ref(updated, tenant, signal, config), [ref["k"] | below ++ superseded]}
    end
  end

  defp supersede(root, [], _deadline, _tenant, _signal, _config), do: root

  defp supersede(root, keys, deadline, tenant, signal, config) do
    p = root.paging
    g = p["g"] ++ Enum.map(keys, &[&1, deadline])

    if length(g) <= @gc_inline do
      %{root | paging: %{p | "g" => g}}
    else
      ref =
        put_page("g", Pulso.JSON.encode_to_iodata!(Enum.map(g, &hd/1)), "gc", tenant, signal, config)
        |> Map.merge(%{"d" => g |> Enum.map(&List.last/1) |> Enum.max(), "c" => length(g)})

      if length(p["gr"]) >= @gc_refs, do: fail(:manifest_gc_backlog)
      %{root | paging: %{p | "g" => [], "gr" => p["gr"] ++ [ref]}}
    end
  end

  # Entries in max_ts order, so leaves cover narrow, mostly disjoint ranges.
  defp items(%Manifest{segments: segments, retired: retired}) do
    Enum.map(segments, &{&1.max_ts, &1.key, &1}) ++
      Enum.map(retired, fn {key, retirement} -> {key_max_ts!(key), key, retirement} end)
  end

  defp chunk(items) do
    {chunks, current, _count, _bytes} =
      items
      |> Enum.sort_by(fn {ts, key, _} -> {ts, key} end)
      |> Enum.reduce({[], [], 0, 0}, fn {_ts, key, value} = item, {chunks, current, count, bytes} ->
        size = item_bytes(key, value)

        if current != [] and (count >= @leaf_entries or bytes + size > @leaf_bytes),
          do: {[Enum.reverse(current) | chunks], [item], 1, size},
          else: {chunks, [item | current], count + 1, bytes + size}
      end)

    chunks = if current == [], do: chunks, else: [Enum.reverse(current) | chunks]

    chunks
    |> Enum.reverse()
    |> Enum.map(fn chunk ->
      %Manifest{
        version: 2,
        segments: for({_, _, %Segment{} = s} <- chunk, do: s) |> Enum.sort_by(& &1.max_ts, :desc),
        retired: Map.new(for {_, key, %Retirement{} = r} <- chunk, do: {key, r})
      }
    end)
  end

  defp item_bytes(_key, %Segment{} = s),
    do: s |> Segment.to_wire() |> Pulso.JSON.encode_to_iodata!() |> IO.iodata_length()

  defp item_bytes(key, %Retirement{}), do: byte_size(key) + 48

  defp put_leaves([], _tenant, _signal, _config), do: []
  defp put_leaves([leaf], tenant, signal, config), do: [leaf_ref(leaf, tenant, signal, config)]

  defp put_leaves(leaves, tenant, signal, config), do: concurrently(leaves, &leaf_ref(&1, tenant, signal, config))

  defp concurrently(items, fun) do
    items
    |> Pulso.Runtime.Task.async_stream(
      fn item ->
        try do
          {:ok, fun.(item)}
        catch
          {:journal_manifest, reason} -> {:error, reason}
        end
      end,
      max_concurrency: 16,
      timeout: :infinity
    )
    |> Enum.map(fn
      {:ok, {:ok, value}} -> value
      {:ok, {:error, reason}} -> fail(reason)
    end)
  end

  # Append one leaf, then carry: 16 trailing references of one height become
  # one index page a height up.
  defp push(refs, ref, tenant, signal, config), do: carry(refs ++ [ref], tenant, signal, config)

  defp carry(refs, tenant, signal, config) do
    height = List.last(refs)["h"]
    run = refs |> Enum.reverse() |> Enum.take_while(&(&1["h"] == height))

    if length(run) >= @fanout do
      {rest, packed} = Enum.split(refs, length(refs) - @fanout)
      carry(rest ++ [index_ref(packed, tenant, signal, config)], tenant, signal, config)
    else
      refs
    end
  end

  # Bottom-up packing for a whole history at once: full groups of 16 move up,
  # the newest remainder stays at its height, so the result is what repeated
  # `push/5` would produce.
  defp build([], _tenant, _signal, _config), do: []

  defp build(refs, tenant, signal, config) do
    {packed, rest} = Enum.split(refs, div(length(refs), @fanout) * @fanout)

    if packed == [] do
      rest
    else
      parents = packed |> Enum.chunk_every(@fanout) |> concurrently(&index_ref(&1, tenant, signal, config))
      build(parents, tenant, signal, config) ++ rest
    end
  end

  defp leaf_ref(%Manifest{segments: segments, retired: retired} = leaf, tenant, signal, config) do
    all = Enum.map(segments, & &1.max_ts) ++ Enum.map(Map.keys(retired), &key_max_ts!/1)
    pending = for {_, r} <- retired, not r.deleted?, do: r.delete_after

    stats = %{
      "h" => 0,
      "n" => length(segments),
      "e" => length(all),
      "l" => 1,
      "mn" => segments |> Enum.map(& &1.min_ts) |> Enum.min(fn -> nil end),
      "mx" => segments |> Enum.map(& &1.max_ts) |> Enum.max(fn -> nil end),
      "kn" => Enum.min(all),
      "kx" => Enum.max(all),
      "rd" => Enum.min(pending, fn -> nil end)
    }

    stats =
      if signal == "traces" and segments != [],
        do: Map.put(stats, "q", TraceSummary.page_summary(Enum.map(segments, &Segment.to_wire/1))),
        else: stats

    "m"
    |> put_page(Manifest.encode(%{leaf | version: 2, paging: nil, cleanup_cursor: nil}), "leaf", tenant, signal, config)
    |> Map.merge(stats)
    |> bounded_reference()
  end

  defp index_ref(children, tenant, signal, config) do
    # Leaf references keep their trace summaries, so a read of this index can
    # still skip leaves; index references themselves carry none.
    children
    |> Pulso.JSON.encode_to_iodata!()
    |> then(&put_page("r", &1, "index", tenant, signal, config))
    |> Map.merge(%{
      "h" => hd(children)["h"] + 1,
      "n" => Enum.sum_by(children, & &1["n"]),
      "e" => Enum.sum_by(children, & &1["e"]),
      "l" => Enum.sum_by(children, & &1["l"]),
      "mn" => children |> Enum.map(& &1["mn"]) |> Enum.reject(&is_nil/1) |> Enum.min(fn -> nil end),
      "mx" => children |> Enum.map(& &1["mx"]) |> Enum.reject(&is_nil/1) |> Enum.max(fn -> nil end),
      "kn" => children |> Enum.map(& &1["kn"]) |> Enum.min(),
      "kx" => children |> Enum.map(& &1["kx"]) |> Enum.max(),
      "rd" => children |> Enum.map(& &1["rd"]) |> Enum.reject(&is_nil/1) |> Enum.min(fn -> nil end)
    })
  end

  defp bounded_reference(ref) do
    ref = if ref_bytes(ref) > @ref_bytes, do: Map.delete(ref, "q"), else: ref
    if ref_bytes(ref) > @ref_bytes, do: fail(:manifest_capacity)
    ref
  end

  defp ref_bytes(ref), do: IO.iodata_length(Pulso.JSON.encode_to_iodata!(ref))

  # ---- page I/O --------------------------------------------------------------

  # `{"n":"<nonce>","<tag>":<payload>}`: the nonce makes every page key unique,
  # so a superseded page can never be referenced again once it is deleted.
  defp put_page(tag, payload, kind, tenant, signal, config) do
    check_deadline!(deadline())
    body = IO.iodata_to_binary(["{\"n\":\"", nonce(), "\",\"", tag, "\":", payload, "}"])
    if byte_size(body) > @page_limit, do: fail(:manifest_capacity)
    sha = digest(body)
    key = prefix(tenant, signal) <> sha <> "-" <> kind <> ".json"

    case ObjectStore.put_if_none_match(config, key, body) do
      {:ok, _} -> :ok
      {:error, reason} when reason in [:already_exists, :precondition_failed] -> :ok
      {:error, reason} -> fail(reason)
    end

    ref = %{"k" => key, "b" => byte_size(body), "sha" => sha}
    MetadataCache.put(ref, body)
    ref
  end

  defp read_leaf(ref, tenant, signal, config, deadline), do: read(ref, "m", tenant, signal, config, deadline)
  defp read_index(ref, tenant, signal, config, deadline), do: read(ref, "r", tenant, signal, config, deadline)
  defp read_gc(ref, tenant, signal, config), do: read(ref, "g", tenant, signal, config, deadline())

  defp read(ref, tag, tenant, signal, config, deadline) do
    memo = Process.get(@memo)

    case memo && Map.fetch(memo, ref["k"]) do
      {:ok, data} ->
        data

      _ ->
        data = ref |> fetch(config, deadline) |> decode_page!(tag, ref, tenant, signal)
        if memo, do: Process.put(@memo, Map.put(Process.get(@memo), ref["k"], data))
        data
    end
  end

  # Pages are cached as their verified bytes: compact refcounted binaries,
  # decoded by each reader, keep the bounded page cache's byte accounting
  # honest.
  defp fetch(ref, config, deadline) do
    cached = %{"k" => ref["k"], "b" => ref["b"], "sha" => ref["sha"]}

    case MetadataCache.get(cached) do
      {:ok, body} when is_binary(body) ->
        body

      _ ->
        check_deadline!(deadline)
        download!(ref, cached, config)
    end
  end

  defp download!(ref, cached, config) do
    case ObjectStore.get_bounded(config, ref["k"], nil, @page_limit) do
      {:ok, _etag, body} ->
        if byte_size(body) != ref["b"] or digest(body) != ref["sha"], do: fail(:invalid_manifest_page)
        MetadataCache.put(cached, body)
        body

      {:error, :not_found} ->
        fail(:manifest_page_missing)

      {:error, :response_too_large} ->
        fail(:invalid_manifest_page)

      {:error, reason} ->
        fail(reason)
    end
  end

  defp decode_page!(
         <<"{\"n\":\"", _nonce::binary-size(32), "\",\"", tag::binary-size(1), "\":", rest::binary>>,
         tag,
         ref,
         tenant,
         signal
       )
       when byte_size(rest) > 1 do
    if :binary.last(rest) != ?}, do: fail(:invalid_manifest_page)
    payload = binary_part(rest, 0, byte_size(rest) - 1)
    decode_payload!(tag, payload, ref, tenant, signal)
  end

  defp decode_page!(_body, _tag, _ref, _tenant, _signal), do: fail(:invalid_manifest_page)

  defp decode_payload!("m", payload, ref, tenant, signal) do
    case Manifest.decode(payload) do
      {:ok, %Manifest{version: 2, paging: nil} = leaf} ->
        if length(leaf.segments) + map_size(leaf.retired) > @leaf_read_entries, do: fail(:invalid_manifest_page)
        if ref["h"] not in [nil, 0], do: fail(:invalid_manifest_page)

        try do
          Enum.each(leaf.segments, &validate_segment!(&1, tenant, signal))
          Enum.each(Map.keys(leaf.retired), &validate_retired_key!(&1, tenant, signal))
        catch
          {:journal_manifest, _} -> fail(:invalid_manifest_page)
        end

        leaf

      _ ->
        fail(:invalid_manifest_page)
    end
  end

  defp decode_payload!("r", payload, ref, tenant, signal) do
    prefix = prefix(tenant, signal)

    case Pulso.JSON.decode(payload) do
      {:ok, children} when is_list(children) and length(children) in 1..@fanout ->
        children = Pulso.JSON.detach(children)

        if not Enum.all?(children, &(valid_ref?(&1) and &1["h"] == ref["h"] - 1 and page_key?(&1["k"], prefix))),
          do: fail(:invalid_manifest_page)

        children

      _ ->
        fail(:invalid_manifest_page)
    end
  end

  defp decode_payload!("g", payload, _ref, tenant, signal) do
    prefix = prefix(tenant, signal)

    case Pulso.JSON.decode(payload) do
      {:ok, keys} when is_list(keys) and length(keys) <= @gc_page_keys ->
        keys = Pulso.JSON.detach(keys)
        if not Enum.all?(keys, &(is_binary(&1) and page_key?(&1, prefix))), do: fail(:invalid_manifest_page)
        keys

      _ ->
        fail(:invalid_manifest_page)
    end
  end

  # Leaves to read, descending into matching index pages.
  defp collect_leaves(refs, select, tenant, signal, config, deadline) do
    Enum.flat_map(refs, fn ref ->
      cond do
        not select.(ref) ->
          []

        ref["h"] == 0 ->
          [ref]

        true ->
          collect_leaves(read_index(ref, tenant, signal, config, deadline), select, tenant, signal, config, deadline)
      end
    end)
  end

  defp read_leaves(refs, tenant, signal, config, deadline, fun),
    do: refs |> stream_leaves(tenant, signal, config, deadline, fun) |> Enum.to_list()

  # Leaf reads run eight at a time; each task returns only what `fun` keeps.
  defp stream_leaves(refs, tenant, signal, config, deadline, fun) when length(refs) <= 1,
    do: Enum.map(refs, &fun.(read_leaf(&1, tenant, signal, config, deadline)))

  defp stream_leaves(refs, tenant, signal, config, deadline, fun) do
    refs
    |> Pulso.Runtime.Task.async_stream(
      fn ref ->
        try do
          {:ok, fun.(read_leaf(ref, tenant, signal, config, deadline))}
        catch
          {:journal_manifest, reason} -> {:error, reason}
        end
      end,
      max_concurrency: 8,
      timeout: :infinity
    )
    |> Stream.map(fn
      {:ok, {:ok, value}} -> value
      {:ok, {:error, reason}} -> fail(reason)
    end)
  end

  # ---- helpers ---------------------------------------------------------------

  defp with_ops(fun) do
    owner = Process.get(@memo) == nil
    if owner, do: Process.put(@memo, %{})

    try do
      {:ok, fun.()}
    catch
      {:journal_manifest, reason} -> {:error, reason}
    after
      if owner, do: Process.delete(@memo)
    end
  end

  defp fail(reason), do: throw({:journal_manifest, reason})

  # Queries and maintenance run under the caller's metadata deadline.
  defp deadline do
    case Process.get(:pulso_metadata_budget) do
      %{deadline: deadline} -> deadline
      _ -> nil
    end
  end

  defp check_deadline!(nil), do: :ok

  defp check_deadline!(deadline) do
    if System.monotonic_time(:millisecond) >= deadline, do: fail(:metadata_scan_limit)
  end

  defp digest(body), do: :crypto.hash(:sha256, body) |> Base.encode16(case: :lower)

  defp key_max_ts!(key) do
    case ManifestOwner.segment_from_key(key) do
      {:ok, segment} -> segment.max_ts
      :skip -> fail(:invalid_segment_key)
    end
  end

  defp valid_key_scope?(key, tenant, signal) do
    is_binary(key) and byte_size(key) <= 1024 and String.starts_with?(key, scope(tenant, signal)) and
      not String.contains?(key, "/../")
  end

  # Active segments keep the flat format's contract: valid bounds inside the
  # tenant/signal scope. Retired keys must also encode their bounds, which
  # locate them for lookups and cleanup.
  defp validate_segment!(s, tenant, signal) do
    if not (non_negative?(s.min_ts) and is_integer(s.max_ts) and s.max_ts >= s.min_ts and
              valid_key_scope?(s.key, tenant, signal)),
       do: fail(:invalid_segment_key)
  end

  defp validate_retired_key!(key, tenant, signal) do
    if not valid_key_scope?(key, tenant, signal), do: fail(:invalid_segment_key)
    key_max_ts!(key)
  end
end
