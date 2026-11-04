defmodule Pulso.Storage.S3.TraceSummary do
  @moduledoc false
  import Bitwise

  alias Pulso.Codec.NIF
  alias Pulso.Storage.S3.TraceAttributeSummary

  @zero_id String.duplicate("0", 32)
  @max_bloom_bytes 8192
  @write_bloom_bytes 1024
  @max_lookup_ids 256
  @canonical_bytes 2048
  defstruct [:services, :bits, :lookup_hashes, :lookup_bits, :attributes, pruning_safe?: false]

  @type t :: %__MODULE__{
          services: [String.t()] | nil,
          bits: binary() | nil,
          lookup_hashes: binary() | nil,
          lookup_bits: binary() | nil,
          attributes: TraceAttributeSummary.t(),
          pruning_safe?: boolean()
        }

  # Only the encoder/compactor's proof for these exact bytes may set this bit:
  # all rows rich, strict core/column agreement, and original string service.name
  # is represented by the promoted service column. Legacy/unknown never prove it.
  def build(records, pruning_safe? \\ false) do
    services = Enum.reduce_while(records, MapSet.new(), &collect_service/2)
    ids = Enum.reduce_while(records, MapSet.new(), &collect_id/2)
    %{summary(services, ids, pruning_safe?) | attributes: TraceAttributeSummary.build(records)}
  end

  @doc false
  def from_values(services, ids, pruning_safe? \\ false) do
    services =
      if is_list(services),
        do: Enum.reduce_while(services, MapSet.new(), fn value, acc -> collect_service(%{service: value}, acc) end)

    ids = Enum.reduce_while(ids, MapSet.new(), fn id, acc -> collect_id(%{trace_id: id}, acc) end)
    summary(services, ids, pruning_safe?)
  end

  defp summary(services, ids, pruning_safe?) do
    %__MODULE__{
      services: if(services, do: services |> Enum.map(&:binary.copy/1) |> Enum.sort()),
      # Wide sets use a canonical bloom instead of retaining an incomplete
      # prefix or two redundant blooms. Old readers treat absent b/h as unknown.
      bits: if(ids && MapSet.size(ids) <= @max_lookup_ids, do: bloom(ids)),
      lookup_hashes: if(ids, do: lookup_hashes(ids)),
      lookup_bits: if(ids && MapSet.size(ids) > @max_lookup_ids, do: canonical_bloom(ids)),
      pruning_safe?: pruning_safe? == true
    }
  end

  defp collect_service(%{service: value}, values) when is_binary(value) and byte_size(value) in 1..256 do
    values = MapSet.put(values, value)
    if MapSet.size(values) <= 128, do: {:cont, values}, else: {:halt, nil}
  end

  defp collect_service(_, _), do: {:halt, nil}

  defp collect_id(%{trace_id: id}, ids) when is_binary(id) do
    if id != @zero_id and Regex.match?(~r/\A[0-9a-f]{32}\z/, id),
      do: {:cont, MapSet.put(ids, id)},
      else: {:halt, nil}
  end

  defp collect_id(_, _), do: {:halt, nil}

  defp bloom(ids) do
    # Stored size controls hashing: old 8 KiB blooms remain readable. New
    # segments cap blooms at 1 KiB so a single entry cannot dominate a page.
    bytes = bloom_bytes(MapSet.size(ids), 16)
    size = bytes * 8
    bits = Enum.reduce(ids, 0, fn id, bits -> Enum.reduce(positions(id, size), bits, &bor(&2, bsl(1, &1))) end)
    <<bits::unsigned-size(size)>>
  end

  defp bloom_bytes(wanted, bytes) when bytes >= wanted or bytes >= @write_bloom_bytes, do: bytes
  defp bloom_bytes(wanted, bytes), do: bloom_bytes(wanted, bytes * 2)

  defp positions(id, size) do
    <<a::64, b::64, c::64, d::64>> = :crypto.hash(:sha256, id)
    Enum.map([a, b, c, d], &rem(&1, size))
  end

  def plan_catalog_limit, do: 8192

  @doc """
  `query_traces`-style summary filters for a Tempo selector's AND-ed original
  string predicates (`[]` for expressions). Projections of original strings
  equal them, so a source no summary value satisfies holds no match; any
  subset of the conjuncts prunes soundly, so repeated keys keep the first.
  `proven_only` limits pruning to all-rich, publication-proven sources:
  legacy rows always potentially match.
  """
  def tempo_filters(predicates) when is_list(predicates) do
    Enum.reduce(predicates, [proven_only: true], fn {scope, key, op, value}, acc ->
      field =
        case {scope, op} do
          {"resource", "="} -> :resource
          {"resource", "=~"} -> :resource_regex
          {"span", "="} -> :attributes
          {"span", "=~"} -> :attributes_regex
        end

      Keyword.update(acc, field, %{key => value}, &Map.put_new(&1, key, value))
    end)
  end

  def tempo_filters(_expression), do: []

  def to_wire(nil), do: nil

  def to_wire(%__MODULE__{services: services, bits: bits, lookup_hashes: hashes, lookup_bits: canonical} = summary) do
    wire = %{"v" => 1, "s" => services, "b" => if(bits, do: Base.encode64(bits))}
    wire = if hashes, do: Map.put(wire, "h", Base.encode64(hashes)), else: wire
    wire = if canonical, do: Map.put(wire, "c", %{"v" => 1, "b" => Base.encode64(canonical)}), else: wire
    wire = if summary.attributes, do: Map.put(wire, "a", TraceAttributeSummary.to_wire(summary.attributes)), else: wire
    if summary.pruning_safe? == true, do: Map.put(wire, "k", true), else: wire
  end

  def parse(%{"v" => 1} = wire) do
    %__MODULE__{
      services: parse_services(wire["s"]),
      bits: parse_bits(wire["b"]),
      lookup_hashes: parse_hashes(wire["h"]),
      lookup_bits: parse_canonical(wire["c"]),
      attributes: TraceAttributeSummary.parse(wire["a"]),
      pruning_safe?: wire["k"] == true
    }
  end

  def parse(_), do: nil

  def context_filter(%__MODULE__{lookup_hashes: hashes}) when is_binary(hashes), do: {hashes, nil}
  def context_filter(%__MODULE__{lookup_bits: bits}) when is_binary(bits), do: {nil, bits}
  def context_filter(_summary), do: {nil, nil}

  defp parse_services([_ | _] = services) do
    if length(services) <= 128 and Enum.all?(services, &(is_binary(&1) and byte_size(&1) in 1..256)),
      do: Enum.map(services, &:binary.copy/1)
  end

  defp parse_services(_), do: nil

  defp parse_bits(value) when is_binary(value) and byte_size(value) <= 10_924 do
    with {:ok, bits} <- Base.decode64(value),
         size = byte_size(bits),
         true <- size in 16..@max_bloom_bytes and band(size, size - 1) == 0,
         true <- bits != :binary.copy(<<0>>, size) do
      :binary.copy(bits)
    else
      _ -> nil
    end
  end

  defp parse_bits(_), do: nil

  # Complete bounded 64-bit SHA-256 prefixes allow a leaf bloom to be rebuilt
  # without guessing a union of incompatible adaptive segment bloom sizes.
  # Hash collisions only add reads. Legacy and over-budget sets stay unknown.
  defp lookup_hashes(ids) do
    if MapSet.size(ids) in 1..@max_lookup_ids do
      ids |> Enum.map(&lookup_hash/1) |> Enum.uniq() |> Enum.sort() |> Enum.map(&<<&1::64>>) |> IO.iodata_to_binary()
    end
  end

  defp lookup_hash(id) do
    <<hash::64, _::binary>> = :crypto.hash(:sha256, id)
    hash
  end

  defp parse_hashes(value) when is_binary(value) and byte_size(value) <= 10_924 do
    with {:ok, hashes} <- Base.decode64(value),
         true <- byte_size(hashes) in 8..(@max_lookup_ids * 8) and rem(byte_size(hashes), 8) == 0 do
      :binary.copy(hashes)
    else
      _ -> nil
    end
  end

  defp parse_hashes(_), do: nil

  def page_summary(entries) do
    summaries = Enum.map(entries, &parse(&1["t"]))
    services = page_services(summaries)
    hashes = page_hashes(summaries)
    bits = if hashes, do: page_bloom(hashes), else: page_canonical(summaries)
    page = %{"v" => 1, "s" => services, "b" => if(bits, do: Base.encode64(bits))}

    # Like each entry's own flag: every row's promoted service is its original
    # string resource service.name, so resource service.name filters prune.
    page =
      if services && Enum.all?(summaries, &match?(%__MODULE__{pruning_safe?: true}, &1)),
        do: Map.put(page, "k", true),
        else: page

    put_page_attributes(page, summaries)
  end

  # References are capped at 4 KiB and lose their whole summary beyond it, so
  # attribute sets join only while the service/ID summary keeps room.
  @page_bytes 3584
  defp put_page_attributes(page, summaries) do
    attributes = summaries |> Enum.map(&(&1 && &1.attributes)) |> TraceAttributeSummary.merge()
    with_attributes = if attributes, do: Map.put(page, "a", TraceAttributeSummary.to_wire(attributes)), else: page

    if IO.iodata_length(Pulso.JSON.encode_to_iodata!(with_attributes)) <= @page_bytes,
      do: with_attributes,
      else: page
  end

  defp page_services(summaries) do
    Enum.reduce_while(summaries, MapSet.new(), fn
      %__MODULE__{services: [_ | _] = services}, acc ->
        values = Enum.reduce(services, acc, &MapSet.put(&2, &1))
        if MapSet.size(values) <= 128, do: {:cont, values}, else: {:halt, nil}

      _, _ ->
        {:halt, nil}
    end)
    |> case do
      nil ->
        nil

      services ->
        values = Enum.sort(services)
        if IO.iodata_length(Pulso.JSON.encode_to_iodata!(values)) <= 512, do: values
    end
  end

  defp page_hashes(summaries) do
    Enum.reduce_while(summaries, MapSet.new(), fn
      %__MODULE__{lookup_hashes: hashes}, acc when is_binary(hashes) ->
        {:cont, Enum.reduce(for(<<hash::64 <- hashes>>, do: hash), acc, &MapSet.put(&2, &1))}

      _, _ ->
        {:halt, nil}
    end)
  end

  defp page_bloom(hashes) do
    wanted = div(MapSet.size(hashes) * 12 + 7, 8)
    bytes = Enum.find([32, 64, 128, 256, 512, 1024, 2048], 2048, &(&1 >= wanted))
    size = bytes * 8

    bits =
      Enum.reduce(hashes, 0, fn hash, bits ->
        Enum.reduce(lookup_positions(hash, size), bits, &bor(&2, bsl(1, &1)))
      end)

    <<bits::unsigned-size(size)>>
  end

  defp canonical_bloom(ids) do
    bits = Enum.reduce(ids, 0, fn id, bits -> add_lookup_hash(bits, lookup_hash(id), @canonical_bytes * 8) end)
    <<bits::unsigned-size(@canonical_bytes * 8)>>
  end

  defp parse_canonical(%{"v" => 1, "b" => encoded}) do
    case parse_bits(encoded) do
      bits when is_binary(bits) and byte_size(bits) == @canonical_bytes -> bits
      _ -> nil
    end
  end

  defp parse_canonical(_), do: nil

  # OR only identically sized, versioned canonical blooms. Small complete hash
  # sets are rebuilt at that exact geometry. Unknown entries poison the union.
  defp page_canonical(summaries) do
    Enum.reduce_while(summaries, 0, fn
      %__MODULE__{lookup_bits: bits}, acc when is_binary(bits) ->
        <<value::unsigned-size(@canonical_bytes * 8)>> = bits
        {:cont, bor(acc, value)}

      %__MODULE__{lookup_hashes: hashes}, acc when is_binary(hashes) ->
        value = Enum.reduce(for(<<hash::64 <- hashes>>, do: hash), acc, &add_lookup_hash(&2, &1, @canonical_bytes * 8))
        {:cont, value}

      _, _ ->
        {:halt, nil}
    end)
    |> case do
      nil -> nil
      bits -> <<bits::unsigned-size(@canonical_bytes * 8)>>
    end
  end

  defp add_lookup_hash(bits, hash, size), do: Enum.reduce(lookup_positions(hash, size), bits, &bor(&2, bsl(1, &1)))

  defp lookup_positions(hash, size) do
    a = bsr(hash, 32)
    b = bor(band(hash, 0xFFFFFFFF), 1)
    Enum.map(0..3, &rem(a + &1 * b, size))
  end

  def page_matches?(%{"v" => 1} = wire, opts) do
    services = parse_services(wire["s"])
    bits = parse_bits(wire["b"])
    service = opts[:service]
    id = opts[:trace_id]

    (is_nil(service) or is_nil(services) or service in services) and page_id_matches?(bits, id) and
      page_context_matches?(bits, opts[:context_hashes]) and
      resource_services_match?(wire["k"] == true, services, opts) and
      attributes_match?(wire["k"] == true, TraceAttributeSummary.parse(wire["a"]), opts)
  end

  def page_matches?(_wire, _opts), do: true
  defp page_id_matches?(nil, _id), do: true
  defp page_id_matches?(_bits, nil), do: true

  defp page_id_matches?(bits, id), do: page_hash_matches?(bits, lookup_hash(id))

  defp page_hash_matches?(bits, hash) do
    size = byte_size(bits) * 8
    <<value::unsigned-size(^size)>> = bits
    Enum.all?(lookup_positions(hash, size), &(band(value, bsl(1, &1)) != 0))
  end

  # A Tempo search's context plan reads only pages whose ID bloom may hold one
  # of its candidate traces (64-bit SHA-256 prefixes, as in `lookup_hashes`).
  # Too many candidates read every page rather than spend unbounded probes.
  defp page_context_matches?(nil, _hashes), do: true
  defp page_context_matches?(_bits, nil), do: true

  defp page_context_matches?(bits, hashes) when is_list(hashes) and length(hashes) <= 4096,
    do: Enum.any?(hashes, &page_hash_matches?(bits, &1))

  defp page_context_matches?(_bits, _hashes), do: true

  def matches?(nil, _opts), do: true

  def matches?(%__MODULE__{services: services, bits: bits, lookup_bits: canonical} = summary, opts) do
    service = Keyword.get(opts, :service)
    trace_id = Keyword.get(opts, :trace_id)

    (is_nil(service) or is_nil(services) or service in services) and
      resource_services_match?(summary.pruning_safe?, services, opts) and
      attributes_match?(summary.pruning_safe?, summary.attributes, opts) and
      if(canonical,
        do: page_id_matches?(canonical, trace_id),
        else: is_nil(trace_id) or is_nil(bits) or member?(bits, trace_id)
      )
  end

  # `query_traces` resource filters on service.name (exact `resource` strings,
  # `resource_regex` patterns) can only match a row whose original string
  # service.name satisfies them. Where the summary proves that column is each
  # row's promoted service, a source none of whose services satisfies them
  # holds no match. Without that proof, sources are always read.
  defp resource_services_match?(true, [_ | _] = services, opts) do
    exact = opts |> Keyword.get(:resource) |> service_name()
    pattern = opts |> Keyword.get(:resource_regex) |> service_name()

    (is_nil(exact) and is_nil(pattern)) or
      Enum.any?(
        services,
        &((not is_binary(exact) or &1 == exact) and (not is_binary(pattern) or regex_match?(pattern, &1)))
      )
  end

  defp resource_services_match?(_proven, _services, _opts), do: true

  # Original-string (Tempo) filters prune only proven sources; `query_traces`
  # compares the projections every row carries.
  defp attributes_match?(proven?, attributes, opts),
    do: (opts[:proven_only] == true and proven? != true) or TraceAttributeSummary.matches?(attributes, opts)

  defp service_name(%{"service.name" => value}) when is_binary(value), do: value
  defp service_name(_filters), do: nil

  # The regex engine and anchoring `Pulso.Traces.RegexFilters` evaluates rows
  # with. Summaries repeat the same few services; memoized per process.
  @regex_memo {__MODULE__, :regex_memo}
  @doc false
  def regex_match?(pattern, service) do
    memo = Process.get(@regex_memo, %{})

    case memo do
      %{{^pattern, ^service} => matched} ->
        matched

      _ ->
        # A pattern the engine rejects fails the query's own scan; never prune on it.
        matched =
          try do
            NIF.match_metric_regex(Pulso.QueryRegex.anchor(pattern), service) == true
          rescue
            ArgumentError -> true
          end

        if map_size(memo) < 4096, do: Process.put(@regex_memo, Map.put(memo, {pattern, service}, matched))
        matched
    end
  end

  defp member?(bits, id) do
    size = byte_size(bits) * 8
    <<value::unsigned-size(^size)>> = bits
    Enum.all?(positions(id, size), &(band(value, bsl(1, &1)) != 0))
  end
end
