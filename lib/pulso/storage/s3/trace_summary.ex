defmodule Pulso.Storage.S3.TraceSummary do
  @moduledoc false
  import Bitwise

  @zero_id String.duplicate("0", 32)
  @max_bloom_bytes 8192
  @write_bloom_bytes 1024
  @max_lookup_ids 256
  @canonical_bytes 2048
  defstruct [:services, :bits, :lookup_hashes, :lookup_bits, pruning_safe?: false]

  @type t :: %__MODULE__{
          services: [String.t()] | nil,
          bits: binary() | nil,
          lookup_hashes: binary() | nil,
          lookup_bits: binary() | nil,
          pruning_safe?: boolean()
        }

  # Only the encoder/compactor's proof for these exact bytes may set this bit:
  # all rows rich, strict core/column agreement, and original string service.name
  # is represented by the promoted service column. Legacy/unknown never prove it.
  def build(records, pruning_safe? \\ false) do
    services = Enum.reduce_while(records, MapSet.new(), &collect_service/2)
    ids = Enum.reduce_while(records, MapSet.new(), &collect_id/2)
    summary(services, ids, pruning_safe?)
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

  def service_filter(predicates) when is_list(predicates) do
    Enum.find_value(predicates, fn
      {"resource", "service.name", "=", value} when is_binary(value) -> value
      _ -> nil
    end)
  end

  def service_filter(_predicates), do: nil

  def to_wire(nil), do: nil

  def to_wire(%__MODULE__{services: services, bits: bits, lookup_hashes: hashes, lookup_bits: canonical} = summary) do
    wire = %{"v" => 1, "s" => services, "b" => if(bits, do: Base.encode64(bits))}
    wire = if hashes, do: Map.put(wire, "h", Base.encode64(hashes)), else: wire
    wire = if canonical, do: Map.put(wire, "c", %{"v" => 1, "b" => Base.encode64(canonical)}), else: wire
    if summary.pruning_safe? == true, do: Map.put(wire, "k", true), else: wire
  end

  def parse(%{"v" => 1} = wire) do
    %__MODULE__{
      services: parse_services(wire["s"]),
      bits: parse_bits(wire["b"]),
      lookup_hashes: parse_hashes(wire["h"]),
      lookup_bits: parse_canonical(wire["c"]),
      pruning_safe?: wire["k"] == true
    }
  end

  def parse(_), do: nil

  def candidate_source?(%__MODULE__{pruning_safe?: true, services: services}, target) when is_list(services),
    do: target in services

  def candidate_source?(_summary, _target), do: true

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
    %{"v" => 1, "s" => services, "b" => if(bits, do: Base.encode64(bits))}
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
    (is_nil(service) or is_nil(services) or service in services) and page_id_matches?(bits, id)
  end

  def page_matches?(_wire, _opts), do: true
  defp page_id_matches?(nil, _id), do: true
  defp page_id_matches?(_bits, nil), do: true

  defp page_id_matches?(bits, id) do
    size = byte_size(bits) * 8
    <<value::unsigned-size(^size)>> = bits
    Enum.all?(lookup_positions(lookup_hash(id), size), &(band(value, bsl(1, &1)) != 0))
  end

  def matches?(nil, _opts), do: true

  def matches?(%__MODULE__{services: services, bits: bits, lookup_bits: canonical}, opts) do
    service = Keyword.get(opts, :service)
    trace_id = Keyword.get(opts, :trace_id)

    (is_nil(service) or is_nil(services) or service in services) and
      if(canonical,
        do: page_id_matches?(canonical, trace_id),
        else: is_nil(trace_id) or is_nil(bits) or member?(bits, trace_id)
      )
  end

  defp member?(bits, id) do
    size = byte_size(bits) * 8
    <<value::unsigned-size(^size)>> = bits
    Enum.all?(positions(id, size), &(band(value, bsl(1, &1)) != 0))
  end
end
