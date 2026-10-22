defmodule Pulso.Storage.S3.TraceSummaryTest do
  use Pulso.Test.Case, async: true

  alias Pulso.Storage.S3.TraceSummary

  test "adaptive bounded bloom survives wire round trips with no false negatives" do
    records =
      for n <- 1..2000, do: %{trace_id: Base.encode16(<<n::128>>, case: :lower), service: "service-#{rem(n, 5)}"}

    summary = TraceSummary.build(records)
    assert summary.bits == nil
    assert byte_size(summary.lookup_bits) == 2048
    assert TraceSummary.parse(TraceSummary.to_wire(summary)) == summary

    for record <- records do
      assert TraceSummary.matches?(summary, trace_id: record.trace_id, service: record.service)
    end

    refute TraceSummary.matches?(summary, service: "missing")
    small = TraceSummary.build(Enum.take(records, 1))
    assert byte_size(small.bits) == 16
    refute TraceSummary.matches?(small, trace_id: String.duplicate("f", 32))
  end

  test "leaf summaries preserve complete unions across differently sized segment blooms" do
    records = for n <- 1..2000, do: %{trace_id: Base.encode16(<<n::128>>, case: :lower), service: "api"}
    entries = for batch <- Enum.chunk_every(records, 137), do: %{"t" => TraceSummary.to_wire(TraceSummary.build(batch))}
    summary = TraceSummary.page_summary(entries)
    for record <- records, do: assert(TraceSummary.page_matches?(summary, trace_id: record.trace_id, service: "api"))
    refute TraceSummary.page_matches?(summary, service: "absent")
    legacy = Enum.map(entries, &update_in(&1["t"], fn t -> Map.delete(t, "h") end))
    unknown = TraceSummary.page_summary(legacy)
    assert unknown["b"] == nil
    assert TraceSummary.page_matches?(unknown, trace_id: String.duplicate("f", 32))
    large = TraceSummary.build(records)
    assert large.lookup_hashes == nil
    canonical = TraceSummary.page_summary([%{"t" => TraceSummary.to_wire(large)} | Enum.take(entries, 1)])
    assert byte_size(Base.decode64!(canonical["b"])) == 2048
    for record <- records, do: assert(TraceSummary.page_matches?(canonical, trace_id: record.trace_id))

    # An old reader only sees unknown b/h, never an incomplete set.
    old = large |> TraceSummary.to_wire() |> Map.delete("c") |> TraceSummary.parse()
    assert TraceSummary.matches?(old, trace_id: String.duplicate("f", 32))
    assert TraceSummary.page_summary([%{"t" => TraceSummary.to_wire(old)}])["b"] == nil

    for wire <- [nil, %{"v" => 9}, %{"v" => 1, "b" => "corrupt"}],
        do: assert(TraceSummary.page_matches?(wire, trace_id: String.duplicate("f", 32)))
  end

  test "canonical geometry is versioned and unknown entries prevent leaf ID pruning" do
    records = for n <- 1..5000, do: %{trace_id: Base.encode16(<<n::128>>, case: :lower), service: "api"}
    summary = TraceSummary.build(records)
    assert TraceSummary.from_values(["api"], Enum.map(records, & &1.trace_id)) == summary
    assert TraceSummary.parse(TraceSummary.to_wire(summary)) == summary
    leaf = TraceSummary.page_summary([%{"t" => TraceSummary.to_wire(summary)}])

    for record <- records do
      assert TraceSummary.matches?(summary, trace_id: record.trace_id)
      assert TraceSummary.page_matches?(leaf, trace_id: record.trace_id)
    end

    for c <- [
          nil,
          %{"v" => 2, "b" => Base.encode64(summary.lookup_bits)},
          %{"v" => 1, "b" => Base.encode64(:binary.copy(<<255>>, 1024))},
          %{"v" => 1, "b" => Base.encode64(:binary.copy(<<0>>, 2048))},
          %{"v" => 1, "b" => "bad"}
        ] do
      wire = Map.put(TraceSummary.to_wire(summary), "c", c)
      assert TraceSummary.parse(wire).lookup_bits == nil
      assert TraceSummary.matches?(TraceSummary.parse(wire), trace_id: String.duplicate("f", 32))
      assert TraceSummary.page_summary([%{"t" => wire}, %{"t" => TraceSummary.to_wire(summary)}])["b"] == nil
    end
  end

  test "unknown versions, malformed/absent fields and legacy summaries cannot exclude data" do
    for wire <- [
          nil,
          %{},
          %{"v" => 2},
          %{"v" => 1, "s" => [], "b" => "not-base64"},
          %{"v" => 1, "b" => Base.encode64("short")},
          %{"v" => 1, "b" => Base.encode64(:binary.copy(<<0>>, 16))}
        ] do
      assert TraceSummary.matches?(TraceSummary.parse(wire), trace_id: String.duplicate("f", 32), service: "unknown")
    end

    summary = TraceSummary.build([%{trace_id: "bad", service: nil}])
    assert TraceSummary.matches?(summary, trace_id: String.duplicate("f", 32), service: "unknown")
    records = for n <- 1..129, do: %{trace_id: Base.encode16(<<n::128>>, case: :lower), service: "s#{n}"}
    assert TraceSummary.build(records).services == nil
  end
end
