defmodule Pulso.OTLP.TraceMetadataTest do
  use Pulso.Test.Case, async: true

  alias Pulso.Codec.NIF
  alias Pulso.OTLP.Traces
  alias Pulso.Record.{Span, SpanKey}
  alias Pulso.Storage.S3
  alias Pulso.Storage.SortOrder
  alias Pulso.Test.OTLPTraceFixture, as: Fixture
  alias Pulso.Traces.Assembly

  defp official do
    wire = File.read!(Path.expand("../../fixtures/otlp/protoc-trace-values.protobuf", __DIR__))

    assert Base.encode16(:crypto.hash(:sha256, wire), case: :lower) ==
             "345cbbcb764b0e9959735462671d4ed65265b726ed7d75bb98461d637db95234"

    assert {:ok, [span], 0} = Traces.decode_protobuf(wire)
    span
  end

  defp values(pairs), do: Map.new(pairs, &{&1["key"], &1["value"]})

  test "independent official protobuf preserves type and order at every attribute owner" do
    span = official()
    assert span.trace_id == "30313233343536373839616263646566"
    assert span.span_id == "6465616462656566"
    assert span.otlp["version"] == 1

    owners = [
      span.otlp["resource"],
      span.otlp["scope"],
      span.otlp["attributes"],
      hd(span.otlp["events"]),
      hd(span.otlp["links"])
    ]

    for owner <- owners do
      assert values(owner)["blob"] == %{"bytesValue" => "AP8="}
      assert values(owner)["text"] == %{"stringValue" => "AP8="}
    end

    attributes = values(span.otlp["attributes"])
    assert attributes["integer"] == %{"intValue" => "1"}
    assert attributes["double"] === %{"doubleValue" => 1.0}
    assert attributes["minimum"] == %{"intValue" => "-9223372036854775808"}
    assert attributes["maximum"] == %{"intValue" => "9223372036854775807"}
    assert attributes["empty"] == %{}
    assert <<128, 0, 0, 0, 0, 0, 0, 0>> == <<attributes["negative_zero"]["doubleValue"]::float-64>>

    assert [%{"boolValue" => false}, %{"kvlistValue" => %{"values" => nested}}] =
             attributes["nested"]["arrayValue"]["values"]

    assert values(nested) == %{"blob" => %{"bytesValue" => "AP8="}, "text" => %{"stringValue" => "AP8="}}

    assert Enum.map(span.otlp["attributes"], & &1["key"]) ==
             ~w(blob text integer double negative_zero minimum maximum empty nested)
  end

  test "native Parquet, mixed legacy compaction, projection and complete assembly retain the envelope" do
    span = official()
    legacy = %{span | otlp: nil}
    refute Map.has_key?(Span.to_map(legacy), "otlp")
    assert Span.from_map(Span.to_map(legacy)) === legacy
    assert SpanKey.size(legacy) == byte_size(SpanKey.encode(Span.to_map(legacy)))
    assert SpanKey.size(span) == byte_size(SpanKey.encode(Span.to_map(span)))
    assert {:ok, rich, _, _} = S3.encode_segment(:traces, [span])
    assert {:ok, old, _, _} = S3.encode_segment(:traces, [legacy])
    assert {:ok, [decoded]} = S3.decode_segment(:traces, rich, nil, nil, [])
    assert :erlang.term_to_binary(decoded.otlp) == :erlang.term_to_binary(span.otlp)
    assert {:ok, merged, _, _, 2, _, _} = NIF.compact_trace_segments([old, rich], 100)
    assert {:ok, records} = S3.decode_segment(:traces, merged, nil, nil, [])
    assert Enum.count(records, &is_nil(&1.otlp)) == 1
    assert Enum.find(records, & &1.otlp).otlp === span.otlp
    assert {:ok, query} = NIF.trace_query_new(nil, nil, %{"limit" => 2}, ["otlp"], 100)
    assert {:ok, _} = NIF.trace_query_add(query, merged)
    assert {:ok, json} = NIF.trace_query_finish(query, 100)
    assert Enum.find(Pulso.JSON.decode!(json), &Map.has_key?(&1, "otlp"))["otlp"] === span.otlp
    assert {:ok, query} = NIF.trace_query_new_trace(span.trace_id, nil, 100)
    assert {:ok, _} = NIF.trace_query_add(query, merged)
    assert {:ok, json} = NIF.trace_query_finish_trace(query, 100, span.trace_id)
    assembled = Pulso.JSON.decode!(json)
    assert assembled["span_count"] == 2
    assert assembled["conflicting_span_ids"] == []
    assert assembled["metadata_unknown_span_ids"] == [span.span_id]
  end

  test "independent legacy Arrow stays readable and malformed rich Arrow fails closed on reads and compaction" do
    read = fn name -> File.read!(Path.expand("../../fixtures/traces/trace-#{name}.parquet", __DIR__)) end
    old = read.("legacy")
    assert {:ok, [legacy]} = S3.decode_segment(:traces, old, nil, nil, [])
    assert legacy.otlp == nil
    assert legacy.attributes == %{"blob" => "AP8="}

    for name <- ~w(invalid-type invalid-version invalid-value) do
      blob = read.(name)
      assert {:error, {:decode_failed, _}} = S3.decode_segment(:traces, blob, nil, nil, [])
      assert :fallback = NIF.compact_trace_segments([old, blob], 100)
      assert {:ok, query} = NIF.trace_query_new(nil, nil, %{"limit" => 1}, nil, 100)
      assert :fallback = NIF.trace_query_add(query, blob)
      assert :fallback = NIF.trace_query_finish(query, 100)
    end
  end

  test "typed metadata participates in keyed identity; nil leaves legacy maps unchanged" do
    span = official()

    other =
      put_in(
        span.otlp["attributes"],
        Enum.map(span.otlp["attributes"], fn pair ->
          if pair["key"] == "blob", do: %{pair | "value" => %{"stringValue" => "AP8="}}, else: pair
        end)
      )

    assert span.attributes == other.attributes
    refute S3.caller_content_hash(:traces, [span]) == S3.caller_content_hash(:traces, [other])
    assert {:ok, reference} = Assembly.json(SortOrder.sort([span, other], :traces), span.trace_id)
    assert Pulso.JSON.decode!(reference)["conflicting_span_ids"] == [span.span_id]
    assert {:ok, blob, _, _} = S3.encode_segment(:traces, [span, other])
    assert {:ok, query} = NIF.trace_query_new_trace(span.trace_id, nil, 100)
    assert {:ok, _} = NIF.trace_query_add(query, blob)
    assert {:ok, native} = NIF.trace_query_finish_trace(query, 100, span.trace_id)
    assert Pulso.JSON.decode!(native) == Pulso.JSON.decode!(reference)
    assert SpanKey.encode(Span.to_map(span)) != SpanKey.encode(Span.to_map(other))
  end

  test "counter proves the escaped full payload budget and closes malformed envelope shapes" do
    span = official()
    map = Span.to_map(span)
    expected = byte_size(Pulso.JSON.encode!(map))
    assert {:ok, ^expected} = NIF.trace_payload_size([map], expected)
    assert {:error, :payload_too_large} = NIF.trace_payload_size([map], expected - 1)

    for invalid <- [[], "bad", %{}, Map.put(span.otlp, "version", 2), Map.put(span.otlp, "events", [])] do
      assert :fallback = NIF.encode_trace_segment_parquet([Map.put(map, "otlp", invalid)])
    end
  end

  test "rich sort-key ceilings fail instead of dropping metadata even with narrow projection" do
    raw = Fixture.span() |> Map.put("events", []) |> Map.put("links", [])
    attrs = Map.new(1..25, &{"attr.#{&1}", %{"stringValue" => "v#{&1}"}})
    assert {:ok, [span], 0} = Traces.decode(Fixture.payload([Map.put(raw, "attributes", Fixture.pairs(attrs))]))
    assert SpanKey.size(span) * 5000 > 16_777_216
    assert {:ok, blob, _, _} = S3.encode_segment(:traces, List.duplicate(span, 5000))
    assert {:ok, query} = NIF.trace_query_new(nil, nil, %{"limit" => 5000}, ["timestamp_ns"], 10_000)
    assert {:error, :query_resource_limit} = NIF.trace_query_add(query, blob)
    assert :fallback = NIF.trace_query_finish(query, 10_000)
  end

  test "repeated owner metadata exceeding normalized ceiling rejects before append" do
    payload = Fixture.payload(List.duplicate(Fixture.span(), 1000))

    payload =
      put_in(payload, ["resourceSpans", Access.at(0), "resource", "attributes"], [
        %{"key" => "large", "value" => %{"stringValue" => String.duplicate("x", 16_384)}}
      ])

    assert {:error, :payload_too_large} = Traces.decode(payload)
  end
end
