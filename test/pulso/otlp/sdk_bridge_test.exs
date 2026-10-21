defmodule Pulso.OTLP.SDKBridgeTest do
  use Pulso.Test.Case, async: true

  alias Pulso.Codec.NIF
  alias Pulso.OTLP.Traces
  alias Pulso.Storage.S3
  alias Pulso.Storage.SortOrder
  alias Pulso.Test.CollectorBridgeTap
  alias Pulso.Test.SDKBridgeAssertions
  alias Pulso.Traces.{Assembly, Tempo}

  @fixtures Path.expand("../../fixtures/traces/collector_bridge", __DIR__)
  @hashes %{
    "../produce_sdk_bridge.go.txt" => "3d64e6f315152d9d95223637a4f53e3ce9de1e6fceeff8fa0c70d1801e6ff082",
    "../sdk-bridge.go.mod.txt" => "6d5f7aa51683242a7d2f3e7b7e60d8b634231d8767777bd97ccda2e92aae2cdb",
    "../sdk-bridge.go.sum.txt" => "3c0e4cc1e2fe9edf04b373eaa2fc0928559602c05f5c29ab0134be7a54b86001",
    "sdk-request.protobuf" => "1ca97ee4617977b66ab1af855513272d444355224c88a00f9ad74cede66b311d",
    "collector-request.protobuf" => "38b8d8b31c4eb5b8626c13d4394fb029011cde4d65e8ddd97e00a8283a92a913",
    "sdk-oracle.json" => "cf52697ccfa9b85c7dc978a2cae310506132441073afb37c4f1f72264fa15f0d",
    "sdk-buildinfo.txt" => "42e934d97ebccf053ecb7896b5cfc5680417f55f1538352471b1ad8bcfdf28f1",
    "split-collector-request.protobuf" => "b78065e716a77c276da3c44c1dad5beae97c494bdaed9189c5356ab5004b4236",
    "partial-producer.protobuf" => "977747b0c64211e2b07187a7d5c333ead8cb542ec35e12707247e185c54ce967",
    "partial-collector.protobuf" => "19cdc88fac71f448c59140a510ac07c8e67eeae7f002abaf213f93baab971bfe",
    "partial-result.json" => "a85bb10c2fe3983a53c315cdb3ac52f4f45e347a3167a31b5ff9bddc3126a1fe"
  }

  defp fixture(name) do
    bytes = File.read!(Path.join(@fixtures, name))
    assert Base.encode16(:crypto.hash(:sha256, bytes), case: :lower) == Map.fetch!(@hashes, name)
    bytes
  end

  defp rows(name) do
    assert {:ok, rows, 0} = Traces.decode_protobuf(fixture(name))
    Enum.sort_by(rows, & &1.span_id)
  end

  test "real SDK export request and non-rebatching Collector preserve the same complete typed spans" do
    for name <- ~w(../produce_sdk_bridge.go.txt ../sdk-bridge.go.mod.txt ../sdk-bridge.go.sum.txt sdk-buildinfo.txt),
        do: fixture(name)

    sdk = rows("sdk-request.protobuf")
    delivered = rows("collector-request.protobuf")
    assert sdk === delivered
    oracle = Pulso.JSON.decode!(fixture("sdk-oracle.json"))
    SDKBridgeAssertions.assert_spans(delivered, oracle)

    blobs =
      Enum.map(delivered, fn row ->
        assert {:ok, blob, _, _} = S3.encode_segment(:traces, [row])
        blob
      end)

    assert {:ok, merged, _, _, 3, _, _} = NIF.compact_trace_segments(blobs, 100)
    assert {:ok, decoded} = S3.decode_segment(:traces, merged, nil, nil, [])
    SDKBridgeAssertions.assert_spans(decoded, oracle)
    trace_id = hd(delivered).trace_id
    assert {:ok, state} = NIF.trace_query_new_trace(trace_id, nil, 100)
    assert {:ok, _} = NIF.trace_query_add(state, merged)
    assert {:ok, native} = NIF.trace_query_finish_trace(state, 100, trace_id)
    assert {:ok, reference} = Assembly.json(SortOrder.sort(decoded, :traces), trace_id)
    assert Pulso.JSON.decode!(native) === Pulso.JSON.decode!(reference)

    for format <- ["json", "json_v2", "protobuf", "protobuf_v2"] do
      assert {:ok, actual} = NIF.tempo_trace_export(native, format)
      assert {:ok, expected} = Tempo.reference(reference, format)

      if String.starts_with?(format, "json"),
        do: assert(Pulso.JSON.decode!(actual) === Pulso.JSON.decode!(expected)),
        else: assert(actual == expected)
    end
  end

  test "captured Collector split loses schema URLs before Pulso; readers do not invent them" do
    sdk = rows("sdk-request.protobuf")
    split = rows("split-collector-request.protobuf")
    assert Enum.map(sdk, & &1.span_id) == Enum.map(split, & &1.span_id)
    different = Enum.filter(Enum.zip(sdk, split), fn {a, b} -> a !== b end)
    assert [{source, lost}] = different
    assert source.name == "database request"
    assert source.resource_schema_url != "" and source.scope_schema_url != ""
    assert lost.resource_schema_url == "" and lost.scope_schema_url == ""
    assert %{source | resource_schema_url: "", scope_schema_url: ""} === lost
    assert {:ok, blob, _, _} = S3.encode_segment(:traces, split)
    assert {:ok, decoded} = S3.decode_segment(:traces, blob, nil, nil, [])
    assert Enum.sort_by(decoded, & &1.span_id) === split
  end

  test "capture exhaustion preserves evidence and retry state instead of restarting its Agent" do
    state = %{
      count: 64,
      requests: List.duplicate(:owned_evidence, 64),
      failures: %{"alpha" => [429, 503]},
      owner: self()
    }

    agent = start_supervised!({Agent, fn -> state end})
    conn = Plug.Conn.put_req_header(Plug.Test.conn("POST", "/v1/traces", ""), "x-scope-orgid", "alpha")

    assert_raise RuntimeError, "bridge request evidence limit exceeded", fn ->
      CollectorBridgeTap.call(conn, agent: agent)
    end

    assert Agent.get(agent, & &1) == state
  end

  test "public malformed-span probe remains partial without Collector repair or whole-batch replay" do
    assert {:ok, [producer], 1} = Traces.decode_protobuf(fixture("partial-producer.protobuf"))
    assert {:ok, [delivered], 1} = Traces.decode_protobuf(fixture("partial-collector.protobuf"))
    assert producer === delivered
    assert delivered.name == "partial valid"
    result = Pulso.JSON.decode!(fixture("partial-result.json"))
    assert result["grpc_code"] == "OK"
    assert result["response"] == "partial_success:{}"
    # Actual no-retry/backend rejectedSpans=1 observations live in the optional
    # process harness, not established by this fixture-only replay.
  end
end
