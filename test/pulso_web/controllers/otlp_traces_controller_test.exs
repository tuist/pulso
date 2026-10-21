defmodule PulsoWeb.OTLPTracesControllerTest do
  use PulsoWeb.ConnCase, async: true

  alias Pulso.Auth.SharedSecret
  alias Pulso.Storage
  alias Pulso.Storage.Memory
  alias Pulso.Test.FailingStorage
  alias Pulso.Test.OTLPTraceFixture, as: Fixture

  defp request(body, type \\ "application/json", headers \\ []) do
    conn = build_conn() |> put_req_header("content-type", type)
    conn = Enum.reduce(headers, conn, fn {k, v}, c -> put_req_header(c, k, v) end)
    post(conn, "/v1/traces", body)
  end

  test "JSON and protobuf accept gzip, mirror encoding, and isolate tenants" do
    for {type, body} <- [
          {"application/json", Pulso.JSON.encode!(Fixture.payload())},
          {"application/x-protobuf", Fixture.protobuf()}
        ],
        gzip? <- [false, true] do
      Memory.reset()
      headers = [{"x-scope-orgid", "acme"}, {"accept", type}] ++ if(gzip?, do: [{"content-encoding", "gzip"}], else: [])
      conn = request(if(gzip?, do: :zlib.gzip(body), else: body), type, headers)
      assert conn.status == 200
      assert get_resp_header(conn, "content-type") == [type]
      assert {:ok, [%{service: "tuist-server", trace_id: id}]} = Storage.query(:traces, "acme")
      assert id == Fixture.trace_id()
      assert {:ok, []} = Storage.query(:traces, "default")
      assert {:ok, []} = Storage.query(:logs, "acme")
      assert {:ok, []} = Storage.query(:metrics, "acme")
    end
  end

  test "partial success counts invalid spans in the request encoding" do
    raw = Map.put(Fixture.span(), "spanId", String.duplicate("0", 16))
    payload = Fixture.payload([Fixture.span(), raw])
    conn = request(Pulso.JSON.encode!(payload))
    assert json_response(conn, 200)["partialSuccess"]["rejectedSpans"] == "1"
    assert get_resp_header(conn, "content-type") == ["application/json"]
    Memory.reset()
    conn = request(Fixture.protobuf(payload), "application/x-protobuf")
    assert conn.status == 200
    assert get_resp_header(conn, "content-type") == ["application/x-protobuf"]
    assert <<10, _length, 8, 1, 18, _message_length, _message::binary>> = conn.resp_body
    assert {:ok, [_]} = Storage.query(:traces, "default")
  end

  test "empty batches and malformed requests" do
    assert json_response(request("{}"), 200) == %{}
    assert request(<<>>, "application/x-protobuf").status == 200
    assert request(<<0>>, "application/x-protobuf").status == 400
    assert request(~s({"resourceSpans":null})).status == 400
    assert request("bad", "application/octet-stream").status == 415
    assert request("{}", "application/json", [{"content-encoding", "snappy"}]).status == 415
    assert request("{}", "application/json", [{"x-scope-orgid", "../evil"}]).status == 400
    assert request("bad", "application/x-protobuf", [{"content-encoding", "gzip"}]).status == 400
  end

  test "native projection rejection is permanent, not an exporter retry loop" do
    Pulso.Runtime.put_env(:pulso, Storage, adapter: FailingStorage)
    Pulso.Runtime.put_env(:pulso, FailingStorage, {:error, :trace_projection_invalid})

    for {body, type} <- [
          {Pulso.JSON.encode!(Fixture.payload()), "application/json"},
          {Fixture.protobuf(), "application/x-protobuf"}
        ] do
      conn = request(body, type, [{"idempotency-key", "stable"}])
      assert conn.status == 400
      assert get_resp_header(conn, "retry-after") == []
    end
  end

  test "unkeyed publication expiry retries fresh siblings but keyed expiry stays permanent" do
    Pulso.Runtime.put_env(:pulso, Storage, adapter: FailingStorage)
    Pulso.Runtime.put_env(:pulso, FailingStorage, {:error, :retention_expired})
    body = Pulso.JSON.encode!(Fixture.payload())
    assert request(body).status == 503
    assert request(body, "application/json", [{"idempotency-key", "stable"}]).status == 400
  end

  test "ambiguous headers and dot path tenants reject without appending" do
    for name <- ["x-scope-orgid", "idempotency-key"] do
      conn = build_conn() |> put_req_header("content-type", "application/json")
      conn = %{conn | req_headers: [{name, "a"}, {name, "b"} | conn.req_headers]}
      assert post(conn, "/v1/traces", Pulso.JSON.encode!(Fixture.payload())).status == 400
    end

    for tenant <- [".", ".."], do: assert(request("{}", "application/json", [{"x-scope-orgid", tenant}]).status == 400)
    assert {:ok, []} = Storage.query(:traces, "default")
  end

  test "auth precedes semantic validation, with the same tenant policy as other receivers" do
    hash = "sha256$" <> Base.encode16(:crypto.hash(:sha256, "secret"), case: :lower)
    Pulso.Runtime.put_env(:pulso, Pulso.Auth, module: SharedSecret, tokens: %{"acme" => hash})
    assert request(<<0>>, "application/x-protobuf", [{"x-scope-orgid", "acme"}]).status == 401

    assert request(Pulso.JSON.encode!(Fixture.payload()), "application/json", [
             {"x-scope-orgid", "acme"},
             {"authorization", "Bearer wrong"}
           ]).status == 401

    assert request(Pulso.JSON.encode!(Fixture.payload()), "application/json", [
             {"x-scope-orgid", "acme"},
             {"authorization", "Bearer secret"}
           ]).status == 200
  end

  test "wire and expanded protobuf body caps return 413 without storing spans" do
    assert request(:binary.copy(<<0>>, 4 * 1024 * 1024 + 1), "application/x-protobuf").status == 413
    Pulso.Runtime.put_env(:pulso, PulsoWeb.CompressedBodyReader, max_decompressed_bytes: 100)
    conn = request(:zlib.gzip(Fixture.protobuf()), "application/x-protobuf", [{"content-encoding", "gzip"}])
    assert conn.status == 413
    assert get_resp_header(conn, "content-type") |> hd() |> String.starts_with?("application/x-protobuf")
    assert {:ok, []} = Storage.query(:traces, "default")
  end

  test "413 rejects the whole request and storage failure remains retryable" do
    Pulso.Runtime.put_env(:pulso, Pulso.IngestLimits, max_records: 1)
    payload = Fixture.payload([Fixture.span(), Fixture.span()])
    assert request(Pulso.JSON.encode!(payload)).status == 413
    assert request(Fixture.protobuf(payload), "application/x-protobuf").status == 413
    assert {:ok, []} = Storage.query(:traces, "default")
    Pulso.Runtime.put_env(:pulso, Storage, adapter: FailingStorage)
    conn = request(Fixture.protobuf(), "application/x-protobuf")
    assert conn.status == 503
    assert get_resp_header(conn, "retry-after") == ["1"]
  end
end
