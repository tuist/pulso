defmodule PulsoWeb.LokiControllerTest do
  use PulsoWeb.ConnCase, async: false

  alias Pulso.Auth.Open
  alias Pulso.Auth.SharedSecret
  alias Pulso.Loki.PushProto
  alias Pulso.Record.Log
  alias Pulso.Storage
  alias Pulso.Storage.Memory

  setup do
    Memory.reset()
    :ok
  end

  defp payload(ts \\ "1700000000000000000", body \\ "hello") do
    %{
      "streams" => [
        %{
          "stream" => %{"service_name" => "api", "level" => "info"},
          "values" => [[ts, body]]
        }
      ]
    }
  end

  defp proto_payload(opts \\ []) do
    labels = Keyword.get(opts, :labels, ~s({service_name="api", level="info"}))
    line = Keyword.get(opts, :line, "hello")
    seconds = Keyword.get(opts, :seconds, 1_700_000_000)
    nanos = Keyword.get(opts, :nanos, 0)
    metadata = Keyword.get(opts, :metadata, [])

    %PushProto.PushRequest{
      streams: [
        %PushProto.Stream{
          labels: labels,
          entries: [
            %PushProto.Entry{
              timestamp: %PushProto.Timestamp{seconds: seconds, nanos: nanos},
              line: line,
              structured_metadata:
                Enum.map(metadata, fn {k, v} ->
                  %PushProto.LabelPair{name: k, value: v}
                end)
            }
          ]
        }
      ]
    }
  end

  defp encode_proto(request) do
    {iodata, _size} = PushProto.PushRequest.encode!(request)
    IO.iodata_to_binary(iodata)
  end

  defp encode_snappy(bytes) do
    {:ok, compressed} = :snappyer.compress(bytes)
    compressed
  end

  test "POST /loki/api/v1/push returns 204 and stores records under X-Scope-OrgID",
       %{conn: conn} do
    conn =
      conn
      |> put_req_header("content-type", "application/json")
      |> put_req_header("x-scope-orgid", "acme")
      |> post(~p"/loki/api/v1/push", payload())

    assert conn.status == 204
    assert conn.resp_body == ""
    assert {:ok, [%Log{service: "api", body: "hello", severity_text: "info"}]} = Storage.query("acme")
    assert {:ok, []} = Storage.query("other")
  end

  test "POST /loki/api/v1/push falls back to the default tenant when no header is set",
       %{conn: conn} do
    conn =
      conn
      |> put_req_header("content-type", "application/json")
      |> post(~p"/loki/api/v1/push", payload())

    assert conn.status == 204
    assert {:ok, [%Log{body: "hello"}]} = Storage.query("default")
  end

  test "POST /loki/api/v1/push accepts an empty streams array", %{conn: conn} do
    conn =
      conn
      |> put_req_header("content-type", "application/json")
      |> post(~p"/loki/api/v1/push", %{"streams" => []})

    assert conn.status == 204
    assert {:ok, []} = Storage.query("default")
  end

  test "POST /loki/api/v1/push returns 400 for an unsafe tenant name", %{conn: conn} do
    conn =
      conn
      |> put_req_header("content-type", "application/json")
      |> put_req_header("x-scope-orgid", "bad/name")
      |> post(~p"/loki/api/v1/push", payload())

    assert json_response(conn, 400) == %{"error" => "invalid_tenant"}
  end

  test "POST /loki/api/v1/push surfaces rejected records in X-Pulso-Rejected-Records",
       %{conn: conn} do
    payload = %{
      "streams" => [
        %{
          "stream" => %{"service_name" => "api"},
          "values" => [
            ["1", "ok"],
            [nil, "no ts"]
          ]
        }
      ]
    }

    conn =
      conn
      |> put_req_header("content-type", "application/json")
      |> post(~p"/loki/api/v1/push", payload)

    assert conn.status == 204
    assert get_resp_header(conn, "x-pulso-rejected-records") == ["1"]
    assert {:ok, [%Log{body: "ok"}]} = Storage.query("default")
  end

  test "POST /loki/api/v1/push omits the rejected header when all records land",
       %{conn: conn} do
    conn =
      conn
      |> put_req_header("content-type", "application/json")
      |> post(~p"/loki/api/v1/push", payload())

    assert conn.status == 204
    assert get_resp_header(conn, "x-pulso-rejected-records") == []
  end

  test "POST /loki/api/v1/push passes Idempotency-Key through to storage",
       %{conn: conn} do
    # The Memory adapter ignores :idempotency_key; this test only asserts
    # the controller accepts the header and hands the request off cleanly.
    # S3 dedup behavior lives in the S3 adapter tests.
    conn =
      conn
      |> put_req_header("content-type", "application/json")
      |> put_req_header("idempotency-key", "req-1")
      |> post(~p"/loki/api/v1/push", payload())

    assert conn.status == 204
  end

  test "POST /loki/api/v1/push decompresses a gzipped body", %{conn: conn} do
    body = JSON.encode!(payload())
    gzipped = :zlib.gzip(body)

    conn =
      conn
      |> put_req_header("content-type", "application/json")
      |> put_req_header("content-encoding", "gzip")
      |> put_req_header("x-scope-orgid", "acme")
      |> post(~p"/loki/api/v1/push", gzipped)

    assert conn.status == 204
    assert {:ok, [%Log{service: "api", body: "hello"}]} = Storage.query("acme")
  end

  test "POST /loki/api/v1/push accepts Snappy-compressed protobuf",
       %{conn: conn} do
    body =
      proto_payload(
        line: "hello proto",
        metadata: [{"trace_id", "abc"}, {"user_id", "u1"}]
      )
      |> encode_proto()
      |> encode_snappy()

    conn =
      conn
      |> put_req_header("content-type", "application/x-protobuf")
      |> put_req_header("content-encoding", "snappy")
      |> put_req_header("x-scope-orgid", "acme")
      |> post(~p"/loki/api/v1/push", body)

    assert conn.status == 204

    assert {:ok,
            [
              %Log{
                service: "api",
                severity_text: "info",
                body: "hello proto",
                trace_id: "abc",
                attributes: %{"user_id" => "u1"},
                resource: %{"service_name" => "api", "level" => "info"}
              }
            ]} = Storage.query("acme")
  end

  test "POST /loki/api/v1/push treats an absent Content-Encoding on protobuf as snappy",
       %{conn: conn} do
    # Alloy's default push_config sends Snappy-compressed protobuf with
    # no Content-Encoding header, matching how Loki itself defaults. A
    # stock Alloy config must land without operator changes.
    body = proto_payload() |> encode_proto() |> encode_snappy()

    conn =
      conn
      |> put_req_header("content-type", "application/x-protobuf")
      |> put_req_header("x-scope-orgid", "acme")
      |> post(~p"/loki/api/v1/push", body)

    assert conn.status == 204
    assert {:ok, [%Log{body: "hello"}]} = Storage.query("acme")
  end

  test "POST /loki/api/v1/push surfaces protobuf decode rejects in the header",
       %{conn: conn} do
    request = %PushProto.PushRequest{
      streams: [
        %PushProto.Stream{
          labels: ~s({service_name="api"}),
          entries: [
            %PushProto.Entry{
              timestamp: %PushProto.Timestamp{seconds: 1, nanos: 0},
              line: "ok",
              structured_metadata: []
            },
            %PushProto.Entry{
              timestamp: nil,
              line: "no ts",
              structured_metadata: []
            }
          ]
        }
      ]
    }

    body = request |> encode_proto() |> encode_snappy()

    conn =
      conn
      |> put_req_header("content-type", "application/x-protobuf")
      |> post(~p"/loki/api/v1/push", body)

    assert conn.status == 204
    assert get_resp_header(conn, "x-pulso-rejected-records") == ["1"]
    assert {:ok, [%Log{body: "ok"}]} = Storage.query("default")
  end

  test "POST /loki/api/v1/push returns 400 for a body that is not valid Snappy",
       %{conn: conn} do
    conn =
      conn
      |> put_req_header("content-type", "application/x-protobuf")
      |> post(~p"/loki/api/v1/push", "not snappy data at all")

    assert json_response(conn, 400) == %{"error" => "invalid_snappy"}
  end

  test "POST /loki/api/v1/push returns 400 for valid Snappy that is not a PushRequest",
       %{conn: conn} do
    conn =
      conn
      |> put_req_header("content-type", "application/x-protobuf")
      |> post(~p"/loki/api/v1/push", encode_snappy("random bytes"))

    assert json_response(conn, 400) == %{"error" => "invalid_protobuf"}
  end

  test "POST /loki/api/v1/push refuses a snappy bomb before decompressing",
       %{conn: conn} do
    # Snappy tops out around 21x on zeros, so 64 MiB compresses to ~3 MiB:
    # under the compressed-read cap, which means this reaches the length
    # header check rather than being stopped by the read cap first.
    huge = 64 * 1024 * 1024
    {:ok, bomb} = :snappyer.compress(:binary.copy(<<0>>, huge))
    assert byte_size(bomb) < 4 * 1024 * 1024
    assert :snappyer.uncompressed_length(bomb) == {:ok, huge}

    conn =
      conn
      |> put_req_header("content-type", "application/x-protobuf")
      |> post(~p"/loki/api/v1/push", bomb)

    assert json_response(conn, 413) == %{"error" => "payload_too_large"}
    assert {:ok, []} = Storage.query("default")
  end

  test "POST /loki/api/v1/push rejects protobuf with gzip Content-Encoding as 415",
       %{conn: conn} do
    # We may add gzip-over-protobuf later, but Alloy never sends it and
    # returning 415 keeps the "missing feature" case distinct from the
    # 400-invalid_snappy path so operators can tell them apart.
    body = proto_payload() |> encode_proto() |> :zlib.gzip()

    conn =
      conn
      |> put_req_header("content-type", "application/x-protobuf")
      |> put_req_header("content-encoding", "gzip")
      |> post(~p"/loki/api/v1/push", body)

    assert json_response(conn, 415) == %{"error" => "unsupported_content_type"}
  end

  describe "with Pulso.Auth.SharedSecret enabled" do
    setup do
      hex = Base.encode16(:crypto.hash(:sha256, "the-token"), case: :lower)

      Application.put_env(:pulso, Pulso.Auth,
        module: SharedSecret,
        tokens: %{"acme" => "sha256$#{hex}"}
      )

      on_exit(fn ->
        Application.put_env(:pulso, Pulso.Auth, module: Open)
      end)

      :ok
    end

    test "accepts the correct token", %{conn: conn} do
      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> put_req_header("x-scope-orgid", "acme")
        |> put_req_header("authorization", "Bearer the-token")
        |> post(~p"/loki/api/v1/push", payload())

      assert conn.status == 204
      assert {:ok, [%Log{service: "api", body: "hello"}]} = Storage.query("acme")
    end

    test "rejects a request with a bad token", %{conn: conn} do
      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> put_req_header("x-scope-orgid", "acme")
        |> put_req_header("authorization", "Bearer wrong")
        |> post(~p"/loki/api/v1/push", payload())

      assert json_response(conn, 401) == %{"error" => "invalid_token"}
      assert {:ok, []} = Storage.query("acme")
    end

    test "rejects a tenant with no configured token", %{conn: conn} do
      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> put_req_header("x-scope-orgid", "unknown")
        |> put_req_header("authorization", "Bearer the-token")
        |> post(~p"/loki/api/v1/push", payload())

      assert json_response(conn, 401) == %{"error" => "unknown_tenant"}
    end

    test "rejects a request with no authorization header", %{conn: conn} do
      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> put_req_header("x-scope-orgid", "acme")
        |> post(~p"/loki/api/v1/push", payload())

      assert json_response(conn, 401) == %{"error" => "missing_token"}
    end
  end
end
