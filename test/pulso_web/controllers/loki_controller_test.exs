defmodule PulsoWeb.LokiControllerTest do
  use PulsoWeb.ConnCase, async: false

  alias Pulso.Auth.Open
  alias Pulso.Auth.SharedSecret
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

  test "POST /loki/api/v1/push rejects Snappy protobuf content-type with 415",
       %{conn: conn} do
    # Alloy's default push_config sends Snappy-framed protobuf. Until we
    # add a decoder for that, respond 415 explicitly rather than 400 —
    # the operator needs to know this is a missing feature, not a bad
    # body.
    conn =
      conn
      |> put_req_header("content-type", "application/x-protobuf")
      |> post(~p"/loki/api/v1/push", "irrelevant")

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
