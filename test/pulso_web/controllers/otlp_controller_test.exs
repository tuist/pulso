defmodule PulsoWeb.OTLPControllerTest do
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

  defp payload(ts \\ 1_700_000_000_000_000_000, body \\ "hello") do
    %{
      "resourceLogs" => [
        %{
          "resource" => %{
            "attributes" => [
              %{"key" => "service.name", "value" => %{"stringValue" => "api"}}
            ]
          },
          "scopeLogs" => [
            %{
              "logRecords" => [
                %{
                  "timeUnixNano" => Integer.to_string(ts),
                  "severityText" => "INFO",
                  "body" => %{"stringValue" => body}
                }
              ]
            }
          ]
        }
      ]
    }
  end

  test "POST /v1/logs stores records under the tenant from X-Scope-OrgID", %{conn: conn} do
    conn =
      conn
      |> put_req_header("content-type", "application/json")
      |> put_req_header("x-scope-orgid", "acme")
      |> post(~p"/v1/logs", payload())

    assert json_response(conn, 200) == %{}
    assert {:ok, [%Log{service: "api", body: "hello"}]} = Storage.query("acme")
    assert {:ok, []} = Storage.query("other")
  end

  test "POST /v1/logs falls back to the default tenant when no header is set", %{conn: conn} do
    conn =
      conn
      |> put_req_header("content-type", "application/json")
      |> post(~p"/v1/logs", payload())

    assert json_response(conn, 200) == %{}
    assert {:ok, [%Log{body: "hello"}]} = Storage.query("default")
  end

  test "POST /v1/logs accepts an empty batch without error", %{conn: conn} do
    conn =
      conn
      |> put_req_header("content-type", "application/json")
      |> post(~p"/v1/logs", %{"resourceLogs" => []})

    assert json_response(conn, 200) == %{}
    assert {:ok, []} = Storage.query("default")
  end

  test "POST /v1/logs returns 400 for a tenant name that would escape the prefix",
       %{conn: conn} do
    conn =
      conn
      |> put_req_header("content-type", "application/json")
      |> put_req_header("x-scope-orgid", "bad/name")
      |> post(~p"/v1/logs", payload())

    assert json_response(conn, 400) == %{"error" => "invalid_tenant"}
  end

  test "POST /v1/logs surfaces rejected records via partialSuccess per the OTLP spec",
       %{conn: conn} do
    # One valid record + one missing timeUnixNano. The receiver must
    # signal the drop rather than acknowledging silently — otherwise the
    # sender never retries the record that was never stored.
    payload = %{
      "resourceLogs" => [
        %{
          "scopeLogs" => [
            %{
              "logRecords" => [
                %{"timeUnixNano" => "1700000000000000000", "body" => %{"stringValue" => "ok"}},
                %{"body" => %{"stringValue" => "missing ts"}}
              ]
            }
          ]
        }
      ]
    }

    conn =
      conn
      |> put_req_header("content-type", "application/json")
      |> post(~p"/v1/logs", payload)

    body = json_response(conn, 200)
    assert %{"partialSuccess" => %{"rejectedLogRecords" => 1}} = body
    assert body["partialSuccess"]["errorMessage"] =~ "timeUnixNano"
  end

  test "POST /v1/logs passes the Idempotency-Key header through", %{conn: conn} do
    # The Idempotency-Key header should end up in Storage.append opts. Two
    # POSTs with the same key + same payload are the same write to the store,
    # so downstream queries see one record even if two arrived over the wire.
    # (The Memory adapter ignores :idempotency_key, so we only assert the
    # controller accepts the header; S3 adapter coverage of the actual
    # dedup lives in s3_test.exs.)
    conn =
      conn
      |> put_req_header("content-type", "application/json")
      |> put_req_header("idempotency-key", "req-1")
      |> post(~p"/v1/logs", payload())

    assert json_response(conn, 200) == %{}
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
        |> post(~p"/v1/logs", payload())

      assert json_response(conn, 200) == %{}
      assert {:ok, [%Log{service: "api", body: "hello"}]} = Storage.query("acme")
    end

    test "rejects a request with a bad token", %{conn: conn} do
      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> put_req_header("x-scope-orgid", "acme")
        |> put_req_header("authorization", "Bearer wrong")
        |> post(~p"/v1/logs", payload())

      assert json_response(conn, 401) == %{"error" => "invalid_token"}
      assert {:ok, []} = Storage.query("acme")
    end

    test "rejects a tenant with no configured token", %{conn: conn} do
      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> put_req_header("x-scope-orgid", "unknown")
        |> put_req_header("authorization", "Bearer the-token")
        |> post(~p"/v1/logs", payload())

      assert json_response(conn, 401) == %{"error" => "unknown_tenant"}
    end

    test "rejects a request with no authorization header", %{conn: conn} do
      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> put_req_header("x-scope-orgid", "acme")
        |> post(~p"/v1/logs", payload())

      assert json_response(conn, 401) == %{"error" => "missing_token"}
    end
  end
end
