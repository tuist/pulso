defmodule PulsoWeb.OTLPControllerTest do
  use PulsoWeb.ConnCase, async: false

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
end
