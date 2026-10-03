defmodule PulsoWeb.MCPControllerTest do
  use PulsoWeb.ConnCase, async: false

  alias Pulso.Auth
  alias Pulso.Auth.SharedSecret
  alias Pulso.Record.MetricSample
  alias Pulso.Storage
  alias Pulso.Storage.Memory
  alias PulsoWeb.Plugs.MCPTransport

  test "tool discovery exposes read-only annotations", %{conn: conn} do
    response = request(conn, "tools/list", %{})
    assert length(response["result"]["tools"]) == 4

    for tool <- response["result"]["tools"] do
      assert tool["annotations"]["readOnlyHint"] == true
    end
  end

  test "malformed arguments return an explicit error without failing the request", %{conn: conn} do
    for arguments <- [nil, [], %{"tenant" => "acme", "limit" => "bad"}] do
      response = request(conn, "tools/call", %{"name" => "query_logs", "arguments" => arguments})
      assert response["id"] == 1
      assert response["result"]["isError"] == true
      assert hd(response["result"]["content"])["text"] =~ "invalid_arguments"
    end
  end

  test "every query tool fails closed without tenant credentials", %{conn: conn} do
    original = Application.fetch_env!(:pulso, Auth)
    Application.put_env(:pulso, Auth, module: SharedSecret, tokens: %{})
    on_exit(fn -> Application.put_env(:pulso, Auth, original) end)

    for name <- ["query_logs", "query_metrics", "query_logql", "query_promql"] do
      response =
        request(conn, "tools/call", %{
          "name" => name,
          "arguments" => %{"tenant" => "acme", "query" => "not a valid query"}
        })

      assert response["result"]["isError"] == true
      assert hd(response["result"]["content"])["text"] =~ "unauthorized"
    end
  end

  test "invalid patterns and excessive log metric ranges return tool errors", %{conn: conn} do
    Memory.reset()
    :ok = Storage.append(:metrics, "acme", [%MetricSample{timestamp_ns: 1, value: 1.0, labels: %{"a" => "x"}}])

    for op <- ["=~", "!~"] do
      response =
        request(conn, "tools/call", %{
          "name" => "query_metrics",
          "arguments" => %{"tenant" => "acme", "matchers" => [%{"name" => "a", "op" => op, "value" => "("}]}
        })

      assert response["result"]["isError"] == true
      assert hd(response["result"]["content"])["text"] =~ "invalid_arguments"
    end

    response =
      request(conn, "tools/call", %{
        "name" => "query_logql",
        "arguments" => %{
          "tenant" => "acme",
          "query" => ~s|rate({service="api"}[5m])|,
          "start_ts_ns" => 0,
          "end_ts_ns" => 9_000_000_000_000_000_000,
          "step_ms" => 1
        }
      })

    assert response["result"]["isError"] == true
    assert hd(response["result"]["content"])["text"] =~ "too_many_steps"
  end

  test "tenant authorization precedes query argument validation", %{conn: conn} do
    original = Application.fetch_env!(:pulso, Auth)
    Application.put_env(:pulso, Auth, module: SharedSecret, tokens: %{})
    on_exit(fn -> Application.put_env(:pulso, Auth, original) end)

    for name <- ["query_logs", "query_metrics", "query_logql", "query_promql"] do
      response =
        request(conn, "tools/call", %{
          "name" => name,
          "arguments" => %{"tenant" => "acme", "start_ts_ns" => "bad", "query" => 1}
        })

      assert response["result"]["isError"] == true
      assert hd(response["result"]["content"])["text"] =~ "unauthorized"
    end
  end

  describe "lifecycle" do
    test "initialize echoes a supported protocol version", %{conn: conn} do
      for version <- Pulso.MCP.supported_protocol_versions() do
        response = request(conn, "initialize", %{"protocolVersion" => version})
        assert response["result"]["protocolVersion"] == version
        assert response["result"]["serverInfo"]["name"] == "pulso"
      end
    end

    test "initialize answers with the latest version for an unknown one", %{conn: conn} do
      response = request(conn, "initialize", %{"protocolVersion" => "1999-01-01"})
      assert response["result"]["protocolVersion"] == "2025-06-18"
    end

    test "initialize rejects a missing or malformed protocol version", %{conn: conn} do
      for params <- [%{}, %{"protocolVersion" => 1}] do
        assert request(conn, "initialize", params)["error"]["code"] == -32_602
      end
    end

    test "non-object params are an invalid params error", %{conn: conn} do
      assert request(conn, "tools/list", [1])["error"]["code"] == -32_602
    end

    test "notifications and client responses are acknowledged with 202", %{conn: conn} do
      for message <- [
            %{"jsonrpc" => "2.0", "method" => "notifications/initialized"},
            %{"jsonrpc" => "2.0", "id" => 7, "result" => %{}},
            %{"jsonrpc" => "2.0", "id" => 7, "error" => %{"code" => -1, "message" => "no"}}
          ] do
        conn = conn |> put_req_header("content-type", "application/json") |> post("/mcp", JSON.encode!(message))
        assert response(conn, 202) == ""
      end
    end

    test "batches are rejected for 2025-06-18", %{conn: conn} do
      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> put_req_header("mcp-protocol-version", "2025-06-18")
        |> post("/mcp", JSON.encode!([%{"jsonrpc" => "2.0", "id" => 1, "method" => "ping"}]))

      assert json_response(conn, 400)["error"]["code"] == -32_600
    end

    test "batches are answered for 2025-03-26, explicit or assumed" do
      batch = JSON.encode!([%{"jsonrpc" => "2.0", "id" => 1, "method" => "ping"}])

      for headers <- [[], [{"mcp-protocol-version", "2025-03-26"}]] do
        conn =
          Enum.reduce(headers, put_req_header(build_conn(), "content-type", "application/json"), fn {k, v}, c ->
            put_req_header(c, k, v)
          end)

        assert [%{"id" => 1}] = conn |> post("/mcp", batch) |> json_response(200)
      end
    end

    test "batches are answered, and notification-only batches get 202", %{conn: conn} do
      batch = [
        %{"jsonrpc" => "2.0", "id" => 1, "method" => "ping"},
        %{"jsonrpc" => "2.0", "method" => "notifications/initialized"}
      ]

      conn = conn |> put_req_header("content-type", "application/json") |> post("/mcp", JSON.encode!(batch))
      assert [%{"id" => 1, "result" => %{}}] = json_response(conn, 200)

      conn =
        build_conn()
        |> put_req_header("content-type", "application/json")
        |> post("/mcp", JSON.encode!([%{"jsonrpc" => "2.0", "method" => "notifications/initialized"}]))

      assert response(conn, 202) == ""

      conn = build_conn() |> put_req_header("content-type", "application/json") |> post("/mcp", "[]")
      assert json_response(conn, 400)["error"]["code"] == -32_600
    end
  end

  describe "transport" do
    test "GET and DELETE are not supported, even when asking for an event stream", %{conn: conn} do
      conn = conn |> put_req_header("accept", "text/event-stream") |> get("/mcp")
      assert response(conn, 405) == ""
      assert get_resp_header(conn, "allow") == ["POST"]

      conn = build_conn() |> delete("/mcp")
      assert response(conn, 405) == ""
    end

    test "a supported MCP-Protocol-Version header is accepted", %{conn: conn} do
      conn = put_req_header(conn, "mcp-protocol-version", "2025-06-18")
      assert request(conn, "ping", %{})["result"] == %{}
    end

    test "an unsupported MCP-Protocol-Version header is rejected with 400", %{conn: conn} do
      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> put_req_header("mcp-protocol-version", "1999-01-01")
        |> post("/mcp", JSON.encode!(%{"jsonrpc" => "2.0", "id" => 1, "method" => "ping"}))

      assert json_response(conn, 400)["error"]["message"] =~ "Unsupported MCP-Protocol-Version"
    end

    test "requests with an unlisted Origin are rejected with 403", %{conn: conn} do
      conn =
        conn
        |> put_req_header("content-type", "application/json")
        |> put_req_header("origin", "https://evil.example")
        |> post("/mcp", JSON.encode!(%{"jsonrpc" => "2.0", "id" => 1, "method" => "ping"}))

      assert json_response(conn, 403)["error"]["message"] == "Origin not allowed"
    end

    test "requests from an allowed Origin are served", %{conn: conn} do
      original = Application.get_env(:pulso, MCPTransport)
      Application.put_env(:pulso, MCPTransport, allowed_origins: ["https://app.example"])

      on_exit(fn ->
        if original,
          do: Application.put_env(:pulso, MCPTransport, original),
          else: Application.delete_env(:pulso, MCPTransport)
      end)

      assert request(put_req_header(conn, "origin", "https://app.example"), "ping", %{})["result"] == %{}
    end
  end

  defp request(conn, method, params) do
    conn
    |> put_req_header("content-type", "application/json")
    |> post("/mcp", JSON.encode!(%{"jsonrpc" => "2.0", "id" => 1, "method" => method, "params" => params}))
    |> json_response(200)
  end
end
