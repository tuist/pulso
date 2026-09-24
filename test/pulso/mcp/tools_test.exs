defmodule Pulso.MCP.ToolsTest do
  use ExUnit.Case, async: false

  alias Pulso.Auth.Open
  alias Pulso.Auth.SharedSecret
  alias Pulso.MCP.Tools
  alias Pulso.Record.Log
  alias Pulso.Storage
  alias Pulso.Storage.Memory

  setup do
    Memory.reset()
    :ok
  end

  test "lists the query_logs tool" do
    tools = Tools.list()
    assert [%{"name" => "query_logs", "inputSchema" => schema}] = tools
    assert schema["required"] == ["tenant"]
  end

  test "query_logs returns records for the tenant" do
    :ok =
      Storage.append("acme", [
        %Log{timestamp_ns: 10, service: "api", body: "one"},
        %Log{timestamp_ns: 20, service: "web", body: "two"}
      ])

    assert {:ok, [%{"type" => "text", "text" => text}]} = Tools.call("query_logs", %{"tenant" => "acme"})
    assert [%{"body" => "two"}, %{"body" => "one"}] = Jason.decode!(text)
  end

  test "query_logs applies service and limit filters" do
    :ok =
      Storage.append("acme", [
        %Log{timestamp_ns: 10, service: "api", body: "a"},
        %Log{timestamp_ns: 20, service: "web", body: "b"},
        %Log{timestamp_ns: 30, service: "api", body: "c"}
      ])

    assert {:ok, [%{"text" => text}]} =
             Tools.call("query_logs", %{"tenant" => "acme", "service" => "api", "limit" => 1})

    assert [%{"body" => "c", "service" => "api"}] = Jason.decode!(text)
  end

  test "query_logs errors when tenant is missing" do
    assert {:error, {:invalid_arguments, _}} = Tools.call("query_logs", %{})
  end

  describe "auth on the read path" do
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

    test "rejects a query_logs call with no bearer token" do
      Storage.append("acme", [%Log{timestamp_ns: 1}])

      assert {:error, {:unauthorized, :missing_token}} =
               Tools.call("query_logs", %{"tenant" => "acme"}, %{conn: %Plug.Conn{}})
    end

    test "rejects a query_logs call with a bad token" do
      Storage.append("acme", [%Log{timestamp_ns: 1}])

      conn = %Plug.Conn{} |> Plug.Conn.put_req_header("authorization", "Bearer wrong")

      assert {:error, {:unauthorized, :invalid_token}} =
               Tools.call("query_logs", %{"tenant" => "acme"}, %{conn: conn})
    end

    test "accepts a query_logs call with the correct token" do
      Storage.append("acme", [%Log{timestamp_ns: 1, body: "ok"}])

      conn = %Plug.Conn{} |> Plug.Conn.put_req_header("authorization", "Bearer the-token")

      assert {:ok, [%{"text" => text}]} =
               Tools.call("query_logs", %{"tenant" => "acme"}, %{conn: conn})

      assert [%{"body" => "ok"}] = Jason.decode!(text)
    end
  end
end
