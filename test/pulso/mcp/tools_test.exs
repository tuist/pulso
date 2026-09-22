defmodule Pulso.MCP.ToolsTest do
  use ExUnit.Case, async: false

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
end
