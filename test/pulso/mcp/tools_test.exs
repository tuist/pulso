defmodule Pulso.MCP.ToolsTest do
  use ExUnit.Case, async: false

  alias Pulso.Auth.Open
  alias Pulso.Auth.SharedSecret
  alias Pulso.MCP.Tools
  alias Pulso.Record.Log
  alias Pulso.Record.MetricSample
  alias Pulso.Storage
  alias Pulso.Storage.Memory

  setup do
    Memory.reset()
    :ok
  end

  test "lists query_logs, query_metrics, and query_logql tools" do
    tools = Tools.list()
    names = Enum.map(tools, & &1["name"])
    assert "query_logs" in names
    assert "query_metrics" in names
    assert "query_logql" in names

    query_logs = Enum.find(tools, &(&1["name"] == "query_logs"))
    assert query_logs["inputSchema"]["required"] == ["tenant"]

    query_metrics = Enum.find(tools, &(&1["name"] == "query_metrics"))
    assert query_metrics["inputSchema"]["required"] == ["tenant"]

    query_logql = Enum.find(tools, &(&1["name"] == "query_logql"))
    assert query_logql["inputSchema"]["required"] == ["tenant", "query"]
  end

  describe "query_logql" do
    test "returns a Loki-shaped streams envelope for a log query" do
      :ok =
        Storage.append(:logs, "acme", [
          %Log{timestamp_ns: 10, service: "api", body: "timeout"},
          %Log{timestamp_ns: 20, service: "api", body: "ok"}
        ])

      assert {:ok, [%{"type" => "text", "text" => text}]} =
               Tools.call("query_logql", %{
                 "tenant" => "acme",
                 "query" => ~s({service="api"} |= "timeout")
               })

      envelope = JSON.decode!(text)
      assert envelope["status"] == "success"
      assert envelope["data"]["resultType"] == "streams"
      streams = envelope["data"]["result"]
      lines = for %{"values" => vs} <- streams, [_ts, line] <- vs, do: line
      assert lines == ["timeout"]
    end

    test "returns a matrix envelope for a range metric query" do
      :ok =
        Storage.append(:logs, "acme", [
          %Log{timestamp_ns: 1_000_000_000, service: "api", body: "a"},
          %Log{timestamp_ns: 2_000_000_000, service: "api", body: "b"},
          %Log{timestamp_ns: 3_000_000_000, service: "api", body: "c"}
        ])

      assert {:ok, [%{"text" => text}]} =
               Tools.call("query_logql", %{
                 "tenant" => "acme",
                 "query" => "count_over_time({service=\"api\"}[1s])",
                 "start_ts_ns" => 1_000_000_000,
                 "end_ts_ns" => 3_000_000_000,
                 "step_ms" => 1000
               })

      envelope = JSON.decode!(text)
      assert envelope["data"]["resultType"] == "matrix"
    end

    test "surfaces LogQL parse errors" do
      assert {:error, {:invalid_arguments, {:logql_parse_error, _}}} =
               Tools.call("query_logql", %{"tenant" => "acme", "query" => "not a query"})
    end

    test "errors on missing tenant or query" do
      assert {:error, {:invalid_arguments, _}} = Tools.call("query_logql", %{})
      assert {:error, {:invalid_arguments, _}} = Tools.call("query_logql", %{"tenant" => "acme"})
    end
  end

  test "query_logs returns records for the tenant" do
    :ok =
      Storage.append(:logs, "acme", [
        %Log{timestamp_ns: 10, service: "api", body: "one"},
        %Log{timestamp_ns: 20, service: "web", body: "two"}
      ])

    assert {:ok, [%{"type" => "text", "text" => text}]} = Tools.call("query_logs", %{"tenant" => "acme"})
    assert [%{"body" => "two"}, %{"body" => "one"}] = JSON.decode!(text)
  end

  test "query_logs applies service and limit filters" do
    :ok =
      Storage.append(:logs, "acme", [
        %Log{timestamp_ns: 10, service: "api", body: "a"},
        %Log{timestamp_ns: 20, service: "web", body: "b"},
        %Log{timestamp_ns: 30, service: "api", body: "c"}
      ])

    assert {:ok, [%{"text" => text}]} =
             Tools.call("query_logs", %{"tenant" => "acme", "service" => "api", "limit" => 1})

    assert [%{"body" => "c", "service" => "api"}] = JSON.decode!(text)
  end

  test "query_logs errors when tenant is missing" do
    assert {:error, {:invalid_arguments, _}} = Tools.call("query_logs", %{})
  end

  describe "query_metrics" do
    test "returns samples for the tenant filtered by label matchers" do
      :ok =
        Storage.append(:metrics, "acme", [
          %MetricSample{timestamp_ns: 10, value: 1.0, labels: %{"__name__" => "up", "svc" => "api"}},
          %MetricSample{timestamp_ns: 20, value: 2.0, labels: %{"__name__" => "up", "svc" => "web"}}
        ])

      assert {:ok, [%{"type" => "text", "text" => text}]} =
               Tools.call("query_metrics", %{
                 "tenant" => "acme",
                 "matchers" => [%{"name" => "svc", "op" => "=", "value" => "api"}]
               })

      assert [sample] = JSON.decode!(text)
      assert sample["labels"]["svc"] == "api"
      assert sample["value"] == 1.0
    end

    test "errors on an unknown matcher op" do
      assert {:error, {:invalid_arguments, _}} =
               Tools.call("query_metrics", %{
                 "tenant" => "acme",
                 "matchers" => [%{"name" => "svc", "op" => "???", "value" => "api"}]
               })
    end

    test "errors when tenant is missing" do
      assert {:error, {:invalid_arguments, _}} = Tools.call("query_metrics", %{})
    end
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
      Storage.append(:logs, "acme", [%Log{timestamp_ns: 1}])

      assert {:error, {:unauthorized, :missing_token}} =
               Tools.call("query_logs", %{"tenant" => "acme"}, %{conn: %Plug.Conn{}})
    end

    test "fails closed when no conn is passed at all" do
      # Previous version had a "no conn = allow" fallback for in-process
      # callers. That was a bypass: any code path that forgot the context
      # would silently read another tenant's data under shared-secret
      # auth. Now the fallback constructs an empty %Plug.Conn{}, which
      # SharedSecret.verify sees as :missing_token.
      Storage.append(:logs, "acme", [%Log{timestamp_ns: 1}])

      assert {:error, {:unauthorized, :missing_token}} =
               Tools.call("query_logs", %{"tenant" => "acme"}, %{})
    end

    test "rejects a query_logs call with a bad token" do
      Storage.append(:logs, "acme", [%Log{timestamp_ns: 1}])

      conn = %Plug.Conn{} |> Plug.Conn.put_req_header("authorization", "Bearer wrong")

      assert {:error, {:unauthorized, :invalid_token}} =
               Tools.call("query_logs", %{"tenant" => "acme"}, %{conn: conn})
    end

    test "accepts a query_logs call with the correct token" do
      Storage.append(:logs, "acme", [%Log{timestamp_ns: 1, body: "ok"}])

      conn = %Plug.Conn{} |> Plug.Conn.put_req_header("authorization", "Bearer the-token")

      assert {:ok, [%{"text" => text}]} =
               Tools.call("query_logs", %{"tenant" => "acme"}, %{conn: conn})

      assert [%{"body" => "ok"}] = JSON.decode!(text)
    end
  end
end
