defmodule Pulso.MCP.ToolsTest do
  use ExUnit.Case, async: false

  alias Pulso.Auth.Open
  alias Pulso.Auth.SharedSecret
  alias Pulso.Codec.NIF
  alias Pulso.MCP.Tools
  alias Pulso.Record.Log
  alias Pulso.Record.MetricSample
  alias Pulso.Storage
  alias Pulso.Storage.Memory
  alias Pulso.Test.NativeQueryStorage

  setup do
    Memory.reset()
    :ok
  end

  test "lists all four query tools" do
    tools = Tools.list()
    names = Enum.map(tools, & &1["name"])
    assert "query_logs" in names
    assert "query_metrics" in names
    assert "query_logql" in names
    assert "query_promql" in names

    query_logs = Enum.find(tools, &(&1["name"] == "query_logs"))
    assert query_logs["inputSchema"]["required"] == ["tenant"]

    query_metrics = Enum.find(tools, &(&1["name"] == "query_metrics"))
    assert query_metrics["inputSchema"]["required"] == ["tenant"]

    query_logql = Enum.find(tools, &(&1["name"] == "query_logql"))
    assert query_logql["inputSchema"]["required"] == ["tenant", "query"]
  end

  describe "tool contracts" do
    test "discovery declares every query read-only" do
      {:reply, response} = Pulso.MCP.dispatch(%{"id" => 1, "method" => "tools/list"})
      tools = response["result"]["tools"]
      assert length(tools) == 4

      for tool <- tools do
        assert tool["annotations"] == %{
                 "readOnlyHint" => true,
                 "destructiveHint" => false,
                 "idempotentHint" => true,
                 "openWorldHint" => false
               }
      end
    end

    test "all tools reject malformed arguments and out-of-range timestamps" do
      for tool <- Tools.list() do
        name = tool["name"]
        valid = %{"tenant" => "acme", "query" => expression(name)}

        for args <- [nil, [], "invalid", 1, %{}, %{"tenant" => nil}, %{"tenant" => ""}] do
          assert {:error, {:invalid_arguments, _}} = Tools.call(name, args)
        end

        for field <- ["start_ts_ns", "end_ts_ns"],
            value <- ["10", 1.5, true, 9_223_372_036_854_775_808, -9_223_372_036_854_775_809] do
          assert {:error, {:invalid_arguments, _}} = Tools.call(name, Map.put(valid, field, value))
        end

        assert {:error, {:invalid_arguments, _}} =
                 Tools.call(name, Map.merge(valid, %{"start_ts_ns" => 20, "end_ts_ns" => 10}))
      end
    end

    test "limit boundaries apply to every tool advertising a limit" do
      for name <- ["query_logs", "query_metrics", "query_logql"] do
        valid = %{"tenant" => "acme", "query" => expression(name)}

        for value <- ["1", 1.5, 0, -1, 5001] do
          assert {:error, {:invalid_arguments, _}} = Tools.call(name, Map.put(valid, "limit", value))
        end

        for value <- [1, 5000] do
          assert {:ok, _} = Tools.call(name, Map.put(valid, "limit", value))
        end
      end
    end

    test "query strings and steps have consistent types and bounds" do
      for name <- ["query_logql", "query_promql"] do
        valid = %{"tenant" => "acme", "query" => expression(name)}

        for value <- [nil, 1, [], ""] do
          assert {:error, {:invalid_arguments, _}} = Tools.call(name, Map.put(valid, "query", value))
        end

        for value <- ["1", 1.5, 0, -1] do
          assert {:error, {:invalid_arguments, _}} = Tools.call(name, Map.put(valid, "step_ms", value))
        end
      end
    end

    test "rejects invalid service, direction, and nested matcher values" do
      assert {:error, {:invalid_arguments, _}} =
               Tools.call("query_logs", %{"tenant" => "acme", "service" => 1})

      for value <- [1, "sideways"] do
        assert {:error, {:invalid_arguments, _}} =
                 Tools.call("query_logql", %{
                   "tenant" => "acme",
                   "query" => expression("query_logql"),
                   "direction" => value
                 })
      end

      for matchers <- [
            %{},
            [nil],
            [%{}],
            [%{"name" => 1, "op" => "=", "value" => "x"}],
            [%{"name" => "a", "op" => "=", "value" => nil}],
            [%{"name" => "a", "op" => "???", "value" => "x"}]
          ] do
        assert {:error, {:invalid_arguments, _}} =
                 Tools.call("query_metrics", %{"tenant" => "acme", "matchers" => matchers})
      end
    end

    test "Prometheus range queries require both times and a step" do
      for fields <- [
            %{"start_ts_ns" => 0},
            %{"step_ms" => 1},
            %{"start_ts_ns" => 0, "end_ts_ns" => 10},
            %{"end_ts_ns" => 10, "step_ms" => 1}
          ] do
        assert {:error, {:invalid_arguments, _}} =
                 Tools.call("query_promql", Map.merge(%{"tenant" => "acme", "query" => "up"}, fields))
      end

      assert {:ok, _} =
               Tools.call("query_promql", %{
                 "tenant" => "acme",
                 "query" => "up",
                 "start_ts_ns" => 0,
                 "end_ts_ns" => 1_000_000,
                 "step_ms" => 1
               })
    end

    test "malformed calls return a tool error through the dispatcher" do
      for arguments <- [nil, [], %{"tenant" => "acme", "limit" => "bad"}] do
        {:reply, response} =
          Pulso.MCP.dispatch(%{
            "id" => 1,
            "method" => "tools/call",
            "params" => %{"name" => "query_logs", "arguments" => arguments}
          })

        assert response["result"]["isError"]
        assert [%{"type" => "text", "text" => text}] = response["result"]["content"]
        assert text =~ "invalid_arguments"
      end
    end

    test "omitted optional fields and extra fields preserve existing calls" do
      for name <- ["query_logs", "query_metrics", "query_logql", "query_promql"] do
        assert {:ok, _} =
                 Tools.call(name, %{
                   "tenant" => "acme",
                   "query" => expression(name),
                   "future_option" => true
                 })
      end
    end

    test "optional nulls preserve defaults while required nulls are rejected" do
      for tool <- Tools.list() do
        args = %{"tenant" => "acme", "query" => expression(tool["name"])}
        required = tool["inputSchema"]["required"]

        for {field, _schema} <- tool["inputSchema"]["properties"], field not in required do
          assert {:ok, _} = Tools.call(tool["name"], Map.put(args, field, nil))
        end
      end
    end

    test "integer-valued decimal numbers follow the published integer schema" do
      for name <- ["query_logs", "query_metrics", "query_logql", "query_promql"] do
        args = %{"tenant" => "acme", "query" => expression(name), "end_ts_ns" => 1_000_000.0}
        assert {:ok, _} = Tools.call(name, args)
      end

      assert {:ok, _} = Tools.call("query_logs", %{"tenant" => "acme", "limit" => 1.0})

      assert {:ok, _} =
               Tools.call("query_promql", %{
                 "tenant" => "acme",
                 "query" => "up",
                 "start_ts_ns" => 0.0,
                 "end_ts_ns" => 1_000_000.0,
                 "step_ms" => 1.0
               })
    end

    test "tool schemas only use enforced validation keywords" do
      for tool <- Tools.list(), do: assert_supported_schema(tool["inputSchema"])
    end

    test "step conversion stays inside signed nanoseconds" do
      for name <- ["query_logql", "query_promql"] do
        args = %{"tenant" => "acme", "query" => expression(name), "start_ts_ns" => 0, "end_ts_ns" => 0}
        assert {:ok, _} = Tools.call(name, Map.put(args, "step_ms", 9_223_372_036_854))
        assert {:error, {:invalid_arguments, _}} = Tools.call(name, Map.put(args, "step_ms", 9_223_372_036_855))
      end
    end

    test "metric regular expressions fail explicitly before storage, with or without samples" do
      for samples <- [[], [%MetricSample{timestamp_ns: 1, value: 1.0, labels: %{"__name__" => "up"}}]] do
        Memory.reset()
        :ok = Storage.append(:metrics, "acme", samples)

        for op <- ["=~", "!~"], pattern <- ["(", "[", String.duplicate("x", 1025)] do
          assert {:error, {:invalid_arguments, _}} =
                   Tools.call("query_metrics", %{
                     "tenant" => "acme",
                     "matchers" => [%{"name" => "__name__", "op" => op, "value" => pattern}]
                   })
        end
      end

      assert {:ok, _} =
               Tools.call("query_metrics", %{
                 "tenant" => "acme",
                 "matchers" => [%{"name" => "__name__", "op" => "=~", "value" => "u.*"}]
               })

      for op <- ["=~", "!~"] do
        assert {:ok, _} =
                 Tools.call("query_metrics", %{
                   "tenant" => "acme",
                   "matchers" => [%{"name" => "__name__", "op" => op, "value" => String.duplicate("x", 1024)}]
                 })
      end
    end

    test "log metric ranges reject excessive steps before constructing the timeline" do
      for finish <- [11_000_000_000, 9_000_000_000_000_000_000] do
        assert {:error, _} =
                 Tools.call("query_logql", %{
                   "tenant" => "acme",
                   "query" => ~s|rate({service="api"}[5m])|,
                   "start_ts_ns" => 0,
                   "end_ts_ns" => finish,
                   "step_ms" => 1
                 })
      end
    end

    test "all tools handle signed timestamp boundaries, including native decoding" do
      minimum = -9_223_372_036_854_775_808
      maximum = 9_223_372_036_854_775_807
      records = for ts <- [minimum, -1, maximum], do: %Log{timestamp_ns: ts, service: "api", body: "entry"}

      samples =
        for ts <- [minimum, -1, maximum], do: %MetricSample{timestamp_ns: ts, value: 1.0, labels: %{"__name__" => "up"}}

      :ok = Storage.append(:logs, "acme", records)
      :ok = Storage.append(:metrics, "acme", samples)

      {:ok, log_blob, _, _, _} = NIF.encode_log_segment_parquet(records)
      {:ok, metric_blob, _, _, _} = NIF.encode_metric_segment_parquet(samples)
      original = Application.get_env(:pulso, Storage)
      Application.put_env(:pulso, Storage, adapter: NativeQueryStorage)

      on_exit(fn ->
        if original == nil,
          do: Application.delete_env(:pulso, Storage),
          else: Application.put_env(:pulso, Storage, original)
      end)

      for ts <- [minimum, -1, maximum] do
        assert {:ok, [_]} = NIF.decode_log_segment_parquet(log_blob, max(ts - 1, minimum), ts, nil, [], [])
        assert {:ok, [_]} = NIF.decode_metric_segment_parquet(metric_blob, max(ts - 1, minimum), ts, [])

        for tool <- Tools.list() do
          assert {:ok, _} =
                   Tools.call(tool["name"], %{
                     "tenant" => "acme",
                     "query" => expression(tool["name"]),
                     "end_ts_ns" => ts
                   })
        end

        assert {:ok, [%{"text" => text}]} =
                 Tools.call("query_promql", %{"tenant" => "acme", "query" => "up", "end_ts_ns" => ts})

        assert length(JSON.decode!(text)["data"]["result"]) == 1

        assert {:ok, _} =
                 Tools.call("query_logql", %{
                   "tenant" => "acme",
                   "query" => ~s|count_over_time({service="api"}[1s])|,
                   "end_ts_ns" => ts
                 })
      end

      for fields <- [%{}, %{"start_ts_ns" => minimum, "step_ms" => 1}] do
        args =
          Map.merge(
            %{
              "tenant" => "acme",
              "query" => ~s|count_over_time({service="api"}[1s] offset 1s)|,
              "end_ts_ns" => minimum
            },
            fields
          )

        assert {:ok, [%{"text" => text}]} = Tools.call("query_logql", args)
        assert JSON.decode!(text)["data"]["result"] == []
      end

      assert {:ok, [%{"text" => text}]} =
               Tools.call("query_promql", %{
                 "tenant" => "acme",
                 "query" => "up offset 1s",
                 "end_ts_ns" => minimum
               })

      assert JSON.decode!(text)["data"]["result"] == []
    end

    test "unknown tools keep their existing error" do
      assert {:error, {:unknown_tool, "missing"}} = Tools.call("missing", %{})
    end
  end

  defp assert_supported_schema(schema) do
    supported = ~w(type properties required items enum minimum maximum minLength description)
    assert Map.keys(schema) -- supported == []
    for {_name, child} <- schema["properties"] || %{}, do: assert_supported_schema(child)
    if schema["items"], do: assert_supported_schema(schema["items"])
  end

  defp expression("query_logql"), do: ~s({service="api"})
  defp expression(_), do: "up"

  describe "argument validation" do
    @queries [
      {"query_logs", %{"tenant" => "acme"}},
      {"query_metrics", %{"tenant" => "acme"}},
      {"query_logql", %{"tenant" => "acme", "query" => ~s({service="api"})}},
      {"query_promql", %{"tenant" => "acme", "query" => "up"}}
    ]

    test "all advertised tools declare read-only behavior" do
      assert length(Tools.list()) == 4
      assert Enum.all?(Tools.list(), &(&1["annotations"]["readOnlyHint"] == true))
    end

    test "rejects non-object arguments and invalid tenants across all tools" do
      for {name, args} <- @queries do
        assert {:error, {:invalid_arguments, _}} = Tools.call(name, Map.delete(args, "tenant"))

        if Map.has_key?(args, "query") do
          assert {:error, {:invalid_arguments, _}} = Tools.call(name, Map.delete(args, "query"))
        end

        for invalid <- [nil, [], "acme", 42] do
          assert {:error, {:invalid_arguments, _}} = Tools.call(name, invalid)
        end

        for invalid <- [nil, [], 42, true] do
          assert {:error, {:invalid_arguments, _}} = Tools.call(name, Map.put(args, "tenant", invalid))
        end
      end
    end

    test "rejects malformed and reversed time bounds across all tools" do
      for {name, args} <- @queries do
        for key <- ["start_ts_ns", "end_ts_ns"],
            invalid <- ["10", 1.5, [], true, -9_223_372_036_854_775_809, 9_223_372_036_854_775_808] do
          assert {:error, {:invalid_arguments, _}} = Tools.call(name, Map.put(args, key, invalid))
        end

        assert {:error, {:invalid_arguments, _}} =
                 Tools.call(name, Map.merge(args, %{"start_ts_ns" => 20, "end_ts_ns" => 10}))
      end
    end

    test "enforces record limits for supplied values" do
      for {name, args} <- @queries, name != "query_promql" do
        for invalid <- [0, -1, 5001, "1", 1.5, true] do
          assert {:error, {:invalid_arguments, _}} = Tools.call(name, Map.put(args, "limit", invalid))
        end

        for limit <- [1, 5000] do
          assert {:ok, _} = Tools.call(name, Map.put(args, "limit", limit))
        end
      end
    end

    test "rejects invalid query strings, steps, directions, and services" do
      for {name, args} <- @queries, name in ["query_logql", "query_promql"] do
        for invalid <- [nil, [], 42, true] do
          assert {:error, {:invalid_arguments, _}} = Tools.call(name, Map.put(args, "query", invalid))
        end

        for invalid <- [0, -1, "1000", 1.5, true] do
          assert {:error, {:invalid_arguments, _}} = Tools.call(name, Map.put(args, "step_ms", invalid))
        end
      end

      for invalid <- ["sideways", 1, true] do
        assert {:error, {:invalid_arguments, _}} =
                 Tools.call("query_logql", %{"tenant" => "acme", "query" => ~s({service="api"}), "direction" => invalid})
      end

      for invalid <- [1, [], true] do
        assert {:error, {:invalid_arguments, _}} =
                 Tools.call("query_logs", %{"tenant" => "acme", "service" => invalid})
      end
    end

    test "validates nested matchers without rejecting empty matcher values" do
      for invalid <- [
            %{},
            "up",
            [nil],
            [%{}],
            [%{"name" => 1, "op" => "=", "value" => "api"}],
            [%{"name" => "svc", "op" => "???", "value" => "api"}],
            [%{"name" => "svc", "op" => "=", "value" => nil}]
          ] do
        assert {:error, {:invalid_arguments, _}} =
                 Tools.call("query_metrics", %{"tenant" => "acme", "matchers" => invalid})
      end

      for op <- ["=", "!=", "=~", "!~"] do
        assert {:ok, _} =
                 Tools.call("query_metrics", %{
                   "tenant" => "acme",
                   "matchers" => [%{"name" => "svc", "op" => op, "value" => ""}]
                 })
      end
    end

    test "accepts equal time bounds, empty matchers, and additional properties" do
      for {name, args} <- @queries do
        args = Map.merge(args, %{"start_ts_ns" => 10, "end_ts_ns" => 10, "extension" => true})
        args = if name == "query_promql", do: Map.put(args, "step_ms", 1), else: args
        assert {:ok, _} = Tools.call(name, args)
      end

      assert {:ok, _} = Tools.call("query_metrics", %{"tenant" => "acme", "matchers" => []})
    end

    test "normalizes integral numbers before execution" do
      for {name, args} <- @queries do
        args = Map.merge(args, %{"start_ts_ns" => 10.0, "end_ts_ns" => 10.0})
        args = if name == "query_promql", do: Map.put(args, "step_ms", 1.0), else: args
        args = if name == "query_promql", do: args, else: Map.put(args, "limit", 1.0)
        assert {:ok, _} = Tools.call(name, args)
      end
    end

    test "reports the failing argument path" do
      assert {:error, {:invalid_arguments, "arguments.limit must be at most 5000"}} =
               Tools.call("query_logs", %{"tenant" => "acme", "limit" => 5001})

      assert {:error, {:invalid_arguments, "arguments.matchers[0].value must be string"}} =
               Tools.call("query_metrics", %{
                 "tenant" => "acme",
                 "matchers" => [%{"name" => "svc", "op" => "=", "value" => nil}]
               })
    end

    test "valid arguments still require tenant authorization across all tools" do
      original = Application.fetch_env!(:pulso, Pulso.Auth)
      Application.put_env(:pulso, Pulso.Auth, module: SharedSecret, tokens: %{})
      on_exit(fn -> Application.put_env(:pulso, Pulso.Auth, original) end)

      for {name, args} <- @queries do
        assert {:error, {:unauthorized, :missing_token}} = Tools.call(name, args)
        assert {:error, {:invalid_arguments, _}} = Tools.call(name, Map.put(args, "tenant", nil))
      end
    end

    test "unknown tools retain their error even with malformed arguments" do
      assert {:error, {:unknown_tool, "unknown"}} = Tools.call("unknown", nil)
    end

    test "the dispatcher exposes annotations and returns validation errors" do
      assert {:reply, %{"result" => %{"tools" => tools}}} =
               Pulso.MCP.dispatch(%{"id" => 1, "method" => "tools/list"})

      assert Enum.all?(tools, &(&1["annotations"]["readOnlyHint"] == true))

      for {name, args} <- @queries do
        assert {:reply, %{"result" => %{"isError" => true, "content" => [%{"text" => text}]}}} =
                 Pulso.MCP.dispatch(%{
                   "id" => 2,
                   "method" => "tools/call",
                   "params" => %{"name" => name, "arguments" => Map.put(args, "end_ts_ns", "bad")}
                 })

        assert text =~ "invalid_arguments"
      end
    end
  end

  describe "query_promql" do
    test "evaluates stored metrics and rejects invalid steps" do
      :ok =
        Storage.append(:metrics, "acme", [
          %MetricSample{timestamp_ns: 1_000_000_000, value: 2.0, labels: %{"__name__" => "gauge"}}
        ])

      assert {:ok, [%{"text" => text}]} =
               Tools.call("query_promql", %{
                 "tenant" => "acme",
                 "query" => "sum(gauge)",
                 "end_ts_ns" => 1_000_000_000
               })

      assert JSON.decode!(text)["data"]["result"] == [%{"metric" => %{}, "value" => [1.0, "2"]}]

      assert {:error, {:invalid_arguments, _}} =
               Tools.call("query_promql", %{
                 "tenant" => "acme",
                 "query" => "gauge",
                 "step_ms" => 0
               })
    end

    test "requires tenant authorization before parsing or reading" do
      original = Application.fetch_env!(:pulso, Pulso.Auth)
      Application.put_env(:pulso, Pulso.Auth, module: SharedSecret, tokens: %{})
      on_exit(fn -> Application.put_env(:pulso, Pulso.Auth, original) end)
      assert {:error, {:unauthorized, _}} = Tools.call("query_promql", %{"tenant" => "acme", "query" => "gauge"})
    end
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
