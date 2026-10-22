defmodule PulsoWeb.CompatControllerTest do
  use PulsoWeb.ConnCase, async: true

  alias Pulso.Auth
  alias Pulso.Auth.SharedSecret
  alias Pulso.Record.Log
  alias Pulso.Storage

  describe "datasource build information" do
    test "Prometheus, Loki and Tempo report the API versions they follow", %{conn: conn} do
      assert %{"status" => "success", "data" => %{"version" => "3.5.0"}} =
               conn |> get("/api/v1/status/buildinfo") |> json_response(200)

      assert %{"version" => "3.5.0", "revision" => "pulso-" <> _} =
               build_conn() |> get("/loki/api/v1/status/buildinfo") |> json_response(200)

      assert %{"version" => "2.9.0"} = build_conn() |> get("/api/status/buildinfo") |> json_response(200)
      assert build_conn() |> get("/api/echo") |> text_response(200) == "echo"
    end
  end

  describe "Prometheus metadata endpoints" do
    test "metadata and exemplars are empty rather than invented", %{conn: conn} do
      assert %{"status" => "success", "data" => %{}} = conn |> get("/api/v1/metadata") |> json_response(200)

      assert %{"status" => "success", "data" => []} =
               build_conn() |> post("/api/v1/query_exemplars", %{"query" => "up"}) |> json_response(200)
    end

    test "format_query validates and echoes the query", %{conn: conn} do
      assert %{"data" => "sum(rate(up[5m]))"} =
               conn |> get("/api/v1/format_query", %{"query" => "sum(rate(up[5m]))"}) |> json_response(200)

      assert %{"errorType" => "bad_data"} =
               build_conn() |> get("/api/v1/format_query", %{"query" => "sum(("}) |> json_response(400)
    end

    test "metadata requires the tenant's credentials", %{conn: conn} do
      token = "secret"
      digest = "sha256$" <> Base.encode16(:crypto.hash(:sha256, token), case: :lower)
      Pulso.Runtime.put_env(:pulso, Auth, module: SharedSecret, tokens: %{"acme" => digest})

      assert conn |> get("/api/v1/metadata") |> json_response(401)

      basic = "Basic " <> Base.encode64("acme:" <> token)

      assert %{"status" => "success"} =
               build_conn() |> put_req_header("authorization", basic) |> get("/api/v1/metadata") |> json_response(200)
    end
  end

  describe "Loki index endpoints" do
    setup do
      tenant = "index-#{System.unique_integer([:positive])}"

      :ok =
        Storage.append(:logs, tenant, [
          %Log{timestamp_ns: 10, service: "api", body: "12345", resource: %{"namespace" => "prod", "pod" => "a"}},
          %Log{timestamp_ns: 20, service: "api", body: "123", resource: %{"namespace" => "prod", "pod" => "b"}},
          %Log{timestamp_ns: 30, service: "web", body: "1", resource: %{"namespace" => "staging", "pod" => "c"}}
        ])

      {:ok, tenant: tenant}
    end

    test "index/stats estimates matching volume", %{conn: conn, tenant: tenant} do
      stats =
        conn
        |> put_req_header("x-scope-orgid", tenant)
        |> get("/loki/api/v1/index/stats", %{"query" => ~s({service_name="api"}), "start" => "0", "end" => "100"})
        |> json_response(200)

      assert %{"entries" => 2, "bytes" => 8, "streams" => 0} = stats
    end

    test "index/volume returns bytes per selector-label group, largest first", %{conn: conn, tenant: tenant} do
      params = %{"query" => ~s({namespace=~".+"}), "start" => "0", "end" => "100"}

      result =
        conn
        |> put_req_header("x-scope-orgid", tenant)
        |> get("/loki/api/v1/index/volume", params)
        |> json_response(200)

      assert result["data"]["resultType"] == "vector"

      assert Enum.map(result["data"]["result"], &{&1["metric"], List.last(&1["value"])}) == [
               {%{"namespace" => "prod"}, "8"},
               {%{"namespace" => "staging"}, "1"}
             ]
    end

    test "index/volume honors targetLabels and limit", %{conn: conn, tenant: tenant} do
      params = %{
        "query" => ~s({namespace="prod"}),
        "targetLabels" => "pod",
        "limit" => "1",
        "start" => "0",
        "end" => "100"
      }

      result =
        conn
        |> put_req_header("x-scope-orgid", tenant)
        |> get("/loki/api/v1/index/volume", params)
        |> json_response(200)

      assert [%{"metric" => %{"pod" => "a"}, "value" => [_, "5"]}] = result["data"]["result"]
    end

    test "index/volume_range returns a matrix", %{conn: conn, tenant: tenant} do
      params = %{"query" => ~s({namespace="prod"}), "start" => "0", "end" => "100", "step" => "1s"}

      result =
        conn
        |> put_req_header("x-scope-orgid", tenant)
        |> get("/loki/api/v1/index/volume_range", params)
        |> json_response(200)

      assert result["data"]["resultType"] == "matrix"
      assert [%{"metric" => %{"namespace" => "prod"}}] = result["data"]["result"]
    end

    test "volume rejects unsupported aggregation and invalid labels", %{conn: conn, tenant: tenant} do
      for extra <- [%{"aggregateBy" => "labels"}, %{"targetLabels" => "service.name"}, %{"limit" => "0"}] do
        params = Map.merge(%{"query" => ~s({namespace="prod"}), "start" => "0", "end" => "100"}, extra)

        assert conn
               |> put_req_header("x-scope-orgid", tenant)
               |> get("/loki/api/v1/index/volume", params)
               |> json_response(400)
      end
    end
  end

  describe "ingest path aliases" do
    test "/otlp/v1/logs is Loki's and Grafana Cloud's OTLP path", %{conn: conn} do
      tenant = "otlp-alias-#{System.unique_integer([:positive])}"

      payload = %{
        "resourceLogs" => [
          %{
            "resource" => %{"attributes" => [%{"key" => "service.name", "value" => %{"stringValue" => "api"}}]},
            "scopeLogs" => [
              %{"logRecords" => [%{"timeUnixNano" => "1700000000000000000", "body" => %{"stringValue" => "hi"}}]}
            ]
          }
        ]
      }

      assert conn
             |> put_req_header("content-type", "application/json")
             |> put_req_header("x-scope-orgid", tenant)
             |> post("/otlp/v1/logs", Pulso.JSON.encode!(payload))
             |> json_response(200)

      assert {:ok, [%Log{service: "api", body: "hi"}]} = Storage.query(:logs, tenant)
    end

    test "Grafana Cloud and Mimir remote-write paths reach the remote-write receiver", %{conn: conn} do
      canonical = conn |> put_req_header("content-type", "text/plain") |> post("/api/v1/write", "x")

      for path <- ["/api/prom/push", "/api/v1/push"] do
        aliased = build_conn() |> put_req_header("content-type", "text/plain") |> post(path, "x")
        assert aliased.status == canonical.status
        assert aliased.resp_body == canonical.resp_body
      end
    end
  end
end
