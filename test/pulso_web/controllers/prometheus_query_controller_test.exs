defmodule PulsoWeb.PrometheusQueryControllerTest do
  use PulsoWeb.ConnCase, async: true

  alias Pulso.Auth
  alias Pulso.Auth.SharedSecret
  alias Pulso.PromQL.Evaluator
  alias Pulso.Record.MetricSample
  alias Pulso.Storage
  alias Pulso.Storage.Memory
  alias Pulso.Test.BlockingMetricStorage

  setup do
    Memory.reset()

    :ok =
      Storage.append(:metrics, "acme", [
        %MetricSample{timestamp_ns: 10_000_000_000, value: 10.0, labels: %{"__name__" => "requests_total"}},
        %MetricSample{timestamp_ns: 20_000_000_000, value: 20.0, labels: %{"__name__" => "requests_total"}}
      ])

    :ok
  end

  test "instant and range queries return Prometheus envelopes", %{conn: conn} do
    conn = put_req_header(conn, "x-scope-orgid", "acme")

    assert %{"data" => %{"resultType" => "vector", "result" => [%{"value" => [20.0, "20"]}]}} =
             conn |> get("/api/v1/query", %{query: "requests_total", time: "20"}) |> json_response(200)

    assert %{"data" => %{"resultType" => "matrix", "result" => [%{"values" => [[10.0, "10"], [20.0, "20"]]}]}} =
             conn
             |> get("/api/v1/query_range", %{query: "requests_total", start: "10", end: "20", step: "10s"})
             |> json_response(200)

    assert %{"status" => "success"} =
             conn
             |> post("/api/v1/query", %{query: "requests_total", time: "1970-01-01T00:00:20Z"})
             |> json_response(200)
  end

  test "invalid syntax and parameters return structured errors", %{conn: conn} do
    for params <- [%{}, %{query: "unsupported(x)"}, %{query: "x", time: "bad"}, %{query: "x", time: "1e300"}] do
      assert %{"status" => "error", "errorType" => "bad_data"} =
               conn |> get("/api/v1/query", params) |> json_response(400)
    end

    for step <- ["0", "-1", "garbage", "1e300"] do
      assert %{"status" => "error"} =
               conn
               |> get("/api/v1/query_range", %{query: "x", start: "0", end: "20", step: step})
               |> json_response(400)
    end
  end

  test "decimal boundaries, default tenants, and unsupported overrides", %{conn: conn} do
    :ok =
      Storage.append(:metrics, "default", [
        %MetricSample{timestamp_ns: 375_000_000, value: 1.0, labels: %{"__name__" => "boundary"}}
      ])

    for time <- ["0.375", "375e-3", "1970-01-01T00:00:00.375Z"] do
      assert %{"data" => %{"result" => [%{"value" => [0.375, "1"]}]}} =
               conn
               |> put_req_header("x-scope-orgid", "")
               |> get("/api/v1/query", %{query: "boundary", time: time})
               |> json_response(200)
    end

    for key <- ["limit", "lookback_delta"] do
      assert %{"errorType" => "bad_data"} =
               conn |> get("/api/v1/query", %{"query" => "boundary", key => "1"}) |> json_response(400)
    end

    assert %{"errorType" => "bad_data"} =
             conn |> get("/api/v1/query", %{query: "boundary", time: "9999-01-01T00:00:00Z"}) |> json_response(400)
  end

  test "client timeouts are accepted and invalid durations rejected", %{conn: conn} do
    for timeout <- ["30s", "1s", "0.1"] do
      assert %{"status" => "success"} =
               conn |> get("/api/v1/query", %{query: "m", timeout: timeout}) |> json_response(200)
    end

    for timeout <- ["bad", "0s", "-1s"] do
      assert %{"errorType" => "bad_data"} =
               conn |> get("/api/v1/query", %{query: "m", timeout: timeout}) |> json_response(400)
    end
  end

  test "scan limits and timeouts are execution errors rather than storage failures", %{conn: conn} do
    adapter = BlockingMetricStorage
    Pulso.Runtime.put_env(:pulso, Storage, adapter: adapter)

    for reason <- [:query_scan_limit, :query_timeout] do
      Pulso.Runtime.put_env(:pulso, adapter, {:error, reason})
      assert %{"errorType" => "execution"} = conn |> get("/api/v1/query", %{query: "m"}) |> json_response(422)
    end

    Pulso.Runtime.put_env(:pulso, adapter, self())

    assert %{"errorType" => "execution"} =
             conn |> get("/api/v1/query", %{query: "m", timeout: "5ms"}) |> json_response(422)
  end

  test "sample budgets return an execution error", %{conn: conn} do
    Pulso.Runtime.put_env(:pulso, Evaluator, max_samples: 1)

    assert %{"errorType" => "execution"} =
             conn
             |> put_req_header("x-scope-orgid", "acme")
             |> get("/api/v1/query", %{query: "requests_total", time: "20"})
             |> json_response(422)
  end

  test "a valid token for another tenant cannot read acme", %{conn: conn} do
    token = fn value -> "sha256$" <> Base.encode16(:crypto.hash(:sha256, value), case: :lower) end

    Pulso.Runtime.put_env(:pulso, Auth,
      module: SharedSecret,
      tokens: %{"acme" => token.("acme-key"), "beta" => token.("beta-key")}
    )

    conn = conn |> put_req_header("x-scope-orgid", "acme") |> put_req_header("authorization", "Bearer beta-key")

    for path <- ["/api/v1/query", "/api/v1/query_range"] do
      assert %{"errorType" => "unauthorized"} = conn |> get(path, %{query: "requests_total"}) |> json_response(401)
    end
  end

  test "reads require tenant authorization", %{conn: conn} do
    Pulso.Runtime.put_env(:pulso, Auth, module: SharedSecret, tokens: %{})

    assert %{"errorType" => "unauthorized"} =
             conn |> get("/api/v1/query", %{query: "requests_total"}) |> json_response(401)

    assert %{"errorType" => "unauthorized"} =
             conn |> get("/api/v1/query_range", %{query: "requests_total"}) |> json_response(401)
  end
end
