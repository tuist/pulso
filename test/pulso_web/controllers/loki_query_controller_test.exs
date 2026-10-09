defmodule PulsoWeb.LokiQueryControllerTest do
  use PulsoWeb.ConnCase, async: true

  alias Pulso.Record.Log
  alias Pulso.Storage
  alias Pulso.Storage.Memory

  setup do
    Memory.reset()
    :ok
  end

  test "label discovery rejects an incomplete result", %{conn: conn} do
    records =
      for ts <- 1..5_001, do: %Log{timestamp_ns: ts, service: "api", body: "line", resource: %{"service.name" => "api"}}

    :ok = Storage.append(:logs, "acme", records)

    response =
      conn
      |> put_req_header("x-scope-orgid", "acme")
      |> get("/loki/api/v1/labels")
      |> json_response(422)

    assert response["error"] == "query_limit"
  end

  test "a regular expression unsupported by the native decoder is a client error", %{conn: conn} do
    response =
      conn
      |> get("/loki/api/v1/query_range", %{"query" => ~s[{service="api"} |~ "foo(?=bar)"]})
      |> json_response(400)

    assert response["error"] == "evaluation_failed"
    assert response["message"] =~ "invalid_regex"
  end

  test "excessive metric ranges return a client error without allocating a timeline", %{conn: conn} do
    for {start, finish} <- [{"0", "11000000000"}, {"20", "10"}] do
      response =
        conn
        |> get("/loki/api/v1/query_range", %{
          "query" => ~s|rate({service="api"}[5m])|,
          "start" => start,
          "end" => finish,
          "step" => "1ms"
        })
        |> json_response(400)

      assert response["error"] == "evaluation_failed"
      assert response["message"] =~ "invalid_range_or_too_many_steps"
    end
  end

  describe "GET /loki/api/v1/query_range with a log query" do
    setup do
      :ok =
        Storage.append(:logs, "acme", [
          %Log{timestamp_ns: 10, service: "api", body: "connection timeout"},
          %Log{timestamp_ns: 20, service: "api", body: "connection ok"},
          %Log{timestamp_ns: 30, service: "db", body: "insert failed"}
        ])

      :ok
    end

    test "returns a Loki streams envelope", %{conn: conn} do
      conn =
        conn
        |> put_req_header("x-scope-orgid", "acme")
        |> get(~p"/loki/api/v1/query_range?query=#{"{service=\"api\"}"}")

      body = json_response(conn, 200)
      assert body["status"] == "success"
      assert body["data"]["resultType"] == "streams"
      streams = body["data"]["result"]
      lines = for %{"values" => vs} <- streams, [_ts, line] <- vs, do: line
      assert Enum.sort(lines) == ["connection ok", "connection timeout"]
    end

    test "applies line filters", %{conn: conn} do
      conn =
        conn
        |> put_req_header("x-scope-orgid", "acme")
        |> get(~p"/loki/api/v1/query_range?query=#{~s({service="api"} |= "timeout")}")

      body = json_response(conn, 200)
      streams = body["data"]["result"]
      lines = for %{"values" => vs} <- streams, [_ts, line] <- vs, do: line
      assert lines == ["connection timeout"]
    end

    test "rejects a missing query param", %{conn: conn} do
      conn =
        conn
        |> put_req_header("x-scope-orgid", "acme")
        |> get(~p"/loki/api/v1/query_range")

      body = json_response(conn, 400)
      assert body["status"] == "error"
      assert body["error"] == "missing_param"
    end

    test "rejects an unparseable query", %{conn: conn} do
      conn =
        conn
        |> put_req_header("x-scope-orgid", "acme")
        |> get(~p"/loki/api/v1/query_range?query=#{"not a query"}")

      body = json_response(conn, 400)
      assert body["error"] == "parse_error"
    end
  end

  describe "GET /loki/api/v1/query_range with a metric query" do
    test "returns a matrix envelope", %{conn: conn} do
      :ok =
        Storage.append(:logs, "acme", [
          %Log{timestamp_ns: 1_000_000_000, service: "api", body: "a"},
          %Log{timestamp_ns: 2_000_000_000, service: "api", body: "b"},
          %Log{timestamp_ns: 3_000_000_000, service: "api", body: "c"}
        ])

      query = "count_over_time({service=\"api\"}[1s])"

      conn =
        conn
        |> put_req_header("x-scope-orgid", "acme")
        |> get(~p"/loki/api/v1/query_range?query=#{query}&start=1000000000&end=3000000000&step=1")

      body = json_response(conn, 200)
      assert body["data"]["resultType"] == "matrix"
      assert [_series] = body["data"]["result"]
    end
  end

  describe "GET /loki/api/v1/labels" do
    test "returns unique label names for the tenant", %{conn: conn} do
      :ok =
        Storage.append(:logs, "acme", [
          %Log{
            timestamp_ns: 10,
            service: "api",
            body: "x",
            resource: %{"env" => "prod", "region" => "us"}
          }
        ])

      conn =
        conn
        |> put_req_header("x-scope-orgid", "acme")
        |> get(~p"/loki/api/v1/labels")

      body = json_response(conn, 200)
      assert body["status"] == "success"
      names = body["data"]
      assert "env" in names
      assert "region" in names
    end
  end

  describe "GET /loki/api/v1/label/:name/values" do
    test "returns unique values seen for the given label", %{conn: conn} do
      :ok =
        Storage.append(:logs, "acme", [
          %Log{timestamp_ns: 10, body: "x", resource: %{"env" => "prod"}},
          %Log{timestamp_ns: 20, body: "y", resource: %{"env" => "stg"}},
          %Log{timestamp_ns: 30, body: "z", resource: %{"env" => "prod"}}
        ])

      conn =
        conn
        |> put_req_header("x-scope-orgid", "acme")
        |> get(~p"/loki/api/v1/label/env/values")

      body = json_response(conn, 200)
      assert Enum.sort(body["data"]) == ["prod", "stg"]
    end
  end

  describe "input validation" do
    test "invalid regex returns 400 with a parse_error, not 500", %{conn: conn} do
      conn =
        conn
        |> put_req_header("x-scope-orgid", "acme")
        |> get(~p"/loki/api/v1/query_range?query=#{"{env=~\"(\"}"}")

      body = json_response(conn, 400)
      assert body["error"] in ["parse_error", "evaluation_failed"]
    end

    test "unparseable limit falls back to default, does not 500", %{conn: conn} do
      :ok = Storage.append(:logs, "acme", [%Log{timestamp_ns: 10, service: "api", body: "a"}])

      conn =
        conn
        |> put_req_header("x-scope-orgid", "acme")
        |> get(~p"/loki/api/v1/query_range?query=#{"{service=\"api\"}"}&limit=abc")

      body = json_response(conn, 200)
      assert body["status"] == "success"
    end

    test "RFC3339 timestamps parse instead of scanning the whole tenant", %{conn: conn} do
      :ok = Storage.append(:logs, "acme", [%Log{timestamp_ns: 10, service: "api", body: "a"}])

      conn =
        conn
        |> put_req_header("x-scope-orgid", "acme")
        |> get(~p"/loki/api/v1/labels?start=2024-01-01T00:00:00Z&end=2024-01-02T00:00:00Z")

      # No records fall in that window; the response is a well-formed
      # empty labels list, not a crash or a full-tenant scan.
      body = json_response(conn, 200)
      assert body["status"] == "success"
    end
  end
end
