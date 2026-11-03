defmodule PulsoWeb.RolesTest do
  use PulsoWeb.ConnCase, async: true

  alias Pulso.Storage
  alias Pulso.Test.OTLPTraceFixture, as: Fixture

  defp traces(conn),
    do: conn |> put_req_header("content-type", "application/x-protobuf") |> post("/v1/traces", Fixture.protobuf())

  defp mcp(conn) do
    body = Pulso.JSON.encode!(%{"jsonrpc" => "2.0", "id" => 1, "method" => "tools/list", "params" => %{}})
    conn |> put_req_header("content-type", "application/json") |> post("/mcp", body)
  end

  test "an instance refuses roles it does not run with a retryable 503, and always serves health" do
    Pulso.Runtime.put_env(:pulso, Pulso.Roles, roles: [:query])
    conn = traces(build_conn())
    assert conn.status == 503
    assert get_resp_header(conn, "retry-after") == ["5"]
    assert {:ok, []} = Storage.query(:traces, "default")

    # Legacy aliases resolve before classification.
    assert build_conn()
           |> put_req_header("content-type", "application/x-protobuf")
           |> post("/api/prom/push", "")
           |> Map.fetch!(:status) == 503

    refute mcp(build_conn()).status == 503
    assert build_conn() |> get("/healthz") |> Map.fetch!(:status) == 200

    Pulso.Runtime.put_env(:pulso, Pulso.Roles, roles: [:ingest])
    assert traces(build_conn()).status == 200
    assert {:ok, [_]} = Storage.query(:traces, "default")
    assert mcp(build_conn()).status == 503
    assert build_conn() |> get("/loki/api/v1/labels") |> Map.fetch!(:status) == 503
    assert build_conn() |> get("/metrics") |> Map.fetch!(:status) == 200
  end

  test "role lists parse strictly" do
    assert Pulso.Roles.parse!(" query , ingest,query") == [:query, :ingest]

    for raw <- ["", " , ", "query,unknown", "Query"] do
      assert_raise RuntimeError, fn -> Pulso.Roles.parse!(raw) end
    end
  end
end
