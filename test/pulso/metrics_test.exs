defmodule Pulso.MetricsTest do
  use PulsoWeb.ConnCase, async: false

  alias Plug.Parsers.ParseError
  alias Plug.Parsers.RequestTooLargeError
  alias Pulso.MCP.Tools
  alias Pulso.Metrics
  alias Pulso.ObjectStore
  alias Pulso.Record.Log
  alias Pulso.Storage.S3
  alias Pulso.Storage.S3.ManifestRegistry
  alias Pulso.Test.CompactionStore
  alias Pulso.Test.FailingStorage

  defp value(name, labels) do
    case :ets.lookup(Metrics, {name, labels}) do
      [{_, value}] -> value
      [] -> 0
    end
  end

  defp labels(kind, operation, outcome, purpose \\ "none") do
    [{"kind", kind}, {"operation", operation}, {"purpose", purpose}, {"outcome", outcome}]
  end

  defp requests(kind, operation, outcome) do
    value("pulso_operations_total", labels(kind, operation, outcome))
  end

  defp records(signal, outcome) do
    value("pulso_ingest_records_total", [{"signal", signal}, {"outcome", outcome}])
  end

  defp otlp(records) do
    %{"resourceLogs" => [%{"scopeLogs" => [%{"logRecords" => records}]}]}
  end

  test "scrapes use Prometheus text, ignore JSON Accept, and do not observe themselves", %{conn: conn} do
    before = :ets.tab2list(Metrics) |> Enum.sort()
    conn = conn |> put_req_header("accept", "text/plain") |> get("/metrics")
    assert conn.status == 200
    assert get_resp_header(conn, "content-type") == ["text/plain; version=0.0.4; charset=utf-8"]
    assert get_resp_header(conn, "cache-control") == ["no-store"]
    assert conn.resp_body =~ "# TYPE pulso_operations_total counter\n"
    assert conn.resp_body =~ "pulso_manifest_pending_segments 0\n"
    assert conn.resp_body =~ "pulso_query_occupied_slots 0\n"
    assert :ets.tab2list(Metrics) |> Enum.sort() == before
  end

  test "each metric family keeps its headers and samples in one group" do
    Metrics.measure(:query, "query_logs", fn -> :ok end)
    Metrics.measure(:object, "get", fn -> {:ok, "body"} end, fn _ -> %{read: 4} end, "segment")
    Metrics.measure(:compaction, "compact", fn -> {:ok, %{merged: 2}} end, fn _ -> %{segments: 2} end)
    assert :ok = Metrics.append(:logs, "metrics-grouping", [%Log{}], 1, [])

    Metrics.render()
    |> String.split("\n", trim: true)
    |> Enum.reduce(nil, fn line, family ->
      case String.split(line, " ", parts: 4) do
        ["#", "HELP", name, _] ->
          name

        ["#", "TYPE", name, _] ->
          assert name == family
          family

        _ ->
          [name | _] = String.split(line, ["{", " "], parts: 2)
          assert name in [family, family <> "_bucket", family <> "_count", family <> "_sum"]
          family
      end
    end)
  end

  test "router-equivalent paths count requests and pre-router parser errors" do
    for path <- ["/v1/logs/", "/v1//logs", "/v1/%6cogs/"] do
      before = requests("ingest", "otlp", "ok")
      assert (build_conn() |> put_req_header("content-type", "application/json") |> post(path, otlp([]))).status == 200
      assert requests("ingest", "otlp", "ok") == before + 1
      rejected = requests("ingest", "otlp", "rejected")

      assert_raise ParseError, fn ->
        build_conn() |> put_req_header("content-type", "application/json") |> post(path, "{")
      end

      assert requests("ingest", "otlp", "rejected") == rejected + 1
    end

    before = requests("query", "http_promql", "rejected")
    assert (build_conn() |> get("/api//v1/query/", %{"query" => "bad("})).status == 400
    assert requests("query", "http_promql", "rejected") == before + 1
  end

  test "encoded separators and nonexistent label routes do not become query or ingest attempts" do
    before = :ets.tab2list(Metrics) |> Enum.sort()
    assert (build_conn() |> get("/loki/api/v1/label/a/b/c/d")).status == 404
    assert (build_conn() |> get("/loki/api/v1/label/a%2Fb/c/values")).status == 404

    assert (build_conn() |> put_req_header("content-type", "application/json") |> post("/v1%2Flogs", otlp([]))).status ==
             404

    assert :ets.tab2list(Metrics) |> Enum.sort() == before
    before_labels = requests("query", "http_labels", "ok")
    assert (build_conn() |> get("/loki/api/v1/label/service_name/values/")).status == 200
    assert requests("query", "http_labels", "ok") == before_labels + 1
  end

  test "successful and partially rejected ingest records are counted after append", %{conn: conn} do
    accepted = records("logs", "accepted")
    rejected = records("logs", "rejected")
    requests = requests("ingest", "otlp", "ok")

    conn =
      conn
      |> put_req_header("content-type", "application/json")
      |> post(
        "/v1/logs",
        otlp([
          %{"timeUnixNano" => "1700000000000000000", "body" => %{"stringValue" => "hello"}},
          "malformed"
        ])
      )

    assert conn.status == 200
    assert records("logs", "accepted") == accepted + 1
    assert records("logs", "rejected") == rejected + 1
    assert requests("ingest", "otlp", "ok") == requests + 1
  end

  test "pre-decoding rejections count requests, not invented record counts", %{conn: conn} do
    accepted = records("metrics", "accepted")
    rejected = records("metrics", "rejected")
    requests = requests("ingest", "remote_write", "rejected")
    assert (conn |> put_req_header("content-type", "text/plain") |> post("/api/v1/write", "not protobuf")).status == 415
    assert requests("ingest", "remote_write", "rejected") == requests + 1
    assert records("metrics", "accepted") == accepted
    assert records("metrics", "rejected") == rejected
  end

  test "parser failures before the controller are counted exactly once", %{conn: conn} do
    before = requests("ingest", "otlp", "rejected")

    assert_raise ParseError, fn ->
      conn |> put_req_header("content-type", "application/json") |> post("/v1/logs", "{")
    end

    assert requests("ingest", "otlp", "rejected") == before + 1
  end

  test "gzip expansion rejections before decoding are counted", %{conn: conn} do
    previous = Application.get_env(:pulso, PulsoWeb.CompressedBodyReader)
    Application.put_env(:pulso, PulsoWeb.CompressedBodyReader, max_decompressed_bytes: 16)

    on_exit(fn ->
      if previous,
        do: Application.put_env(:pulso, PulsoWeb.CompressedBodyReader, previous),
        else: Application.delete_env(:pulso, PulsoWeb.CompressedBodyReader)
    end)

    before = requests("ingest", "otlp", "rejected")

    assert_raise RequestTooLargeError, fn ->
      conn
      |> put_req_header("content-type", "application/json")
      |> put_req_header("content-encoding", "gzip")
      |> post("/v1/logs", :zlib.gzip(String.duplicate(" ", 100)))
    end

    assert requests("ingest", "otlp", "rejected") == before + 1
  end

  test "both Loki wire formats and remote write count durably accepted records" do
    alias Pulso.Loki.PushProto

    accepted_logs = records("logs", "accepted")
    accepted_metrics = records("metrics", "accepted")
    loki_requests = requests("ingest", "loki", "ok")
    metric_requests = requests("ingest", "remote_write", "ok")

    conn =
      build_conn()
      |> put_req_header("content-type", "application/json")
      |> post("/loki/api/v1/push", %{
        "streams" => [%{"stream" => %{"service_name" => "api"}, "values" => [["1700000000000000000", "line"]]}]
      })

    assert conn.status == 204

    request = %PushProto.PushRequest{
      streams: [
        %PushProto.Stream{
          labels: "{service_name=\"api\"}",
          entries: [%PushProto.Entry{timestamp: %PushProto.Timestamp{seconds: 1_700_000_000}, line: "line"}]
        }
      ]
    }

    {:ok, loki_body} = request |> PushProto.encode() |> IO.iodata_to_binary() |> :snappyer.compress()

    assert (build_conn()
            |> put_req_header("content-type", "application/x-protobuf")
            |> post("/loki/api/v1/push", loki_body)).status == 204

    # Independent, literal remote-write protobuf: one `up=1` sample at 1ms.
    {:ok, metric_body} =
      :snappyer.compress(<<10, 29, 10, 14, 10, 8, "__name__", 18, 2, "up", 18, 11, 9, 1.0::little-float-64, 16, 1>>)

    assert (build_conn()
            |> put_req_header("content-type", "application/x-protobuf")
            |> put_req_header("content-encoding", "snappy")
            |> post("/api/v1/write", metric_body)).status == 204

    assert records("logs", "accepted") == accepted_logs + 2
    assert records("metrics", "accepted") == accepted_metrics + 1
    assert requests("ingest", "loki", "ok") == loki_requests + 2
    assert requests("ingest", "remote_write", "ok") == metric_requests + 1
  end

  test "whole decoded batches are rejected when storage fails" do
    previous = Application.get_env(:pulso, Pulso.Storage)
    Application.put_env(:pulso, Pulso.Storage, adapter: S3)

    on_exit(fn ->
      if previous,
        do: Application.put_env(:pulso, Pulso.Storage, previous),
        else: Application.delete_env(:pulso, Pulso.Storage)
    end)

    accepted = records("logs", "accepted")
    rejected = records("logs", "rejected")
    assert {:error, {:invalid_tenant, _}} = Metrics.append(:logs, "bad/name", [%Log{}], 2, [])
    assert records("logs", "accepted") == accepted
    assert records("logs", "rejected") == rejected + 3
  end

  test "Loki and remote write storage failures count requests and decoded records" do
    use_failing_storage(:error)
    log_count = records("logs", "rejected")
    metric_count = records("metrics", "rejected")
    loki_requests = requests("ingest", "loki", "error")
    metric_requests = requests("ingest", "remote_write", "error")

    assert (build_conn()
            |> put_req_header("content-type", "application/json")
            |> post("/loki/api/v1/push", %{
              "streams" => [%{"stream" => %{"job" => "api"}, "values" => [["1700000000000000000", "line"]]}]
            })).status == 500

    {:ok, body} =
      :snappyer.compress(<<10, 29, 10, 14, 10, 8, "__name__", 18, 2, "up", 18, 11, 9, 1.0::little-float-64, 16, 1>>)

    assert (build_conn()
            |> put_req_header("content-type", "application/x-protobuf")
            |> put_req_header("content-encoding", "snappy")
            |> post("/api/v1/write", body)).status == 503

    assert records("logs", "rejected") == log_count + 1
    assert records("metrics", "rejected") == metric_count + 1
    assert requests("ingest", "loki", "error") == loki_requests + 1
    assert requests("ingest", "remote_write", "error") == metric_requests + 1
  end

  test "controller exceptions are counted once, including their decoded rejected records" do
    use_failing_storage(:raise)
    count = records("logs", "rejected")
    errors = requests("ingest", "otlp", "error") + requests("ingest", "otlp", "exception")

    assert_raise RuntimeError, "storage crashed", fn ->
      build_conn()
      |> put_req_header("content-type", "application/json")
      |> post("/v1/logs", otlp([%{"body" => %{"stringValue" => "line"}}]))
    end

    assert records("logs", "rejected") == count + 1
    assert requests("ingest", "otlp", "error") + requests("ingest", "otlp", "exception") == errors + 1
  end

  defp use_failing_storage(mode) do
    previous = Application.get_env(:pulso, Pulso.Storage)
    Application.put_env(:pulso, Pulso.Storage, adapter: FailingStorage)
    Application.put_env(:pulso, FailingStorage, mode)

    on_exit(fn ->
      if previous,
        do: Application.put_env(:pulso, Pulso.Storage, previous),
        else: Application.delete_env(:pulso, Pulso.Storage)

      Application.delete_env(:pulso, FailingStorage)
    end)
  end

  test "HTTP failures and MCP tool errors have separate bounded operation labels", %{conn: conn} do
    before_http = requests("query", "http_promql", "rejected")
    assert (conn |> get("/api/v1/query", %{"query" => "unsupported()"})).status == 400
    assert requests("query", "http_promql", "rejected") == before_http + 1
    before_tool = requests("query", "query_logs", "error")
    assert {:error, _} = Tools.call("query_logs", %{})
    assert requests("query", "query_logs", "error") == before_tool + 1
    assert {:error, _} = Tools.call("untrusted-name-secret", %{})
    refute Metrics.render() =~ "untrusted-name-secret"
  end

  test "duration histogram has every cumulative bucket, count and seconds sum" do
    metadata = %{kind: :compaction, operation: "compact", purpose: "none", outcome: "ok"}
    labels = labels("compaction", "compact", "ok")
    small = labels ++ [{"le", "0.005"}]
    large = labels ++ [{"le", "1.0"}]
    infinity = labels ++ [{"le", "+Inf"}]
    before_small = value("pulso_operation_duration_seconds_bucket", small)
    before_large = value("pulso_operation_duration_seconds_bucket", large)
    before_infinity = value("pulso_operation_duration_seconds_bucket", infinity)
    before_sum = value("pulso_operation_duration_seconds_sum", labels)
    duration = System.convert_time_unit(750_000, :microsecond, :native)
    :telemetry.execute([:pulso, :operation, :stop], %{duration: duration, segments: 3}, metadata)
    assert value("pulso_operation_duration_seconds_bucket", small) == before_small
    assert value("pulso_operation_duration_seconds_bucket", large) == before_large + 1
    assert value("pulso_operation_duration_seconds_bucket", infinity) == before_infinity + 1
    assert value("pulso_operation_duration_seconds_sum", labels) == before_sum + 750_000

    bounds =
      Metrics.render()
      |> String.split("\n")
      |> Enum.filter(
        &String.starts_with?(
          &1,
          ~s(pulso_operation_duration_seconds_bucket{kind="compaction",operation="compact",purpose="none",outcome="ok",)
        )
      )
      |> Enum.map(fn line ->
        [_, bound] = Regex.run(~r/le="([^"]+)"/, line)
        bound
      end)

    assert bounds == ["0.005", "0.01", "0.05", "0.1", "0.5", "1.0", "5.0", "10.0", "+Inf"]

    assert Metrics.render() =~
             ~s(pulso_operation_duration_seconds_bucket{kind="compaction",operation="compact",purpose="none",outcome="ok",le="0.005"})
  end

  test "measurement preserves successes, errors, throws and exceptions" do
    assert {:ok, :body} = Metrics.measure(:object, "get", fn -> {:ok, :body} end)
    assert {:error, :not_found} = Metrics.measure(:object, "get", fn -> {:error, :not_found} end)
    assert catch_throw(Metrics.measure(:object, "get", fn -> throw(:original) end)) == :original

    assert_raise ArgumentError, "original", fn ->
      Metrics.measure(:object, "get", fn -> raise ArgumentError, "original" end)
    end

    assert_raise ArgumentError, fn -> ObjectStore.get(%{}, "tenants/secret/v4/signal=logs/file.parquet") end
    refute Metrics.render() =~ "tenants/secret"
  end

  test "untrusted labels cannot create new time series" do
    size = :ets.info(Metrics, :size)

    for n <- 1..100 do
      :telemetry.execute([:pulso, :operation, :stop], %{duration: 1}, %{
        kind: :query,
        operation: "user-#{n}",
        purpose: "none",
        outcome: "ok"
      })
    end

    assert :ets.info(Metrics, :size) == size
  end

  test "concurrent increments do not lose updates or enqueue reporting messages" do
    labels = labels("object", "list_prefixes", "ok", "other")
    before = value("pulso_operations_total", labels)

    1..100
    |> Task.async_stream(
      fn _ -> Metrics.measure(:object, "list_prefixes", fn -> {:ok, []} end, fn _ -> %{} end, "other") end,
      max_concurrency: 8
    )
    |> Stream.run()

    assert value("pulso_operations_total", labels) == before + 100
    assert {:message_queue_len, 0} = Process.info(Process.whereis(Metrics), :message_queue_len)
  end

  test "queue gauges use registry metadata and survive owner exit" do
    start_supervised!({Registry, keys: :unique, name: ManifestRegistry})
    parent = self()

    pid =
      start_supervised!(
        {Task,
         fn ->
           {:ok, _} = Registry.register(ManifestRegistry, {"secret-tenant", "logs"}, %{pending: 4, waiters: 2})
           send(self(), :queued)
           send(self(), :queued)
           send(self(), :queued)
           send(parent, :registered)

           receive do
             :finish -> Registry.unregister(ManifestRegistry, {"secret-tenant", "logs"})
           end
         end}
      )

    ref = Process.monitor(pid)
    assert_receive :registered
    rendered = Metrics.render()
    assert rendered =~ "pulso_manifest_pending_segments 4\n"
    assert rendered =~ "pulso_manifest_waiting_requests 2\n"
    refute rendered =~ "secret-tenant"
    send(pid, :finish)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    assert Registry.lookup(ManifestRegistry, {"secret-tenant", "logs"}) == []
    assert Metrics.render() =~ "pulso_manifest_pending_segments 0\n"
  end

  test "object counters distinguish successful bodies, conflicts, conditional reads, and missing objects" do
    agent =
      start_supervised!(
        {Agent,
         fn ->
           %{
             objects: %{},
             reads: [],
             version: 0,
             hook: nil,
             faults: %{},
             barriers: %{},
             lists: 0,
             deletes: [],
             requests: []
           }
         end}
      )

    server = start_supervised!({Bandit, plug: {CompactionStore, agent: agent}, port: 0})
    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)

    config = %{
      bucket: "pulso",
      endpoint: "http://localhost:#{port}",
      region: "us-east-1",
      access_key_id: "test",
      secret_access_key: "test",
      allow_http: true
    }

    key = "tenants/private/v4/signal=logs/manifest.json"
    write_labels = [{"operation", "put_if_none_match"}, {"purpose", "manifest"}, {"direction", "write"}]
    read_labels = [{"operation", "get_if_none_match"}, {"purpose", "manifest"}, {"direction", "read"}]
    writes = value("pulso_object_bytes_total", write_labels)
    reads = value("pulso_object_bytes_total", read_labels)
    conflicts = value("pulso_operations_total", labels("object", "put_if_none_match", "conflict", "manifest"))
    unchanged = value("pulso_operations_total", labels("object", "get_if_none_match", "not_modified", "manifest"))
    missing = value("pulso_operations_total", labels("object", "get_if_none_match", "not_found", "manifest"))

    assert {:ok, etag} = ObjectStore.put_if_none_match(config, key, "body")
    assert {:error, :already_exists} = ObjectStore.put_if_none_match(config, key, "body")
    assert :not_modified = ObjectStore.get_if_none_match(config, key, etag)
    assert {:ok, _, "body"} = ObjectStore.get_if_none_match(config, key, nil)
    assert {:error, :not_found} = ObjectStore.get_if_none_match(config, "tenants/private/missing/manifest.json", nil)
    assert value("pulso_object_bytes_total", write_labels) == writes + 4
    assert value("pulso_object_bytes_total", read_labels) == reads + 4

    assert value("pulso_operations_total", labels("object", "put_if_none_match", "conflict", "manifest")) ==
             conflicts + 1

    assert value("pulso_operations_total", labels("object", "get_if_none_match", "not_modified", "manifest")) ==
             unchanged + 1

    assert value("pulso_operations_total", labels("object", "get_if_none_match", "not_found", "manifest")) ==
             missing + 1

    assert {:ok, _} = ObjectStore.list(config, "tenants/private/")
    assert {:ok, _} = ObjectStore.list_prefixes(config, "tenants/")
    assert {:ok, "body"} = ObjectStore.get(config, key)
    assert :ok = ObjectStore.delete(config, key)

    byte_labels =
      :ets.tab2list(Metrics)
      |> Enum.flat_map(fn
        {{"pulso_object_bytes_total", labels}, _} -> [Map.new(labels)]
        _ -> []
      end)

    refute Enum.any?(byte_labels, &(&1["operation"] in ["list", "list_prefixes", "delete"]))
    refute Enum.any?(byte_labels, &(&1["operation"] in ["get", "get_if_none_match"] and &1["direction"] == "write"))

    refute Enum.any?(
             byte_labels,
             &(&1["operation"] in ["put", "put_if_match", "put_if_none_match"] and &1["direction"] == "read")
           )

    refute Metrics.render() =~ "tenants/private"
  end

  test "metrics restart resets counters without duplicating handlers, and unavailable reporting does not fail work" do
    :ok = Supervisor.terminate_child(Pulso.Supervisor, Metrics)

    on_exit(fn ->
      if Process.whereis(Metrics) == nil, do: Supervisor.restart_child(Pulso.Supervisor, Metrics)
    end)

    assert :ok = Metrics.measure(:query, "query_logs", fn -> :ok end)
    assert {:ok, _} = Supervisor.restart_child(Pulso.Supervisor, Metrics)
    assert requests("query", "query_logs", "ok") == 0
    assert :ok = Metrics.measure(:query, "query_logs", fn -> :ok end)
    assert requests("query", "query_logs", "ok") == 1
  end

  test "monitoring remains independent of unavailable storage" do
    previous = Application.get_env(:pulso, Pulso.Storage)
    Application.put_env(:pulso, Pulso.Storage, adapter: S3)

    on_exit(fn ->
      if previous,
        do: Application.put_env(:pulso, Pulso.Storage, previous),
        else: Application.delete_env(:pulso, Pulso.Storage)
    end)

    assert (build_conn() |> get("/metrics")).status == 200
  end
end
