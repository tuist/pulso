defmodule PulsoWeb.SentryControllerTest do
  use PulsoWeb.ConnCase, async: true

  alias Pulso.Auth.SharedSecret
  alias Pulso.MCP.Tools
  alias Pulso.PromQL.QuerySlots
  alias Pulso.PromQL.TaskSupervisor
  alias Pulso.Runtime
  alias Pulso.Storage
  alias Pulso.Storage.S3
  alias Pulso.Storage.S3.Manifest
  alias Pulso.Storage.S3.ManifestCache
  alias Pulso.Storage.S3.ManifestOwner
  alias Pulso.Storage.S3.ManifestSupervision
  alias Pulso.Storage.S3.Retention
  alias Pulso.Test.CompactionStore
  alias Pulso.Test.FailingStorage
  alias Pulso.Test.MCPMessages

  @id "0123456789abcdef0123456789abcdef"
  @other_id "abcdef0123456789abcdef0123456789"
  @key "test-public-key"

  setup context do
    project = %{
      "tenant" => "acme",
      "service" => "api",
      "public_key_hashes" => [Base.encode16(:crypto.hash(:sha256, @key), case: :lower)]
    }

    Runtime.put_env(:pulso, Pulso.Sentry, projects: %{"42" => project, "43" => project})
    if context[:s3], do: s3_setup(), else: :ok
  end

  defp event(overrides \\ %{}) do
    Map.merge(
      %{
        "event_id" => @id,
        "timestamp" => "2026-01-01T00:00:00Z",
        "environment" => "production",
        "release" => "app@1.0",
        "exception" => %{
          "values" => [
            %{
              "type" => "RuntimeError",
              "value" => "customer 123 failed",
              "stacktrace" => %{"frames" => [%{"filename" => "app.py", "function" => "run", "in_app" => true}]}
            }
          ]
        },
        "contexts" => %{"trace" => %{"trace_id" => String.duplicate("a", 32), "span_id" => String.duplicate("b", 16)}}
      },
      overrides
    )
  end

  defp envelope(event, headers \\ %{}) do
    payload = Pulso.JSON.encode!(event)

    Pulso.JSON.encode!(headers) <>
      "\n" <> Pulso.JSON.encode!(%{type: "event", length: byte_size(payload)}) <> "\n" <> payload <> "\n"
  end

  defp send_envelope(body, path \\ "/api/42/envelope/") do
    build_conn()
    |> put_req_header("content-type", "application/x-sentry-envelope")
    |> put_req_header("x-sentry-auth", "Sentry sentry_version=7, sentry_key=#{@key}, sentry_client=test/1")
    |> post(path, body)
  end

  defp query(name, extra \\ %{}) do
    args = Map.merge(%{"tenant" => "acme", "start_ts_ns" => 0, "end_ts_ns" => 1_800_000_000_000_000_000}, extra)
    message = MCPMessages.request(1, "tools/call", %{"name" => name, "arguments" => args})

    response =
      build_conn()
      |> put_req_header("content-type", "application/json")
      |> put_req_header("accept", "application/json, text/event-stream")
      |> put_req_header("authorization", "Bearer read-token")
      |> put_req_header("mcp-protocol-version", MCPMessages.version())
      |> put_req_header("mcp-method", "tools/call")
      |> put_req_header("mcp-name", name)
      |> post("/mcp", Pulso.JSON.encode!(message))
      |> json_response(200)

    assert %{"result" => %{"content" => [%{"text" => text}]}} = response
    refute response["result"]["isError"]
    Pulso.JSON.decode!(text)
  end

  defp s3_setup do
    # Exercise the actual native codec and object-store client against an owned
    # conditional-write HTTP store, without requiring shared cloud credentials.
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

    server =
      start_supervised!(
        {Bandit,
         plug: {CompactionStore, agent: agent},
         ip: {127, 0, 0, 1},
         port: 0,
         thousand_island_options: [shutdown_timeout: 100]}
      )

    {:ok, {_, port}} = ThousandIsland.listener_info(server)

    config = %{
      bucket: "pulso",
      endpoint: "http://127.0.0.1:#{port}",
      region: "us-east-1",
      access_key_id: "sentry-#{System.unique_integer([:positive])}",
      secret_access_key: "test",
      allow_http: true
    }

    Runtime.put_env(:pulso, Storage, adapter: S3)
    Runtime.put_env(:pulso, S3, config)
    start_supervised!(ManifestSupervision)
    %{agent: agent, config: config}
  end

  @tag :s3
  test "SDK event survives real Parquet/S3 publication and cold manifest reload", %{agent: agent, config: config} do
    Runtime.put_env(:pulso, Pulso.Auth,
      module: SharedSecret,
      tokens: %{"acme" => "sha256$" <> Base.encode16(:crypto.hash(:sha256, "read-token"), case: :lower)}
    )

    payload =
      event(%{
        "user" => %{"email" => "private@example.com"},
        "request" => %{"url" => "https://example.com/?private=value"},
        "extra" => %{
          "token" => "secret-value",
          "headers" => [["x-private", "secret-header"]],
          "credentials" => [["password", "secret-password"]]
        }
      })

    assert %{"id" => @id} = payload |> envelope() |> send_envelope() |> json_response(200)
    {:ok, owner} = ManifestOwner.ensure_started("acme", "logs", config)
    :ok = GenServer.stop(owner)
    ManifestCache.drop("acme", "logs")

    assert %{"matched_events" => 1, "results" => [result]} = query("query_errors", %{"project" => "42"})
    assert result["event_id"] == @id
    assert result["event"]["exception"] == payload["exception"]
    assert result["event"]["user"] == "[Filtered]"
    assert result["event"]["request"] == "[Filtered]"
    assert result["event"]["extra"]["token"] == "[Filtered]"
    assert result["event"]["extra"]["headers"] == "[Filtered]"
    assert result["event"]["extra"]["credentials"] == [["password", "[Filtered]"]]
    assert result["trace_id"] == String.duplicate("a", 32)
    assert result["service"] == "api"
    assert %{"results" => [%{"count" => 1}]} = query("list_error_issues")
    assert %{"results" => []} = query("query_errors", %{"environment" => "staging"})
    assert %{"results" => []} = query("query_errors", %{"issue_id" => "not-this-issue"})
    assert %{"results" => []} = query("query_errors", %{"release" => "app@2.0"})
    assert {:ok, []} = Storage.query(:logs, "another")
    objects = Agent.get(agent, &Map.keys(&1.objects))
    assert Enum.any?(objects, &String.ends_with?(&1, ".parquet"))
    assert Enum.any?(objects, &String.ends_with?(&1, "/manifest.json"))
  end

  @tag :s3
  test "independent stateless ingest instances publish concurrently and a cold instance reads every acknowledgment" do
    original = Runtime.current()
    first = %{original | instance: __MODULE__.FirstInstance}
    second = %{original | instance: __MODULE__.SecondInstance}

    for {runtime, id} <- [{first, :first_manifest}, {second, :second_manifest}] do
      Runtime.with(runtime, fn ->
        start_supervised!(ManifestSupervision, id: id)
        start_supervised!(Pulso.Metrics, id: {id, :metrics})
        start_supervised!(Pulso.SelfMetrics, id: {id, :self_metrics})
        start_supervised!({Task.Supervisor, name: TaskSupervisor}, id: {id, :queries})
        start_supervised!({Registry, keys: :unique, name: QuerySlots}, id: {id, :slots})
      end)
    end

    supervisor = start_supervised!({Task.Supervisor, []}, id: :ingest_clients)

    uploads =
      for n <- 1..20 do
        Task.Supervisor.async_nolink(
          supervisor,
          Runtime.capture(fn ->
            runtime = if rem(n, 2) == 0, do: first, else: second
            id = n |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(32, "0")

            Runtime.with(runtime, fn ->
              assert %{"id" => ^id} = event(%{"event_id" => id}) |> envelope() |> send_envelope() |> json_response(200)
            end)
          end)
        )
      end

    Enum.each(uploads, &Task.await(&1, 15_000))
    first_page = Runtime.with(first, fn -> query("query_errors", %{"limit" => 7}) end)
    assert length(first_page["results"]) == 7
    assert is_binary(first_page["next_cursor"])
    # Stop both ingest owners/caches; the original read instance has no local
    # manifest state and cannot recover accepted data from either mailbox.
    stop_supervised!(:first_manifest)
    stop_supervised!(:second_manifest)
    assert ManifestCache.get("acme", "logs") == nil

    assert %{"results" => remaining, "next_cursor" => nil} =
             query("query_errors", %{"cursor" => first_page["next_cursor"]})

    assert length(remaining) == 13
    assert %{"matched_events" => 20, "results" => [%{"count" => 20}]} = query("list_error_issues")
    assert %{"results" => events} = query("query_errors")
    assert length(Enum.uniq_by(events, & &1["event_id"])) == 20
    assert first_page["results"] ++ remaining == events
  end

  @tag :s3
  test "S3 enforces the occurrence ceiling across segments before deduplication or result limiting" do
    records =
      for id <- 1..5000 do
        assert {:ok, record} =
                 Pulso.Errors.record(
                   event(%{"event_id" => String.pad_leading(Integer.to_string(id, 16), 32, "0")}),
                   %{},
                   "42",
                   %{"service" => "api"}
                 )

        record
      end

    assert :ok = Storage.append(:logs, "acme", Enum.take(records, 4999))
    assert :ok = Storage.append(:logs, "acme", Enum.drop(records, 4999))
    assert %{"matched_events" => 5000, "results" => [%{"count" => 5000}]} = query("list_error_issues", %{"limit" => 1})
    assert :ok = Storage.append(:logs, "acme", [hd(records)])

    for name <- ["query_errors", "list_error_issues"] do
      assert {:error, :query_sample_limit} =
               Tools.call(name, %{
                 "tenant" => "acme",
                 "start_ts_ns" => 0,
                 "end_ts_ns" => 1_800_000_000_000_000_000,
                 "limit" => 1
               })
    end
  end

  @tag :s3
  test "same-service unrelated logs and projects prune before candidate scan budgets", %{agent: agent, config: config} do
    assert {:ok, record} = Pulso.Errors.record(event(), %{}, "42", %{"service" => "api"})
    ordinary = %{record | resource: %{}, body: "ordinary log"}
    other_project = %{record | resource: Map.put(record.resource, "error_project", "43")}
    assert :ok = Storage.append(:logs, "acme", List.duplicate(ordinary, 100))
    assert :ok = Storage.append(:logs, "acme", List.duplicate(other_project, 100))
    assert :ok = Storage.append(:logs, "acme", [record])
    Runtime.put_env(:pulso, Pulso.QueryRunner, max_scan_rows: 1, max_scan_segments: 1)

    assert %{"matched_events" => 1} = query("query_errors", %{"project" => "42", "service" => "api"})
    opts = [service: "api", matchers: [{"error_source", :eq, "sentry"}, {"error_project", :eq, "42"}]]
    assert {:ok, %{segments: [segment], floor_ns: 0, etag: etag}} = S3.log_snapshot("acme", opts)
    assert is_binary(etag)
    assert segment.row_count == 1
    assert {:error, :query_scan_limit} = S3.log_snapshot("acme", Keyword.put(opts, :max_scan_rows, 0))

    # Old segments have no error summary and must still be scanned, not hidden.
    assert {:ok, root, _} = Retention.load("acme", "logs", config)
    root = %{root | segments: Enum.map(root.segments, &%{&1 | log_labels: nil})}

    assert {:ok, _} =
             Pulso.ObjectStore.put(
               config,
               Manifest.manifest_key("acme", "logs"),
               IO.iodata_to_binary(Manifest.encode(root))
             )

    args = %{"tenant" => "acme", "project" => "42", "start_ts_ns" => 0, "end_ts_ns" => 1_800_000_000_000_000_000}
    assert {:error, :query_scan_limit} = Tools.call("query_errors", args)
    assert Agent.get(agent, & &1.lists) == 1
  end

  @tag :s3
  test "S3 expanded-byte allowance is cumulative across small compressed objects", %{config: config, agent: agent} do
    for {id, timestamp} <- [{@id, 10}, {@other_id, 20}] do
      assert {:ok, record} =
               Pulso.Errors.record(
                 event(%{"event_id" => id, "timestamp" => timestamp, "message" => String.duplicate("x", 700_000)}),
                 %{},
                 "42",
                 %{"service" => "api"}
               )

      assert :ok = Storage.append(:logs, "acme", [record])
    end

    assert {:ok, %{segments: segments}} = S3.log_snapshot("acme", [])
    assert length(segments) == 2
    assert Enum.sum(Enum.map(segments, & &1.byte_size)) < 20_000

    for segment <- segments do
      assert {:ok, payload} = Pulso.ObjectStore.get(config, segment.key)
      assert {:ok, [_], charged} = S3.decode_log_segment_bounded(payload, nil, nil, max_decoded_bytes: 1_048_576)
      assert charged >= 700_000
      assert charged < 800_000
    end

    assert {:ok, [_, _]} = Storage.query(:logs, "acme", max_decoded_bytes: 2_097_152)
    assert {:error, :query_decoded_limit} = Storage.query(:logs, "acme", max_decoded_bytes: 1_048_576)

    # Cleanup removes the older object while a query is in flight. A fresh
    # retry must not reset the allowance already spent decoding the newer one.
    older = Enum.min_by(segments, & &1.min_ts)
    assert :ok = Pulso.ObjectStore.delete(config, older.key)
    owner = self()
    Agent.update(agent, &%{&1 | barriers: %{{"GET", older.key} => owner}})

    task =
      Pulso.Runtime.Task.Supervisor.async_nolink(TaskSupervisor, fn ->
        Storage.query(:logs, "acme", fresh: true, max_decoded_bytes: 1_048_576)
      end)

    key = older.key
    assert_receive {:storage_barrier, request, "GET", ^key}, 5000
    assert {:ok, root, _} = Retention.load("acme", "logs", config)
    root = %{root | segments: Enum.reject(root.segments, &(&1.key == key))}

    assert {:ok, _} =
             Pulso.ObjectStore.put(
               config,
               Manifest.manifest_key("acme", "logs"),
               IO.iodata_to_binary(Manifest.encode(root))
             )

    send(request, {:release_storage, key})
    assert {:error, :query_decoded_limit} = Task.await(task)
    assert {:ok, [_]} = Storage.query(:logs, "acme", fresh: true, max_decoded_bytes: 1_048_576)
  end

  @tag :s3
  test "small response pages cannot bypass the whole-window expanded-byte ceiling" do
    records =
      for id <- 1..40 do
        assert {:ok, record} =
                 Pulso.Errors.record(
                   event(%{
                     "event_id" => String.pad_leading(Integer.to_string(id, 16), 32, "0"),
                     "message" => String.duplicate("x", 900_000)
                   }),
                   %{},
                   "42",
                   %{"service" => "api"}
                 )

        record
      end

    for batch <- Enum.chunk_every(records, 10), do: assert(:ok = Storage.append(:logs, "acme", batch))
    assert {:ok, %{segments: segments}} = S3.log_snapshot("acme", [])
    assert Enum.sum(Enum.map(segments, & &1.byte_size)) < 100_000

    for name <- ["query_errors", "list_error_issues"] do
      assert {:error, :query_decoded_limit} =
               Tools.call(name, %{
                 "tenant" => "acme",
                 "start_ts_ns" => 0,
                 "end_ts_ns" => 1_800_000_000_000_000_000,
                 "limit" => 1
               })
    end
  end

  @tag :s3
  test "fresh diagnostic reads and source snapshots never use stale roots or reconstruct missing manifests", %{
    agent: agent,
    config: config
  } do
    assert %{"id" => @id} = event() |> envelope() |> send_envelope() |> json_response(200)
    assert %{"matched_events" => 1} = query("query_errors")
    key = Manifest.manifest_key("acme", "logs")
    lists = Agent.get(agent, & &1.lists)
    Agent.update(agent, &%{&1 | faults: %{{"GET", key} => {403, :before, 100}}})
    args = %{"tenant" => "acme", "start_ts_ns" => 0, "end_ts_ns" => 1_800_000_000_000_000_000}
    assert {:error, _} = Tools.call("query_errors", args)
    assert {:error, _} = S3.log_snapshot("acme", [])
    Agent.update(agent, &%{&1 | faults: %{}})
    assert :ok = Pulso.ObjectStore.delete(config, key)
    assert {:error, :not_found} = Tools.call("query_errors", args)
    assert {:error, :not_found} = S3.log_snapshot("acme", [])
    assert Agent.get(agent, & &1.lists) == lists
  end

  @tag :s3
  test "paged source snapshots expose the fresh retained floor and invalidate pages when retained results change", %{
    config: config
  } do
    now = System.system_time(:nanosecond)

    for {id, timestamp} <- [{@id, now - 10_000_000_000}, {@other_id, now - 5_000_000_000}] do
      assert {:ok, record} = Pulso.Errors.record(event(%{"event_id" => id}), %{}, "42", %{"service" => "api"})
      assert :ok = Storage.append(:logs, "acme", [%{record | timestamp_ns: timestamp}])
    end

    args = %{"tenant" => "acme", "start_ts_ns" => 0, "end_ts_ns" => now, "limit" => 1}
    assert {:ok, [%{"text" => json}]} = Tools.call("query_errors", args)
    cursor = Pulso.JSON.decode!(json)["next_cursor"]

    config =
      Map.merge(config, %{
        logs_retention_days: 1,
        retention_enabled: true,
        retention_mode: "enforce",
        retention_tail_entries: 1
      })

    Runtime.put_env(:pulso, S3, config)
    assert {:ok, _} = Retention.advance("acme", "logs", config, now_ns: now)
    assert {:ok, root, _} = Retention.load("acme", "logs", config)
    assert root.version == 3
    floor = now - 86_400_000_000_000

    assert {:ok, %{segments: [_, _], floor_ns: ^floor}} =
             S3.log_snapshot("acme", matchers: [{"error_project", :eq, "42"}])

    assert {:ok, _} = Tools.call("query_errors", Map.put(args, "cursor", cursor))

    future = now + 86_400_000_000_000 - 7_000_000_000
    floor = now - 7_000_000_000
    assert {:ok, _} = Retention.advance("acme", "logs", config, now_ns: future)
    assert {:ok, %{segments: [_], floor_ns: ^floor}} = S3.log_snapshot("acme", [])
    assert {:error, :error_query_reset_required} = Tools.call("query_errors", Map.put(args, "cursor", cursor))
    assert {:ok, [%{"text" => json}]} = Tools.call("query_errors", args)
    assert %{"matched_events" => 1, "results" => [%{"event_id" => @other_id}]} = Pulso.JSON.decode!(json)
  end

  test "raw byte framing accepts multiline UTF-8 JSON and skips binary attachments" do
    payload = ~s({\n"event_id":"#{@id}",\n"message":"échec"\n})

    body =
      ~s({}\n{"type":"attachment","length":4}\n) <>
        <<255, 10, 0, 13>> <>
        ~s(\n{"type":"event","length":#{byte_size(payload)}}\n) <> payload

    response = send_envelope(body)
    assert %{"id" => @id, "discarded_items" => %{"attachment" => 1}} = json_response(response, 200)
    assert get_resp_header(response, "x-pulso-discarded-items") == ["1"]
    assert %{"results" => [%{"event" => %{"message" => "échec"}}]} = query("query_errors")
  end

  test "browser and SDK authentication forms agree and cannot override tenant attribution" do
    headers = %{"dsn" => "https://#{@key}@errors.example.com/42", "event_id" => @other_id}
    body = envelope(event(), headers)

    for type <- ["text/plain", "application/x-www-form-urlencoded", "multipart/form-data", "application/json"] do
      response =
        build_conn()
        |> put_req_header("content-type", type)
        |> put_req_header("x-scope-orgid", "victim")
        |> post("/api/42/envelope/?sentry_key=#{@key}", body)

      assert %{"id" => @other_id} = json_response(response, 200)
      assert get_resp_header(response, "access-control-allow-origin") == ["*"]
    end

    assert {:ok, []} = Storage.query(:logs, "victim")
    assert %{"results" => [%{"event_id" => @other_id}]} = query("query_errors")
    assert :post |> Plug.Test.conn("/api/42/envelope/", body) |> PulsoWeb.Endpoint.call([]) |> json_response(200)

    assert :post
           |> Plug.Test.conn("/api/42/envelope/?sentry_key=wrong", body)
           |> PulsoWeb.Endpoint.call([])
           |> json_response(403)

    assert body |> send_envelope("/api/43/envelope/") |> json_response(403)
    assert envelope(event()) |> send_envelope("/api/999/envelope/") |> json_response(403)

    assert :post
           |> Plug.Test.conn("/api/42/envelope/", envelope(event()))
           |> PulsoWeb.Endpoint.call([])
           |> json_response(403)

    response = build_conn() |> put_req_header("origin", "https://app.example.com") |> options("/api/42/envelope/")
    assert response.status == 204
    assert get_resp_header(response, "access-control-allow-origin") == ["*"]
  end

  test "gzip envelopes and legacy JSON store requests work without SDK modifications" do
    response =
      build_conn()
      |> put_req_header("content-type", "application/x-sentry-envelope")
      |> put_req_header("content-encoding", "gzip")
      |> post("/api/42/envelope/?sentry_key=#{@key}", :zlib.gzip(envelope(event())))

    assert %{"id" => @id} = json_response(response, 200)

    response =
      build_conn()
      |> put_req_header("content-type", "application/json")
      |> post("/api/42/store/?sentry_key=#{@key}", Pulso.JSON.encode!(event()))

    assert %{"id" => @id} = json_response(response, 200)
    assert %{"matched_events" => 1} = query("query_errors")
  end

  test "malformed, truncated and duplicate event envelopes append nothing" do
    valid = envelope(event())

    for body <- [
          "bad",
          ~s({}\n{"type":"event","length":99999}\n{}),
          valid <> "{}\n",
          valid <> ~s({"type":"event"}\n{}\n),
          ~s({}\n{"type":"event","length":-1}\n{})
        ] do
      assert body |> send_envelope() |> json_response(400)
    end

    for overrides <- [
          %{"event_id" => "bad"},
          %{"timestamp" => "bad"},
          %{"timestamp" => -1},
          %{"timestamp" => "2500-01-01T00:00:00Z"},
          %{"fingerprint" => [123]}
        ] do
      assert event(overrides) |> envelope() |> send_envelope() |> json_response(400)
    end

    assert {:ok, []} = Storage.query(:logs, "acme")
  end

  test "unsupported-only envelopes report discarded data rather than inventing events" do
    assert %{"id" => nil, "discarded_items" => %{"transaction" => 1}} =
             ~s({}\n{"type":"transaction"}\n{}\n) |> send_envelope() |> json_response(200)

    assert {:ok, []} = Storage.query(:logs, "acme")
  end

  test "request, expansion, event, item and structure budgets reject without publication" do
    for body <- [
          String.duplicate("a", 4 * 1024 * 1024 + 1),
          "{}\n" <> String.duplicate(~s({"type":"session"}\n{}\n), 101),
          envelope(event(%{"message" => String.duplicate("a", 1_048_576)})),
          envelope(event(%{"extra" => Enum.to_list(1..1100)}))
        ] do
      assert body |> send_envelope() |> json_response(413)
    end

    response =
      build_conn()
      |> put_req_header("content-type", "application/x-sentry-envelope")
      |> put_req_header("content-encoding", "gzip")
      |> post("/api/42/envelope/?sentry_key=#{@key}", :zlib.gzip(String.duplicate("a", 17 * 1024 * 1024)))

    assert json_response(response, 413)
    assert {:ok, []} = Storage.query(:logs, "acme")
  end

  test "publication failures return retryable, non-sensitive errors rather than success" do
    Runtime.put_env(:pulso, Storage, adapter: FailingStorage)
    Runtime.put_env(:pulso, FailingStorage, {:error, "secret-provider-url"})
    response = envelope(event()) |> send_envelope()
    assert json_response(response, 503) == %{"error" => "storage_unavailable"}
    assert get_resp_header(response, "retry-after") == ["5"]
  end
end
