defmodule Pulso.Errors.ProjectionTest do
  use Pulso.Test.Case, async: true

  alias Pulso.Errors
  alias Pulso.Errors.Projection
  alias Pulso.Errors.Worker
  alias Pulso.MCP.Tools
  alias Pulso.ObjectStore
  alias Pulso.Runtime
  alias Pulso.Runtime.Task
  alias Pulso.Storage.S3
  alias Pulso.Storage.S3.ManifestSupervision
  alias Pulso.Storage.S3.Retention
  alias Pulso.Test.CompactionStore

  @day 86_400_000_000_000

  setup do
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

    server = start_supervised!({Bandit, plug: {CompactionStore, agent: agent}, ip: {127, 0, 0, 1}, port: 0})
    {:ok, {_, port}} = ThousandIsland.listener_info(server)

    config = %{
      bucket: "pulso",
      endpoint: "http://127.0.0.1:#{port}",
      region: "us-east-1",
      access_key_id: "test-#{System.unique_integer([:positive])}",
      secret_access_key: "test",
      allow_http: true,
      refresh_stale_ms: 0
    }

    Runtime.put_env(:pulso, S3, config)
    start_supervised!(ManifestSupervision)

    %{
      agent: agent,
      config: config,
      tenant: "errors-#{System.unique_integer([:positive])}",
      now: System.system_time(:nanosecond)
    }
  end

  defp record(id, timestamp, fingerprint \\ "one") do
    {:ok, row} =
      Errors.record(
        %{
          "event_id" => String.pad_leading(Integer.to_string(id, 16), 32, "0"),
          "timestamp" => 1,
          "message" => fingerprint,
          "fingerprint" => [fingerprint]
        },
        %{},
        "1",
        %{"service" => "api"}
      )

    %{row | timestamp_ns: timestamp}
  end

  defp step(ctx, now \\ nil), do: Projection.step(ctx.tenant, "1", now_ns: now || ctx.now)

  defp changes(ctx, cursor \\ nil, limit \\ 100),
    do: Projection.read(:changes, ctx.tenant, "1", %{"cursor" => cursor, "limit" => limit})

  test "runtime monitoring configuration is opt-in and validates whole-day and cadence bounds" do
    path = Path.expand("../../../config/runtime.exs", __DIR__)
    read = fn -> Config.Reader.read!(path, env: :test, target: :host)[:pulso][Projection] end
    assert read.() == [enabled: false, history_days: 1, interval_ms: 5000]
    Runtime.put_env("PULSO_ERROR_PROJECTION_ENABLED", "true")
    Runtime.put_env("PULSO_ERROR_HISTORY_DAYS", "7")
    Runtime.put_env("PULSO_ERROR_PROJECTION_INTERVAL_MS", "1000")
    assert read.() == [enabled: true, history_days: 7, interval_ms: 1000]

    for {name, invalid} <- [
          {"PULSO_ERROR_HISTORY_DAYS", ["0", "366", "1.5", "7junk"]},
          {"PULSO_ERROR_PROJECTION_INTERVAL_MS", ["999", "60001", "5000.5", "5000junk"]}
        ],
        value <- invalid do
      runtime = %{Runtime.current() | env: Map.put(Runtime.current().env, name, value)}
      Runtime.with(runtime, fn -> assert_raise RuntimeError, fn -> read.() end end)
    end
  end

  test "opt-in supervised discovery publishes compact summaries and a resumable MCP feed", ctx do
    assert Pulso.Errors.Supervisor.children() == []
    Runtime.put_env(:pulso, Projection, enabled: true, history_days: 1, interval_ms: 60_000)
    Runtime.put_env(:pulso, Pulso.Sentry, projects: %{"1" => %{"tenant" => ctx.tenant, "service" => "api"}})
    assert :ok = S3.append(:logs, ctx.tenant, [record(1, ctx.now)])
    [child] = Pulso.Errors.Supervisor.children()
    start_supervised!(child)
    _ = :sys.get_state(Runtime.name(Worker))
    args = %{"tenant" => ctx.tenant, "project" => "1"}
    assert {:ok, [%{"text" => json}]} = Tools.call("list_indexed_error_issues", args)
    assert %{"results" => [%{"retained_count" => 1}], "complete_at_index" => true} = Pulso.JSON.decode!(json)
    assert {:ok, [%{"text" => json}]} = Tools.call("list_error_changes", args)
    assert %{"changes" => [%{"kind" => "baseline_ready"}], "next_cursor" => cursor} = Pulso.JSON.decode!(json)
    assert {:ok, [%{"text" => json}]} = Tools.call("list_error_changes", Map.put(args, "cursor", cursor))
    assert %{"changes" => [], "next_cursor" => next} = Pulso.JSON.decode!(json)
    assert is_binary(next)
  end

  test "ordinary Loki and OTLP logs cannot poison error reads or subsequent projection passes", ctx do
    Runtime.put_env(:pulso, Pulso.Storage, adapter: S3)
    valid = record(1, ctx.now)
    assert :ok = S3.append(:logs, ctx.tenant, [valid])
    assert {:ok, _} = step(ctx)
    assert {:ok, %{"next_cursor" => cursor}} = changes(ctx)

    for labels <- [
          %{"error_source" => "sentry", "error_project" => "1"},
          valid.resource
        ] do
      loki = %{
        streams: [%{stream: labels, values: [[to_string(ctx.now), "ordinary log"]]}]
      }

      otlp = %{
        resourceLogs: [
          %{
            resource: %{
              attributes: Enum.map(labels, fn {key, value} -> %{key: key, value: %{stringValue: value}} end)
            },
            scopeLogs: [%{logRecords: [%{timeUnixNano: to_string(ctx.now), body: %{stringValue: "ordinary log"}}]}]
          }
        ]
      }

      for {path, payload, status} <- [{"/loki/api/v1/push", loki, 204}, {"/v1/logs", otlp, 200}] do
        conn =
          :post
          |> Plug.Test.conn(path, Pulso.JSON.encode!(payload))
          |> Plug.Conn.put_req_header("content-type", "application/json")
          |> Plug.Conn.put_req_header("x-scope-orgid", ctx.tenant)
          |> PulsoWeb.Endpoint.call([])

        assert conn.status == status
      end
    end

    assert :ok = S3.append(:logs, ctx.tenant, [record(2, ctx.now)])
    for _ <- 1..2, do: assert({:ok, %{"complete_at_index" => true}} = step(ctx))
    assert {:ok, %{"results" => [%{"retained_count" => 2}]}} = Projection.read(:issues, ctx.tenant, "1")

    assert {:ok, %{"changes" => [%{"kind" => "issue_observation", "issue" => %{"retained_count" => 2}}]}} =
             changes(ctx, cursor)

    args = %{"project" => "1", "start_ts_ns" => 0, "end_ts_ns" => ctx.now}

    for kind <- [:events, :issues] do
      assert {:ok, json} = Errors.query(kind, ctx.tenant, args)
      assert %{"matched_events" => 2} = Pulso.JSON.decode!(json)
    end
  end

  test "bootstrap is a baseline, late uploads are discovered, SDK copies deduplicate deterministically", ctx do
    old = record(1, ctx.now - 1_000_000_000)
    assert :ok = S3.append(:logs, ctx.tenant, [old])
    assert {:ok, %{"baseline_pending" => false}} = step(ctx)
    assert {:ok, %{"changes" => [%{"kind" => "baseline_ready"}], "next_cursor" => cursor}} = changes(ctx)

    # Both copies have one event ID, but disagree about issue and timestamp.
    # The greatest occurrence timestamp wins independently of ingest order.
    winner = record(2, ctx.now - 3_000_000_000, "winner")
    loser = record(2, ctx.now - 4_000_000_000, "loser")
    assert :ok = S3.append(:logs, ctx.tenant, [winner])
    assert :ok = S3.append(:logs, ctx.tenant, [loser])
    assert :ok = S3.append(:logs, ctx.tenant, [%{winner | observed_timestamp_ns: ctx.now + 1}])
    assert {:ok, _} = step(ctx)
    assert {:ok, %{"changes" => [change], "next_cursor" => next}} = changes(ctx, cursor)
    assert change["kind"] == "new_in_retained_history"
    assert change["issue"]["title"] == "winner"
    assert change["issue"]["retained_count"] == 1
    assert change["issue"]["first_seen_ns"] == winner.timestamp_ns
    assert change["indexed_at_ns"] == ctx.now

    assert {:ok, %{"results" => issues}} = Projection.read(:issues, ctx.tenant, "1")
    assert Enum.sort(Enum.map(issues, & &1["title"])) == ["one", "winner"]
    assert {:ok, _} = step(ctx)
    assert {:ok, %{"changes" => []}} = changes(ctx, next)
    assert {:error, :reset_required} = Projection.read(:changes, ctx.tenant, "2", %{"cursor" => next})
  end

  test "diagnosis and indexed summaries agree on conflicting retries and same-time examples", ctx do
    Runtime.put_env(:pulso, Pulso.Storage, adapter: S3)
    conflicts = for variant <- 1..10, do: record(1, ctx.now, "variant #{variant}")
    examples = for id <- 2..11, do: record(id, ctx.now, "examples")
    assert :ok = S3.append(:logs, ctx.tenant, conflicts ++ examples)
    assert :ok = S3.append(:logs, ctx.tenant, Enum.reverse(conflicts ++ examples))
    assert {:ok, _} = step(ctx)
    args = %{"project" => "1", "start_ts_ns" => ctx.now, "end_ts_ns" => ctx.now}
    assert {:ok, json} = Errors.query(:issues, ctx.tenant, args)
    assert %{"matched_events" => 11, "results" => diagnosed} = Pulso.JSON.decode!(json)
    assert {:ok, %{"results" => indexed}} = Projection.read(:issues, ctx.tenant, "1")

    expected =
      Map.new(diagnosed, fn issue ->
        {issue["issue_id"], {issue["count"], issue["title"], issue["latest_event_id"]}}
      end)

    actual =
      Map.new(indexed, fn issue ->
        {issue["issue_id"], {issue["retained_count"], issue["title"], issue["latest_example"]["event_id"]}}
      end)

    assert actual == expected
    example_issue = Enum.find(indexed, &(&1["title"] == "examples"))
    assert example_issue["latest_example"]["event_id"] == String.duplicate("0", 31) <> "b"
    assert example_issue["retained_count"] == 10
  end

  test "indexed titles are byte-bounded even when a single grapheme contains thousands of codepoints", ctx do
    row = record(1, ctx.now)
    message = "e" <> String.duplicate("\u0301", 2000)
    row = %{row | body: Map.put(row.body, "message", message)}
    assert :ok = S3.append(:logs, ctx.tenant, [row])
    assert {:ok, _} = step(ctx)
    assert {:ok, %{"results" => [%{"title" => title}]}} = Projection.read(:issues, ctx.tenant, "1")
    assert String.valid?(title)
    assert byte_size(title) <= 2000
    assert String.starts_with?(title, "e\u0301")
  end

  test "one pass charges expanded payloads across sources before publishing any progress", ctx do
    assert :ok = S3.append(:logs, ctx.tenant, [record(1, ctx.now)])
    assert {:ok, _} = step(ctx)
    assert {:ok, %{"next_cursor" => cursor}} = changes(ctx)
    payload = String.duplicate("x", 900_000)

    records =
      for id <- 2..40 do
        row = record(id, ctx.now - id)
        %{row | body: Map.put(row.body, "extra", %{"context" => payload})}
      end

    for batch <- Enum.chunk_every(records, 13), do: assert(:ok = S3.append(:logs, ctx.tenant, batch))
    assert {:error, :query_decoded_limit} = step(ctx)
    assert {:ok, %{"changes" => []}} = changes(ctx, cursor)
    assert {:ok, %{"results" => [%{"retained_count" => 1}]}} = Projection.read(:issues, ctx.tenant, "1")
  end

  test "bounded catchup never publishes partial counts or announces historical issues as new", ctx do
    for n <- 1..33, do: assert(:ok = S3.append(:logs, ctx.tenant, [record(n, ctx.now - n)]))
    assert {:ok, %{"baseline_pending" => true, "backlog_segments_at_index" => 1}} = step(ctx)
    assert {:error, :projection_catching_up} = Projection.read(:issues, ctx.tenant, "1")
    assert {:ok, %{"changes" => []}} = changes(ctx)
    assert {:ok, %{"baseline_pending" => false, "backlog_segments_at_index" => 0}} = step(ctx)
    assert {:ok, %{"results" => [%{"retained_count" => 33}]}} = Projection.read(:issues, ctx.tenant, "1")
    assert {:ok, %{"changes" => [%{"kind" => "baseline_ready"}]}} = changes(ctx)
  end

  test "failed projection publication cannot acknowledge or emit independent changes; restart reconciles once", ctx do
    assert :ok = S3.append(:logs, ctx.tenant, [record(1, ctx.now - 10)])
    assert {:ok, _} = step(ctx)
    assert {:ok, %{"next_cursor" => cursor}} = changes(ctx)
    key = Projection.key(ctx.tenant, "1")
    Agent.update(ctx.agent, &%{&1 | faults: %{{"PUT", key} => {403, :before, 1}}})
    assert :ok = S3.append(:logs, ctx.tenant, [record(2, ctx.now - 5)])
    assert {:error, :projection_conflict_or_unconfirmed} = step(ctx)
    assert {:ok, %{"changes" => []}} = changes(ctx, cursor)
    assert {:ok, %{"results" => [%{"retained_count" => 1}]}} = Projection.read(:issues, ctx.tenant, "1")
    assert {:ok, _} = step(ctx)

    assert {:ok, %{"changes" => [%{"kind" => "issue_observation", "issue" => %{"retained_count" => 2}}]}} =
             changes(ctx, cursor)

    # A response lost after the CAS is resolved by exact-byte readback.
    Agent.update(ctx.agent, &%{&1 | faults: %{{"PUT", key} => {403, :after, 1}}})
    assert :ok = S3.append(:logs, ctx.tenant, [record(3, ctx.now - 1)])
    assert {:ok, _} = step(ctx)
    assert {:ok, %{"results" => [%{"retained_count" => 3}]}} = Projection.read(:issues, ctx.tenant, "1")
  end

  test "competing projectors publish one coverage and change sequence", ctx do
    assert :ok = S3.append(:logs, ctx.tenant, [record(1, ctx.now - 10)])
    assert {:ok, _} = step(ctx)
    assert {:ok, %{"next_cursor" => cursor}} = changes(ctx)
    assert :ok = S3.append(:logs, ctx.tenant, [record(2, ctx.now - 5)])
    limits = Runtime.get_env(:pulso, Pulso.QueryRunner, [])
    Runtime.put_env(:pulso, Pulso.QueryRunner, Keyword.put(limits, :class_limits, %{alerting: 2}))
    key = Projection.key(ctx.tenant, "1")
    owner = self()
    Agent.update(ctx.agent, &%{&1 | barriers: %{{"GET", key} => owner}})
    supervisor = start_supervised!({Task.Supervisor, []}, id: :contenders)
    tasks = for _ <- 1..2, do: Task.Supervisor.async_nolink(supervisor, fn -> step(ctx) end)
    assert_receive {:storage_barrier, first, "GET", ^key}, 5000
    assert_receive {:storage_barrier, second, "GET", ^key}, 5000
    Agent.update(ctx.agent, &%{&1 | barriers: %{}})
    send(first, {:release_storage, key})
    send(second, {:release_storage, key})
    results = Enum.map(tasks, &Task.await(&1, 10_000))
    assert Enum.count(results, &match?({:ok, _}, &1)) == 1
    assert Enum.count(results, &(&1 == {:error, :projection_conflict_or_unconfirmed})) == 1
    assert {:ok, %{"changes" => [%{"kind" => "issue_observation", "sequence" => 2}]}} = changes(ctx, cursor)
    assert {:ok, %{"results" => [%{"retained_count" => 2}]}} = Projection.read(:issues, ctx.tenant, "1")
  end

  test "an unseen source that appears and expires during downtime forces reset and a fresh baseline", ctx do
    assert :ok = S3.append(:logs, ctx.tenant, [record(1, ctx.now - 10)])
    assert {:ok, _} = step(ctx)
    assert {:ok, %{"next_cursor" => cursor}} = changes(ctx)
    assert :ok = S3.append(:logs, ctx.tenant, [record(2, ctx.now - 5, "unseen")])
    config = Map.merge(ctx.config, %{logs_retention_days: 1, retention_mode: "enforce"})
    later = ctx.now + 2 * @day
    assert {:ok, _} = Retention.advance(ctx.tenant, "logs", config, now_ns: later, now_ms: div(later, 1_000_000))
    Runtime.put_env(:pulso, S3, config)
    assert {:ok, %{"last_reset_reason" => "source_history_unverifiable"}} = step(ctx, later)
    assert {:ok, %{"changes" => [%{"kind" => "reset"}, %{"kind" => "baseline_ready"}]}} = changes(ctx, cursor)
    assert {:ok, %{"results" => []}} = Projection.read(:issues, ctx.tenant, "1")
  end

  test "reappearance after the explicit history horizon is new within history, never first ever", ctx do
    assert :ok = S3.append(:logs, ctx.tenant, [record(1, ctx.now - 10)])
    assert {:ok, _} = step(ctx)
    assert {:ok, %{"next_cursor" => cursor}} = changes(ctx)
    later = ctx.now + @day + 100
    assert :ok = S3.append(:logs, ctx.tenant, [record(2, later - 1)])
    assert {:ok, _} = step(ctx, later)
    assert {:ok, %{"changes" => [%{"kind" => "new_in_retained_history", "issue" => issue}]}} = changes(ctx, cursor)
    assert issue["retained_count"] == 1
    assert issue["first_seen_ns"] == later - 1
  end

  test "issue continuation resets on changed snapshots and replay cursors reset beyond the retained floor", ctx do
    assert :ok = S3.append(:logs, ctx.tenant, [record(1, ctx.now - 10, "a"), record(2, ctx.now - 9, "b")])
    assert {:ok, _} = step(ctx)
    assert {:ok, %{"next_cursor" => cursor}} = changes(ctx)
    assert {:ok, %{"next_cursor" => issue_cursor}} = Projection.read(:issues, ctx.tenant, "1", %{"limit" => 1})

    assert {:ok, %{"results" => [%{"title" => "a"}]}} =
             Projection.read(:issues, ctx.tenant, "1", %{"limit" => 1, "cursor" => issue_cursor})

    for n <- 3..67 do
      assert :ok = S3.append(:logs, ctx.tenant, [record(n, ctx.now - 1, "a")])
      assert {:ok, _} = step(ctx)
    end

    assert {:error, :reset_required} = changes(ctx, cursor)
    assert {:error, :reset_required} = Projection.read(:issues, ctx.tenant, "1", %{"cursor" => issue_cursor})
    assert {:ok, %{"replay_floor" => floor, "changes" => [change]}} = changes(ctx, nil, 1)
    assert floor > 1
    assert change["sequence"] == floor + 1
  end

  test "missing retained replay pages fail reset rather than silently skipping changes", ctx do
    assert :ok = S3.append(:logs, ctx.tenant, [record(1, ctx.now)])
    assert {:ok, _} = step(ctx)
    assert {:ok, bytes} = ObjectStore.get(ctx.config, Projection.key(ctx.tenant, "1"))
    root = Pulso.JSON.decode!(bytes)
    assert :ok = ObjectStore.delete(ctx.config, hd(root["pages"])["key"])
    assert {:error, :reset_required} = changes(ctx)
  end
end
