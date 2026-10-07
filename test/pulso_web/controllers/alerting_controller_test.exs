defmodule PulsoWeb.AlertingControllerTest do
  use PulsoWeb.ConnCase, async: false

  alias Pulso.Alerting.Canonical
  alias Pulso.Alerting.Evaluator
  alias Pulso.Alerting.Notifier
  alias Pulso.Alerting.Repository
  alias Pulso.Alerting.Resources
  alias Pulso.Alerting.Worker
  alias Pulso.MCP.Tools
  alias Pulso.Record.MetricSample
  alias Pulso.Test.CompactionStore
  alias Pulso.Test.MCPMessages

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

    actor = %{
      tenant: "transport-alerts",
      id: "pedro",
      type: "human",
      token_hash: Canonical.digest("admin-token"),
      capabilities: ~w(alert:read alert:rules:write alert:audit:read alert:preview alert:evaluate alert:import)
    }

    old = Application.get_env(:pulso, Pulso.Alerting)
    Application.put_env(:pulso, Pulso.Alerting, principals: [actor], store_config: config)

    on_exit(fn ->
      if old, do: Application.put_env(:pulso, Pulso.Alerting, old), else: Application.delete_env(:pulso, Pulso.Alerting)
    end)

    %{config: config, actor: actor}
  end

  defp rule,
    do: %{
      "name" => "Backup failed",
      "query" => "max(backup_failed)",
      "threshold" => %{"op" => "gt", "value" => 0},
      "enabled" => true,
      "cadence_ms" => 1000
    }

  defp params(op, revision \\ nil),
    do: %{"rule" => rule(), "operation_id" => op, "reason" => "Watch backups", "expected_revision" => revision}

  defp request(method, path, body \\ %{}, token \\ "admin-token") do
    build_conn()
    |> put_req_header("authorization", "Bearer " <> token)
    |> put_req_header("x-scope-orgid", "transport-alerts")
    |> put_req_header("content-type", "application/json")
    |> Phoenix.ConnTest.dispatch(@endpoint, method, path, Pulso.JSON.encode!(body))
  end

  test "HTTP config, history, preconditions and restore share the durable service" do
    created = request(:post, "/api/v1/alerting/rules/backup", params("create")) |> json_response(200)
    changed = request(:put, "/api/v1/alerting/rules/backup", params("edit", created["revision"])) |> json_response(200)
    assert request(:put, "/api/v1/alerting/rules/backup", params("stale", created["revision"])) |> json_response(409)
    history = request(:get, "/api/v1/alerting/rules/backup/changes") |> json_response(200)
    assert length(history["changes"]) == 2

    restored =
      request(:post, "/api/v1/alerting/rules/backup/restore", %{
        "revision" => created["revision"],
        "expected_revision" => changed["revision"],
        "operation_id" => "restore",
        "reason" => "Restore approved baseline"
      })
      |> json_response(200)

    assert restored["state"] == "disabled"
    assert restored["actor"]["id"] == "pedro"
  end

  test "unauthorized calls fail before malformed rule validation" do
    response = request(:post, "/api/v1/alerting/rules/backup", %{"rule" => "malformed"}, "wrong") |> json_response(401)
    assert response == %{"error" => "unauthorized"}
  end

  test "native evaluation works on stored metrics, and preview makes no writes", ctx do
    now = System.system_time(:nanosecond)

    :ok =
      Pulso.Storage.append(:metrics, "transport-alerts", [
        %MetricSample{
          timestamp_ns: now - 2_000_000_000,
          value: 1.0,
          labels: %{"__name__" => "backup_failed"}
        }
      ])

    assert request(:post, "/api/v1/alerting/rules/backup", params("create")) |> json_response(200)
    preview = request(:post, "/api/v1/alerting/rules/backup/preview") |> json_response(200)
    assert [%{"status" => "firing"}] = Map.values(preview["instances"])
    before = request(:get, "/api/v1/alerting/rules/backup/state") |> json_response(200)
    assert before["completed_at_ns"] == "-1"
    assert before["instances"] == %{}
    assert {:ok, 1} = Worker.run_once([store_config: ctx.config], [node()])
    after_eval = request(:get, "/api/v1/alerting/rules/backup/state") |> json_response(200)
    assert [%{"status" => "firing"}] = Map.values(after_eval["instances"])
    events = request(:post, "/api/v1/alerting/rules/backup/events") |> json_response(200)
    assert Enum.any?(events["events"], &(&1["type"] == "firing"))
  end

  test "resources expose authorized replay frontiers without acknowledging transitions", ctx do
    assert {:ok, _} = Pulso.Alerting.create(ctx.actor, "backup", params("create"))

    conn =
      build_conn()
      |> put_req_header("authorization", "Bearer admin-token")
      |> put_req_header("x-scope-orgid", ctx.actor.tenant)

    assert {:ok, listing} = Resources.list(conn, %{})
    assert length(listing["resources"]) == 3
    uri = Resources.uri(ctx.actor.tenant, "backup", "events")
    assert {:ok, response} = Resources.read(conn, uri)
    checkpoint = response["contents"] |> hd() |> Map.fetch!("text") |> Pulso.JSON.decode!()
    assert checkpoint["latest_sequence"] == "0"
    assert checkpoint["replay_floor"] == "1"
    assert is_binary(checkpoint["checkpoint_cursor"])
    assert response["ttlMs"] == 0
    assert {:error, :unauthorized} = Resources.read(build_conn(), uri)
    assert {:error, :unauthorized} = Resources.read(conn, Resources.uri("other", "backup", "events"))
    refute Enum.any?(listing["resources"], &String.contains?(&1["uri"], checkpoint["checkpoint_cursor"]))
  end

  test "native notifications drain committed Slack outboxes on a successful pass", ctx do
    target = %{
      "tenant" => ctx.actor.tenant,
      "id" => "slack",
      "type" => "slack_webhook",
      "secret_env" => "PULSO_ALERTING_TEST_WEBHOOK"
    }

    System.put_env("PULSO_ALERTING_TEST_WEBHOOK", "https://notifications.invalid/private-webhook")
    on_exit(fn -> System.delete_env("PULSO_ALERTING_TEST_WEBHOOK") end)

    Application.put_env(:pulso, Pulso.Alerting,
      principals: [ctx.actor],
      store_config: ctx.config,
      notification_targets: [target]
    )

    input = params("notify") |> put_in(["rule", "notification_targets"], ["slack"])
    assert {:ok, _} = Pulso.Alerting.create(ctx.actor, "backup", input)

    query = fn _, _, _ ->
      {:ok,
       %{
         "data" => %{
           "resultType" => "vector",
           "result" => [%{"metric" => %{"service" => "<!channel>"}, "value" => [1, "1"]}]
         }
       }}
    end

    assert {:ok, _} = Evaluator.evaluate(ctx.actor, "backup", timestamp_ns: 1_000_000_000, query: query)
    owner = self()

    Req.Test.stub(:native_slack, fn conn ->
      {:ok, bytes, conn} = Plug.Conn.read_body(conn)
      body = Pulso.JSON.decode!(bytes)
      assert hd(body["blocks"])["text"]["type"] == "plain_text"
      assert body["text"] =~ "Pulso alert"
      send(owner, :slack_delivered)
      Plug.Conn.send_resp(conn, 200, "ok")
    end)

    assert 1 = Notifier.run_once(ctx.actor.tenant, "backup", http_options: [plug: {Req.Test, :native_slack}])
    assert_receive :slack_delivered
    assert 0 = Notifier.run_once(ctx.actor.tenant, "backup", http_options: [plug: {Req.Test, :native_slack}])
    refute_receive :slack_delivered
    assert {:ok, head, _} = Repository.load(ctx.actor.tenant, "backup")
    assert Enum.all?(head["outboxes"], fn {_key, box} -> box["queue"] == [] end)
    assert {:ok, events} = Pulso.Alerting.events(ctx.actor, "backup")
    assert Enum.any?(events["events"], &(&1["type"] == "firing"))
    refute Pulso.JSON.encode!(head) =~ "private-webhook"
  end

  test "failed notifications retain durable work and lease ownership across restarts", ctx do
    target = %{
      "tenant" => ctx.actor.tenant,
      "id" => "slack",
      "type" => "slack_webhook",
      "secret_env" => "PULSO_ALERTING_TEST_WEBHOOK"
    }

    System.put_env("PULSO_ALERTING_TEST_WEBHOOK", "https://notifications.invalid/private-webhook")
    on_exit(fn -> System.delete_env("PULSO_ALERTING_TEST_WEBHOOK") end)

    Application.put_env(:pulso, Pulso.Alerting,
      principals: [ctx.actor],
      store_config: ctx.config,
      notification_targets: [target]
    )

    input = params("notify") |> put_in(["rule", "notification_targets"], ["slack"])
    assert {:ok, _} = Pulso.Alerting.create(ctx.actor, "backup", input)

    query = fn _, _, _ ->
      {:ok, %{"data" => %{"resultType" => "vector", "result" => [%{"metric" => %{}, "value" => [1, "1"]}]}}}
    end

    assert {:ok, _} = Evaluator.evaluate(ctx.actor, "backup", timestamp_ns: 1_000_000_000, query: query)
    Req.Test.stub(:failed_slack, &Plug.Conn.send_resp(&1, 503, "unavailable"))
    assert 1 = Notifier.run_once(ctx.actor.tenant, "backup", http_options: [plug: {Req.Test, :failed_slack}])
    assert 0 = Notifier.run_once(ctx.actor.tenant, "backup", http_options: [plug: {Req.Test, :failed_slack}])
    assert {:ok, head, _} = Repository.load(ctx.actor.tenant, "backup")
    assert [{_, box}] = Map.to_list(head["outboxes"])
    assert length(box["queue"]) == 1
    assert is_map(box["claim"])
    Req.Test.stub(:recovered_slack, &Plug.Conn.send_resp(&1, 200, "ok"))
    after_expiry = String.to_integer(box["claim"]["expires_ns"]) + 1

    assert 1 =
             Notifier.run_once(ctx.actor.tenant, "backup",
               timestamp_ns: after_expiry,
               http_options: [plug: {Req.Test, :recovered_slack}]
             )

    assert {:ok, drained, _} = Repository.load(ctx.actor.tenant, "backup")
    assert drained["outboxes"] == %{}
  end

  test "real HTTP subscription acknowledges before hints and closes on credential revocation", ctx do
    assert {:ok, created} = Pulso.Alerting.create(ctx.actor, "backup", params("create"))
    http = start_supervised!(Supervisor.child_spec({Bandit, plug: PulsoWeb.Endpoint, port: 0}, id: :mcp_http))
    {:ok, {_ip, port}} = ThousandIsland.listener_info(http)
    uri = Resources.uri(ctx.actor.tenant, "backup", "changes")

    message =
      MCPMessages.request("watch", "subscriptions/listen", %{"notifications" => %{"resourceSubscriptions" => [uri]}})

    headers = [
      {"mcp-protocol-version", "2026-07-28"},
      {"mcp-method", "subscriptions/listen"},
      {"accept", "application/json, text/event-stream"},
      {"authorization", "Bearer admin-token"}
    ]

    owner = self()
    supervisor = start_supervised!({Task.Supervisor, []})

    task =
      Task.Supervisor.async_nolink(supervisor, fn ->
        Req.post("http://localhost:#{port}/mcp",
          json: message,
          headers: headers,
          retry: false,
          receive_timeout: 15_000,
          into: fn {:data, bytes}, {req, response} ->
            send(owner, {:sse_bytes, bytes})
            {:cont, {req, response}}
          end
        )
      end)

    {ack, buffer} = next_sse("")
    assert ack["method"] == "notifications/subscriptions/acknowledged"
    assert ack["params"]["notifications"] == %{"resourceSubscriptions" => [uri]}
    assert {:ok, _} = Pulso.Alerting.update(ctx.actor, "backup", params("edit", created["revision"]))
    {hint, buffer} = next_sse(buffer)
    assert hint["method"] == "notifications/resources/updated"
    assert hint["params"]["uri"] == uri
    assert hint["params"]["_meta"]["io.modelcontextprotocol/subscriptionId"] == "watch"
    Application.put_env(:pulso, Pulso.Alerting, principals: [], store_config: ctx.config)
    {completion, _buffer} = next_sse(buffer)
    assert completion["id"] == "watch"
    assert completion["result"]["resultType"] == "complete"
    assert {:ok, %{status: 200}} = Task.await(task)
  end

  defp next_sse(buffer) do
    case String.split(buffer, "\n\n", parts: 2) do
      ["data: " <> json, rest] ->
        {Pulso.JSON.decode!(json), rest}

      [_comment, rest] ->
        next_sse(rest)

      [_partial] ->
        assert_receive {:sse_bytes, bytes}, 5000
        next_sse(buffer <> bytes)
    end
  end

  test "MCP writes use configured principals, never open/shared query auth" do
    args =
      Map.merge(params("mcp-create"), %{"tenant" => "transport-alerts", "id" => "backup"})
      |> Map.delete("expected_revision")

    assert {:error, :unauthorized} = Tools.call("create_alert_rule", args)
    conn = build_conn() |> put_req_header("authorization", "Bearer admin-token")
    assert {:ok, [%{"text" => text}]} = Tools.call("create_alert_rule", args, %{conn: conn})
    result = Pulso.JSON.decode!(text)
    assert result["actor"]["id"] == "pedro"

    assert {:ok, [%{"text" => history}]} =
             Tools.call("list_alert_rule_changes", %{"tenant" => "transport-alerts", "id" => "backup"}, %{conn: conn})

    assert length(Pulso.JSON.decode!(history)["changes"]) == 1
  end
end
