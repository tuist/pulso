defmodule Pulso.Alerting.RepositoryTest do
  use ExUnit.Case, async: true

  alias Pulso.Alerting
  alias Pulso.Alerting.{Canonical, Evaluator, Importer, Repository}
  alias Pulso.Test.CompactionStore

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
      tenant: "alerts",
      id: "pedro",
      type: "human",
      capabilities: ~w(alert:read alert:rules:write alert:audit:read alert:preview alert:evaluate alert:import)
    }

    opts = [store_config: config, cursor_secret: "a-stable-test-cursor-secret"]
    %{actor: actor, opts: opts, agent: agent}
  end

  defp rule(extra \\ %{}),
    do:
      Map.merge(
        %{
          "name" => "Backup failed",
          "query" => "max(backup_failed)",
          "threshold" => %{"op" => "gt", "value" => 0},
          "enabled" => true,
          "cadence_ms" => 1000
        },
        extra
      )

  defp request(rule, op, revision \\ nil),
    do: %{"rule" => rule, "operation_id" => op, "reason" => "Review backup failures", "expected_revision" => revision}

  defp query(value),
    do: fn _, _, _ ->
      {:ok,
       %{"data" => %{"resultType" => "vector", "result" => [%{"metric" => %{}, "value" => [0, to_string(value)]}]}}}
    end

  test "publishes configuration and its verified audit together and exact retries reuse the revision", ctx do
    params = request(rule(), "create")
    assert {:ok, first} = Alerting.create(ctx.actor, "backup", params, ctx.opts)
    assert first["actor"] == %{"id" => "pedro", "type" => "human"}
    assert {:ok, ^first} = Alerting.create(ctx.actor, "backup", params, ctx.opts)
    assert {:ok, current} = Alerting.get(ctx.actor, "backup", ctx.opts)
    assert current["revision"] == first["revision"]
    assert {:ok, page} = Alerting.changes(ctx.actor, "backup", %{}, ctx.opts)
    assert length(page["changes"]) == 1
    assert page["next_cursor"] == nil

    assert {:error, :operation_reused} =
             Alerting.create(ctx.actor, "backup", request(rule(%{"name" => "Other"}), "create"), ctx.opts)
  end

  test "actual native commit-then-500/412 is read back as success", ctx do
    key = "tenants/alerts/alerting/v1/rules/backup/head.json"
    Agent.update(ctx.agent, &%{&1 | faults: %{{"PUT", key} => {500, :after, 1}}})
    assert {:ok, created} = Alerting.create(ctx.actor, "backup", request(rule(), "lost-response"), ctx.opts)
    assert {:ok, page} = Alerting.changes(ctx.actor, "backup", %{}, ctx.opts)
    assert [change] = page["changes"]
    assert change["revision"] == created["revision"]
  end

  test "stale edits cannot overwrite newer configuration and no orphan is audit history", ctx do
    assert {:ok, first} = Alerting.create(ctx.actor, "backup", request(rule(), "create"), ctx.opts)

    assert {:ok, second} =
             Alerting.update(
               ctx.actor,
               "backup",
               request(rule(%{"name" => "New"}), "edit", first["revision"]),
               ctx.opts
             )

    assert {:error, :conflict} =
             Alerting.update(
               ctx.actor,
               "backup",
               request(rule(%{"name" => "Old"}), "stale", first["revision"]),
               ctx.opts
             )

    assert {:ok, page} = Alerting.changes(ctx.actor, "backup", %{}, ctx.opts)
    assert Enum.map(page["changes"], & &1["revision"]) == [second["revision"], first["revision"]]
  end

  test "frozen-tip pagination continues even when another edit moves the head", ctx do
    assert {:ok, first} = Alerting.create(ctx.actor, "backup", request(rule(), "create"), ctx.opts)
    assert {:ok, second} = Alerting.update(ctx.actor, "backup", request(rule(), "edit", first["revision"]), ctx.opts)
    assert {:ok, page} = Alerting.changes(ctx.actor, "backup", %{"limit" => 1}, ctx.opts)
    assert {:ok, _third} = Alerting.update(ctx.actor, "backup", request(rule(), "edit2", second["revision"]), ctx.opts)

    assert {:ok, continuation} =
             Alerting.changes(ctx.actor, "backup", %{"cursor" => page["next_cursor"], "limit" => 1}, ctx.opts)

    assert Enum.map(continuation["changes"], & &1["revision"]) == [first["revision"]]
    other = %{ctx.actor | id: "other"}
    assert {:error, :reset_required} = Alerting.changes(other, "backup", %{"cursor" => page["next_cursor"]}, ctx.opts)
  end

  test "deleted generations keep audit ancestry and recreation gets a fresh nonce", ctx do
    assert {:ok, first} = Alerting.create(ctx.actor, "backup", request(rule(), "create"), ctx.opts)
    params = Map.delete(request(rule(), "delete", first["revision"]), "rule")
    assert {:ok, deleted} = Alerting.delete(ctx.actor, "backup", params, ctx.opts)
    assert {:error, :not_found} = Alerting.get(ctx.actor, "backup", ctx.opts)

    assert {:ok, recreated} =
             Alerting.create(ctx.actor, "backup", request(rule(), "recreate", deleted["revision"]), ctx.opts)

    refute recreated["generation"] == first["generation"]
    assert {:ok, historical} = Alerting.historical(ctx.actor, "backup", first["revision"], ctx.opts)
    assert historical["rule"]["name"] == "Backup failed"
    assert {:ok, page} = Alerting.changes(ctx.actor, "backup", %{}, ctx.opts)
    assert length(page["changes"]) == 3
  end

  test "restore creates a disabled revision rather than replaying state or rewriting history", ctx do
    assert {:ok, first} = Alerting.create(ctx.actor, "backup", request(rule(), "create"), ctx.opts)

    assert {:ok, second} =
             Alerting.update(
               ctx.actor,
               "backup",
               request(rule(%{"name" => "New"}), "edit", first["revision"]),
               ctx.opts
             )

    params = %{
      "revision" => first["revision"],
      "expected_revision" => second["revision"],
      "operation_id" => "restore",
      "reason" => "Undo title change"
    }

    assert {:ok, restored} = Alerting.restore(ctx.actor, "backup", params, ctx.opts)
    assert restored["rule"]["name"] == "Backup failed"
    assert restored["rule"]["enabled"] == false
    refute restored["revision"] == first["revision"]
  end

  test "normal evaluations are fenced and pending/firing/hold state survives evaluator restart", ctx do
    assert {:ok, _} =
             Alerting.create(
               ctx.actor,
               "backup",
               request(rule(%{"for_ms" => 1000, "keep_firing_ms" => 1000}), "create"),
               ctx.opts
             )

    assert {:ok, first} =
             Evaluator.evaluate(ctx.actor, "backup", ctx.opts ++ [timestamp_ns: 1_000_000_000, query: query(1)])

    assert [%{"status" => "pending"}] = Map.values(first["instances"])

    assert {:ok, second} =
             Evaluator.evaluate(ctx.actor, "backup", ctx.opts ++ [timestamp_ns: 2_000_000_000, query: query(1)])

    assert [%{"status" => "firing"}] = Map.values(second["instances"])

    assert {:ok, third} =
             Evaluator.evaluate(ctx.actor, "backup", ctx.opts ++ [timestamp_ns: 3_000_000_000, query: query(0)])

    assert [%{"status" => "recovering"}] = Map.values(third["instances"])

    assert {:ok, fourth} =
             Evaluator.evaluate(ctx.actor, "backup", ctx.opts ++ [timestamp_ns: 4_000_000_000, query: query(0)])

    assert fourth["instances"] == %{}

    assert {:error, :not_due} =
             Evaluator.evaluate(ctx.actor, "backup", ctx.opts ++ [timestamp_ns: 3_000_000_000, query: query(1)])

    assert {:ok, events} = Alerting.events(ctx.actor, "backup", %{}, ctx.opts)
    assert Enum.map(events["events"], & &1["type"]) == ["pending", "health_changed", "firing", "recovering", "resolved"]
  end

  test "source failure and local admission skip are not healthy empty results", ctx do
    assert {:ok, _} = Alerting.create(ctx.actor, "backup", request(rule(), "create"), ctx.opts)

    assert {:ok, firing} =
             Evaluator.evaluate(ctx.actor, "backup", ctx.opts ++ [timestamp_ns: 1_000_000_000, query: query(1)])

    error = fn _, _, _ -> {:error, :backend_unavailable} end

    assert {:ok, degraded} =
             Evaluator.evaluate(ctx.actor, "backup", ctx.opts ++ [timestamp_ns: 2_000_000_000, query: error])

    assert degraded["instances"] == firing["instances"]
    assert degraded["health"] == "error"
    busy = fn _, _, _ -> {:error, :query_overloaded} end

    assert {:error, :evaluation_skipped} =
             Evaluator.evaluate(ctx.actor, "backup", ctx.opts ++ [timestamp_ns: 3_000_000_000, query: busy])

    assert {:ok, state} = Alerting.state(ctx.actor, "backup", ctx.opts)
    assert state["completed_at_ns"] == "2000000000"
  end

  test "sealed pages provide forward replay, and candidate integrity is checked", ctx do
    assert {:ok, _} = Alerting.create(ctx.actor, "backup", request(rule(), "create"), ctx.opts)

    for n <- 1..70 do
      assert {:ok, _} =
               Evaluator.evaluate(
                 ctx.actor,
                 "backup",
                 ctx.opts ++ [timestamp_ns: n * 1_000_000_000, query: query(rem(n, 2))]
               )
    end

    assert {:ok, head, _} = Repository.load("alerts", "backup", ctx.opts)
    assert length(head["pages"]) == 1
    assert {:ok, page1} = Alerting.events(ctx.actor, "backup", %{"limit" => 10}, ctx.opts)

    assert {:ok, page2} =
             Alerting.events(ctx.actor, "backup", %{"limit" => 10, "cursor" => page1["next_cursor"]}, ctx.opts)

    assert hd(page2["events"])["sequence"] == "11"
    ref = head["revision"]

    Agent.update(ctx.agent, fn state ->
      {etag, _bytes} = state.objects[ref["key"]]
      %{state | objects: Map.put(state.objects, ref["key"], {etag, Canonical.encode(%{"id" => "backup"})})}
    end)

    assert {:error, :integrity_error} = Alerting.get(ctx.actor, "backup", ctx.opts)
  end

  test "in-flight old evaluation cannot publish after a disable edit", ctx do
    assert {:ok, created} = Alerting.create(ctx.actor, "backup", request(rule(), "create"), ctx.opts)
    owner = self()

    query = fn _, _, _ ->
      send(owner, {:query_started, self()})

      receive do
        :release -> query(1).("", "", %{})
      end
    end

    supervisor = start_supervised!({Task.Supervisor, []})

    task =
      Task.Supervisor.async_nolink(supervisor, fn ->
        Evaluator.evaluate(ctx.actor, "backup", ctx.opts ++ [timestamp_ns: 1_000_000_000, query: query])
      end)

    assert_receive {:query_started, pid}

    assert {:ok, _} =
             Alerting.update(
               ctx.actor,
               "backup",
               request(rule(%{"enabled" => false}), "disable", created["revision"]),
               ctx.opts
             )

    send(pid, :release)
    assert {:error, :ambiguous_head_write} = Task.await(task)
    assert {:ok, state} = Alerting.state(ctx.actor, "backup", ctx.opts)
    assert state["state"] == "disabled"
    assert state["instances"] == %{}
    assert {:ok, events} = Alerting.events(ctx.actor, "backup", %{}, ctx.opts)
    assert events["events"] == []
  end

  test "removing an imported source does not declassify old audit content", ctx do
    original =
      File.read!("plans/alerting/grafana-rule-inventory.json") |> Pulso.JSON.decode!() |> Map.fetch!("rules") |> hd()

    imported = %{"kind" => "grafana", "original" => original, "enabled" => false}
    id = original["uid"]
    assert {:ok, created} = Alerting.create(ctx.actor, id, request(imported, "import"), ctx.opts)
    assert {:ok, _} = Alerting.update(ctx.actor, id, request(rule(), "native", created["revision"]), ctx.opts)
    reader = %{ctx.actor | capabilities: ctx.actor.capabilities -- ["alert:import"]}
    assert {:ok, _} = Alerting.get(reader, id, ctx.opts)
    assert {:error, :forbidden} = Alerting.changes(reader, id, %{}, ctx.opts)
    assert {:error, :forbidden} = Alerting.historical(reader, id, created["revision"], ctx.opts)
    assert {:ok, page} = Alerting.changes(ctx.actor, id, %{}, ctx.opts)
    [_, change] = page["changes"]

    assert {:ok, snapshot} =
             Alerting.historical(
               ctx.actor,
               id,
               %{"revision" => change["revision"], "revision_cursor" => change["revision_cursor"]},
               ctx.opts
             )

    assert snapshot["rule"]["original"] == original
  end

  test "rejects broken/cyclic Grafana dependency graphs without writing", ctx do
    original = %{
      "uid" => "broken",
      "condition" => "A",
      "data" => [
        %{"refId" => "A", "datasourceUid" => "__expr__", "model" => %{"type" => "threshold", "expression" => "B"}},
        %{"refId" => "B", "datasourceUid" => "__expr__", "model" => %{"type" => "threshold", "expression" => "A"}}
      ]
    }

    assert {:error, :invalid_rule} =
             Alerting.create(
               ctx.actor,
               "broken",
               request(%{"kind" => "grafana", "original" => original}, "cycle"),
               ctx.opts
             )

    assert {:error, :not_found} = Repository.load(ctx.actor.tenant, "broken", ctx.opts)
  end

  test "all 115 Grafana definitions round-trip disabled with explicit execution blockers", ctx do
    inventory = File.read!("plans/alerting/grafana-rule-inventory.json") |> Pulso.JSON.decode!()
    assert {:ok, reports} = Importer.preview(inventory["rules"])
    assert length(reports) == 115
    assert Enum.map(reports, & &1["rule"]["original"]) == inventory["rules"]
    assert Enum.count(reports, &(&1["kind"] == "recording")) == 4
    assert Enum.all?(reports, &(&1["executable"] == false and &1["rule"]["enabled"] == false))

    for report <- reports do
      assert {:ok, _} =
               Alerting.create(ctx.actor, report["id"], request(report["rule"], "import-" <> report["id"]), ctx.opts)

      assert {:ok, stored} = Alerting.get(ctx.actor, report["id"], ctx.opts)
      assert stored["rule"]["original"] == report["rule"]["original"]
      assert stored["state"] == "disabled"
    end
  end

  test "ordinary read and shared tenant identity cannot grant alert administration", ctx do
    reader = %{ctx.actor | capabilities: ["alert:read"]}
    assert {:error, :forbidden} = Alerting.create(reader, "backup", request(rule(), "create"), ctx.opts)
    assert {:error, :invalid_id} = Alerting.create(ctx.actor, "../other", request(rule(), "create"), ctx.opts)

    assert {:error, :invalid_arguments} =
             Alerting.create(ctx.actor, "backup", Map.put(request(rule(), "create"), "actor", "admin"), ctx.opts)
  end
end
