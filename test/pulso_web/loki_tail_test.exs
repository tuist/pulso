defmodule PulsoWeb.LokiTailTest do
  use Pulso.Test.Case, async: true

  alias Pulso.LogQL.Parser
  alias Pulso.Loki.Tail
  alias Pulso.Record.Log
  alias Pulso.Runtime
  alias Pulso.Test.RuntimePlug
  alias Pulso.Test.WebSocketClient

  setup do
    Runtime.share()
    api = start_supervised!({Bandit, plug: {RuntimePlug, plug: PulsoWeb.Endpoint, runtime: Runtime.current()}, port: 0})
    {:ok, {_ip, port}} = ThousandIsland.listener_info(api)
    {:ok, port: port, tenant: "tail-#{System.unique_integer([:positive])}"}
  end

  test "mailbox admission overflow closes explicitly with WebSocket 1013" do
    assert {:stop, :normal, {1013, "tail backlog limit"}, %{}} =
             PulsoWeb.LokiTail.handle_info(:tail_overloaded, %{})
  end

  defp push(ctx, stream, lines) do
    values = Enum.map(lines, fn {ts, line} -> [Integer.to_string(ts), line] end)

    response =
      Req.post!("http://127.0.0.1:#{ctx.port}/loki/api/v1/push",
        json: %{"streams" => [%{"stream" => stream, "values" => values}]},
        headers: [{"x-scope-orgid", ctx.tenant}],
        retry: false
      )

    assert response.status == 204
  end

  defp tail(ctx, query, params \\ []) do
    path = "/loki/api/v1/tail?" <> URI.encode_query([{"query", query} | params])
    WebSocketClient.connect(ctx.port, path, [{"x-scope-orgid", ctx.tenant}])
  end

  defp now, do: System.system_time(:nanosecond)

  test "sends recent history oldest first, then matching live entries through the pipeline", ctx do
    t = now()
    push(ctx, %{"service_name" => "api"}, [{t - 3_000, "level=info msg=old"}, {t - 2_000, "level=error msg=older"}])
    push(ctx, %{"service_name" => "web"}, [{t - 1_000, "level=error msg=web"}])

    {:ok, socket} = tail(ctx, ~s({service_name="api"} | logfmt | level="error"), limit: "10")

    assert %{"streams" => [history], "dropped_entries" => nil} = WebSocketClient.next_json(socket)
    assert history["values"] == [[Integer.to_string(t - 2_000), "level=error msg=older"]]
    assert history["stream"]["service_name"] == "api"
    assert history["stream"]["msg"] == "older"

    push(ctx, %{"service_name" => "web"}, [{t + 1_000, "level=error msg=other"}])
    push(ctx, %{"service_name" => "api"}, [{t + 3_000, "level=error msg=b"}, {t + 2_000, "level=error msg=a"}])
    push(ctx, %{"service_name" => "api"}, [{t + 4_000, "level=info msg=skip"}])
    push(ctx, %{"service_name" => "api"}, [{t + 5_000, "level=error msg=c"}])

    assert %{"streams" => streams} = WebSocketClient.next_json(socket)
    lines = for s <- streams, [_ts, line] <- s["values"], do: line
    assert lines == ["level=error msg=a", "level=error msg=b"]

    assert %{"streams" => [%{"values" => [[_, "level=error msg=c"]]}]} = WebSocketClient.next_json(socket)
    WebSocketClient.close(socket)
  end

  test "history keeps only the newest limit entries", ctx do
    t = now()
    push(ctx, %{"service_name" => "api"}, for(i <- 1..5, do: {t - 10_000 + i, "line #{i}"}))

    {:ok, socket} = tail(ctx, ~s({service_name="api"}), limit: "2")
    assert %{"streams" => [%{"values" => values}]} = WebSocketClient.next_json(socket)
    assert Enum.map(values, &List.last/1) == ["line 4", "line 5"]
    WebSocketClient.close(socket)
  end

  test "a closed socket leaves the tenant's tailers", ctx do
    {:ok, socket} = tail(ctx, ~s({service_name="api"}))
    assert %{"streams" => []} = WebSocketClient.next_json(socket)
    [pid] = Tail.tailers(ctx.tenant)
    ref = Process.monitor(pid)
    WebSocketClient.close(socket)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000
    assert Tail.tailers(ctx.tenant) == []
  end

  test "rejects metric queries, plain HTTP and invalid parameters before upgrading", ctx do
    assert {:error, {400, _}} = tail(ctx, ~s<count_over_time({service_name="api"}[1m])>)
    assert {:error, {400, _}} = tail(ctx, ~s({service_name="api"}), limit: "0")
    assert {:error, {400, _}} = tail(ctx, ~s({service_name="api"}), delay_for: "6")
    assert {:error, {400, _}} = tail(ctx, ~s<{service_name=~"("}>)

    response =
      Req.get!("http://127.0.0.1:#{ctx.port}/loki/api/v1/tail",
        params: [query: ~s({service_name="api"})],
        headers: [{"x-scope-orgid", ctx.tenant}],
        retry: false
      )

    assert response.status == 400
    assert Tail.tailers(ctx.tenant) == []
  end

  test "limits concurrent tails per tenant", ctx do
    Runtime.put_env(:pulso, Tail, max_per_tenant: 1)
    {:ok, socket} = tail(ctx, ~s({service_name="api"}))
    assert %{"streams" => []} = WebSocketClient.next_json(socket)
    assert {:error, {429, _}} = tail(ctx, ~s({service_name="api"}))
    WebSocketClient.close(socket)
  end

  test "an overloaded tailer drops batches and reports them" do
    state = %{compiled: Tail.compile(parse!(~s({service_name="api"}))), limit: 100, max_backlog: -1}
    record = %Log{timestamp_ns: 42, service: "api", body: "x", resource: %{}, attributes: %{}}

    assert {:push, {:text, frame}, ^state} = PulsoWeb.LokiTail.handle_info({Tail, [record]}, state)

    assert %{"streams" => [], "dropped_entries" => [%{"timestamp" => "42", "labels" => %{"service_name" => "api"}}]} =
             JSON.decode!(frame)
  end

  defp parse!(query) do
    {:ok, ast} = Parser.parse(query)
    ast
  end
end
