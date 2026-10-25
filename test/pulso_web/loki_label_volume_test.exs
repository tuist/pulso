defmodule PulsoWeb.LokiLabelVolumeTest do
  use Pulso.Test.Case, async: true

  alias Pulso.Auth
  alias Pulso.Auth.SharedSecret
  alias Pulso.PromQL.Evaluator
  alias Pulso.PromQL.QuerySlots
  alias Pulso.PromQL.TaskSupervisor
  alias Pulso.Runtime
  alias Pulso.Storage
  alias Pulso.Storage.S3
  alias Pulso.Test.CompactionStore
  alias Pulso.Test.RuntimePlug

  @second 1_000_000_000
  @start 1_700_000_000 * @second

  setup do
    store =
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

    server = start_supervised!({Bandit, plug: {CompactionStore, agent: store}, ip: {127, 0, 0, 1}, port: 0})
    {:ok, {_, port}} = ThousandIsland.listener_info(server)

    Runtime.put_env(:pulso, Storage, adapter: S3)

    Runtime.put_env(:pulso, S3, %{
      bucket: "pulso",
      region: "us-east-1",
      endpoint: "http://127.0.0.1:#{port}",
      access_key_id: "label-volume-#{System.unique_integer([:positive])}",
      secret_access_key: "fixture",
      allow_http: true,
      refresh_stale_ms: 0
    })

    token = "sha256$" <> Base.encode16(:crypto.hash(:sha256, "volume-token"), case: :lower)
    Runtime.put_env(:pulso, Auth, module: SharedSecret, tokens: %{"volume" => token, "other" => token})
    start_supervised!(S3.ManifestSupervision)

    {:ok, store: store}
  end

  # Semantic cases from grafana/loki v3.5.0, TestTSDBIndex_Volume's
  # "aggregate by labels" subtests and queryrange Test_toPrometheusResponse.
  # Sizes are actual line bytes here, not Loki's estimated chunk KB.
  test "HTTP push and label volumes match Loki's label-name aggregation cases", ctx do
    api = api(:writer)
    seed(api)
    reader = api(:reader, __MODULE__.Reader)

    cases = [
      {~s({}), %{}, [{"foo", "210"}, {"fizz", "100"}, {"in", "70"}, {"us", "30"}]},
      {~s({foo=~".+"}), %{}, [{"foo", "210"}, {"fizz", "100"}, {"in", "70"}, {"us", "30"}]},
      {~s({foo="bar",fizz=~".+"}), %{}, [{"fizz", "100"}, {"foo", "100"}, {"in", "70"}, {"us", "30"}]},
      {~s({fizz!~"buzz"}), %{}, [{"foo", "180"}, {"fizz", "70"}, {"in", "70"}]},
      {~s({us="them"}), %{}, [{"fizz", "30"}, {"foo", "30"}, {"us", "30"}]},
      {~s({foo="boo"}), %{}, []},
      {~s({foo="bar"}), %{"targetLabels" => "fizz"}, [{"fizz", "100"}]},
      {~s({fizz=~".+"}), %{"targetLabels" => "foo"}, [{"foo", "100"}]},
      {~s({foo=~".+"}), %{"targetLabels" => "fizz,us"}, [{"fizz", "30"}, {"us", "30"}]},
      {~s({foo=~".+"}), %{"targetLabels" => "missing"}, []},
      {~s({foo=~".+"}), %{"targetLabels" => "empty"}, []},
      {~s({foo=~".+",missing=""}), %{"targetLabels" => "foo,missing"}, [{"foo", "210"}]},
      {~s({foo=~".+"}), %{"limit" => "2"}, [{"foo", "210"}, {"fizz", "100"}]}
    ]

    for {query, extra, expected} <- cases, method <- [:get, :post] do
      response = volume(reader, query, extra, method)
      assert response.status == 200, inspect(response.body)
      assert response.body["data"]["resultType"] == "vector"

      actual =
        Enum.map(response.body["data"]["result"], fn row ->
          assert [name] = Map.keys(row["metric"])
          assert row["metric"][name] == ""
          assert [timestamp, value] = row["value"]
          assert timestamp == 1_700_000_002
          {name, value}
        end)

      assert actual == expected
    end

    # The default series aggregation still keeps label values.
    control = volume(reader, ~s({foo="bar"}), %{"aggregateBy" => "series"})
    assert [%{"metric" => %{"foo" => "bar"}, "value" => [_, "100"]}] = control.body["data"]["result"]

    before = volume(reader, ~s({foo=~".+"})).body
    config = Runtime.get_env(:pulso, S3)
    assert {:ok, %{merged: 3}} = S3.LogCompactor.compact("volume", config, grace_ms: 0)
    assert {:ok, 3} = S3.LogCompactor.cleanup("volume", config)
    assert :ok = stop_supervised({:manifest, :reader})

    reader_runtime = %{Runtime.current() | instance: __MODULE__.Reader}

    Runtime.with(reader_runtime, fn ->
      start_supervised!(Supervisor.child_spec(S3.ManifestSupervision, id: {:manifest, :reader}))
    end)

    assert volume(reader, ~s({foo=~".+"})).body == before

    # Separate authority/cache processes, same published Parquet objects.
    assert Agent.get(ctx.store, fn state ->
             Enum.any?(state.requests, fn {method, key} -> method == "GET" and String.ends_with?(key, ".parquet") end)
           end)
  end

  test "range buckets rank independently, include boundaries, and keep UTF-8 byte counts" do
    api = api(:range)

    push(api, [
      stream(%{"foo" => "a", "first" => "yes"}, [{@start - 1, "outside"}, {@start, "é"}, {@start + @second - 1, "abc"}]),
      stream(%{"foo" => "b", "last" => "yes"}, [
        {@start + @second, "1234567"},
        {@start + 2 * @second, "z"},
        {@start + 2 * @second + 1, "outside"}
      ])
    ])

    # A three-second epoch grid has partial initial/final buckets here.
    for method <- [:get, :post], step <- ["1s", "3s"] do
      response = volume(api, ~s({foo=~".+"}), %{"step" => step, "limit" => "1"}, method, "volume_range")
      assert response.status == 200, inspect(response.body)
      assert response.body["data"]["resultType"] == "matrix"

      assert response.body["data"]["result"] == [
               %{"metric" => %{"foo" => ""}, "values" => [[1_700_000_002, "8"]]},
               %{"metric" => %{"first" => ""}, "values" => [[1_700_000_000.999, "5"]]}
             ]
    end

    subsecond = volume(api, ~s({foo=~".+"}), %{"step" => "0.5", "limit" => "1"}, :get, "volume_range")

    assert subsecond.body["data"]["result"] == [
             %{"metric" => %{"foo" => ""}, "values" => [[1_700_000_001.499, "7"], [1_700_000_002, "1"]]},
             %{"metric" => %{"first" => ""}, "values" => [[1_700_000_000.499, "2"], [1_700_000_000.999, "3"]]}
           ]

    single = volume(api, ~s({foo=~".+"}), %{"end" => Integer.to_string(@start)}, :get, "volume_range")
    assert single.body["data"]["resultType"] == "matrix"

    assert single.body["data"]["result"] == [
             %{"metric" => %{"first" => ""}, "values" => [[1_700_000_000, "2"]]},
             %{"metric" => %{"foo" => ""}, "values" => [[1_700_000_000, "2"]]}
           ]

    empty = volume(api, ~s({foo="missing"}), %{}, :get, "volume_range")
    assert empty.body["data"] == %{"resultType" => "matrix", "result" => [], "stats" => %{}}
  end

  test "the complete name dictionary is bounded before top-k truncation" do
    api = api(:dictionary)

    streams =
      1..4999
      |> Enum.chunk_every(100)
      |> Enum.map(fn keys ->
        labels = Map.new(keys, &{"key#{&1}", "v"}) |> Map.put("foo", "bar")
        stream(labels, [{@start + 1, "x"}])
      end)

    push(api, streams)
    result = volume(api, ~s({foo="bar"}), %{"limit" => "1"})
    assert result.status == 200
    assert [%{"metric" => %{"foo" => ""}, "value" => [_, "50"]}] = result.body["data"]["result"]

    push(api, [stream(%{"foo" => "bar", "overflow" => "yes"}, [{@start + 1, "x"}])], "overflow")
    result = volume(api, ~s({foo="bar"}), %{"limit" => "1"})
    assert result.status == 422
    refute Map.has_key?(result.body, "data")
  end

  test "name bytes bind independently of name count and top-k" do
    api = api(:name_bytes)
    # 4095 * 256 + 253 + byte_size("foo") = exactly 1 MiB, below 5000 names.
    keys = Enum.map(1..4095, &String.pad_trailing("key#{&1}", 256, "x")) ++ [String.duplicate("z", 253)]

    streams =
      keys
      |> Enum.chunk_every(100)
      |> Enum.map(fn names ->
        labels = Map.new(names, &{&1, "v"}) |> Map.put("foo", "bar")
        stream(labels, [{@start + 1, "x"}])
      end)

    push(api, streams)
    result = volume(api, ~s({foo="bar"}), %{"limit" => "1"})
    assert result.status == 200
    assert [%{"metric" => %{"foo" => ""}, "value" => [_, "41"]}] = result.body["data"]["result"]

    push(api, [stream(%{"foo" => "bar", "q" => "v"}, [{@start + 1, "x"}])], "one-more-byte")
    result = volume(api, ~s({foo="bar"}), %{"limit" => "1"})
    assert result.status == 422
    refute Map.has_key?(result.body, "data")
  end

  test "authentication, validation, resource budgets and unreadable sources fail whole", ctx do
    api = api(:failures)
    seed(api)
    assert volume(api, ~s({foo=~".+"}), %{}, :get, "volume", "other").body["data"]["result"] == []

    denied = Req.get!(api <> "/loki/api/v1/index/volume", params: params(~s({foo=~".+"})), retry: false)
    assert denied.status == 401

    for extra <- [%{"aggregateBy" => "invalid"}, %{"targetLabels" => "foo.bad"}, %{"limit" => "0"}] do
      assert volume(api, ~s({foo=~".+"}), extra).status == 400
    end

    for step <- ["0", "bad", "0.000001s"] do
      assert volume(api, ~s({foo=~".+"}), %{"step" => step}, :get, "volume_range").status == 400
    end

    for {finish, status} <- [{"11000", 200}, {"11001", 400}] do
      assert volume(api, ~s({foo="bar"}), %{"start" => "0", "end" => finish, "step" => "1s"}, :get, "volume_range").status ==
               status
    end

    assert volume(api, ~s({foo="bar"} | logfmt)).status == 400

    for path <- ["volume", "volume_range"], method <- [:get, :post], op <- ["=~", "!~"] do
      response = volume(api, "{foo#{op}\"[\"}", %{}, method, path)
      assert response.status == 400, inspect(response.body)
      assert response.body["error"] == "evaluation_failed"
    end

    for {key, value} <- [
          max_work: 1,
          max_log_samples: 1,
          max_materialized_samples: 1,
          max_result_points: 1
        ] do
      Runtime.put_env(:pulso, Evaluator, [{key, value}])
      limited = api({:limited, key})
      response = volume(limited, ~s({foo=~".+"}))
      assert response.status == 422, inspect({key, response.body})
      refute Map.has_key?(response.body, "data")
    end

    Runtime.put_env(:pulso, Evaluator, [])
    Runtime.put_env(:pulso, Pulso.QueryRunner, max_scan_rows: 1)
    assert volume(api(:scan_limited), ~s({foo=~".+"})).status == 422
    Runtime.put_env(:pulso, Pulso.QueryRunner, [])

    Agent.update(ctx.store, fn state ->
      %{
        state
        | objects:
            Map.new(state.objects, fn {key, {etag, body}} ->
              {key, {etag, if(String.ends_with?(key, ".parquet"), do: "corrupt", else: body)}}
            end)
      }
    end)

    broken = api(:broken)
    response = volume(broken, ~s({foo=~".+"}))
    assert response.status == 503
    refute Map.has_key?(response.body, "data")
  end

  defp api(id, instance \\ nil) do
    runtime = Runtime.current()

    runtime =
      if instance do
        independent = %{runtime | instance: instance, shared: nil}
        Runtime.install(independent)
        start_supervised!(Supervisor.child_spec(S3.ManifestSupervision, id: {:manifest, id}))
        start_supervised!(Supervisor.child_spec(Pulso.Metrics, id: {:metrics, id}))
        start_supervised!(Supervisor.child_spec(Pulso.SelfMetrics, id: {:self_metrics, id}))

        start_supervised!(Supervisor.child_spec({Task.Supervisor, name: TaskSupervisor}, id: {:tasks, id}))

        start_supervised!(Supervisor.child_spec({Registry, keys: :unique, name: QuerySlots}, id: {:slots, id}))

        Runtime.install(runtime)
        independent
      else
        runtime
      end

    server =
      start_supervised!(
        Supervisor.child_spec(
          {Bandit, plug: {RuntimePlug, plug: PulsoWeb.Endpoint, runtime: runtime}, ip: {127, 0, 0, 1}, port: 0},
          id: id
        )
      )

    {:ok, {_, port}} = ThousandIsland.listener_info(server)
    "http://127.0.0.1:#{port}"
  end

  defp seed(api) do
    streams = [
      stream(%{"foo" => "bar", "fizz" => "buzz", "us" => "them", "__loki_tenant__" => "fake"}, [
        {@start + 1, String.duplicate("a", 30)}
      ]),
      stream(%{"foo" => "bar", "fizz" => "fizz", "in" => "out", "__loki_tenant__" => "fake"}, [
        {@start + 2, String.duplicate("b", 70)}
      ]),
      stream(%{"foo" => "baz", "empty" => "", "__loki_tenant__" => "fake"}, [{@start + 3, String.duplicate("c", 110)}])
    ]

    # Separate publications exercise merging across segments; exact keyed
    # retries must not double their volumes.
    for {stream, i} <- Enum.with_index(streams) do
      push(api, [stream], "fixture-#{i}")
      push(api, [stream], "fixture-#{i}")
    end
  end

  defp stream(labels, lines) do
    %{
      "stream" => labels,
      "values" => Enum.map(lines, fn {ts, line} -> [Integer.to_string(ts), line, %{"metadata_only" => "ignored"}] end)
    }
  end

  defp push(api, streams, key \\ "fixture") do
    response =
      Req.post!(api <> "/loki/api/v1/push",
        headers: headers("volume") ++ [{"idempotency-key", key}],
        json: %{"streams" => streams},
        retry: false
      )

    assert response.status == 204, inspect(response.body)
  end

  defp params(query) do
    %{
      "query" => query,
      "aggregateBy" => "labels",
      "start" => Integer.to_string(@start),
      "end" => Integer.to_string(@start + 2 * @second)
    }
  end

  defp volume(api, query, extra \\ %{}, method \\ :get, path \\ "volume", tenant \\ "volume") do
    opts = [method: method, url: api <> "/loki/api/v1/index/" <> path, headers: headers(tenant), retry: false]
    data = Map.merge(params(query), extra)
    Req.request!(opts ++ if(method == :post, do: [form: data], else: [params: data]))
  end

  defp headers(tenant), do: [{"authorization", "Bearer volume-token"}, {"x-scope-orgid", tenant}]
end
