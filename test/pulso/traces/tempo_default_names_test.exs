defmodule Pulso.Traces.TempoDefaultNamesTest do
  use Pulso.Test.Case, async: true

  alias Pulso.{Codec.NIF, MCP.Tools, OTLP.Traces, Runtime, Storage}
  alias Pulso.Storage.S3
  alias Pulso.Test.FailingStorage
  alias Pulso.Test.ScopedTraceDiscoveryStorage
  alias Pulso.Traces.{TempoDiscovery, TempoIntrinsics, TempoSearch}

  defp fixture do
    dir = Path.expand("../../fixtures/traces", __DIR__)
    raw = File.read!(Path.join(dir, "tempo-default-names-oracle.json"))

    assert Base.encode16(:crypto.hash(:sha256, raw), case: :lower) ==
             "d72a29397577c847d3abbaffb7b7a2e6f82eb06c2c982713baf7d129392d6287"

    datasets =
      for name <- ~w(promoted generic no-service), into: %{} do
        {:ok, rows, 0} = Traces.decode_protobuf(File.read!(Path.join(dir, "tempo-filtered-names-#{name}.protobuf")))
        {name, rows}
      end

    {datasets, Pulso.JSON.decode!(raw)}
  end

  defp state(q, floor \\ 0) do
    {:ok, p} = TempoDiscovery.filter("none", "", q)

    if p == [],
      do: NIF.tempo_discovery_new(floor, "none", ""),
      else: NIF.tempo_filtered_discovery_new(floor, "none", "", p)
  end

  defp native(rows, q) do
    {:ok, s} = state(q)
    {:ok, blob, _, _} = S3.encode_segment(:traces, rows)
    with {:ok, _} <- NIF.tempo_discovery_add(s, blob), do: NIF.tempo_discovery_finish(s)
  end

  test "default names preserve scopes and intrinsic suggestions without upstream shared-key omissions" do
    {datasets, cases} = fixture()
    assert length(cases) == 15

    for c <- cases do
      rows = datasets[c["dataset"]]

      for spans <- [rows, Enum.reverse(rows), rows ++ rows] do
        assert {:ok, json} = native(spans, c["query"])
        assert Pulso.JSON.decode!(json) == c["complete"], inspect(c)
        assert {:ok, ref} = TempoDiscovery.json(spans, "none", "", discovery_query: c["query"])
        assert Pulso.JSON.decode!(ref) == c["complete"], inspect(c)
      end

      {:ok, s} = state(c["query"])

      for row <- rows do
        {:ok, blob, _, _} = S3.encode_segment(:traces, [row])
        assert {:ok, 1} = NIF.tempo_discovery_add(s, blob)
      end

      assert {:ok, json} = NIF.tempo_discovery_finish(s)
      assert Pulso.JSON.decode!(json) == c["complete"]
      assert :fallback = NIF.tempo_discovery_finish(s)
    end
  end

  test "omitted MCP scope and old scoped adapters use one bounded complete fallback; v1 deduplicates and excludes intrinsics" do
    {datasets, cases} = fixture()
    Runtime.put_env(:pulso, TempoSearch, enabled: true)
    for {name, rows} <- datasets, do: assert(:ok = Storage.append(:traces, name, rows))
    Runtime.put_env(:pulso, Storage, adapter: ScopedTraceDiscoveryStorage)

    for c <- cases do
      for extra <- [%{}, %{"scope" => "none"}] do
        assert {:ok, [%{"text" => json}]} =
                 Tools.call("discover_trace_tags", Map.merge(%{"tenant" => c["dataset"], "query" => c["query"]}, extra))

        assert Pulso.JSON.decode!(json) == c["complete"]
      end

      assert {:ok, json} = TempoDiscovery.query(c["dataset"], "none", "", 1, "json", c["query"])
      assert Pulso.JSON.decode!(json) == c["v1"]
      assert {:ok, wire} = TempoDiscovery.query(c["dataset"], "none", "", 2, "protobuf", c["query"])
      assert byte_size(wire) > 0
    end

    assert {:ok, _} = TempoDiscovery.query("promoted", "none", "shared")
    assert {:error, :unsupported_trace_discovery} = TempoDiscovery.query("promoted", "none", "", 2, "json", "{true}")
    assert {:ok, unscoped} = NIF.tempo_discovery_new(0, "none", "shared")
    assert {:ok, ~s'{"tagValues":[]}'} = NIF.tempo_discovery_finish(unscoped)
  end

  test "empty visible context retains only static v2 suggestions; legacy and corrupt nonmatches never produce partial suggestions" do
    {datasets, _} = fixture()
    {:ok, s} = state("")
    assert {:ok, json} = NIF.tempo_discovery_finish(s)
    assert json == TempoIntrinsics.json("")
    assert {:ok, json} = TempoDiscovery.json([], "none", "", [])
    assert json == TempoIntrinsics.json("")
    [first | _] = datasets["promoted"]

    assert {:error, :trace_metadata_unknown} = native([%{first | otlp: nil}], ~s'{resource.service.name="absent"}')
    {:ok, bad_state} = state(~s'{resource.service.name="absent"}')
    bad_blob = File.read!(Path.expand("../../fixtures/traces/trace-invalid-version.parquet", __DIR__))
    assert :fallback = NIF.tempo_discovery_add(bad_state, bad_blob)
    assert :fallback = NIF.tempo_discovery_finish(bad_state)

    for row <- [%{first | otlp: nil}, %{first | otlp: Map.put(first.otlp, "version", 99)}] do
      assert {:error, _} = TempoDiscovery.json([row], "none", "", discovery_query: ~s'{resource.service.name="absent"}')
    end

    {:ok, s} = state("", first.timestamp_ns + 1)
    {:ok, blob, _, _} = S3.encode_segment(:traces, [first])
    assert {:ok, 1} = NIF.tempo_discovery_add(s, blob)
    assert {:ok, json} = NIF.tempo_discovery_finish(s)
    assert json == TempoIntrinsics.json("")
    assert {:error, :query_scan_limit} = TempoDiscovery.json([first], "none", "", max_scan_rows: 0)
  end

  test "actual frontend and unscoped-value evidence do not authorize a five-scope value union" do
    dir = Path.expand("../../fixtures/traces", __DIR__)

    for {file, hash} <- [
          {"tempo-default-name-handlers-oracle.json",
           "c5b8635054fe6ad5b7af934baa9ed9fee4c6ee7f9a402a8109ae040d811cf3a2"},
          {"produce_tempo_default_name_handlers.go.txt",
           "1ad0b7e06fbbd4ae854ae32c122e1b662b34c97badd731936662e713dc9123b4"},
          {"produce_tempo_default_names.go.txt", "834f59f572e03ac32d3bc731dff61f7aa25b28d0ff6b16b331f3ce809973045c"},
          {"produce_tempo_unscoped_values.go.txt", "0a42e3a3cae2bc5477d84d4819c5a309b481b678f5341e8d2ee33f1d2426b8e2"},
          {"tempo-unscoped-values-oracle.json", "42108f2c413443d6722c041b49b8ac512ba7a576ed4bb3beadeae53f52b8c31d"}
        ] do
      assert Base.encode16(:crypto.hash(:sha256, File.read!(Path.join(dir, file))), case: :lower) == hash
    end

    handlers = Pulso.JSON.decode!(File.read!(Path.join(dir, "tempo-default-name-handlers-oracle.json")))
    assert handlers["cases"] == 90 and handlers["v1_excludes_intrinsics"] and handlers["v2_adds_intrinsics"]
    assert handlers["backend"] == "owned_stub"
    values = Pulso.JSON.decode!(File.read!(Path.join(dir, "tempo-unscoped-values-oracle.json")))
    assert Enum.all?(Enum.filter(values, &(&1["target"] in ["shared", "http.method"])), &(not &1["valid"]))
    shared = Enum.find(values, &(&1["target"] == ".shared" and &1["query"] == ""))
    assert Enum.map(shared["response"]["tagValues"], & &1["value"]) == ~w(instrumentation resource span)
    assert Enum.all?(Enum.filter(values, &(&1["target"] == ".phase")), &(&1["response"]["tagValues"] == []))
    service = Enum.find(values, &(&1["target"] == ".service.name" and &1["query"] != ""))
    assert Enum.map(service["response"]["tagValues"], & &1["value"]) == ~w(frontend worker)
    assert :ok = TempoDiscovery.validate("none", "shared")
  end

  test "default discovery fails whole on storage errors instead of returning only intrinsic suggestions" do
    Runtime.put_env(:pulso, TempoSearch, enabled: true)
    Runtime.put_env(:pulso, Storage, adapter: FailingStorage)
    assert {:error, :storage_unavailable} = TempoDiscovery.query("default", "none", "")
    assert {:ok, _} = TempoDiscovery.query("default", "intrinsic", "")
    Runtime.put_env(:pulso, TempoSearch, enabled: false)
    assert {:error, :tempo_search_disabled} = TempoDiscovery.query("default", "none", "")
  end

  test "default dictionary bound is global across scopes and includes intrinsic reservations" do
    {datasets, _} = fixture()
    base = hd(datasets["generic"])

    rows =
      for n <- 1..5 do
        attrs = for i <- 1..1000, do: %{"key" => "unique_#{n}_#{i}", "value" => %{"stringValue" => "x"}}
        %{base | attributes: Map.new(attrs, &{&1["key"], "x"}), otlp: Map.put(base.otlp, "attributes", attrs)}
      end

    {:ok, s} = state("")

    for row <- Enum.take(rows, 4) do
      {:ok, blob, _, _} = S3.encode_segment(:traces, [row])
      assert {:ok, 1} = NIF.tempo_discovery_add(s, blob)
    end

    {:ok, blob, _, _} = S3.encode_segment(:traces, [List.last(rows)])
    assert {:error, :query_resource_limit} = NIF.tempo_discovery_add(s, blob)
    assert :fallback = NIF.tempo_discovery_finish(s)
    assert {:error, :query_resource_limit} = TempoDiscovery.json(rows, "none", "", [])
  end

  test "default names charge all five scope owners and attrs before false filters under one work ceiling" do
    {:ok, rows, 0} =
      Traces.decode_protobuf(File.read!(Path.expand("../../fixtures/traces/tempo-scopes-input.protobuf", __DIR__)))

    base = hd(rows)

    dense = %{
      base
      | events: List.duplicate(Map.put(hd(base.events), "attributes", %{}), 2000),
        otlp: Map.put(base.otlp, "events", List.duplicate([], 2000))
    }

    meta = dense.otlp

    cost =
      3 + length(meta["resource"]) + length(meta["scope"]) + length(meta["attributes"]) +
        Enum.reduce(meta["events"] ++ meta["links"], 0, &(1 + length(&1) + &2))

    rounds = div(2_000_000, cost)
    q = ~s'{resource.service.name="absent"}'
    {:ok, s} = state(q)
    {:ok, blob, _, _} = S3.encode_segment(:traces, [dense])
    for _ <- 1..rounds, do: assert({:ok, 1} = NIF.tempo_discovery_add(s, blob))
    assert {:error, :trace_discovery_work_limit} = NIF.tempo_discovery_add(s, blob)
    assert :fallback = NIF.tempo_discovery_finish(s)

    assert {:error, :trace_discovery_work_limit} =
             TempoDiscovery.json(List.duplicate(dense, rounds + 1), "none", "", discovery_query: q)

    {:ok, scoped} = NIF.tempo_discovery_new(0, "resource", "")
    for _ <- 1..(rounds + 1), do: assert({:ok, 1} = NIF.tempo_discovery_add(scoped, blob))
    assert {:ok, _} = NIF.tempo_discovery_finish(scoped)
  end
end
