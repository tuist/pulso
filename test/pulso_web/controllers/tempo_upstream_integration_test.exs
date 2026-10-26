# SPDX-License-Identifier: AGPL-3.0-only
# Adapted from Grafana Tempo integration/api/search/{search,search_tags}_test.go.
# Filtering cases: integration/api/tracebyid/trace_by_id_filtering_test.go.
# Metric cases: integration/api/metrics/query_range_test.go.
# Source revision, adaptations and license: test/fixtures/traces/tempo_integration/README.md.
defmodule PulsoWeb.TempoUpstreamIntegrationTest do
  use Pulso.Test.Case, async: true

  alias Pulso.Auth
  alias Pulso.Auth.SharedSecret
  alias Pulso.OTLP.Traces
  alias Pulso.Runtime
  alias Pulso.Storage
  alias Pulso.Storage.S3
  alias Pulso.Storage.S3.ManifestSupervision
  alias Pulso.Test.CompactionStore
  alias Pulso.Test.OTLPTraceFixture
  alias Pulso.Test.RuntimePlug
  alias Pulso.Traces.Protobuf
  alias Pulso.Traces.Tempo
  alias Pulso.Traces.TempoFilter
  alias Pulso.Traces.TempoSearch

  @cases "../../fixtures/traces/tempo_integration/cases.json"
         |> Path.expand(__DIR__)
         |> File.read!()
         |> Pulso.JSON.decode!()

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

    object_server = start_supervised!({Bandit, plug: {CompactionStore, agent: store}, ip: {127, 0, 0, 1}, port: 0})
    {:ok, {_, object_port}} = ThousandIsland.listener_info(object_server)

    config = %{
      bucket: "pulso",
      endpoint: "http://127.0.0.1:#{object_port}",
      region: "us-east-1",
      access_key_id: "tempo-upstream-#{System.unique_integer([:positive])}",
      secret_access_key: "test",
      allow_http: true,
      refresh_stale_ms: 0
    }

    tenant = "tempo-upstream-#{System.unique_integer([:positive])}"
    token = "upstream-test-token"
    digest = "sha256$" <> Base.encode16(:crypto.hash(:sha256, token), case: :lower)
    Runtime.put_env(:pulso, Auth, module: SharedSecret, tokens: %{tenant => digest})
    Runtime.put_env(:pulso, TempoSearch, enabled: true)
    Runtime.put_env(:pulso, Storage, adapter: S3)
    Runtime.put_env(:pulso, S3, config)
    start_supervised!(ManifestSupervision)

    server =
      start_supervised!(
        {Bandit, plug: {RuntimePlug, plug: PulsoWeb.Endpoint, runtime: Runtime.current()}, ip: {127, 0, 0, 1}, port: 0},
        id: :tempo_upstream_api
      )

    {:ok, {_, port}} = ThousandIsland.listener_info(server)

    %{
      url: "http://127.0.0.1:#{port}",
      headers: [{"authorization", "Bearer " <> token}, {"x-scope-orgid", tenant}],
      tenant: tenant,
      now: System.system_time(:second)
    }
  end

  test "TestTagEndpoints: v1 unscoped names and service values from persisted data", ctx do
    ingest(ctx, @cases["batches"])

    for window <- windows(ctx) do
      assert get(ctx, "/api/search/tags", window) == %{"tagNames" => @cases["tagNames"]}
      assert_values(ctx, 1, "service.name", ["my-service"], window)
    end
  end

  test "TestSearchTraceQL: original resource and span attributes find the persisted trace with default or explicit bounds",
       ctx do
    ingest(ctx, @cases["batches"])

    # Replace the upstream randomly selected original string with one fixed
    # attribute from each owner; trace 1 is the first upstream batch above.
    for key <- ["firstRes", "firstSpan"], window <- windows(ctx) do
      query = ~s({ .#{key} = "bar" })
      response = get(ctx, "/api/search", Map.put(window, "q", query))
      assert Enum.map(response["traces"], & &1["traceID"]) == ["1"]
      assert hd(response["traces"])["rootServiceName"] == "my-service"
      assert hd(response["traces"])["spanSet"]["matched"] == 2
    end
  end

  test "omitted and zero search bounds scan retained context, while malformed or partial bounds reject", ctx do
    ingest(ctx, @cases["batches"])
    query = ~s({ resource.firstRes = "bar" })
    expected = get(ctx, "/api/search", %{"q" => query, "start" => ctx.now - 7200, "end" => ctx.now + 7200})

    for window <- [%{}, %{"start" => 0, "end" => 0}] do
      assert get(ctx, "/api/search", Map.put(window, "q", query)) == expected
    end

    for window <- [
          %{"start" => ctx.now - 10},
          %{"end" => ctx.now + 10},
          %{"start" => 0, "end" => ctx.now + 10},
          %{"start" => "-1", "end" => ctx.now},
          %{"start" => "1.5", "end" => ctx.now},
          %{"start" => 4_294_967_296, "end" => 4_294_967_297},
          %{"start" => ctx.now + 1, "end" => ctx.now}
        ] do
      response = Req.get!(ctx.url <> "/api/search", headers: ctx.headers, params: Map.put(window, "q", query))
      assert response.status == 400, inspect(window)
    end
  end

  test "public-engine unscoped collisions retain exact search metadata after HTTP ingestion and restart", ctx do
    dir = Path.expand("../../fixtures/traces/tempo_integration", __DIR__)
    wire = File.read!(Path.join(dir, "search-input.protobuf"))

    response =
      Req.post!(ctx.url <> "/v1/traces",
        headers: [{"content-type", "application/x-protobuf"} | ctx.headers],
        body: wire
      )

    assert response.status == 200
    assert :ok = stop_supervised(ManifestSupervision)
    start_supervised!(ManifestSupervision)

    for c <- dir |> Path.join("search-oracle.json") |> File.read!() |> Pulso.JSON.decode!() do
      params = %{"q" => c["query"], "start" => c["Start"], "end" => c["End"]}
      assert get(ctx, "/api/search", params) === c["response"], c["query"]
    end
  end

  test "TestTagEndpoints: six complete v2 value cases retain upstream filters and expected values", ctx do
    ingest(ctx, @cases["batches"])

    for window <- windows(ctx), row <- @cases["values"] do
      assert_values(ctx, 2, row["tag"], row["expected"], Map.put(window, "q", row["query"]))
    end
  end

  test "TestTagValuesWithSpecialCharacters: all 19 attribute/value pairs in both original scopes", ctx do
    batches =
      for [key, value] <- @cases["specialCharacters"] do
        %{
          "spanCount" => 1,
          "name" => "test-service",
          "resourceAttr" => key,
          "resourceAttVal" => value,
          "spanAttr" => key,
          "spanAttVal" => value
        }
      end

    ingest(ctx, batches)

    for window <- windows(ctx), [key, value] <- @cases["specialCharacters"], scope <- ["resource", "span"] do
      # Tempo quotes identifiers containing TraceQL delimiters, not URL delimiters.
      identifier = if Regex.match?(~r/[\s{}()=~!<>&|^,]/u, key), do: Pulso.JSON.encode!(key), else: key
      assert_values(ctx, 2, scope <> "." <> identifier, [value], window)
    end
  end

  test "TestTagValuesWithSpecialCharacters: slash works escaped and as a path separator", ctx do
    ingest(ctx, [
      %{
        "spanCount" => 1,
        "name" => "test-service",
        "resourceAttr" => "attr/slash",
        "resourceAttVal" => "val/",
        "spanAttr" => "attr/slash",
        "spanAttVal" => "val/"
      }
    ])

    for window <- windows(ctx) do
      assert_values(ctx, 2, "span.attr/slash", ["val/"], window)

      assert get(ctx, "/api/v2/search/tag/span.attr/slash/values", window) ==
               %{"tagValues" => [%{"type" => "string", "value" => "val/"}]}
    end
  end

  test "TestSearchTagValuesV2_badRequest: unknown attribute scope rejects", ctx do
    response = Req.get!(ctx.url <> "/api/v2/search/tag/app.user.id/values", headers: ctx.headers)
    assert response.status == 400
    assert response.body == "invalid or unsupported trace lookup parameters"
  end

  test "quoted names are exact keys, malformed names reject and credentials still bind the tenant", ctx do
    ingest(ctx, [
      %{
        "spanCount" => 1,
        "name" => "test-service",
        "resourceAttr" => "attr%20literal",
        "resourceAttVal" => "literal",
        "spanAttr" => "attr/values/space",
        "spanAttVal" => "space"
      }
    ])

    assert_values(ctx, 1, "attr%20literal", ["literal"], %{})
    assert_values(ctx, 2, ~s(span."attr/values/space"), ["space"], %{})
    assert_values(ctx, 2, ~s(span."missing key"), [], %{})

    for target <- [~s(span."unterminated), ~s(span."attr space"suffix), ~s(span.""), ~S(span."bad\q")] do
      response = Req.get!(ctx.url <> values_path(2, target), headers: ctx.headers)
      assert response.status == 400, "malformed target #{inspect(target)} returned #{response.status}"
    end

    for path <- ["/api/v2/search/tag/span.attr", "/api/v2/search/tag/span.attr/values/extra"] do
      assert Req.get!(ctx.url <> path, headers: ctx.headers).status == 404
    end

    headers = List.keyreplace(ctx.headers, "authorization", 0, {"authorization", "Bearer wrong"})
    assert Req.get!(ctx.url <> values_path(2, ~s(span."attr/values/space")), headers: headers).status == 401
  end

  test "TestTraceByIDV2Filtering: all 31 depth combinations preserve matched sets and partial status", ctx do
    ingest_filter_trace(ctx)
    path = "/api/v2/traces/1"
    full = get(ctx, path, %{})
    assert filter_ids(full) == [1, 2, 3, 4, 5, 6]
    assert Map.get(full, "status", "COMPLETE") == "COMPLETE"

    cases =
      "../../fixtures/traces/tempo_integration/filter-cases.json"
      |> Path.expand(__DIR__)
      |> File.read!()
      |> Pulso.JSON.decode!()

    assert length(cases) == 31

    for row <- cases do
      query = if row["two_matches"], do: "{ .http.status_code > 0 }", else: "{ .http.status_code = 500 }"
      params = Map.put(row["params"], "q", query)
      response = get(ctx, path, params)
      assert filter_ids(response) == row["ids"], row["name"]

      if row["complete"] do
        assert Map.get(response, "status", "COMPLETE") == "COMPLETE"
        assert Map.get(response, "message", "") == ""
      else
        assert response["status"] == "PARTIAL", row["name"]
        assert response["message"] =~ "only a subset of spans matching the filter"
      end

      # Additional transport control: schema fields 3/4 carry PARTIAL/message.
      trace = %{"batches" => Map.get(response["trace"], "resourceSpans", [])}
      envelope = response |> Map.put("trace", trace)
      wire = Req.get!(ctx.url <> path, headers: [{"accept", "application/protobuf"} | ctx.headers], params: params)
      assert wire.status == 200
      assert wire.body == Protobuf.encode(envelope, :response)

      if !row["complete"] do
        message = "Trace filtered, only a subset of spans matching the filter is returned"
        # Independently check the public TraceByIDResponse field numbers/types,
        # rather than deriving this expectation from Pulso's schema encoder.
        assert String.ends_with?(wire.body, <<0x18, 1, 0x22, byte_size(message)>> <> message)
      end
    end
  end

  test "TestTraceByIDV2Filtering: malformed filters and active depths reject, inactive ancestors are ignored", ctx do
    ingest_filter_trace(ctx)
    path = "/api/v2/traces/1"
    query = "{ .http.status_code = 500 }"

    for params <- [
          %{"q" => "{ .a = }"},
          %{"q" => query, "match_depth" => "-2"},
          %{"q" => query, "match_depth" => "abc"},
          %{"q" => query, "keep_hierarchy" => "true", "ancestor_depth" => "-2"},
          %{"q" => query, "keep_hierarchy" => "true", "ancestor_depth" => "abc"}
        ] do
      response = Req.get!(ctx.url <> path, headers: ctx.headers, params: params)
      assert response.status == 400
    end

    for params <- [
          %{"ancestor_depth" => "-2"},
          %{"ancestor_depth" => "abc"},
          %{"keep_hierarchy" => "false", "ancestor_depth" => "-2"}
        ] do
      assert filter_ids(get(ctx, path, Map.put(params, "q", query))) == [3]
    end

    # The default HTTP server rejects the upstream 150KiB filter at its URI
    # boundary before dispatch; do not relax that transport ceiling for parity.
    oversized = ~s({ span.foo = "#{String.duplicate("x", 150 * 1024)}" })
    assert Req.get!(ctx.url <> path, headers: ctx.headers, params: %{"q" => oversized}).status == 414
    assert {:error, :unsupported_parameters} = TempoFilter.parse(%{"q" => oversized})
  end

  test "Pulso filtering controls: empty selections are explicit, and filtering never mutates the full trace", ctx do
    ingest_filter_trace(ctx)
    path = "/api/v2/traces/1"
    full = get(ctx, path, %{})

    for query <- ["", " \t ", "{}", "{  }"] do
      assert get(ctx, path, %{"q" => query}) == full
    end

    for {query, ids} <- [
          {~s'{ resource.service.name = "filter-test" && (.http.status_code = 500 || .http.status_code = 200) }',
           [3, 4]},
          {~s({ resource.service.name =~ "filter.*" }), [1, 2, 3, 4, 5, 6]},
          {"{ duration = 1ms && status = unset }", [1, 2, 3, 4, 5, 6]},
          {~s({ span.http.status_code = "500" }), []}
        ] do
      assert filter_ids(get(ctx, path, %{"q" => query})) == ids
    end

    assert get(ctx, path, %{"q" => " ", "keep_hierarchy" => "invalid", "match_depth" => "-2"}) == full

    params = %{"q" => "{ .http.status_code = 404 }", "match_depth" => "-1", "keep_hierarchy" => "true"}
    empty = Req.get!(ctx.url <> path, headers: ctx.headers, params: params)
    assert empty.status == 200
    assert empty.body["trace"] == %{}
    assert empty.body["status"] == "PARTIAL"
    assert empty.body["message"] =~ "only a subset of spans matching the filter"

    assert empty.headers["x-pulso-trace-coverage"] == [
             "explicit-filter-subset-of-all-visible-stored-spans; exporter-completeness-unknown"
           ]

    wire = Req.get!(ctx.url <> path, headers: [{"accept", "application/protobuf"} | ctx.headers], params: params)
    assert wire.status == 200
    message = "Trace filtered, only a subset of spans matching the filter is returned"
    assert wire.body == <<10, 0, 24, 1, 34, byte_size(message)>> <> message
    assert get(ctx, path, %{}) == full

    query = %{"q" => "{ .http.status_code = 500 }"}
    assert Req.get!(ctx.url <> "/api/traces/1", headers: ctx.headers, params: query).status == 400

    assert Req.get!(ctx.url <> path, headers: ctx.headers, params: Map.put(query, "allowPartialTrace", "true")).status ==
             400

    assert Req.get!(ctx.url <> path, params: query).status == 401

    for value <- ["-2", "9223372036854775808", "1.5", "1x"] do
      assert Req.get!(ctx.url <> path, headers: ctx.headers, params: Map.put(query, "match_depth", value)).status == 400
    end

    assert Req.get!(ctx.url <> path, headers: ctx.headers, params: %{"q" => "{} >> {}"}).status == 400
    too_many_leaves = "{" <> Enum.join(List.duplicate(".http.status_code = 500", 17), " && ") <> "}"
    assert Req.get!(ctx.url <> path, headers: ctx.headers, params: %{"q" => too_many_leaves}).status == 400

    Runtime.put_env(:pulso, Pulso.QueryRunner, max_scan_rows: 1)
    assert {:ok, filter} = TempoFilter.parse(params)
    assert {:error, :query_scan_limit} = Tempo.lookup(ctx.tenant, String.pad_leading("1", 32, "0"), "json_v2", filter)
  end

  test "Pulso filtering controls: bounded graph walks terminate on cycles and do not invent missing parents", ctx do
    ingest_filter_trace(ctx, [
      {1, 2, 0},
      {2, 3, 0},
      {3, 1, 500},
      {4, 3, 0},
      {5, 4, 200},
      {6, 99, 500},
      {7, 6, 0},
      {8, 0, 0}
    ])

    path = "/api/v2/traces/1"
    query = %{"q" => "{ .http.status_code = 500 }"}

    for {params, ids} <- [
          {%{}, [3, 6]},
          {%{"match_depth" => "1"}, [2, 3, 4, 6, 7]},
          {%{"keep_hierarchy" => "true", "ancestor_depth" => "1"}, [1, 3, 6]},
          {%{"keep_hierarchy" => "true", "ancestor_depth" => "1", "match_depth" => "1"}, [1, 2, 3, 4, 6, 7]},
          {%{"match_depth" => "-1"}, [1, 2, 3, 4, 5, 6, 7]},
          {%{"match_depth" => "9223372036854775807"}, [1, 2, 3, 4, 5, 6, 7]},
          {%{"keep_hierarchy" => "true"}, [1, 2, 3, 6]}
        ] do
      result = get(ctx, path, Map.merge(query, params))
      assert filter_ids(result) == ids
      assert result["status"] == "PARTIAL"
    end
  end

  for {change, reason} <- [{:legacy, "trace_metadata_unknown"}, {:conflict, "trace_span_id_conflict"}] do
    test "Pulso filtering controls: nonmatching #{change} spans still fail the entire lookup", ctx do
      ingest_filter_trace(ctx)
      assert {:ok, spans} = Storage.query(:traces, ctx.tenant, [])
      nonmatch = Enum.find(spans, &(&1.span_id == "0000000000000004"))
      changed = if unquote(change) == :legacy, do: %{nonmatch | otlp: nil}, else: %{nonmatch | name: "conflicting"}
      assert :ok = Storage.append(:traces, ctx.tenant, [changed])

      for query <- ["{ .http.status_code = 500 }", "{ .http.status_code = 404 }"] do
        response = Req.get!(ctx.url <> "/api/v2/traces/1", headers: ctx.headers, params: %{"q" => query})
        assert response.status == 409
        assert response.body =~ unquote(reason)
      end
    end
  end

  for owner <- [:links, :events] do
    test "Pulso filtering controls: malformed nonmatching #{owner} still fail the entire lookup", ctx do
      ingest_filter_trace(ctx)
      assert filter_ids(get(ctx, "/api/v2/traces/1", %{"q" => "{ .http.status_code = 500 }"})) == [3]

      assert {:ok, [rich], 0} =
               Traces.decode(OTLPTraceFixture.payload([OTLPTraceFixture.span((ctx.now - 3) * 1_000_000_000)]))

      changed = %{rich | trace_id: String.pad_leading("1", 32, "0"), span_id: "0000000000000007"}

      changed =
        case unquote(owner) do
          :links -> %{changed | links: [Map.put(hd(changed.links), "trace_id", "not-a-trace-id")]}
          :events -> %{changed | events: [Map.put(hd(changed.events), "timestamp_ns", -1)]}
        end

      assert :ok = Storage.append(:traces, ctx.tenant, [changed])

      for query <- [nil, "{ .http.status_code = 500 }", "{ .http.status_code = 404 }"],
          accept <- ["application/json", "application/protobuf"] do
        params = if query, do: %{"q" => query}, else: %{}

        response =
          Req.get!(ctx.url <> "/api/v2/traces/1",
            headers: [{"accept", accept} | ctx.headers],
            params: params,
            retry: false
          )

        assert response.status == 500
        assert response.body == "stored trace cannot be exported faithfully"
      end
    end
  end

  # integration/api/metrics/query_range_test.go: TestQueryRangeExemplars
  # contains these metric-only subtests too. No exemplar assertion is selected.
  describe "TestQueryRangeExemplars / metric-only tables" do
    setup ctx do
      # Freeze twenty half-second producer ticks and random UUIDs. A minute
      # boundary keeps every batch inside both aligned upstream query windows.
      now = div(ctx.now, 60) * 60
      ctx = %{ctx | now: now}
      ingest_metric_batches(ctx)
      %{now: now}
    end

    for {name, offset_ns, step, intervals} <- [
          {"aligned", 0, "1m", 3},
          {"unaligned", 500_000_000, "1m", 4},
          {"default step", 500_000_000, nil, 122}
        ] do
      test "intervals / #{name}", ctx do
        finish = ctx.now * 1_000_000_000 + unquote(offset_ns)
        params = %{"q" => "{} | count_over_time()", "start" => finish - 120_000_000_000, "end" => finish}
        params = if unquote(step), do: Map.put(params, "step", unquote(step)), else: params
        assert %{"series" => [series]} = get(ctx, "/api/metrics/query_range", params)
        assert length(series["samples"]) == unquote(intervals)
        # Independent control: the exact frozen input is sixty single spans.
        assert Enum.sum_by(series["samples"], & &1["value"]) == 60
      end
    end

    for query <- [
          "{status=error} | count_over_time()",
          ~s'{span.randomattr = "doesnotexist"} | count_over_time()'
        ] do
      test "empty results / #{query}", ctx do
        params = %{"q" => unquote(query), "start" => ctx.now - 300, "end" => ctx.now + 60, "step" => "5s"}
        result = get(ctx, "/api/metrics/query_range", params)
        assert [_ | _] = result["series"]
        assert Enum.sum_by(result["series"], &length(Map.get(&1, "exemplars", []))) == 0
      end
    end

    for {name, step} <- [{"step=0 (default step)", "0"}, {"step=0s (default step)", "0s"}, {"small step", "35ms"}] do
      test "HTTP validation / #{name}", ctx do
        params = %{
          "q" => "{} | count_over_time()",
          "start" => ctx.now - 300,
          "end" => ctx.now + 60,
          "step" => unquote(step)
        }

        response = Req.get!(ctx.url <> "/api/metrics/query_range", headers: ctx.headers, params: params)
        # Preserve HTTP rejection; Pulso uses its own bounded-parameter diagnostic.
        assert response.status == 400
      end
    end

    for {name, query, step, reduction, expected} <- [
          {"count_over_time", "{} | count_over_time()", "1s", :sum, 60},
          {"sum_over_time", "{} | sum_over_time(duration)", "1s", :sum, 0.00006},
          {"max_over_time", "{} | max_over_time(duration)", "1s", :max, 0.000001},
          {"min_over_time", "{} | min_over_time(duration)", "1s", :min, 0.000001},
          {"1m step", "{} | count_over_time()", "1m", :sum, 60}
        ] do
      test "instant and range / #{name}", ctx do
        params = %{"q" => unquote(query), "start" => ctx.now - 300, "end" => ctx.now + 60, "step" => unquote(step)}
        assert %{"series" => [range]} = get(ctx, "/api/metrics/query_range", params)
        assert %{"series" => [instant]} = get(ctx, "/api/metrics/query", params)
        values = Enum.map(range["samples"], & &1["value"])

        reduced =
          case unquote(reduction) do
            :sum -> Enum.sum(values)
            :max -> Enum.max(values)
            :min -> Enum.min(values)
          end

        assert_in_delta instant["value"], reduced, 1.0e-12
        assert_in_delta instant["value"], unquote(expected), 1.0e-12
      end
    end

    test "avg_over_time instant query", ctx do
      params = %{"start" => ctx.now - 300, "end" => ctx.now + 60}

      results =
        Map.new(["avg", "sum", "count"], fn op ->
          argument = if op == "count", do: "", else: "duration"
          query = "{} | #{op}_over_time(#{argument})"
          assert %{"series" => [series]} = get(ctx, "/api/metrics/query", Map.put(params, "q", query))
          {op, series["value"]}
        end)

      assert results["count"] == 60
      assert_in_delta results["avg"], results["sum"] / results["count"], 1.0e-12
      assert_in_delta results["avg"], 0.000001, 1.0e-12
    end

    for {name, query, count} <- [
          {"top 1 by span attribute", "{ } | rate() by (span.span_high_cardinality) | topk(1)", 1},
          {"top 10 by span attribute", "{ } | rate() by (span.span_high_cardinality) | topk(10)", 10},
          {"top 2 by resource attribute", "{ } | rate() by (resource.res_high_cardinality) | topk(2)", 2},
          {"bottom 1 by resource attribute", "{ } | rate() by (resource.res_high_cardinality) | bottomk(1)", 1},
          {"bootom 10 by resource attribute", "{ } | rate() by (resource.res_high_cardinality) | bottomk(10)", 10},
          {"bottom 2 by span attribute", "{ } | rate() by (span.span_high_cardinality) | bottomk(2)", 2}
        ] do
      test "ranking / #{name}", ctx do
        params = %{"q" => unquote(query), "start" => ctx.now - 300, "end" => ctx.now + 60, "step" => "1m"}
        result = get(ctx, "/api/metrics/query", params)
        assert length(result["series"]) == unquote(count)
      end
    end
  end

  test "TestQueryRangeTypeHandling / metric-only: string and integer attributes do not lose span-ID series", ctx do
    batches =
      for {id, value} <- [{"00000000000000ab", %{"stringValue" => "123"}}, {"7fffffffffffffff", %{"intValue" => "123"}}] do
        %{
          "spanCount" => 1,
          "name" => "my operation",
          "resourceAttr" => "xx",
          "resourceAttVal" => "",
          "spanAttr" => "x",
          "spanAttVal" => "y",
          "spanId" => id,
          "extraSpanAttributes" => [%{"key" => "foo", "value" => value}]
        }
      end

    ingest(ctx, batches)
    params = %{"q" => "{} | rate() by (span:id)", "start" => ctx.now - 300, "end" => ctx.now, "step" => "5s"}
    result = get(ctx, "/api/metrics/query_range", params)
    assert Map.get(result, "status", "COMPLETE") == "COMPLETE"
    assert length(result["series"]) == 2

    # Pulso control: cardinality alone cannot detect lost ID bits or a wrong label.
    assert Enum.sort(Enum.map(result["series"], & &1["labels"])) == [
             [%{"key" => "span:id", "value" => %{"stringValue" => "00000000000000ab"}}],
             [%{"key" => "span:id", "value" => %{"stringValue" => "7fffffffffffffff"}}]
           ]

    for series <- result["series"] do
      assert_in_delta Enum.sum_by(series["samples"], & &1["value"]), 0.2, 1.0e-12
    end
  end

  test "TestQueryRangeMaxSeriesDisabled / metric-cardinality selection: every frozen producer span returns a series",
       ctx do
    batches =
      for tick <- 1..10 do
        %{
          "spanCount" => 1,
          "name" => "my operation",
          "resourceAttr" => "xx",
          "resourceAttVal" => "",
          "spanAttr" => "x",
          "spanAttVal" => "y",
          "spanId" => tick |> Integer.to_string(16) |> String.pad_leading(16, "0"),
          "startNs" => (ctx.now - 8) * 1_000_000_000 + tick * 500_000_000
        }
      end

    ingest(ctx, batches)
    params = %{"q" => "{} | rate() by (span:id)", "start" => ctx.now - 300, "end" => ctx.now, "step" => "5s"}
    result = get(ctx, "/api/metrics/query_range", params)
    assert Map.get(result, "status", "COMPLETE") == "COMPLETE"
    assert length(result["series"]) == 10
  end

  test "Pulso metric controls: span IDs are core strings, not user attributes or trace-local identities", ctx do
    ctx = %{ctx | now: div(ctx.now, 60) * 60 - 30}

    batches =
      for {id, value} <- [
            {"00000000000000ab", %{"stringValue" => "123"}},
            {"00000000000000ab", %{"intValue" => "123"}},
            {"fedcba9876543210", %{"stringValue" => "123"}}
          ] do
        %{
          "spanCount" => 1,
          "name" => "my operation",
          "resourceAttr" => "id",
          "resourceAttVal" => "resource-id",
          "spanAttr" => "id",
          "spanAttVal" => "attribute-id",
          "spanId" => String.upcase(id),
          "extraSpanAttributes" => [%{"key" => "foo", "value" => value}]
        }
      end

    ingest(ctx, batches)

    for endpoint <- ["query", "query_range"],
        {group, expected} <- [
          {"span:id", %{%{"stringValue" => "00000000000000ab"} => 2, %{"stringValue" => "fedcba9876543210"} => 1}},
          {"span.id", %{%{"stringValue" => "attribute-id"} => 3}},
          {"resource.id", %{%{"stringValue" => "resource-id"} => 3}},
          {"span.foo", %{%{"stringValue" => "123"} => 2, %{"intValue" => "123"} => 1}}
        ] do
      params = %{
        "q" => "{} | count_over_time() by (#{group})",
        "start" => ctx.now - 300,
        "end" => ctx.now,
        "step" => "5s"
      }

      result = get(ctx, "/api/metrics/" <> endpoint, params)

      actual =
        Map.new(result["series"], fn series ->
          assert [%{"key" => ^group, "value" => value}] = series["labels"]
          count = if endpoint == "query", do: series["value"], else: Enum.sum_by(series["samples"], & &1["value"])
          {value, count}
        end)

      assert actual == expected
      assert length(result["series"]) == map_size(expected)
    end

    for query <- [
          "{} | rate() by (id)",
          "{} | rate() by (trace:id)",
          "{} | sum_over_time(span:id)",
          "{} | quantile_over_time(span:id, 0.5)"
        ] do
      response =
        Req.get!(ctx.url <> "/api/metrics/query",
          headers: ctx.headers,
          params: %{"q" => query, "start" => ctx.now - 300, "end" => ctx.now}
        )

      assert response.status == 400
    end
  end

  test "Pulso metric controls: ranking selects actual extremes, not arbitrary k series", ctx do
    ctx = %{ctx | now: div(ctx.now, 60) * 60 - 30}

    batches =
      for {value, count} <- [{"small", 1}, {"middle", 3}, {"large", 5}] do
        %{
          "spanCount" => count,
          "name" => "operation",
          "resourceAttr" => "res_high_cardinality",
          "resourceAttVal" => value,
          "spanAttr" => "span_high_cardinality",
          "spanAttVal" => value
        }
      end

    ingest(ctx, batches)

    for {scope, key} <- [{"span", "span_high_cardinality"}, {"resource", "res_high_cardinality"}],
        {op, value, count} <- [{"topk", "large", 5}, {"bottomk", "small", 1}],
        endpoint <- ["query", "query_range"] do
      label = scope <> "." <> key
      query = "{} | count_over_time() by (#{label}) | #{op}(1)"
      # Every input is in one minute. Empty range intervals may retain another
      # zero-valued tie winner; compare the actual nonzero ranking separately.
      finish = div(ctx.now, 60) * 60 + 60
      params = %{"q" => query, "start" => finish - 60, "end" => finish, "step" => "1m"}
      result = get(ctx, "/api/metrics/" <> endpoint, params)

      if endpoint == "query" do
        assert result["series"] == [
                 %{"labels" => [%{"key" => label, "value" => %{"stringValue" => value}}], "value" => count}
               ]
      else
        assert [series] = Enum.filter(result["series"], &Enum.any?(&1["samples"], fn point -> point["value"] > 0 end))
        assert series["labels"] == [%{"key" => label, "value" => %{"stringValue" => value}}]
        assert Enum.sum_by(series["samples"], & &1["value"]) == count
      end
    end
  end

  test "Pulso metric controls: instant duration averages weight spans, not occupied intervals", ctx do
    start = div(ctx.now, 60) * 60 - 60

    batches =
      for {offset, count, duration_ms} <- [{5, 1, 100}, {15, 3, 200}, {25, 5, 700}] do
        %{
          "spanCount" => count,
          "name" => "operation",
          "resourceAttr" => "res_attr",
          "resourceAttVal" => "res_val",
          "spanAttr" => "span_attr",
          "spanAttVal" => "span_val",
          "startNs" => (start + offset) * 1_000_000_000,
          "durationNs" => duration_ms * 1_000_000
        }
      end

    ingest(ctx, batches)

    for {op, expected, values} <- [
          {"sum", 4.2, [0.1, 0.6, 3.5]},
          {"avg", 4.2 / 9, [0.1, 0.2, 0.7]},
          {"min", 0.1, [0.1, 0.2, 0.7]},
          {"max", 0.7, [0.1, 0.2, 0.7]}
        ] do
      params = %{"q" => "{} | #{op}_over_time(duration)", "start" => start, "end" => start + 30, "step" => "10s"}
      assert %{"series" => [instant]} = get(ctx, "/api/metrics/query", params)
      assert_in_delta instant["value"], expected, 1.0e-12
      assert %{"series" => [range]} = get(ctx, "/api/metrics/query_range", params)
      assert length(range["samples"]) == 3

      for {sample, value, offset} <- Enum.zip([range["samples"], values, [10, 20, 30]]) do
        assert sample["timestampMs"] == Integer.to_string((start + offset) * 1000)
        assert_in_delta sample["value"], value, 1.0e-12
      end
    end
  end

  defp ingest_metric_batches(ctx) do
    batches =
      for tick <- 0..19,
          {name, resource_key, resource_value, span_key, span_value} <- [
            {"my operation", "res_attr", "res_val", "span_attr", "span_val"},
            {"my operation", "res_attr", "res_val2", "span_attr", "span_val2"},
            {"operation with high cardinality", "res_high_cardinality", "resource-#{tick}", "span_high_cardinality",
             "span-#{tick}"}
          ] do
        %{
          "spanCount" => 1,
          "name" => name,
          "resourceAttr" => resource_key,
          "resourceAttVal" => resource_value,
          "spanAttr" => span_key,
          "spanAttVal" => span_value,
          "startNs" => (ctx.now - 13) * 1_000_000_000 + tick * 500_000_000
        }
      end

    ingest(ctx, batches)
  end

  defp ingest_filter_trace(ctx, graph \\ [{1, 0, 0}, {2, 1, 0}, {3, 2, 500}, {4, 1, 200}, {5, 3, 0}, {6, 5, 0}]) do
    spans =
      for {id, parent, code} <- graph do
        span = %{
          "traceId" => String.pad_leading("1", 32, "0"),
          "spanId" => id |> Integer.to_string(16) |> String.pad_leading(16, "0"),
          "name" => "span",
          "startTimeUnixNano" => Integer.to_string((ctx.now - 3) * 1_000_000_000),
          "endTimeUnixNano" => Integer.to_string((ctx.now - 3) * 1_000_000_000 + 1_000_000),
          "attributes" =>
            if(code == 0, do: [], else: [%{"key" => "http.status_code", "value" => %{"intValue" => to_string(code)}}])
        }

        if parent == 0,
          do: span,
          else: Map.put(span, "parentSpanId", parent |> Integer.to_string(16) |> String.pad_leading(16, "0"))
      end

    body = %{
      "resourceSpans" => [
        %{
          "resource" => %{"attributes" => [%{"key" => "service.name", "value" => %{"stringValue" => "filter-test"}}]},
          "scopeSpans" => [%{"spans" => spans}]
        }
      ]
    }

    assert Req.post!(ctx.url <> "/v1/traces", headers: ctx.headers, json: body).status == 200
    assert :ok = stop_supervised(ManifestSupervision)
    start_supervised!(ManifestSupervision)
  end

  defp filter_ids(response) do
    for resource <- Map.get(response["trace"], "resourceSpans", []),
        scope <- resource["scopeSpans"],
        span <- scope["spans"] do
      span["spanId"] |> Base.decode64!() |> :binary.last()
    end
    |> Enum.sort()
  end

  defp ingest(ctx, batches) do
    for {batch, i} <- Enum.with_index(batches, 1) do
      attributes = fn key, value -> [%{"key" => key, "value" => %{"stringValue" => value}}] end
      start = Map.get(batch, "startNs", (ctx.now - 3) * 1_000_000_000)

      spans =
        for j <- 1..batch["spanCount"] do
          %{
            "traceId" => i |> Integer.to_string(16) |> String.pad_leading(32, "0"),
            "spanId" => Map.get(batch, "spanId", j |> Integer.to_string(16) |> String.pad_leading(16, "0")),
            "name" => batch["name"],
            "startTimeUnixNano" => Integer.to_string(start),
            "endTimeUnixNano" => Integer.to_string(start + Map.get(batch, "durationNs", 1000)),
            "attributes" =>
              attributes.(batch["spanAttr"], batch["spanAttVal"]) ++ Map.get(batch, "extraSpanAttributes", [])
          }
        end

      body = %{
        "resourceSpans" => [
          %{
            "resource" => %{
              "attributes" =>
                attributes.("service.name", "my-service") ++ attributes.(batch["resourceAttr"], batch["resourceAttVal"])
            },
            "scopeSpans" => [%{"spans" => spans}]
          }
        ]
      }

      assert Req.post!(ctx.url <> "/v1/traces", headers: ctx.headers, json: body).status == 200
    end

    # Acknowledgements already publish Parquet; fresh supervision removes local manifest state.
    assert :ok = stop_supervised(ManifestSupervision)
    start_supervised!(ManifestSupervision)
  end

  defp windows(ctx), do: [%{}, %{"start" => ctx.now - 7200, "end" => ctx.now + 7200}]

  defp values_path(version, target) do
    prefix = if version == 1, do: "/api", else: "/api/v2"
    prefix <> "/search/tag/" <> URI.encode(target, &URI.char_unreserved?/1) <> "/values"
  end

  defp assert_values(ctx, version, target, values, params) do
    expected = %{
      "tagValues" => if(version == 1, do: values, else: Enum.map(values, &%{"type" => "string", "value" => &1}))
    }

    path = values_path(version, target)
    assert get(ctx, path, params) == expected

    response = Req.get!(ctx.url <> path, headers: [{"accept", "application/protobuf"} | ctx.headers], params: params)
    assert response.status == 200
    assert response.body == Protobuf.encode(expected, if(version == 1, do: :tag_values, else: :tag_values_v2))
  end

  defp get(ctx, path, params) do
    response = Req.get!(ctx.url <> path, headers: ctx.headers, params: params)
    assert response.status == 200, "#{path}: #{inspect(response.body)}"
    response.body
  end
end
