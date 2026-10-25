# SPDX-License-Identifier: AGPL-3.0-only
# Adapted from Grafana Loki integration tests, copyright Grafana Labs contributors.
# Modified for Pulso's Elixir HTTP/S3 harness on 2026-10-25.
# Source revision, case mapping, adaptations and license: test/fixtures/logs/loki-upstream/README.md.
defmodule PulsoWeb.LokiUpstreamIntegrationTest do
  use Pulso.Test.Case, async: true

  alias Pulso.Auth
  alias Pulso.Auth.SharedSecret
  alias Pulso.Runtime
  alias Pulso.Storage
  alias Pulso.Storage.S3
  alias Pulso.Storage.S3.ManifestSupervision
  alias Pulso.Test.CompactionStore
  alias Pulso.Test.RuntimePlug

  @second 1_000_000_000
  @minute 60 * @second
  @week 7 * 24 * 60 * @minute

  setup do
    Runtime.share()

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

    server = start_supervised!({Bandit, plug: {CompactionStore, agent: store}, port: 0})
    {:ok, {_, port}} = ThousandIsland.listener_info(server)

    config = %{
      bucket: "pulso",
      endpoint: "http://localhost:#{port}",
      region: "us-east-1",
      access_key_id: "loki-upstream-#{System.unique_integer([:positive])}",
      secret_access_key: "test",
      allow_http: true,
      refresh_stale_ms: 0
    }

    tenant = "loki-upstream-#{System.unique_integer([:positive])}"
    token = "test-token"
    digest = "sha256$" <> Base.encode16(:crypto.hash(:sha256, token), case: :lower)
    Runtime.put_env(:pulso, Auth, module: SharedSecret, tokens: %{tenant => digest})
    Runtime.put_env(:pulso, Storage, adapter: S3)
    Runtime.put_env(:pulso, S3, config)
    start_supervised!(ManifestSupervision)

    api =
      start_supervised!({Bandit, plug: {RuntimePlug, plug: PulsoWeb.Endpoint, runtime: Runtime.current()}, port: 0})

    {:ok, {_, api_port}} = ThousandIsland.listener_info(api)

    # Preserve upstream relative times, but keep the +3s label-access input in
    # the past so its default-time label APIs cannot race the wall clock.
    %{
      url: "http://localhost:#{api_port}",
      tenant: tenant,
      token: token,
      now: System.system_time(:nanosecond) - 10 * @second,
      store: store
    }
  end

  describe "TestMicroServicesIngestQuery" do
    setup ctx do
      for {line, timestamp} <- [
            {"lineA", ctx.now - 45 * @minute},
            {"lineB", ctx.now - 45 * @minute},
            {"lineC", ctx.now},
            {"lineD", ctx.now}
          ] do
        push(ctx, line, timestamp, %{"job" => "fake"})
      end

      assert_parquet(ctx, 4)
      :ok
    end

    test "query", ctx do
      response = query(ctx, ~s({job="fake"}))
      assert response["data"]["resultType"] == "streams"
      assert lines(response) == ["lineA", "lineB", "lineC", "lineD"]
      assert_parquet_read(ctx)
    end

    test "label-names", ctx do
      assert request(ctx, "/labels")["data"] == ["job"]
      assert_parquet_read(ctx)
    end

    test "label-values", ctx do
      assert request(ctx, "/label/job/values")["data"] == ["fake"]
      assert_parquet_read(ctx)
    end

    test "series", ctx do
      assert request(ctx, "/series", [{"match[]", ~s({job="fake"})} | range(ctx)])["data"] == [%{"job" => "fake"}]
      assert_parquet_read(ctx)
    end

    test "series error", ctx do
      response = request(ctx, "/series", [{"match[]", ~s({job="fake"}|= "search")} | range(ctx)], 400)
      assert response["message"] =~ "only label matchers are supported"
      refute_parquet_read(ctx)
    end

    test "stats error", ctx do
      response = request(ctx, "/index/stats", [{"query", ~s({job="fake"}|= "search")}], 400)
      assert response["message"] =~ "only label matchers are supported"
      refute_parquet_read(ctx)
    end
  end

  describe "TestLabelAccessTestCases/disabled" do
    setup ctx do
      for {line, offset, labels} <- [
            {"line1", 0, %{"env" => "dev", "classification" => "confidential"}},
            {"line2", 1, %{}},
            {"line3", 2, %{"classification" => "secret", "env" => "dev"}},
            {"line4", 3, %{"classification" => "secret", "env" => "prod"}}
          ] do
        push(ctx, line, ctx.now + offset * @second, Map.put(labels, "job", "varlog"))
      end

      assert_parquet(ctx, 4)

      # The upstream disabled case sends this header, but does not enable LBAC.
      policy = ctx.tenant <> ":" <> URI.encode_www_form(~s({env="dev"}))
      %{now: ctx.now + 4 * @second, extra_headers: [{"x-prom-label-policy", policy}]}
    end

    test "logs-query", ctx do
      assert lines(query(ctx, ~s({job="varlog"} |= "line"))) == ["line1", "line2", "line3", "line4"]
      assert_parquet_read(ctx)
    end

    test "label-values-query", ctx do
      labels = request(ctx, "/labels")["data"]
      actual = Map.new(labels, fn label -> {label, request(ctx, "/label/#{label}/values")["data"]} end)

      assert actual == %{
               "classification" => ["confidential", "secret"],
               "env" => ["dev", "prod"],
               "job" => ["varlog"]
             }

      assert_parquet_read(ctx)
    end

    test "series-query", ctx do
      actual = request(ctx, "/series", [{"match[]", "{}"} | range(ctx)])["data"]
      assert Enum.sort(actual) == Enum.sort(access_streams())
      assert_parquet_read(ctx)
    end

    test "metrics-query", ctx do
      response = instant(ctx, ~s'count_over_time({job="varlog"} |= "line" [2h])')
      assert response["data"]["resultType"] == "vector"
      assert Enum.sort(Enum.map(response["data"]["result"], & &1["metric"])) == Enum.sort(access_streams())
      assert_parquet_read(ctx)
    end

    test "metrics-range-query", ctx do
      # Retain the seven-day client window and upstream maximum of four, but
      # request minute resolution: a ~40-minute default grid can miss all four
      # writes in the last four seconds. See the fixture README.
      response =
        request(ctx, "/query_range", [
          {"query", ~s'count_over_time({job="varlog"} |= "line" [2h])'},
          {"step", "60"}
          | range(ctx)
        ])

      assert response["data"]["resultType"] == "matrix"

      per_step =
        for series <- response["data"]["result"], [timestamp, count] <- series["values"], reduce: %{} do
          totals ->
            {value, ""} = Float.parse(count)
            Map.update(totals, timestamp, value, &(&1 + value))
        end

      assert Enum.max(Map.values(per_step)) == 4.0
      assert_parquet_read(ctx)
    end
  end

  describe "TestCategorizedLabels" do
    setup ctx do
      push(ctx, "lineA", ctx.now - @second, %{"job" => "fake"})
      push(ctx, "lineB", ctx.now - 2 * @second, %{"job" => "fake"}, %{"traceID" => "123", "user" => "a"})
      push(ctx, "lineC msg=foo", ctx.now - 3 * @second, %{"job" => "fake"}, %{"traceID" => "456", "user" => "b"})

      push(ctx, "lineD msg=foo text=bar", ctx.now - 4 * @second, %{"job" => "fake"}, %{
        "traceID" => "789",
        "user" => "c"
      })

      assert_parquet(ctx, 4)
      :ok
    end

    test "no header - no parser", ctx do
      response = query(ctx, ~s({job="fake"}))

      assert_streams(response, [
        {%{"job" => "fake"}, ["lineA"]},
        {%{"job" => "fake", "traceID" => "123", "user" => "a"}, ["lineB"]},
        {%{"job" => "fake", "traceID" => "456", "user" => "b"}, ["lineC msg=foo"]},
        {%{"job" => "fake", "traceID" => "789", "user" => "c"}, ["lineD msg=foo text=bar"]}
      ])

      assert_parquet_read(ctx)
    end

    test "no header - with parser", ctx do
      response = query(ctx, ~s({job="fake"} | logfmt))

      assert_streams(response, [
        {%{"job" => "fake"}, ["lineA"]},
        {%{"job" => "fake", "traceID" => "123", "user" => "a"}, ["lineB"]},
        {%{"job" => "fake", "traceID" => "456", "user" => "b", "msg" => "foo"}, ["lineC msg=foo"]},
        {%{"job" => "fake", "traceID" => "789", "user" => "c", "msg" => "foo", "text" => "bar"},
         ["lineD msg=foo text=bar"]}
      ])

      assert_parquet_read(ctx)
    end

    test "with header - no parser", ctx do
      ctx = Map.put(ctx, :extra_headers, [{"x-loki-response-encoding-flags", "categorize-labels"}])
      response = query(ctx, ~s({job="fake"}))

      assert response["data"]["encodingFlags"] == ["categorize-labels"]
      assert response["data"]["resultType"] == "streams"

      assert response["data"]["result"] == [
               %{
                 "stream" => %{"job" => "fake"},
                 "values" => [
                   [to_string(ctx.now - @second), "lineA", %{}],
                   [
                     to_string(ctx.now - 2 * @second),
                     "lineB",
                     %{"structuredMetadata" => %{"traceID" => "123", "user" => "a"}}
                   ],
                   [
                     to_string(ctx.now - 3 * @second),
                     "lineC msg=foo",
                     %{"structuredMetadata" => %{"traceID" => "456", "user" => "b"}}
                   ],
                   [
                     to_string(ctx.now - 4 * @second),
                     "lineD msg=foo text=bar",
                     %{"structuredMetadata" => %{"traceID" => "789", "user" => "c"}}
                   ]
                 ]
               }
             ]

      assert_parquet_read(ctx)
    end

    test "with header - with parser", ctx do
      ctx = Map.put(ctx, :extra_headers, [{"x-loki-response-encoding-flags", "categorize-labels"}])
      response = query(ctx, ~s({job="fake"} | logfmt))

      assert response["data"]["encodingFlags"] == ["categorize-labels"]
      assert response["data"]["resultType"] == "streams"

      assert response["data"]["result"] == [
               %{
                 "stream" => %{"job" => "fake"},
                 "values" => [
                   [to_string(ctx.now - @second), "lineA", %{}],
                   [
                     to_string(ctx.now - 2 * @second),
                     "lineB",
                     %{"structuredMetadata" => %{"traceID" => "123", "user" => "a"}}
                   ],
                   [
                     to_string(ctx.now - 3 * @second),
                     "lineC msg=foo",
                     %{
                       "structuredMetadata" => %{"traceID" => "456", "user" => "b"},
                       "parsed" => %{"msg" => "foo"}
                     }
                   ],
                   [
                     to_string(ctx.now - 4 * @second),
                     "lineD msg=foo text=bar",
                     %{
                       "structuredMetadata" => %{"traceID" => "789", "user" => "c"},
                       "parsed" => %{"msg" => "foo", "text" => "bar"}
                     }
                   ]
                 ]
               }
             ]

      assert_parquet_read(ctx)
    end
  end

  # Pulso-specific controls, not additional upstream ports.
  describe "categorized response encoding controls" do
    test "collisions, repeated parsing and label mutations retain explicit provenance", ctx do
      line = "job=same pod=body token=same keep=kept gone=drop"

      push(
        ctx,
        line,
        ctx.now,
        %{
          "job" => "same",
          "pod" => "stream",
          "pod_extracted" => "old",
          "rename" => "same"
        },
        %{"token" => "same", "untouched" => "u", "gone" => "drop"}
      )

      objects = Agent.get(ctx.store, & &1.objects)

      expression =
        ~s({job="same"} | logfmt | drop pod | logfmt | drop gone) <>
          ~s( | label_format renamed=rename, token="{{.token}}", overwrite="{{.job}}") <>
          ~s( | keep job, pod_extracted, token, untouched, renamed, overwrite, keep)

      ordinary = query(ctx, expression)
      categorized = query(categorize(ctx), expression)

      assert categorized["data"]["result"] == [
               %{
                 "stream" => %{"job" => "same"},
                 "values" => [
                   [
                     to_string(ctx.now),
                     line,
                     %{
                       "structuredMetadata" => %{"untouched" => "u"},
                       "parsed" => %{
                         "pod_extracted" => "body",
                         "token" => "same",
                         "renamed" => "same",
                         "overwrite" => "same",
                         "keep" => "kept"
                       }
                     }
                   ]
                 ]
               }
             ]

      assert flattened_entries(categorized) == flattened_entries(ordinary)
      assert Agent.get(ctx.store, & &1.objects) == objects
      assert_parquet_read(ctx)
    end

    test "repeated lines and exact duplicates preserve global selection and multiplicity", ctx do
      values =
        for {offset, id} <- [{4, "old"}, {2, "a"}, {2, "b"}, {2, "a"}],
            do: [to_string(ctx.now - offset * @second), "value=1", %{"id" => id}]

      body = %{"streams" => [%{"stream" => %{"job" => "fake", "pod" => "one"}, "values" => values}]}
      assert Req.post!(ctx.url <> "/loki/api/v1/push", headers: headers(ctx), json: body).status == 204
      push(ctx, "value=1", ctx.now - @second, %{"job" => "fake", "pod" => "two"}, %{"id" => "new"})

      all = query(categorize(ctx), ~s({job="fake"} | logfmt))
      entries = flattened_entries(all)
      assert Enum.sum(Map.values(entries)) == 5

      assert entries[
               {to_string(ctx.now - 2 * @second), "value=1",
                %{"job" => "fake", "pod" => "one", "id" => "a", "value" => "1"}}
             ] == 2

      assert entries == flattened_entries(query(ctx, ~s({job="fake"} | logfmt)))

      for direction <- ["forward", "backward"], limit <- [1, 3] do
        params = [
          {"query", ~s({job="fake"} |= "value=" | logfmt | id != "old" | line_format "{{.value}}")},
          {"direction", direction},
          {"limit", to_string(limit)}
          | range(ctx)
        ]

        ordinary = request(ctx, "/query_range", params)
        categorized = request(categorize(ctx), "/query_range", params)
        assert flattened_entries(categorized) == flattened_entries(ordinary)
        assert Enum.sum(Map.values(flattened_entries(categorized))) == limit
        assert Enum.uniq(lines(categorized)) == ["1"]
      end

      assert_parquet_read(ctx)
    end

    test "exact flags negotiate GET and form POST on instant and range endpoints", ctx do
      push(ctx, "x=one", ctx.now, %{"job" => "fake"}, %{"token" => "id"})

      for path <- ["/query", "/query_range"] do
        params = [{"query", ~s({job="fake"} | logfmt)}, {"time", to_string(ctx.now + @second)} | range(ctx)]
        ordinary = request(ctx, path, params)

        for {header, flags, enabled?} <- [
              {"future", ["future"], false},
              {"future,categorize-labels,categorize-labels", ["future", "categorize-labels"], true},
              {"CATEGORIZE-LABELS", ["CATEGORIZE-LABELS"], false},
              {",future,", ["", "future"], false},
              {"", [], false}
            ] do
          flagged = Map.put(ctx, :extra_headers, [{"x-loki-response-encoding-flags", header}])
          response = request(flagged, path, params)
          assert request_form(flagged, path, params) == response
          assert Map.get(response["data"], "encodingFlags", []) == flags
          assert flattened_entries(response) == flattened_entries(ordinary)

          if enabled? do
            assert [%{"stream" => %{"job" => "fake"}, "values" => [[_, "x=one", categories]]}] =
                     response["data"]["result"]

            assert categories == %{"structuredMetadata" => %{"token" => "id"}, "parsed" => %{"x" => "one"}}
          else
            assert response["data"]["result"] == ordinary["data"]["result"]
          end
        end
      end
    end

    test "encoding flags do not change metric samples", ctx do
      push(ctx, "x=one", ctx.now, %{"job" => "fake"}, %{"token" => "id"})
      expression = ~s'count_over_time({job="fake"}[1m])'

      for {path, params} <- [
            {"/query", [{"query", expression}, {"time", to_string(ctx.now + @second)}]},
            {"/query_range",
             [
               {"query", expression},
               {"step", "1"},
               {"start", to_string(ctx.now)},
               {"end", to_string(ctx.now + @second)}
             ]}
          ] do
        ordinary = request(ctx, path, params)
        flagged = request(categorize(ctx), path, params)
        assert flagged["data"]["encodingFlags"] == ["categorize-labels"]
        assert Map.delete(flagged["data"], "encodingFlags") == ordinary["data"]
      end
    end

    test "scan budget failures remain whole errors in both encodings", ctx do
      push(ctx, "x=one", ctx.now, %{"job" => "fake"})
      push(ctx, "x=two", ctx.now - @second, %{"job" => "fake"})
      Runtime.put_env(:pulso, Pulso.QueryRunner, max_scan_rows: 1)
      params = [{"query", ~s({job="fake"} | logfmt)} | range(ctx)]
      ordinary = request(ctx, "/query_range", params, 422)
      categorized = request(categorize(ctx), "/query_range", params, 422)
      assert ordinary == categorized
      assert ordinary["status"] == "error"
      refute Map.has_key?(ordinary, "data")
    end
  end

  describe "Test_ExploreLogsApis/detected_fields public results" do
    setup ctx do
      for {line, timestamp} <- [
            {"foo=bar color=red", ctx.now - 45 * @minute},
            {"foo=bar color=blue", ctx.now - 45 * @minute},
            {"foo=bar color=red", ctx.now - 5 * @second},
            {"foo=bar color=purple", ctx.now - 5 * @second},
            {"foo=bar color=green", ctx.now},
            {"foo=bar color=red", ctx.now}
          ] do
        push(ctx, line, timestamp, %{"job" => "fake"})
      end

      assert_parquet(ctx, 6)
      :ok
    end

    test "log query returns all six input lines", ctx do
      response = query(ctx, ~s({job="fake"}))
      assert response["data"]["resultType"] == "streams"

      assert lines(response) ==
               Enum.sort([
                 "foo=bar color=red",
                 "foo=bar color=blue",
                 "foo=bar color=red",
                 "foo=bar color=purple",
                 "foo=bar color=green",
                 "foo=bar color=red"
               ])

      assert_parquet_read(ctx)
    end

    test "non-split queries detect three recent color values", ctx do
      response =
        request(ctx, "/detected_fields", [
          {"query", ~s({job="fake"})},
          {"start", to_string(ctx.now - @minute)},
          {"end", to_string(ctx.now + @minute)}
        ])

      # The upstream response struct decodes only these three field members.
      assert Enum.sort(Enum.map(response["fields"], &Map.take(&1, ["label", "type", "cardinality"]))) ==
               Enum.sort([
                 %{"label" => "foo", "type" => "string", "cardinality" => 1},
                 %{"label" => "color", "type" => "string", "cardinality" => 3}
               ])

      assert_parquet_read(ctx)
    end

    test "split queries detect four retained color values", ctx do
      response =
        request(ctx, "/detected_fields", [
          {"query", ~s({job="fake"})},
          {"start", to_string(ctx.now - 24 * 60 * @minute)},
          {"end", to_string(ctx.now + @minute)}
        ])

      assert Enum.sort(Enum.map(response["fields"], &Map.take(&1, ["label", "type", "cardinality"]))) ==
               Enum.sort([
                 %{"label" => "foo", "type" => "string", "cardinality" => 1},
                 %{"label" => "color", "type" => "string", "cardinality" => 4}
               ])

      assert_parquet_read(ctx)
    end

    # Pulso-authored regression control, not another upstream case.
    test "Pulso control: inclusive bounds and newest sampling work across all six segments", ctx do
      for {start, finish, line_limit} <- [
            {ctx.now - 45 * @minute, ctx.now - 45 * @minute, "100"},
            {ctx.now, ctx.now, "100"},
            {ctx.now - 24 * 60 * @minute, ctx.now + @minute, "2"}
          ] do
        params = [
          {"query", ~s({job="fake"})},
          {"start", to_string(start)},
          {"end", to_string(finish)},
          {"line_limit", line_limit}
        ]

        response = request(ctx, "/detected_fields", params)
        assert request_form(ctx, "/detected_fields", params) == response
        assert Map.new(response["fields"], &{&1["label"], &1["cardinality"]}) == %{"foo" => 1, "color" => 2}
      end

      empty_window = [
        {"query", ~s({job="fake"})},
        {"start", to_string(ctx.now - 45 * @minute + 1)},
        {"end", to_string(ctx.now - 5 * @second - 1)}
      ]

      assert request(ctx, "/detected_fields", empty_window) == %{}
      assert request_form(ctx, "/detected_fields", empty_window) == %{}
      assert_parquet_read(ctx)
    end
  end

  describe "TestSampleQueryStreamOrderingEquivalence public result" do
    test "grouping by every fixture label returns counts a=3, b=2, c=1", ctx do
      for {app, lines} <- [{"a", ["a1", "a2", "a3"]}, {"b", ["b1", "b2"]}, {"c", ["c1"]}],
          {line, index} <- Enum.with_index(lines) do
        push(ctx, line, ctx.now - (30 - index) * @minute, %{"job" => "varlog", "cluster" => "prod", "app" => app})
      end

      assert_parquet(ctx, 6)
      response = instant(ctx, ~s'sum by (cluster, job, app) (count_over_time({cluster="prod"}[1h]))')
      assert length(response["data"]["result"]) == 3

      assert values(response) == %{
               %{"job" => "varlog", "cluster" => "prod", "app" => "a"} => "3",
               %{"job" => "varlog", "cluster" => "prod", "app" => "b"} => "2",
               %{"job" => "varlog", "cluster" => "prod", "app" => "c"} => "1"
             }

      assert_parquet_read(ctx)
    end
  end

  describe "TestSampleQueryStreamDataObjEquivalence public results" do
    setup ctx do
      for {line, minutes, app} <- [
            {"msg=a1 n=10", 6, "a"},
            {"msg=a2 n=20", 5, "a"},
            {"msg=a3 n=30", 4, "a"},
            {"msg=b1 n=1", 3, "b"},
            {"msg=b2 n=2", 2, "b"}
          ] do
        push(ctx, line, ctx.now - minutes * @minute, %{"job" => "dataobjit", "app" => app})
      end

      assert_parquet(ctx, 5)
      :ok
    end

    test "count_over_time complete expected result", ctx do
      response = instant(ctx, ~s'sum by (job, app) (count_over_time({job="dataobjit"}[1h]))')

      assert values(response) == %{
               %{"job" => "dataobjit", "app" => "a"} => "3",
               %{"job" => "dataobjit", "app" => "b"} => "2"
             }

      assert_parquet_read(ctx)
    end

    test "sum_over_time expected sums", ctx do
      response = instant(ctx, ~s'sum by (job, app) (sum_over_time({job="dataobjit"} | logfmt | unwrap n [1h]))')

      assert values(response) == %{
               %{"job" => "dataobjit", "app" => "a"} => "60",
               %{"job" => "dataobjit", "app" => "b"} => "3"
             }

      assert_parquet_read(ctx)
    end

    test "line-filter matching stream", ctx do
      response = instant(ctx, ~s'sum by (job, app) (count_over_time({job="dataobjit"} |= "a" [1h]))')
      assert values(response) == %{%{"job" => "dataobjit", "app" => "a"} => "3"}
      assert_parquet_read(ctx)
    end
  end

  defp headers(ctx) do
    [{"authorization", "Bearer " <> ctx.token}, {"x-scope-orgid", ctx.tenant}] ++ Map.get(ctx, :extra_headers, [])
  end

  defp push(ctx, line, timestamp, labels, metadata \\ %{}) do
    body = %{"streams" => [%{"stream" => labels, "values" => [[to_string(timestamp), line, metadata]]}]}
    assert Req.post!(ctx.url <> "/loki/api/v1/push", headers: headers(ctx), json: body).status == 204
  end

  defp request(ctx, path, params \\ [], status \\ 200) do
    response = Req.get!(ctx.url <> "/loki/api/v1" <> path, headers: headers(ctx), params: params, decode_body: false)
    assert response.status == status, response.body
    Pulso.JSON.decode!(response.body)
  end

  defp request_form(ctx, path, params) do
    response = Req.post!(ctx.url <> "/loki/api/v1" <> path, headers: headers(ctx), form: params, decode_body: false)
    assert response.status == 200, response.body
    Pulso.JSON.decode!(response.body)
  end

  defp categorize(ctx), do: Map.put(ctx, :extra_headers, [{"x-loki-response-encoding-flags", "categorize-labels"}])

  defp flattened_entries(response) do
    for stream <- response["data"]["result"], value <- stream["values"] do
      [timestamp, line | rest] = value
      categories = List.first(rest) || %{}

      labels =
        stream["stream"]
        |> Map.merge(Map.get(categories, "structuredMetadata", %{}))
        |> Map.merge(Map.get(categories, "parsed", %{}))

      {timestamp, line, labels}
    end
    |> Enum.frequencies()
  end

  defp range(ctx), do: [{"start", to_string(ctx.now - @week)}, {"end", to_string(ctx.now + @second)}]
  defp query(ctx, query), do: request(ctx, "/query_range", [{"query", query} | range(ctx)])
  defp instant(ctx, query), do: request(ctx, "/query", query: query, time: to_string(ctx.now + @second))

  defp lines(response) do
    response["data"]["result"]
    |> Enum.flat_map(fn stream -> Enum.map(stream["values"], &Enum.at(&1, 1)) end)
    |> Enum.sort()
  end

  defp assert_streams(response, expected) do
    assert response["data"]["resultType"] == "streams"

    actual =
      Enum.map(response["data"]["result"], fn stream ->
        {stream["stream"], Enum.map(stream["values"], &Enum.at(&1, 1))}
      end)

    assert Enum.sort(actual) == Enum.sort(expected)
    assert Map.get(response["data"], "encodingFlags", []) == []
  end

  defp values(response) do
    assert response["data"]["resultType"] == "vector"
    Map.new(response["data"]["result"], fn series -> {series["metric"], Enum.at(series["value"], 1)} end)
  end

  defp access_streams do
    [
      %{"classification" => "confidential", "env" => "dev", "job" => "varlog"},
      %{"classification" => "secret", "env" => "prod", "job" => "varlog"},
      %{"classification" => "secret", "env" => "dev", "job" => "varlog"},
      %{"job" => "varlog"}
    ]
  end

  defp assert_parquet(ctx, count) do
    objects = Agent.get(ctx.store, & &1.objects)
    segments = Enum.filter(objects, fn {key, _} -> String.ends_with?(key, ".parquet") end)
    assert length(segments) == count

    for {_key, {_etag, bytes}} <- segments do
      assert binary_part(bytes, 0, 4) == "PAR1"
      assert binary_part(bytes, byte_size(bytes) - 4, 4) == "PAR1"
    end
  end

  defp assert_parquet_read(ctx),
    do: assert(Agent.get(ctx.store, &Enum.any?(&1.reads, fn key -> String.ends_with?(key, ".parquet") end)))

  defp refute_parquet_read(ctx),
    do: refute(Agent.get(ctx.store, &Enum.any?(&1.reads, fn key -> String.ends_with?(key, ".parquet") end)))
end
