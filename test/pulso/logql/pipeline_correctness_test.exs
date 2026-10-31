defmodule Pulso.LogQL.PipelineCorrectnessTest do
  use Pulso.Test.Case, async: true

  alias Pulso.LogQL.{Entry, MetricEval, Parser, Pipeline}
  alias Pulso.PromQL.Evaluator
  alias Pulso.Record.Log
  alias Pulso.Runtime
  alias Pulso.Storage
  alias Pulso.Storage.Memory
  alias Pulso.Test.NativeQueryStorage

  defp evaluate(query, tenant, opts \\ %{end_ts_ns: 30}) do
    Process.delete(:promql_budget)
    Process.delete(:promql_deadline_ms)
    {:ok, ast} = Parser.parse(query)
    # Synthetic nanosecond windows exercise the evaluator's AST interface,
    # independently of Loki's millisecond-minimum bracket token grammar.
    {range_ns, opts} = Map.pop(opts, :range_ns)
    ast = if range_ns, do: %{ast | range_ns: range_ns}, else: ast
    MetricEval.evaluate(ast, tenant, opts)
  end

  defp assert_scan(adapter, native?) do
    if adapter == NativeQueryStorage and native?,
      do: assert_received(:native_log_metric_scan),
      else: refute_received(:native_log_metric_scan)
  end

  defp apply_pipeline(text, entry) do
    {:ok, ast} = Parser.parse(~s/{app="a"} / <> text)
    Pipeline.apply(Pipeline.compile(ast.stages), entry)
  end

  test "all parsers track writes independently of equal values and promoted stream labels" do
    for {stage, body} <- [
          {"logfmt", "pod=same token=same"},
          {~s/regexp "pod=(?P<pod>[^ ]+) token=(?P<token>.*)"/, "pod=same token=same"},
          {"json", ~s/{"pod":"same","token":"same"}/},
          {~s/pattern "pod=<pod> token=<token>"/, "pod=same token=same"},
          {"unpack", ~s/{"pod":"same","token":"same","_entry":"payload"}/}
        ] do
      record = %Log{
        timestamp_ns: 10,
        service: "api",
        severity_text: "info",
        body: body,
        resource: %{"app" => "a", "pod" => "same"},
        attributes: %{"token" => "same", "untouched" => true}
      }

      assert {:keep, categorized} = apply_pipeline("| " <> stage, Entry.from_record(record, true))
      assert {:keep, ordinary} = apply_pipeline("| " <> stage, Entry.from_record(record))
      assert categorized.labels == ordinary.labels
      assert categorized.line == ordinary.line
      assert ordinary.label_categories == nil

      assert Entry.categorized_labels(categorized) ==
               {
                 %{
                   "app" => "a",
                   "pod" => "same",
                   "service" => "api",
                   "service_name" => "api",
                   "level" => "info",
                   "detected_level" => "info"
                 },
                 %{
                   "structuredMetadata" => %{"untouched" => "true", "token" => "same"},
                   "parsed" => %{"pod_extracted" => "same", "token_extracted" => "same"}
                 }
               }

      assert {:keep, renamed} =
               apply_pipeline("| label_format token_extracted=token_extracted, absent=missing", categorized)

      assert renamed.labels == Map.delete(categorized.labels, "token_extracted")
      assert renamed.label_categories == Map.delete(categorized.label_categories, "token_extracted")
    end
  end

  for adapter <- [Memory, NativeQueryStorage] do
    @adapter adapter
    describe "#{inspect(adapter)}" do
      setup do
        Runtime.put_env(:pulso, Storage, adapter: @adapter)
        %{tenant: "pipeline-#{System.unique_integer([:positive])}"}
      end

      test "parsers preserve distinct original stream identities and negative predicates", %{tenant: tenant} do
        for {stage, body} <- [
              {"logfmt", "pod=shared"},
              {~s/regexp "pod=(?P<pod>.*)"/, "pod=shared"},
              {"json", ~s/{"pod":"shared"}/},
              {~s/pattern "pod=<pod>"/, "pod=shared"},
              {"unpack", ~s/{"pod":"shared","_entry":"payload"}/}
            ] do
          records =
            for pod <- ["p1", "p2"], do: %Log{timestamp_ns: 10, body: body, resource: %{"app" => stage, "pod" => pod}}

          assert :ok = Storage.append(:logs, tenant, records)
          selector = "{app=#{Pulso.JSON.encode!(stage)}}"
          assert {:ok, {:vector, series}} = evaluate("count_over_time(#{selector} | #{stage} [1m])", tenant)

          assert Enum.sort(
                   Enum.map(series, fn {labels, {30, count}} -> {labels["pod"], labels["pod_extracted"], count} end)
                 ) ==
                   [{"p1", "shared", 1.0}, {"p2", "shared", 1.0}]

          native? = stage == "logfmt" or String.starts_with?(stage, "regexp ")
          assert_scan(@adapter, native?)

          assert {:ok, {:vector, selected}} =
                   evaluate(~s/count_over_time(#{selector} | #{stage} | pod != "shared" [1m])/, tenant)

          assert length(selected) == 2
          assert_scan(@adapter, native?)
        end
      end

      test "structured attributes and promoted stream labels retain their owners", %{tenant: tenant} do
        record = %Log{
          timestamp_ns: 10,
          severity_text: "info",
          body: "level=error pod=body x=new",
          resource: %{"app" => "a"},
          attributes: %{"pod" => "metadata", "x" => "old"}
        }

        assert :ok = Storage.append(:logs, tenant, [record])

        assert {:ok, {:vector, [{labels, {30, 1.0}}]}} =
                 evaluate(~s/count_over_time({app="a"} | logfmt | level="info" [1m])/, tenant)

        assert labels["level"] == "info"
        assert labels["level_extracted"] == "error"
        assert labels["pod"] == "metadata"
        assert labels["x"] == "old"
        assert labels["pod_extracted"] == "body"
        assert labels["x_extracted"] == "new"
        if @adapter == NativeQueryStorage, do: assert_received(:native_log_metric_scan)
      end

      test "present empty and absent promoted projections retain distinct name provenance", %{tenant: tenant} do
        assert :ok =
                 Storage.append(:logs, tenant, [
                   %Log{
                     timestamp_ns: 10,
                     service: "",
                     severity_text: "",
                     body: "service=body level=body",
                     resource: %{"app" => "a"}
                   }
                 ])

        assert {:ok, {:vector, [{labels, {30, 1.0}}]}} = evaluate(~s/count_over_time({app="a"} | logfmt [1m])/, tenant)
        assert labels["service"] == ""
        assert labels["level"] == ""
        assert labels["service_extracted"] == "body"
        assert labels["level_extracted"] == "body"
        if @adapter == NativeQueryStorage, do: assert_received(:native_log_metric_scan)

        absent = tenant <> "-absent"

        assert :ok =
                 Storage.append(:logs, absent, [
                   %Log{timestamp_ns: 10, body: "service=body level=body", resource: %{"app" => "a"}}
                 ])

        assert {:ok, {:vector, [{labels, {30, 1.0}}]}} = evaluate(~s/count_over_time({app="a"} | logfmt [1m])/, absent)
        assert labels["service"] == "body"
        assert labels["level"] == "body"
        refute Map.has_key?(labels, "service_extracted")
        refute Map.has_key?(labels, "level_extracted")
        if @adapter == NativeQueryStorage, do: assert_received(:native_log_metric_scan)
      end

      test "logfmt and regexp retain first extractions after metadata ownership transfers", %{tenant: tenant} do
        assert :ok =
                 Storage.append(:logs, tenant, [
                   %Log{
                     timestamp_ns: 10,
                     body: "k=one second=two k_extracted=three",
                     resource: %{"app" => "a", "k" => "stream"},
                     attributes: %{"k" => "metadata"}
                   }
                 ])

        for {stages, expected} <- [
              {~s/| logfmt | regexp "second=(?P<k>[^ ]+)"/, %{"k_extracted" => "one", "second" => "two"}},
              {~s/| regexp "second=(?P<k>[^ ]+)" | logfmt/, %{"k_extracted" => "two", "second" => "two"}},
              {~s/| regexp "(?P<k_extracted>three)" | logfmt/,
               %{"k_extracted" => "one", "k_extracted_extracted" => "three", "second" => "two"}}
            ] do
          assert {:ok, {:vector, [{labels, {30, 1.0}}]}} =
                   evaluate(~s/count_over_time({app="a"} #{stages} [1m])/, tenant)

          assert labels == Map.merge(%{"app" => "a", "k" => "stream"}, expected)
          assert_scan(@adapter, true)
        end
      end

      @tag :label_conversion_batch
      test "typed conversion errors can be inspected or filtered before metric admission", %{tenant: tenant} do
        records =
          for value <- ["3", "oops", "", "1e10000", nil] do
            attributes = if value, do: %{"n" => value}, else: %{}
            %Log{timestamp_ns: 10, body: "sample", resource: %{"app" => "a"}, attributes: attributes}
          end

        assert :ok = Storage.append(:logs, tenant, records)
        assert {:error, :pipeline_error} = evaluate(~s/count_over_time({app="a"} | n>0 [1m])/, tenant)
        Runtime.put_env(:pulso, Evaluator, max_log_samples: 1)

        assert {:ok, {:vector, [{%{"app" => "a", "n" => "3"}, {30, 1.0}}]}} =
                 evaluate(~s/count_over_time({app="a"} | n>0 | __error__="" [1m])/, tenant)

        Runtime.put_env(:pulso, Evaluator, max_log_samples: 3)

        assert {:ok, {:vector, errors}} =
                 evaluate(~s/count_over_time({app="a"} | n>0 | __error__!="" [1m])/, tenant)

        assert Enum.sort(errors) ==
                 Enum.sort(
                   for {value, details} <- [
                         {"oops", ~s/strconv.ParseFloat: parsing "oops": invalid syntax/},
                         {"", ~s/strconv.ParseFloat: parsing "": invalid syntax/},
                         {"1e10000", ~s/strconv.ParseFloat: parsing "1e10000": value out of range/}
                       ] do
                     {%{
                        "app" => "a",
                        "n" => value,
                        "__error__" => "LabelFilterErr",
                        "__error_details__" => details
                      }, {30, 1.0}}
                   end
                 )

        Runtime.put_env(:pulso, Evaluator, max_work: 2)

        assert {:error, :query_work_limit} =
                 evaluate(~s/count_over_time({app="a"} | n>0 | __error__="" [1m])/, tenant)
      end

      @tag :label_conversion_batch
      test "compound filters thread errors through eager AND and short-circuited OR", %{tenant: tenant} do
        assert :ok =
                 Storage.append(:logs, tenant, [
                   %Log{
                     timestamp_ns: 10,
                     body: "sample",
                     resource: %{"app" => "a"},
                     attributes: %{"n" => "5", "m" => "oops"}
                   }
                 ])

        assert {:ok, {:vector, [{labels, {30, 1.0}}]}} =
                 evaluate(~s/count_over_time({app="a"} | (n<0 and m>0) or __error__="LabelFilterErr" [1m])/, tenant)

        assert labels["__error__"] == "LabelFilterErr"
        assert labels["__error_details__"] == ~s/strconv.ParseFloat: parsing "oops": invalid syntax/

        assert {:ok, {:vector, [{labels, {30, 1.0}}]}} =
                 evaluate(~s/count_over_time({app="a"} | m>0 and n>1s | __error__!="" [1m])/, tenant)

        # Both conversions fail; the later duration failure must not replace
        # the numeric failure already recorded by the left operand.
        assert labels["__error_details__"] == ~s/strconv.ParseFloat: parsing "oops": invalid syntax/

        assert {:ok, {:vector, [{labels, {30, 1.0}}]}} =
                 evaluate(~s/count_over_time({app="a"} | n>0 or m>0 [1m])/, tenant)

        refute Map.has_key?(labels, "__error__")

        original = tenant <> "-original"

        assert :ok =
                 Storage.append(:logs, original, [
                   %Log{
                     timestamp_ns: 10,
                     body: "sample",
                     resource: %{"app" => "a"},
                     attributes: %{"n" => "oops", "__error__" => "Original", "__error_details__" => "first error"}
                   }
                 ])

        assert {:ok, {:vector, [{labels, {30, 1.0}}]}} =
                 evaluate(~s/count_over_time({app="a"} | n>0 | __error__="Original" [1m])/, original)

        assert labels["__error__"] == "Original"
        assert labels["__error_details__"] == "first error"
      end

      @tag :label_conversion_batch
      test "numeric label filters compare Go float values including nonfinite and rounded integers", %{tenant: tenant} do
        for {{value, filter, matches?}, index} <-
              Enum.with_index([
                {"9007199254740993", "n==9007199254740992", true},
                {"9007199254740993", "n>9007199254740992", false},
                {"NaN", "n!=0", true},
                {"NaN", "n==0", false},
                {"NaN", "n>0", false},
                {"+Inf", "n>0", true},
                {"-Inf", "n<0", true},
                {".5", "n==0.5", true},
                {"0x1.8p2", "n==6", true},
                {"1_000", "n==1000", true},
                {"1e-4000", "n==0", true}
              ]) do
          tenant = tenant <> "-#{index}"
          labels = %{"app" => "a", "n" => value}
          assert :ok = Storage.append(:logs, tenant, [%Log{timestamp_ns: 10, body: "sample", resource: labels}])
          expected = if matches?, do: [{labels, {30, 1.0}}], else: []
          assert evaluate(~s/count_over_time({app="a"} | #{filter} [1m])/, tenant) == {:ok, {:vector, expected}}
        end
      end

      test "fractional unwrap conversions retain seconds and byte units", %{tenant: tenant} do
        assert :ok =
                 Storage.append(:logs, tenant, [
                   %Log{timestamp_ns: 10, body: "latency=1.5s size=1.5KB", resource: %{"app" => "a"}},
                   %Log{timestamp_ns: 15, body: "noise", resource: %{"app" => "a"}},
                   %Log{timestamp_ns: 20, body: ~s(latency="" size=""), resource: %{"app" => "a"}}
                 ])

        for conversion <- ["duration", "duration_seconds"] do
          assert {:ok, {:vector, [{_, {30, 1.5}}]}} =
                   evaluate(~s/sum_over_time({app="a"} | logfmt | unwrap #{conversion}(latency) [1m])/, tenant)
        end

        assert {:ok, {:vector, [{_, {30, 1500.0}}]}} =
                 evaluate(~s/sum_over_time({app="a"} | logfmt | unwrap bytes(size) [1m])/, tenant)
      end

      @tag :error_filter_batch
      test "post-unwrap filters see conversion errors and retain only admitted observations", %{tenant: tenant} do
        records =
          for {ts, body} <- [{5, "v=2"}, {8, "noise"}, {9, ~s(v="")}, {10, "v=oops"}, {15, "v=6"}],
              do: %Log{timestamp_ns: ts, body: body, resource: %{"app" => "a"}}

        assert :ok = Storage.append(:logs, tenant, records)
        Runtime.put_env(:pulso, Evaluator, max_log_samples: 2)

        assert {:ok, {:vector, [{%{"app" => "a"}, {30, 8.0}}]}} =
                 evaluate(~s/sum_over_time({app="a"} | logfmt | unwrap v | __error__="" [1m])/, tenant)

        assert {:ok, {:vector, [{%{"app" => "a"}, {30, 6.0}}]}} =
                 evaluate(~s/sum_over_time({app="a"} | logfmt | unwrap v | __error__="" | v > 3 [1m])/, tenant)

        Runtime.put_env(:pulso, Evaluator, max_log_samples: 3)

        assert {:error, :sample_extraction_error} =
                 evaluate(~s/sum_over_time({app="a"} | logfmt | __error__="" | unwrap v [1m])/, tenant)

        Runtime.put_env(:pulso, Evaluator, max_log_samples: 1)

        assert {:error, :query_sample_limit} =
                 evaluate(~s/sum_over_time({app="a"} | logfmt | unwrap v | __error__="" [1m])/, tenant)

        Runtime.put_env(:pulso, Evaluator, max_work: 2)

        assert {:error, :query_work_limit} =
                 evaluate(~s/sum_over_time({app="a"} | logfmt | unwrap v | __error__="" [1m])/, tenant)
      end

      @tag :error_filter_batch
      test "explicit error selection preserves zero samples and error identities across grouping", %{tenant: tenant} do
        assert :ok =
                 Storage.append(:logs, tenant, [
                   %Log{timestamp_ns: 10, body: "v=oops", resource: %{"app" => "a"}},
                   %Log{timestamp_ns: 15, body: "v=5", resource: %{"app" => "a"}}
                 ])

        error_labels = %{
          "__error__" => "SampleExtractionErr",
          "__error_details__" => ~s(strconv.ParseFloat: parsing "oops": invalid syntax)
        }

        for filter <- [~s(__error__!=""), ~s(__error__="SampleExtractionErr"), ~s(__error__=~".+Err")] do
          assert {:ok, {:vector, [{^error_labels, {30, +0.0}}]}} =
                   evaluate(~s/min_over_time({app="a"} | logfmt | unwrap v | #{filter} [1m]) by ()/, tenant)
        end

        assert {:ok, {:vector, [{%{}, {30, +0.0}}]}} =
                 evaluate(~s/min(min_over_time({app="a"} | logfmt | unwrap v | __error__=~".*" [1m]))/, tenant)

        assert {:ok, {:vector, []}} =
                 evaluate(~s/absent_over_time({app="a"} | logfmt | unwrap v | __error__!="" [1m])/, tenant)

        # Reading details does not authorize retaining errors.
        assert {:error, :sample_extraction_error} =
                 evaluate(~s/min_over_time({app="a"} | logfmt | unwrap v | __error_details__!="" [1m])/, tenant)
      end

      @tag :error_filter_batch
      test "incoming errors require query authorization, not a caller-provided preservation label", %{tenant: tenant} do
        errors = %{"__error__" => "JSONParserErr", "__error_details__" => "incoming"}

        assert :ok =
                 Storage.append(:logs, tenant, [
                   %Log{
                     timestamp_ns: 10,
                     body: "msg",
                     resource: %{"app" => "a"},
                     attributes: Map.put(errors, "v", "5")
                   },
                   %Log{timestamp_ns: 15, body: "msg", resource: %{"app" => "a"}, attributes: %{"v" => "7"}}
                 ])

        assert {:ok, {:vector, [{%{"app" => "a"}, {30, 7.0}}]}} =
                 evaluate(~s/sum_over_time({app="a"} | unwrap v | __error__="" [1m])/, tenant)

        assert {:ok, {:vector, [{^errors, {30, 5.0}}]}} =
                 evaluate(~s/sum_over_time({app="a"} | unwrap v | __error__!="" [1m]) by ()/, tenant)

        assert {:error, :pipeline_error} = evaluate(~s/sum_over_time({app="a"} | unwrap v [1m])/, tenant)

        spoofed = tenant <> "-spoofed"

        assert :ok =
                 Storage.append(:logs, spoofed, [
                   %Log{
                     timestamp_ns: 10,
                     body: "msg",
                     resource: %{"app" => "a"},
                     attributes: Map.merge(errors, %{"v" => "5", "__preserve_error__" => "true"})
                   }
                 ])

        assert {:error, :pipeline_error} = evaluate(~s/sum_over_time({app="a"} | unwrap v [1m])/, spoofed)
      end

      @tag :error_filter_batch
      test "duration and byte unwraps can remove failed conversions after extraction", %{tenant: tenant} do
        assert :ok =
                 Storage.append(:logs, tenant, [
                   %Log{timestamp_ns: 10, body: "latency=oops size=oops", resource: %{"app" => "a"}},
                   %Log{timestamp_ns: 15, body: "latency=1.5s size=1.5KB", resource: %{"app" => "a"}}
                 ])

        for {conversion, label, expected} <- [
              {"duration", "latency", 1.5},
              {"duration_seconds", "latency", 1.5},
              {"bytes", "size", 1500.0}
            ] do
          assert {:ok, {:vector, [{%{"app" => "a"}, {30, ^expected}}]}} =
                   evaluate(
                     ~s/sum_over_time({app="a"} | logfmt | unwrap #{conversion}(#{label}) | __error__="" [1m])/,
                     tenant
                   )
        end
      end

      test "unwrap operators skip absent and empty fields but reject malformed observations", %{tenant: tenant} do
        records =
          for {ts, body} <- [{5, "v=2"}, {8, "noise"}, {9, ~s(v="")}, {15, "v=6"}],
              do: %Log{timestamp_ns: ts, body: body, resource: %{"app" => "a"}}

        assert :ok = Storage.append(:logs, tenant, records)

        for {op, param, expected, native?} <- [
              {"sum_over_time", "", 8.0, true},
              {"avg_over_time", "", 4.0, true},
              {"min_over_time", "", 2.0, true},
              {"max_over_time", "", 6.0, true},
              {"stddev_over_time", "", 2.0, true},
              {"stdvar_over_time", "", 4.0, true},
              {"quantile_over_time", "0.75, ", 5.0, true},
              {"rate", "", 2 / 15, true},
              {"first_over_time", "", 2.0, false},
              {"last_over_time", "", 6.0, false},
              {"absent_over_time", "", nil, false}
            ] do
          query = ~s/#{op}(#{param}{app="a"} | logfmt | unwrap v [1m])/
          result = if expected, do: [{%{"app" => "a"}, {30, expected}}], else: []
          assert evaluate(query, tenant) == {:ok, {:vector, result}}
          assert_scan(@adapter, native?)

          for time <- [8, 9] do
            query = ~s/#{op}(#{param}{app="a"} | logfmt | unwrap v [1ms])/
            result = if op == "absent_over_time", do: [{%{"app" => "a"}, {time, 1.0}}], else: []
            assert evaluate(query, tenant, %{end_ts_ns: time, range_ns: 1}) == {:ok, {:vector, result}}
            assert_scan(@adapter, native?)
          end

          malformed = tenant <> "-#{op}"
          record = %Log{timestamp_ns: 10, body: "v=bad", resource: %{"app" => "a"}}
          assert :ok = Storage.append(:logs, malformed, records ++ [record])

          assert {:error, :sample_extraction_error} =
                   evaluate(~s/#{op}(#{param}{app="a"} | logfmt | unwrap v [1m])/, malformed)

          assert_scan(@adapter, native?)
        end
      end

      test "skipped unwrap fields spend work but not retained sample budget", %{tenant: tenant} do
        records =
          for {ts, body} <- [{5, "v=2"}, {8, "noise"}, {9, ~s(v="")}, {15, "v=6"}],
              do: %Log{timestamp_ns: ts, body: body, resource: %{"app" => "a"}}

        assert :ok = Storage.append(:logs, tenant, records)
        query = ~s/sum_over_time({app="a"} | logfmt | unwrap v [1m])/
        Runtime.put_env(:pulso, Evaluator, max_log_samples: 2)
        assert {:ok, {:vector, [{%{"app" => "a"}, {30, 8.0}}]}} = evaluate(query, tenant)
        assert_scan(@adapter, true)

        for {limits, reason} <- [{[max_log_samples: 1], :query_sample_limit}, {[max_work: 2], :query_work_limit}] do
          Runtime.put_env(:pulso, Evaluator, limits)
          assert {:error, ^reason} = evaluate(query, tenant)
        end

        Runtime.put_env(:pulso, Evaluator, max_log_samples: 0)

        assert {:ok, {:vector, []}} =
                 evaluate(~s/sum_over_time({app="a"} | logfmt | unwrap v [1ms])/, tenant, %{end_ts_ns: 9, range_ns: 2})
      end

      test "counter windows and extrapolation thresholds retain nanosecond boundaries", %{tenant: tenant} do
        second = 1_000_000_000

        for {app, samples} <- [
              {"edge",
               [
                 # Outliers expose accidentally closed lower/open upper bounds.
                 {10 * second, 999},
                 {10 * second + 1, 10},
                 {15 * second, 60},
                 {20 * second, 70},
                 {20 * second + 1, 999}
               ]},
              {"threshold", [{22 * second, 100}, {32 * second, 120}]}
            ] do
          records =
            for {ts, value} <- samples, do: %Log{timestamp_ns: ts, body: "v=#{value}", resource: %{"app" => app}}

          assert :ok = Storage.append(:logs, tenant, records)
        end

        for {app, range_ns, time, expected} <- [
              {"edge", 10 * second, 20 * second, 60 / (10 - 1.0e-9)},
              # One nanosecond earlier admits 999 at the old lower edge,
              # excludes 70, and caps the now-distant right edge to 1.25s.
              {"edge", 10 * second, 20 * second - 1, 60 * (6.25 + 1.0e-9) / 50},
              {"threshold", 21 * second, 32 * second, 10 / 7},
              {"threshold", 21 * second - 1, 32 * second, 2.0}
            ] do
          assert {:ok, {:vector, [{%{"app" => ^app}, {^time, value}}]}} =
                   evaluate(~s/rate_counter({app="#{app}"} | logfmt | unwrap v [1ms])/, tenant, %{
                     end_ts_ns: time,
                     range_ns: range_ns
                   })

          assert_in_delta value, expected, 1.0e-12
        end
      end

      test "counter resets at equal timestamps use record order, not numeric or arrival order", %{tenant: tenant} do
        records = [
          %Log{timestamp_ns: 15_000_000_000, observed_timestamp_ns: 1, body: "v=80", resource: %{"app" => "a"}},
          %Log{timestamp_ns: 15_000_000_000, observed_timestamp_ns: 2, body: "v=10", resource: %{"app" => "a"}},
          %Log{timestamp_ns: 20_000_000_000, body: "v=100", resource: %{"app" => "a"}}
        ]

        for {input, suffix} <- [{records, "-forward"}, {Enum.reverse(records), "-reverse"}] do
          tenant = tenant <> suffix
          assert :ok = Storage.append(:logs, tenant, input)

          assert {:ok, {:vector, [{%{"app" => "a"}, {20_000_000_000, 12.5}}]}} =
                   evaluate(~s/rate_counter({app="a"} | logfmt | unwrap v [10s])/, tenant, %{end_ts_ns: 20_000_000_000})
        end
      end

      test "counter special values and zero elapsed time do not crash arithmetic", %{tenant: tenant} do
        for {values, times, expected} <- [
              {["NaN"], [10], 0.0},
              {["NaN", "1"], [10, 20], :nan},
              {["1", "+Inf"], [10, 20], :infinity},
              {["+Inf", "1"], [10, 20], :nan},
              {["1", "-Inf"], [10, 20], :negative_infinity},
              {["-Inf", "1"], [10, 20], :infinity},
              {["40", "100"], [20, 20], :nan}
            ] do
          app = Enum.join(values, ",")

          records =
            for {value, ts} <- Enum.zip(values, times),
                do: %Log{timestamp_ns: ts, body: "v=#{value}", resource: %{"app" => app}}

          assert :ok = Storage.append(:logs, tenant, records)

          assert {:ok, {:vector, [{%{"app" => ^app}, {30, ^expected}}]}} =
                   evaluate(~s/rate_counter({app=#{Pulso.JSON.encode!(app)}} | logfmt | unwrap v [1m])/, tenant)
        end
      end

      test "first and last reject invalid middle samples only inside evaluated windows", %{tenant: tenant} do
        records =
          for {ts, value} <- [{10, "1"}, {15, "oops"}, {20, "3"}],
              do: %Log{timestamp_ns: ts, body: "v=#{value}", resource: %{"app" => "a"}}

        assert :ok = Storage.append(:logs, tenant, records)

        for op <- ["first_over_time", "last_over_time"] do
          query = ~s/#{op}({app="a"} | logfmt | unwrap v [1m])/
          assert {:error, :sample_extraction_error} = evaluate(query, tenant)

          assert {:error, :sample_extraction_error} =
                   evaluate(query, tenant, %{start_ts_ns: 20, end_ts_ns: 30, step_ns: 10})

          # The invalid row is fetched between sparse windows but belongs to neither.
          assert {:ok, {:matrix, [{%{"app" => "a"}, [{10, 1.0}, {20, 3.0}]}]}} =
                   evaluate(~s/#{op}({app="a"} | logfmt | unwrap v [1ms])/, tenant, %{
                     start_ts_ns: 10,
                     end_ts_ns: 20,
                     step_ns: 10,
                     range_ns: 1
                   })
        end
      end

      test "grouped equal-time endpoints follow explicit body and observed-time order", %{tenant: tenant} do
        for {app, high_observed, low_observed, first_value, last_value} <- [
              {"body", nil, nil, 9.0, 2.0},
              {"observed", 100, 1, 1.0, 8.0}
            ] do
          records =
            for {timestamp, pod, value, observed} <- [
                  {5, "p1", 1, high_observed},
                  {5, "p2", 9, low_observed},
                  {25, "p1", 2, high_observed},
                  {25, "p2", 8, low_observed}
                ],
                do: %Log{
                  timestamp_ns: timestamp,
                  observed_timestamp_ns: observed,
                  body: "v=#{value}",
                  resource: %{"app" => app, "pod" => pod}
                }

          assert :ok = Storage.append(:logs, tenant, records)
          # Expected constants do not call the sort helper or comparison model.
          # Body desc chooses 9 first; newer observed time instead chooses 1.
          for {op, value} <- [{"first_over_time", first_value}, {"last_over_time", last_value}] do
            assert {:ok, {:vector, [{%{"app" => ^app}, {30, ^value}}]}} =
                     evaluate(~s/#{op}({app="#{app}"} | logfmt | unwrap v [1m]) by (app)/, tenant)

            assert_scan(@adapter, false)
          end
        end
      end

      test "packed native error samples reject selected windows but not fetched gaps", %{tenant: tenant} do
        records =
          for {timestamp, value} <- [{10, "1"}, {15, "oops"}, {20, "3"}],
              do: %Log{timestamp_ns: timestamp, body: "v=#{value}", resource: %{"app" => "a"}}

        assert :ok = Storage.append(:logs, tenant, records)

        assert {:ok, {:matrix, [{%{"app" => "a"}, [{10, 1.0}, {20, 3.0}]}]}} =
                 evaluate(~s/sum_over_time({app="a"} | logfmt | unwrap v [1ms])/, tenant, %{
                   start_ts_ns: 10,
                   end_ts_ns: 20,
                   step_ns: 10,
                   range_ns: 1
                 })

        assert_scan(@adapter, true)

        assert {:error, :sample_extraction_error} =
                 evaluate(~s/sum_over_time({app="a"} | logfmt | unwrap v [1ms])/, tenant, %{end_ts_ns: 20, range_ns: 10})

        assert_scan(@adapter, true)
      end

      test "unwrap validation uses an open lower and closed upper window boundary", %{tenant: tenant} do
        records =
          for {timestamp, value} <- [{0, "oops"}, {5, "1"}, {10, "3"}, {11, "oops"}],
              do: %Log{timestamp_ns: timestamp, body: "v=#{value}", resource: %{"app" => "a"}}

        assert :ok = Storage.append(:logs, tenant, records)

        for {op, value} <- [{"first_over_time", 1.0}, {"last_over_time", 3.0}] do
          assert {:ok, {:vector, [{%{"app" => "a"}, {10, ^value}}]}} =
                   evaluate(~s/#{op}({app="a"} | logfmt | unwrap v [1ms])/, tenant, %{end_ts_ns: 10, range_ns: 10})
        end

        assert :ok = Storage.append(:logs, tenant, [%Log{timestamp_ns: 10, body: "v=oops", resource: %{"app" => "a"}}])

        for op <- ["first_over_time", "last_over_time"] do
          assert {:error, :sample_extraction_error} =
                   evaluate(~s/#{op}({app="a"} | logfmt | unwrap v [1ms])/, tenant, %{end_ts_ns: 10, range_ns: 10})
        end
      end

      test "duration and byte filters accept ordinary fractional producer values", %{tenant: tenant} do
        for {label, value, filter} <- [
              {"latency", "1.5s", "latency > 1s"},
              {"latency", "1h2m3.5s", "latency > 1h"},
              {"size", "1.5KB", "size >= 1KB"}
            ] do
          app = value

          assert :ok =
                   Storage.append(:logs, tenant, [
                     %Log{timestamp_ns: 10, body: "#{label}=#{value}", resource: %{"app" => app}}
                   ])

          assert {:ok, {:vector, [{_, {30, 1.0}}]}} =
                   evaluate(~s/count_over_time({app="#{app}"} | logfmt | #{filter} [1m])/, tenant)
        end
      end
    end
  end

  test "first and last validate tied endpoints and preserve successful endpoint selection" do
    records =
      for {ts, body} <- [{10, "v=1"}, {20, "v=3"}], do: %Log{timestamp_ns: ts, body: body, resource: %{"app" => "a"}}

    assert :ok = Storage.append(:logs, "endpoints", records)

    for {op, expected} <- [{"first_over_time", 1.0}, {"last_over_time", 3.0}] do
      assert {:ok, {:vector, [{_, {30, ^expected}}]}} =
               evaluate(~s/#{op}({app="a"} | logfmt | unwrap v [1m])/, "endpoints")
    end

    assert :ok = Storage.append(:logs, "endpoints", [%Log{timestamp_ns: 20, body: "v=oops", resource: %{"app" => "a"}}])

    for op <- ["first_over_time", "last_over_time"] do
      assert {:error, :sample_extraction_error} = evaluate(~s/#{op}({app="a"} | logfmt | unwrap v [1m])/, "endpoints")
    end
  end

  test "collision provenance survives drops and repeated parsers without recursive suffix invention" do
    entry =
      Entry.from_record(%Log{
        timestamp_ns: 10,
        body: "pod=new x=one",
        resource: %{"app" => "a", "pod" => "old", "pod_extracted" => "existing"}
      })

    assert {:keep, result} = apply_pipeline(~s/| drop pod | logfmt | logfmt/, entry)
    assert result.labels["pod"] == "new"
    assert result.labels["pod_extracted"] == "existing"
    refute Map.has_key?(result.labels, "pod_extracted_extracted")
    assert result.labels["x"] == "one"
    refute Map.has_key?(result.labels, "x_extracted")
  end

  test "self-renames delete labels and missing rename sources do not invent empty labels" do
    entry = %Entry{timestamp_ns: 10, line: "", labels: %{"app" => "a", "x" => "kept"}}
    assert {:keep, result} = apply_pipeline("| label_format x=x, target=missing", entry)
    assert result.labels == %{"app" => "a"}
  end
end
