defmodule Pulso.LogQL.PipelineCorrectnessTest do
  use Pulso.Test.Case, async: true

  alias Pulso.LogQL.{Entry, MetricEval, Parser, Pipeline}
  alias Pulso.Record.Log
  alias Pulso.Runtime
  alias Pulso.Storage
  alias Pulso.Storage.Memory
  alias Pulso.Test.NativeQueryStorage

  defp evaluate(query, tenant, opts \\ %{end_ts_ns: 30}) do
    Process.delete(:promql_budget)
    Process.delete(:promql_deadline_ms)
    {:ok, ast} = Parser.parse(query)
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

      test "structured attributes remain overridable but promoted stream labels are protected", %{tenant: tenant} do
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
        assert labels["pod"] == "body"
        assert labels["x"] == "new"
        refute Map.has_key?(labels, "pod_extracted")
        refute Map.has_key?(labels, "x_extracted")
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

      test "fractional unwrap conversions retain seconds and byte units", %{tenant: tenant} do
        assert :ok =
                 Storage.append(:logs, tenant, [
                   %Log{timestamp_ns: 10, body: "latency=1.5s size=1.5KB", resource: %{"app" => "a"}}
                 ])

        for conversion <- ["duration", "duration_seconds"] do
          assert {:ok, {:vector, [{_, {30, 1.5}}]}} =
                   evaluate(~s/sum_over_time({app="a"} | logfmt | unwrap #{conversion}(latency) [1m])/, tenant)
        end

        assert {:ok, {:vector, [{_, {30, 1500.0}}]}} =
                 evaluate(~s/sum_over_time({app="a"} | logfmt | unwrap bytes(size) [1m])/, tenant)
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
                   evaluate(~s/#{op}({app="a"} | logfmt | unwrap v [1ns])/, tenant, %{
                     start_ts_ns: 10,
                     end_ts_ns: 20,
                     step_ns: 10
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
                 evaluate(~s/sum_over_time({app="a"} | logfmt | unwrap v [1ns])/, tenant, %{
                   start_ts_ns: 10,
                   end_ts_ns: 20,
                   step_ns: 10
                 })

        assert_scan(@adapter, true)

        assert {:error, :sample_extraction_error} =
                 evaluate(~s/sum_over_time({app="a"} | logfmt | unwrap v [10ns])/, tenant, %{end_ts_ns: 20})

        assert_scan(@adapter, true)
      end

      test "unwrap validation uses an open lower and closed upper window boundary", %{tenant: tenant} do
        records =
          for {timestamp, value} <- [{0, "oops"}, {5, "1"}, {10, "3"}, {11, "oops"}],
              do: %Log{timestamp_ns: timestamp, body: "v=#{value}", resource: %{"app" => "a"}}

        assert :ok = Storage.append(:logs, tenant, records)

        for {op, value} <- [{"first_over_time", 1.0}, {"last_over_time", 3.0}] do
          assert {:ok, {:vector, [{%{"app" => "a"}, {10, ^value}}]}} =
                   evaluate(~s/#{op}({app="a"} | logfmt | unwrap v [10ns])/, tenant, %{end_ts_ns: 10})
        end

        assert :ok = Storage.append(:logs, tenant, [%Log{timestamp_ns: 10, body: "v=oops", resource: %{"app" => "a"}}])

        for op <- ["first_over_time", "last_over_time"] do
          assert {:error, :sample_extraction_error} =
                   evaluate(~s/#{op}({app="a"} | logfmt | unwrap v [10ns])/, tenant, %{end_ts_ns: 10})
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
    refute Map.has_key?(result.labels, "pod")
    assert result.labels["pod_extracted"] == "new"
    refute Map.has_key?(result.labels, "pod_extracted_extracted")
    assert result.labels["x"] == "one"
    refute Map.has_key?(result.labels, "x_extracted")
  end

  test "self-renames preserve labels and missing rename sources do not invent empty labels" do
    entry = %Entry{timestamp_ns: 10, line: "", labels: %{"app" => "a", "x" => "kept"}}
    assert {:keep, result} = apply_pipeline("| label_format x=x, target=missing", entry)
    assert result.labels == entry.labels
  end
end
