# SPDX-License-Identifier: AGPL-3.0-only
# Fixture-driven ports of grafana/tempo TestScalarMath, queryRangeTestCases and
# TestMetricsMathExpression{Errors}. Pin/provenance: tempo_integration/README.md.
defmodule Pulso.Traces.MetricExpressionConformanceTest do
  use Pulso.Test.Case, async: true

  alias Pulso.Codec.NIF
  alias Pulso.OTLP.Traces
  alias Pulso.Storage
  alias Pulso.Traces.MetricExpression
  alias Pulso.Traces.Metrics

  @moduletag :trace_math_batch
  @fixture "../../fixtures/traces/tempo_integration/metrics-math.json"
           |> Path.expand(__DIR__)
           |> File.read!()
           |> Pulso.JSON.decode!()

  test "all upstream metric grammar rows accept or reject through the query boundary" do
    for row <- @fixture["grammar"]["positive"], do: assert({:ok, _} = Metrics.parse(row["query"]), row["query"])
    for row <- @fixture["grammar"]["negative"], do: assert({:error, _} = Metrics.parse(row["query"]), row["query"])
  end

  test "complete scalar engine oracle replays through the memory/native scan" do
    scalar = @fixture["scalar"]
    tenant = "tempo-scalar-#{System.unique_integer([:positive])}"
    spans = Enum.with_index(scalar["spans"], 1) |> Enum.map(fn {span, i} -> input_span(span, i) end)
    assert {:ok, records, 0} = Traces.decode(%{"resourceSpans" => [%{"scopeSpans" => [%{"spans" => spans}]}]})
    assert :ok = Storage.append(:traces, tenant, records)

    for row <- scalar["cases"] do
      assert {:ok, json} = Metrics.query(tenant, row["query"], {1, 3_000_000_000, 1_000_000_000}, false)

      expected =
        Enum.map(row["expected"], fn {value, samples} ->
          %{
            "labels" => [%{"key" => "span.foo", "value" => %{"stringValue" => value}}],
            "samples" =>
              Enum.zip_with(scalar["timestamps_ms"], samples, fn ts, v ->
                %{"timestampMs" => to_string(ts), "value" => wire(v)}
              end)
              |> Enum.reject(&(&1["value"] == "NaN"))
          }
        end)
        |> Enum.reject(&(&1["samples"] == []))

      assert_series(Pulso.JSON.decode!(json)["series"], expected, row["query"])
    end
  end

  test "all storage arithmetic rows preserve original scan end and duplicate-source aggregation" do
    tenant = "tempo-math-storage-#{System.unique_integer([:positive])}"
    storage = @fixture["storage"]

    resources =
      for {trace, i} <- Enum.with_index(storage["traces"], 1) do
        span = trace["spans"] |> hd() |> Map.put("trace_id", trace["trace_id"])

        %{
          "resource" => %{"attributes" => attributes(trace["resource_attributes"])},
          "scopeSpans" => [%{"spans" => [input_span(span, i)]}]
        }
      end

    assert {:ok, records, 0} = Traces.decode(%{"resourceSpans" => resources})
    assert length(records) == 100
    # Two additive sources report the same L1 observations in the original
    # engine harness. Repeated observations preserve count/rate doubling and
    # max invariance before the final arithmetic, without changing expected L3.
    assert :ok = Storage.append(:traces, tenant, records)
    assert :ok = Storage.append(:traces, tenant, records)
    mapper = {0, 60_000_000_000, 15_000_000_000, false}

    for row <- storage["cases"] do
      assert {:ok, parsed} = Metrics.parse(row["query"])

      leaves =
        for leaf <- parsed.leaves do
          assert {:ok, resource} =
                   NIF.trace_metrics_new(
                     leaf.program,
                     leaf.root_only,
                     leaf.op,
                     leaf.quantiles,
                     leaf.value,
                     leaf.by,
                     mapper,
                     leaf.second
                   )

          resource
        end

      assert {:ok, batch} = NIF.trace_metrics_batch_new(leaves)
      # The mapper's last timestamp is 60s, but the original engine request
      # selects only starts through 50s. HTTP alignment is intentionally absent.
      assert {:ok, 100} = Storage.trace_metrics_scan(tenant, [start_ts: 1, end_ts: 50_000_000_000], batch)
      assert {:ok, packed} = NIF.trace_metrics_batch_finish(batch)
      assert {:ok, json} = MetricExpression.finish(parsed, packed, mapper)

      expected =
        for series <- row["expected_l3"] || [] do
          %{
            "labels" => Enum.map(series["labels"], &%{"key" => &1["name"], "value" => %{"stringValue" => &1["value"]}}),
            "samples" =>
              Enum.map(
                series["samples"],
                &%{"timestampMs" => to_string(&1["timestamp_ms"]), "value" => wire(&1["value"])}
              )
          }
        end

      assert_series(Pulso.JSON.decode!(json)["series"], expected, row["query"])
    end
  end

  defp input_span(span, i) do
    %{
      "traceId" => Map.get(span, "trace_id", i |> Integer.to_string(16) |> String.pad_leading(32, "0")),
      "spanId" => Map.get(span, "span_id", i |> Integer.to_string(16) |> String.pad_leading(16, "0")),
      "name" => Map.get(span, "name", "span"),
      "startTimeUnixNano" => to_string(span["start_time_unix_nano"]),
      "endTimeUnixNano" => to_string(span["start_time_unix_nano"] + span["duration_nano"]),
      "attributes" => attributes(Map.get(span, "span_attributes", []))
    }
  end

  defp attributes(attrs), do: Enum.map(attrs, &%{"key" => &1["name"], "value" => %{"stringValue" => &1["value"]}})

  defp assert_series(actual, expected, query) do
    labels = &Enum.sort_by(&1["labels"], fn label -> label["key"] end)
    assert length(actual) == length(expected), query
    actual = Map.new(actual, &{labels.(&1), &1["samples"]})
    expected = Map.new(expected, &{labels.(&1), &1["samples"]})
    assert Map.keys(actual) |> Enum.sort() == Map.keys(expected) |> Enum.sort(), query

    for {key, points} <- expected do
      assert Enum.map(actual[key], & &1["timestampMs"]) == Enum.map(points, & &1["timestampMs"]), query
    end

    for {key, points} <- expected, {a, e} <- Enum.zip(actual[key], points) do
      if is_number(e["value"]),
        do: assert_in_delta(a["value"], e["value"], max(abs(e["value"]) * 1.0e-12, 1.0e-12), query),
        else: assert(a["value"] == e["value"], query)
    end
  end

  defp wire("+Inf"), do: "Infinity"
  defp wire("-Inf"), do: "-Infinity"
  defp wire(value), do: value
end
