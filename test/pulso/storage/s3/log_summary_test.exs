defmodule Pulso.Storage.S3.LogSummaryTest do
  use Pulso.Test.Case, async: true

  alias Pulso.Record.Log
  alias Pulso.Storage.S3
  alias Pulso.Storage.S3.Manifest
  alias Pulso.Storage.S3.Manifest.Segment

  defp segment(records), do: Segment.build("logs.parquet", 0, 100, length(records)) |> Segment.summarize_logs(records)

  test "complete service sets survive manifest persistence and match both promoted aliases" do
    summary = segment(for service <- ["api", "worker", "api"], do: %Log{service: service})
    assert summary.log_services == ["api", "worker"]
    manifest = Manifest.merge(Manifest.new(), [summary])
    assert {:ok, decoded} = manifest |> Manifest.encode() |> IO.iodata_to_binary() |> Manifest.decode()
    assert decoded == manifest

    assert Segment.matches_log_service?(summary, service: "worker")
    refute Segment.matches_log_service?(summary, service: "missing")

    for name <- ["service", "service_name"] do
      assert Segment.matches_log_service?(summary, matchers: [{name, :eq, "api"}])
      refute Segment.matches_log_service?(summary, matchers: [{name, :eq, "absent"}])
      assert Segment.matches_log_service?(summary, matchers: [{name, :neq, "api"}])
      assert Segment.matches_log_service?(summary, matchers: [{name, :re, "a.*"}])
    end

    refute Segment.matches_log_service?(summary, service: "api", matchers: [{"service_name", :eq, "worker"}])
    refute Segment.matches_log_service?(summary, matchers: [{"service", :eq, "api"}, {"service_name", :eq, "worker"}])
  end

  test "null or empty promoted fields, excessive cardinality and long names remain unknown" do
    for service <- [nil, false, "", String.duplicate("s", 257)] do
      summary = segment([%Log{service: "api"}, %Log{service: service, resource: %{"service" => "fallback"}}])
      assert summary.log_services == nil
      assert Segment.matches_log_service?(summary, matchers: [{"service", :eq, "fallback"}])
    end

    assert segment(for i <- 1..128, do: %Log{service: "s#{i}"}).log_services != nil
    assert segment(for i <- 1..129, do: %Log{service: "s#{i}"}).log_services == nil
  end

  test "old or malformed optional summaries must never prune" do
    for services <- [nil, [], "api", [nil], [""], [123], [String.duplicate("s", 257)], Enum.map(1..129, &"s#{&1}")] do
      assert {:ok, summary} = Segment.from_wire(%{"k" => "x", "mn" => 0, "mx" => 100, "ls" => services})
      assert summary.log_services == nil
      assert Segment.matches_log_service?(summary, service: "anything")
    end
  end

  test "randomized pruning never excludes a segment whose native decoder returns matching rows" do
    :rand.seed(:exsss, {123, 456, 789})

    for iteration <- 1..50 do
      services = if rem(iteration, 2) == 0, do: ["api", "worker", "other"], else: [nil, "", "api", "worker", "other"]

      records =
        for i <- 1..40,
            do: %Log{
              timestamp_ns: i,
              service: Enum.random(services),
              resource: %{"service" => "fallback", "service_name" => "alias"}
            }

      summary = segment(records)
      {:ok, payload, _, _} = S3.encode_segment(:logs, records)

      for name <- ["service", "service_name"], value <- ["", "api", "worker", "absent", "fallback", "alias"] do
        opts = [matchers: [{name, :eq, value}]]
        {:ok, found} = S3.decode_segment(:logs, payload, nil, nil, opts)
        assert found == [] or Segment.matches_log_service?(summary, opts)
      end
    end
  end
end
