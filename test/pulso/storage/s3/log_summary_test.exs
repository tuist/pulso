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

  test "complete error-label sets persist independently and unknown summaries never prune" do
    records = [
      %Log{resource: %{"error_source" => "sentry", "error_project" => "42"}},
      %Log{resource: %{}}
    ]

    summary = segment(records)
    assert summary.log_labels == %{"error_source" => ["", "sentry"], "error_project" => ["", "42"]}
    manifest = Manifest.merge(Manifest.new(), [summary])
    assert {:ok, ^manifest} = manifest |> Manifest.encode() |> IO.iodata_to_binary() |> Manifest.decode()
    refute Segment.matches_log_service?(summary, matchers: [{"error_project", :eq, "43"}])
    assert Segment.matches_log_service?(summary, matchers: [{"error_source", :eq, ""}])

    summary = segment(for i <- 1..33, do: %Log{resource: %{"error_source" => "sentry", "error_project" => "#{i}"}})
    assert summary.log_labels == %{"error_source" => ["sentry"]}
    assert Segment.matches_log_service?(summary, matchers: [{"error_project", :eq, "unknown"}])
    refute Segment.matches_log_service?(summary, matchers: [{"error_source", :eq, "other"}])

    for labels <- [
          nil,
          %{},
          %{"error_source" => []},
          %{"error_source" => [1]},
          %{"error_project" => [String.duplicate("x", 257)]}
        ] do
      assert {:ok, summary} = Segment.from_wire(%{"k" => "x", "mn" => 0, "mx" => 100, "el" => labels})
      assert summary.log_labels == nil
      assert Segment.matches_log_service?(summary, matchers: [{"error_source", :eq, "sentry"}])
    end
  end

  test "JSON-normalized atom label values cannot be pruned as absent" do
    records = [%Log{timestamp_ns: 1, resource: %{"error_source" => :sentry, "error_project" => "42"}}]
    {:ok, payload, _, _} = S3.encode_segment(:logs, records)
    opts = [matchers: [{"error_source", :eq, "sentry"}, {"error_project", :eq, "42"}]]
    assert {:ok, [%Log{timestamp_ns: 1}]} = S3.decode_segment(:logs, payload, nil, nil, opts)
    assert Segment.matches_log_service?(segment(records), opts)

    # Unknown fields do not discard independently complete project sets.
    assert segment(records).log_labels == %{"error_project" => ["42"]}
  end

  test "bounded native log decoding counts only matching rows and rejects overflow rather than truncating" do
    records =
      for i <- 1..4,
          do: %Log{timestamp_ns: i, service: "api", resource: %{"error_project" => "#{rem(i, 2)}"}, body: "row #{i}"}

    {:ok, payload, _, _} = S3.encode_segment(:logs, records)
    opts = [max_records: 2, matchers: [{"error_project", :eq, "1"}], line_filters: [{:contains, "row"}]]
    assert {:ok, found} = S3.decode_segment(:logs, payload, nil, nil, opts)
    assert Enum.map(found, & &1.timestamp_ns) == [1, 3]

    assert {:error, :query_sample_limit} =
             S3.decode_segment(:logs, payload, nil, nil, Keyword.put(opts, :max_records, 1))

    assert {:ok, []} = S3.decode_segment(:logs, payload, 4, nil, Keyword.put(opts, :max_records, 0))
    assert {:error, :query_sample_limit} = S3.decode_segment(:logs, payload, 3, 3, Keyword.put(opts, :max_records, 0))
  end

  test "randomized pruning never excludes a segment whose native decoder returns matching rows" do
    :rand.seed(:exsss, {123, 456, 789})

    for iteration <- 1..50 do
      services = if rem(iteration, 2) == 0, do: ["api", "worker", "other"], else: [nil, "", "api", "worker", "other"]

      resources = [
        %{},
        %{"error_source" => "sentry", "error_project" => "42"},
        %{"error_source" => "sentry", "error_project" => "43"}
      ]

      resources =
        cond do
          rem(iteration, 3) == 0 -> [%{error_source: "sentry", error_project: "44"}]
          rem(iteration, 5) == 0 -> [%{"error_source" => false, "error_project" => 42}]
          rem(iteration, 7) == 0 -> [%{"error_source" => :sentry, "error_project" => "44"}]
          true -> resources
        end

      records =
        for i <- 1..40,
            do: %Log{
              timestamp_ns: i,
              service: Enum.random(services),
              resource:
                Map.merge(
                  %{"service" => "fallback", "service_name" => "alias"},
                  Enum.random(resources)
                )
            }

      summary = segment(records)
      {:ok, payload, _, _} = S3.encode_segment(:logs, records)

      for name <- ["service", "service_name", "error_source", "error_project"],
          value <- ["", "api", "worker", "absent", "fallback", "alias", "sentry", "42", "43", "44"] do
        opts = [matchers: [{name, :eq, value}]]
        {:ok, found} = S3.decode_segment(:logs, payload, nil, nil, opts)
        assert found == [] or Segment.matches_log_service?(summary, opts)
      end
    end
  end
end
