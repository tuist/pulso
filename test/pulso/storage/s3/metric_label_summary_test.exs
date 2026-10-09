defmodule Pulso.Storage.S3.MetricLabelSummaryTest do
  use Pulso.Test.Case, async: true

  alias Pulso.Record.MetricSample
  alias Pulso.Storage.S3
  alias Pulso.Storage.S3.Manifest
  alias Pulso.Storage.S3.Manifest.Segment
  alias Pulso.Storage.S3.MetricLabelSummary

  defp sample(labels, ts \\ 1), do: %MetricSample{timestamp_ns: ts, value: ts / 1, labels: labels}

  test "complete bounded sets include missing labels and omit high-cardinality values independently" do
    records = for i <- 1..100, do: sample(%{"job" => "api", "instance" => "node-#{i}", "__name__" => "up"}, i)
    assert MetricLabelSummary.build(records) == %{"job" => ["api"]}
    assert MetricLabelSummary.build([sample(%{"job" => "api"}), sample(%{})]) == %{"job" => ["", "api"]}
    assert MetricLabelSummary.build([sample(%{}), sample(%{"later" => "x"})]) == nil
    assert MetricLabelSummary.matches?(%{"job" => ["api"]}, [{"instance", :eq, "unknown"}])
    refute MetricLabelSummary.matches?(%{"job" => ["api"]}, [{"job", :eq, "absent"}])
    assert MetricLabelSummary.matches?(%{"job" => ["api"]}, [{"job", :re, "absent"}])
  end

  test "long values, invalid UTF-8, malformed summaries and excessive sets remain unknown" do
    assert MetricLabelSummary.build([sample(%{"job" => <<255>>})]) == nil
    assert MetricLabelSummary.build([sample(%{"job" => "api"}), sample(%{<<255>> => "x", "job" => "api"})]) == nil
    assert MetricLabelSummary.build([sample(%{"job" => String.duplicate("x", 257)})]) == nil

    for wire <- [
          nil,
          %{},
          %{"job" => []},
          %{"job" => "api"},
          %{"job" => [123]},
          %{"job" => [<<255>>]},
          %{"job" => Enum.map(1..33, &"v#{&1}")}
        ] do
      assert MetricLabelSummary.parse(wire) == nil
      assert MetricLabelSummary.matches?(MetricLabelSummary.parse(wire), [{"job", :eq, "api"}])
    end
  end

  test "shared dictionaries survive persistence and malformed references never prune" do
    segment = Segment.build("x", 1, 1, 1) |> Segment.summarize_metrics([sample(%{"job" => "api"})])
    manifest = Manifest.merge(Manifest.new(), [%{segment | key: "x"}, %{segment | key: "y"}])
    encoded = manifest |> Manifest.encode() |> IO.iodata_to_binary()
    wire = JSON.decode!(encoded)
    assert wire["labels"] == [%{"job" => ["api"]}]
    assert Enum.all?(wire["s"], &(&1["li"] == 0))
    assert {:ok, decoded} = Manifest.decode(encoded)
    assert Map.new(decoded.segments, &{&1.key, &1}) == Map.new(manifest.segments, &{&1.key, &1})

    for reference <- [-1, 999, "bad", nil] do
      invalid = %{
        "v" => 1,
        "labels" => [%{"job" => ["api"]}],
        "s" => [%{"k" => "x", "mn" => 1, "mx" => 1, "li" => reference}]
      }

      assert {:ok, decoded} = invalid |> JSON.encode!() |> Manifest.decode()
      assert hd(decoded.segments).metric_labels == nil
      assert Segment.matches_metrics?(hd(decoded.segments), [{"job", :eq, "any"}])
    end
  end

  test "dictionary budget exhaustion loses only pruning, never segment membership" do
    segments =
      for i <- 1..400 do
        labels = Map.new(1..16, &{"key#{&1}", String.duplicate("x", 180) <> "#{i}"})
        Segment.build("x#{i}", i, i, 1) |> Segment.summarize_metrics([sample(labels, i)])
      end

    wire = Manifest.merge(Manifest.new(), segments) |> Manifest.encode() |> IO.iodata_to_binary() |> JSON.decode!()
    assert IO.iodata_length(JSON.encode_to_iodata!(wire["labels"])) <= 65_536
    assert {:ok, decoded} = wire |> JSON.encode!() |> Manifest.decode()
    assert length(decoded.segments) == length(segments)
    assert Enum.any?(decoded.segments, &is_nil(&1.metric_labels))
  end

  test "randomized exact-label pruning never excludes native matches" do
    :rand.seed(:exsss, {42, 103, 7})

    for _ <- 1..50 do
      records =
        for i <- 1..50 do
          labels = %{"__name__" => "work", "job" => Enum.random(["api", "", "worker"]), "instance" => "node-#{i}"}
          labels = if rem(i, 3) == 0, do: Map.delete(labels, "job"), else: labels
          sample(labels, i)
        end

      summary = MetricLabelSummary.build(records)
      {:ok, payload, _, _} = S3.encode_segment(:metrics, records)

      for name <- ["job", "instance", "missing"], value <- ["api", "worker", "", "absent", "node-17"] do
        matchers = [{name, :eq, value}]
        {:ok, found} = S3.decode_segment(:metrics, payload, nil, nil, matchers: matchers)
        assert found == [] or MetricLabelSummary.matches?(summary, matchers)
      end
    end
  end
end
