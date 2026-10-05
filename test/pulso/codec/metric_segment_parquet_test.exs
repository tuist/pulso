defmodule Pulso.Codec.MetricSegmentParquetTest do
  # Round-trip tests for the metric Parquet codec. Mirrors the shape of
  # test/pulso/storage/s3_codec_test.exs on the logs side. Hits the
  # Rust NIF directly rather than going through Pulso.Storage, since
  # we're guarding the codec contract — not the storage adapter.
  use ExUnit.Case, async: true

  alias Pulso.Codec.NIF
  alias Pulso.Record.MetricSample
  alias Pulso.Storage.S3

  defp sample(ts, value, labels) when is_integer(ts) and is_map(labels) do
    %MetricSample{timestamp_ns: ts, value: value, labels: labels}
  end

  defp encode!(samples) do
    {:ok, payload, min_ts, max_ts, count} = NIF.encode_metric_segment_parquet(samples)
    {payload, min_ts, max_ts, count}
  end

  defp decode!(payload, opts \\ []) do
    start_ts = Keyword.get(opts, :start_ts)
    end_ts = Keyword.get(opts, :end_ts)
    matchers = Keyword.get(opts, :matchers, [])
    {:ok, records} = NIF.decode_metric_segment_parquet(payload, start_ts, end_ts, matchers)
    records
  end

  test "round-trips a single sample" do
    s = sample(1_700_000_000_000_000_000, 1.5, %{"__name__" => "up", "job" => "pulso"})
    {payload, min_ts, max_ts, count} = encode!([s])

    assert count == 1
    assert min_ts == s.timestamp_ns
    assert max_ts == s.timestamp_ns

    assert [decoded] = decode!(payload)
    assert decoded.timestamp_ns == s.timestamp_ns
    assert decoded.value == 1.5
    assert decoded.labels == s.labels
    # series_id is populated from StableHash on encode when the caller leaves it nil
    assert is_integer(decoded.series_id)
  end

  test "bounded footer statistics do not truncate long Unicode labels or time bounds" do
    prefix = String.duplicate("界🌍", 1000)
    samples = for ts <- [10, 20], do: sample(ts, ts / 1, %{"__name__" => "long", "context" => prefix <> "#{ts}"})
    {payload, 10, 20, 2} = encode!(samples)
    decoded = decode!(payload) |> Enum.sort_by(& &1.timestamp_ns)
    assert Enum.map(decoded, & &1.labels) == Enum.map(samples, & &1.labels)
    assert [kept] = decode!(payload, start_ts: 20, end_ts: 20, matchers: [{"context", :eq, prefix <> "20"}])
    assert kept.value == 20.0
    assert decode!(payload, start_ts: 21) == []
  end

  test "does not encode an unsupported value beside a finite sample" do
    labels = %{"__name__" => "up"}
    assert :fallback = NIF.encode_metric_segment_parquet([sample(1, 1.0, labels), sample(2, nil, labels)])
  end

  test "legacy stale values fail explicitly while filtered finite rows remain readable" do
    blob = File.read!(Path.expand("../../fixtures/metrics/non_finite.parquet", __DIR__))

    assert {:error, :non_finite_sample_value} = NIF.decode_metric_segment_parquet(blob, nil, nil, [])
    assert {:error, :non_finite_sample_value} = NIF.decode_metric_segment_parquet_bounded(blob, nil, nil, [], 10)
    assert {:error, :non_finite_sample_value} = S3.decode_segment(:metrics, blob, nil, nil, [])

    assert {:ok, [finite]} = NIF.decode_metric_segment_parquet(blob, nil, nil, [{"__name__", :eq, "safe"}])
    assert finite.value == 1.0
    assert finite.labels == %{"__name__" => "safe"}
  end

  test "same label set hashes to the same series_id across samples" do
    labels = %{"__name__" => "cpu", "instance" => "node-1"}
    a = sample(10, 0.1, labels)
    b = sample(20, 0.2, labels)
    {payload, _, _, _} = encode!([a, b])
    [d1, d2] = decode!(payload) |> Enum.sort_by(& &1.timestamp_ns)

    assert d1.series_id == d2.series_id
  end

  test "time filtering drops samples outside [start_ts, end_ts]" do
    labels = %{"__name__" => "req_total"}
    samples = for ts <- [10, 20, 30, 40, 50], do: sample(ts, 1.0, labels)
    {payload, _, _, _} = encode!(samples)

    kept = decode!(payload, start_ts: 20, end_ts: 40)
    assert Enum.map(kept, & &1.timestamp_ns) |> Enum.sort() == [20, 30, 40]
  end

  test "matcher :eq keeps only samples whose label equals value" do
    one = sample(1, 1.0, %{"__name__" => "x", "svc" => "api"})
    two = sample(2, 2.0, %{"__name__" => "x", "svc" => "web"})
    {payload, _, _, _} = encode!([one, two])

    assert [kept] = decode!(payload, matchers: [{"svc", :eq, "api"}])
    assert kept.labels["svc"] == "api"
  end

  test "matcher :re evaluates as a regex on the label value" do
    a = sample(1, 1.0, %{"__name__" => "x", "svc" => "api-east"})
    b = sample(2, 2.0, %{"__name__" => "x", "svc" => "web"})
    {payload, _, _, _} = encode!([a, b])

    assert [kept] = decode!(payload, matchers: [{"svc", :re, "^api"}])
    assert kept.labels["svc"] == "api-east"
  end

  test "a time filter that misses the whole segment returns [] (row-group pruning)" do
    labels = %{"__name__" => "x"}
    samples = for ts <- 100..200, do: sample(ts, 1.0, labels)
    {payload, _, _, _} = encode!(samples)

    # Both sides of the range are outside the segment's bounds.
    assert decode!(payload, start_ts: 500, end_ts: 1000) == []
    assert decode!(payload, start_ts: 0, end_ts: 50) == []
  end

  test "a time filter that bisects a multi-row-group segment preserves correctness" do
    # 20_000 samples > 8192 rows per row group → at least 3 row groups.
    # Filtering to the middle third exercises row-group pruning on the
    # bounding groups and a per-row scan on the middle one.
    labels = %{"__name__" => "y"}
    samples = for ts <- 1..20_000, do: sample(ts, 1.0, labels)
    {payload, _, _, _} = encode!(samples)

    kept = decode!(payload, start_ts: 7_000, end_ts: 13_000)
    kept_ts = kept |> Enum.map(& &1.timestamp_ns) |> Enum.sort()
    assert kept_ts == Enum.to_list(7_000..13_000)
  end

  test "encode->decode->encode->decode is idempotent on bytes from the second decode on" do
    # Mirrors the Pulso.Codec.NIF contract documented in AGENTS.md:
    # Parquet is a hard-error codec with no Elixir fallback, so we at
    # least pin round-trip idempotency on the stored form.
    base = for i <- 1..20, do: sample(i * 1_000_000_000, i * 1.0, %{"__name__" => "y", "k" => "v#{rem(i, 3)}"})

    {payload1, _, _, _} = encode!(base)
    decoded1 = decode!(payload1)
    {payload2, _, _, _} = encode!(decoded1)
    decoded2 = decode!(payload2)

    assert decoded1 == decoded2
  end
end
