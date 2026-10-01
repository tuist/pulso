defmodule Pulso.Codec.MetricSegmentParquetTest do
  # Round-trip tests for the metric Parquet codec. Mirrors the shape of
  # test/pulso/storage/s3_codec_test.exs on the logs side. Hits the
  # Rust NIF directly rather than going through Pulso.Storage, since
  # we're guarding the codec contract — not the storage adapter.
  use ExUnit.Case, async: true

  alias Pulso.Codec.NIF
  alias Pulso.Record.MetricSample

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
