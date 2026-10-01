defmodule Pulso.RemoteWrite.Push do
  @moduledoc """
  Decode a Prometheus remote_write v1 `WriteRequest` body into
  `[%Pulso.Record.MetricSample{}]`.

  The on-wire body is **Snappy-compressed** (block format, not frame)
  protobuf, with `Content-Type: application/x-protobuf` and
  `Content-Encoding: snappy`. The Rust NIF (`Pulso.Codec.NIF.decode_remote_write`)
  decompresses, parses the protobuf, computes a Prometheus-compatible
  `StableHash` fingerprint per series, and returns a flat list of
  `{labels_map, samples_list, series_id}` triples where each sample is
  `{ts_ms, value}` — timestamps in **milliseconds**, matching the
  Prometheus wire unit. This module expands each triple into
  per-sample `%MetricSample{}` structs with `timestamp_ns` rescaled.

  Semantic errors inside a series (invalid labels, out-of-range
  timestamp) are counted in `rejected` rather than failing the whole
  request — the same contract as `Pulso.Loki.Push`. A wire-level
  corruption fails the whole request with a typed error.
  """

  alias Pulso.Codec.NIF
  alias Pulso.Record.MetricSample

  @doc """
  Decode a Snappy-compressed `WriteRequest` protobuf body into
  `{:ok, samples, rejected}` or `{:error, reason}` where `reason` is one
  of `:invalid_snappy | :invalid_protobuf | :payload_too_large`.
  """
  @spec decode_protobuf(binary(), pos_integer()) ::
          {:ok, [MetricSample.t()], non_neg_integer()} | {:error, atom()}
  def decode_protobuf(body, max_decompressed) when is_binary(body) and is_integer(max_decompressed) do
    case NIF.decode_remote_write(body, max_decompressed) do
      {:ok, series, rejected} ->
        samples = Enum.flat_map(series, &expand_series/1)
        {:ok, samples, rejected}

      {:error, reason} when is_atom(reason) ->
        {:error, reason}
    end
  end

  # Each series: `{labels_map, [{ts_ms, value}, ...], series_id}`.
  # Fan out one `%MetricSample{}` per sample. The label map is shared
  # across samples in the same series — Elixir maps are immutable so
  # the per-sample struct holds a reference to the one allocation.
  defp expand_series({labels, samples, series_id}) when is_map(labels) and is_list(samples) and is_integer(series_id) do
    Enum.map(samples, fn {ts_ms, value} ->
      %MetricSample{
        series_id: series_id,
        timestamp_ns: ts_ms * 1_000_000,
        value: value,
        labels: labels
      }
    end)
  end
end
