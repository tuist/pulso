defmodule Pulso.AutoReferenceNIF do
  @on_load :load_nif
  def load_nif do
    :erlang.load_nif(~c".auto/reference/libpulso_codec_reference", 0)
  end
  def decode_loki_push(_compressed, _max_decompressed_bytes), do: :erlang.nif_error(:nif_not_loaded)

  def decode_loki_push_limited(_compressed, _max_decompressed_bytes, _limits), do: :erlang.nif_error(:nif_not_loaded)
  def json_decode(_binary), do: :erlang.nif_error(:nif_not_loaded)
  def json_decode_dirty(_binary), do: :erlang.nif_error(:nif_not_loaded)
  def json_encode(_term, _budget), do: :erlang.nif_error(:nif_not_loaded)
  def json_encode_dirty(_term), do: :erlang.nif_error(:nif_not_loaded)
  def encode_log_segment(_records, _mode, _framing), do: :erlang.nif_error(:nif_not_loaded)
  def decode_log_segment(_blob, _start_ts, _end_ts, _service), do: :erlang.nif_error(:nif_not_loaded)
  def encode_log_segment_parquet(_records), do: :erlang.nif_error(:nif_not_loaded)

  def decode_log_segment_parquet(_blob, _start_ts, _end_ts, _service, _matchers, _line_filters),
    do: :erlang.nif_error(:nif_not_loaded)

  def encode_metric_segment_parquet(_samples), do: :erlang.nif_error(:nif_not_loaded)

  def decode_metric_segment_parquet(_blob, _start_ts, _end_ts, _matchers), do: :erlang.nif_error(:nif_not_loaded)

  def decode_metric_segment_parquet_bounded(_blob, _start_ts, _end_ts, _matchers, _max_samples),
    do: :erlang.nif_error(:nif_not_loaded)

  def validate_metric_regex(_pattern), do: :erlang.nif_error(:nif_not_loaded)
  def validate_log_regex(_pattern), do: :erlang.nif_error(:nif_not_loaded)
  def match_metric_regex(_pattern, _value), do: :erlang.nif_error(:nif_not_loaded)

  # Hand-rolled Prometheus remote_write v1 wire decoder: takes a
  # Snappy-compressed `prometheus.WriteRequest` protobuf and a cap on the
  # decompressed size, returns `{:ok, series}` where each series is
  # `{labels_map, samples_list, series_id}` with `samples_list` as
  # `[{timestamp_ms, value}]`. Zero-copy over the decompressed buffer; the
  # Elixir caller converts to `%MetricSample{}` and ns units.
  def decode_remote_write(_compressed, _max_decompressed_bytes), do: :erlang.nif_error(:nif_not_loaded)

  def decode_remote_write_limited(_compressed, _max_decompressed_bytes, _limits), do: :erlang.nif_error(:nif_not_loaded)

  # Rust fast-path JSON encoder for the MCP `query_metrics` response.
  # Returns `{:ok, binary}` on success or `:fallback` when the input
  # is not a well-shaped `[%Pulso.Record.MetricSample{}]`; the Elixir
  # caller falls back to `JSON.encode!` in that case.
  def encode_metric_samples(_samples), do: :erlang.nif_error(:nif_not_loaded)
end
