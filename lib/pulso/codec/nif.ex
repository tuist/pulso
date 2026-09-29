defmodule Pulso.Codec.NIF do
  @moduledoc false

  # Byte-heavy encoding and decoding in Rust (`native/pulso_codec`): Loki
  # push protobuf, JSON (behind `Pulso.JSON`), and log segments (behind
  # `Pulso.Storage.S3` and `Pulso.MCP.Tools`). Distributed the same way as
  # `Pulso.ObjectStore.NIF`: precompiled artifacts from GitHub Releases for
  # consumers, compiled from source when `PULSO_NIF_FORCE_BUILD` is set
  # (local dev and CI).
  use RustlerPrecompiled,
    otp_app: :pulso,
    crate: "pulso_codec",
    base_url: "https://github.com/tuist/pulso/releases/download/v#{Mix.Project.config()[:version]}",
    force_build: System.get_env("PULSO_NIF_FORCE_BUILD") in ["1", "true"],
    version: Mix.Project.config()[:version],
    targets: ~w(
      x86_64-unknown-linux-gnu
      x86_64-unknown-linux-musl
      aarch64-unknown-linux-gnu
      aarch64-unknown-linux-musl
      aarch64-apple-darwin
    ),
    nif_versions: ~w(2.16)

  def decode_loki_push(_compressed, _max_decompressed_bytes), do: :erlang.nif_error(:nif_not_loaded)
  def json_decode(_binary), do: :erlang.nif_error(:nif_not_loaded)
  def json_decode_dirty(_binary), do: :erlang.nif_error(:nif_not_loaded)
  def json_encode(_term, _budget), do: :erlang.nif_error(:nif_not_loaded)
  def json_encode_dirty(_term), do: :erlang.nif_error(:nif_not_loaded)
  def encode_log_segment(_records, _mode, _framing), do: :erlang.nif_error(:nif_not_loaded)
  def decode_log_segment(_blob, _start_ts, _end_ts, _service), do: :erlang.nif_error(:nif_not_loaded)
  def encode_log_segment_parquet(_records), do: :erlang.nif_error(:nif_not_loaded)

  def decode_log_segment_parquet(_blob, _start_ts, _end_ts, _service), do: :erlang.nif_error(:nif_not_loaded)
end
