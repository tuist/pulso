defmodule Pulso.Ingest.NIF do
  @moduledoc false

  # Wire-format decoding for high-volume ingest paths, in Rust
  # (`native/pulso_ingest`). Distributed the same way as
  # `Pulso.ObjectStore.NIF`: precompiled artifacts from GitHub Releases for
  # consumers, compiled from source when `PULSO_NIF_FORCE_BUILD` is set
  # (local dev and CI).
  use RustlerPrecompiled,
    otp_app: :pulso,
    crate: "pulso_ingest",
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
end
