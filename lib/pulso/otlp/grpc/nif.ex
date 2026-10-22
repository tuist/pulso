defmodule Pulso.OTLP.GRPC.NIF do
  @moduledoc false

  # The OTLP/gRPC transport (tonic) lives in its own crate so the codec NIF
  # stays free of an async runtime. Distributed like the other crates; see
  # `Pulso.ObjectStore.NIF` for the precompiled-artifact rationale.
  use RustlerPrecompiled,
    otp_app: :pulso,
    crate: "pulso_grpc",
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

  def otlp_grpc_api_version, do: :erlang.nif_error(:nif_not_loaded)

  def otlp_grpc_start(_ip, _port, _owner, _max_message_bytes, _max_in_flight, _timeout_ms),
    do: :erlang.nif_error(:nif_not_loaded)

  def otlp_grpc_stop(_listener), do: :erlang.nif_error(:nif_not_loaded)
  def otlp_grpc_reply(_reply, _code, _message, _body), do: :erlang.nif_error(:nif_not_loaded)
end
