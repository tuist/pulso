defmodule Pulso.ObjectStore.NIF do
  @moduledoc false

  # Distribute the compiled NIF via GitHub Releases (see
  # `.github/workflows/release.yml`), so a downstream consumer can install
  # Pulso without a Cargo toolchain. `force_build` defaults to true so
  # local dev and CI keep compiling from source — set it to `false` (or
  # unset the env var) to opt into fetching a precompiled artifact from
  # the release matching the mix version.
  use RustlerPrecompiled,
    otp_app: :pulso,
    crate: "pulso_object_store",
    base_url: "https://github.com/tuist/pulso/releases/download/v#{Mix.Project.config()[:version]}",
    force_build: System.get_env("PULSO_NIF_FORCE_BUILD", "true") in ["1", "true"],
    version: Mix.Project.config()[:version],
    targets: ~w(
      x86_64-unknown-linux-gnu
      x86_64-unknown-linux-musl
      aarch64-unknown-linux-gnu
      aarch64-unknown-linux-musl
      x86_64-apple-darwin
      aarch64-apple-darwin
    ),
    nif_versions: ~w(2.16)

  def put(_config, _key, _data), do: :erlang.nif_error(:nif_not_loaded)
  def get(_config, _key), do: :erlang.nif_error(:nif_not_loaded)
  def delete(_config, _key), do: :erlang.nif_error(:nif_not_loaded)
  def list(_config, _prefix), do: :erlang.nif_error(:nif_not_loaded)
end
