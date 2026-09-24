defmodule Pulso.ObjectStore.NIF do
  @moduledoc false

  # Distribute the compiled NIF via GitHub Releases (see
  # `.github/workflows/release.yml`), so a downstream consumer can install
  # Pulso without a Cargo toolchain. `force_build` defaults to false: a
  # `mix deps.get` of `:pulso` fetches the precompiled artifact matching
  # the consumer's OS + arch + NIF version from the tagged release. Local
  # dev and this repo's own CI set `PULSO_NIF_FORCE_BUILD=1` to compile
  # from source instead, which is what proves the source keeps building.
  use RustlerPrecompiled,
    otp_app: :pulso,
    crate: "pulso_object_store",
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

  def put(_config, _key, _data), do: :erlang.nif_error(:nif_not_loaded)
  def get(_config, _key), do: :erlang.nif_error(:nif_not_loaded)
  def delete(_config, _key), do: :erlang.nif_error(:nif_not_loaded)
  def list(_config, _prefix), do: :erlang.nif_error(:nif_not_loaded)
end
