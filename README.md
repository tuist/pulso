# Pulso 🫀

A headless, open-source observability backend: unified logs, metrics, and traces access, built-in alerting, and a native Model Context Protocol (MCP) interface, built on Elixir/OTP with a Rust hot path for columnar data.

Pulso ships no UI. It exists to be talked to by humans through their own dashboards and, first-class, by AI agents through MCP.

> [!WARNING]
> Pulso is in **early scaffolding**. The design is settled in [`docs/architecture.md`](./docs/architecture.md); the codebase is still catching up to it. Expect the shape to change without warning until we tag a first release.

## 🧭 Design

The architecture is documented in **[`docs/architecture.md`](./docs/architecture.md)**. Read that first if you want to understand what Pulso is trying to be. The short version:

- ☁️ **Object storage is the source of truth.** S3 (or R2, GCS, Azure Blob, MinIO) holds every acknowledged record. Local disk is a warm cache and nothing more.
- 🧩 **Shared-nothing nodes.** No shared database, no leader election, no consensus service. Nodes coordinate only through S3 conditional writes and rendezvous hashing.
- ⚙️ **BEAM for orchestration, Rust for bytes.** Elixir/OTP owns concurrency, supervision, backpressure, and the MCP surface. Rust owns Parquet, DataFusion, and the S3 client, called via Rustler NIFs.
- 🤖 **MCP first-class.** The primary read surface is MCP tools. HTTP wire protocols (OTLP, Prometheus `remote_write`, Loki push) exist to accept telemetry from existing agents unchanged.

## 🚀 Getting started

The Erlang, Elixir, and Rust toolchains are needed to build Pulso. Erlang and Elixir are pinned in [`mise.toml`](./mise.toml); a stable Rust toolchain (from `rustup` or your package manager) covers the NIF. With [mise](https://mise.jdx.dev/) installed:

```sh
mise install
mix setup
mix test
```

The first build compiles the Rust NIF under [`native/pulso_object_store`](./native/pulso_object_store) and copies the shared object into `priv/native/`. Subsequent builds are incremental.

To boot the app locally:

```sh
mix phx.server
```

- OTLP/HTTP JSON logs land at `POST /v1/logs`. Tenant is picked up from `X-Scope-OrgID` (Loki/Cortex convention), defaulting to `default`.
- Loki push JSON lands at `POST /loki/api/v1/push` (same tenant convention). Gzip-encoded bodies are decompressed transparently; Snappy-framed protobuf is not yet supported.
- The MCP endpoint is exposed at `POST /mcp`. It speaks JSON-RPC 2.0 (`initialize`, `tools/list`, `tools/call`, `ping`).

## 🐳 Local S3 (MinIO)

The Rust NIF talks to any S3-compatible endpoint. [`docker-compose.yml`](./docker-compose.yml) brings up MinIO and preseeds a bucket:

```sh
docker compose up -d
```

- API on `http://localhost:9000`, console on `http://localhost:9001` (`minioadmin` / `minioadmin`).
- Preseeded bucket: `pulso`.

To run the integration test suite against MinIO:

```sh
PULSO_INTEGRATION=1 mix test --only integration
```

Every `PULSO_MINIO_*` variable defaults to the values docker-compose sets up, so no other environment is needed when running against the local stack.

## 🛠️ Development

Before opening a pull request:

```sh
mix precommit
```

This runs `mix compile --warnings-as-errors`, `mix deps.unlock --unused`, `mix format`, and `mix test` — the same checks CI runs.

## 📄 License

Pulso is released under the [MIT License](./LICENSE).
