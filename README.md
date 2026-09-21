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

The Erlang and Elixir toolchains are pinned in [`mise.toml`](./mise.toml). With [mise](https://mise.jdx.dev/) installed:

```sh
mise install
mix setup
mix test
```

To boot the app locally:

```sh
mix phx.server
```

The MCP endpoint is exposed at `POST /mcp`. It speaks JSON-RPC 2.0 (`initialize`, `tools/list`, `tools/call`, `ping`).

## 🛠️ Development

Before opening a pull request:

```sh
mix precommit
```

This runs `mix compile --warnings-as-errors`, `mix deps.unlock --unused`, `mix format`, and `mix test` — the same checks CI runs.

## 📄 License

Pulso is released under the [MIT License](./LICENSE).
