# Pulso 🫀

An open-source, headless observability backend built for AI agents as much as for humans.

Pulso accepts logs and metrics over the protocols your collectors already speak (Loki push, Prometheus remote write, and OpenTelemetry), stores them as Parquet in an S3-compatible bucket, and lets agents investigate them through a native [Model Context Protocol](https://modelcontextprotocol.io/) interface. Prometheus- and Loki-compatible query APIs keep existing dashboards working.

## Why Pulso

- **One system instead of three.** Logs and metrics (and, later, traces and alerting) share one ingest path, one storage format, and one query surface.
- **Object storage is the source of truth.** No database, no local disks to back up, no consensus service. Nodes are disposable.
- **Agent-native.** Diagnosis tools are part of the server, not a wrapper bolted on afterwards, and they stay read-only by design.
- **No UI.** Bring your own dashboards, or your own agent.

> [!WARNING]
> Pulso is early software. Logs and metrics work end to end; traces, alerting, and retention are not implemented yet. Expect breaking changes between releases.

## Try it locally

You need [mise](https://mise.jdx.dev/), a stable Rust toolchain, and Docker.

```sh
mise install                  # Erlang and Elixir
docker compose up -d          # local S3-compatible storage
mix setup
mix phx.server
```

In another terminal, push a log line and read it back (local development accepts any tenant without a token):

```sh
curl -X POST localhost:4000/loki/api/v1/push \
  -H 'Content-Type: application/json' -H 'X-Scope-OrgID: demo' \
  -d "{\"streams\":[{\"stream\":{\"service_name\":\"api\"},\"values\":[[\"$(date +%s)000000000\",\"hello pulso\"]]}]}"

curl -G localhost:4000/loki/api/v1/query_range \
  -H 'X-Scope-OrgID: demo' --data-urlencode 'query={service_name="api"}'
```

The listener port can differ per checkout; `mix phx.server` prints the one it uses.

## Documentation

The [documentation](docs/README.md) covers deploying Pulso with the Helm chart or container image published with every release, configuration, sending telemetry, querying, limits, and monitoring Pulso itself. The [architecture](docs/architecture.md) explains the design.

## Contributing

Run `mix precommit` before opening a pull request; it runs the same compile, format, and test checks as CI. [`AGENTS.md`](AGENTS.md) describes the codebase conventions.

## License

Pulso is released under the [MIT License](LICENSE).
