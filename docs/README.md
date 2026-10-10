# Pulso documentation

Pulso is a headless observability backend. It accepts logs and metrics over the
protocols your collectors already speak, stores them as Parquet in an
S3-compatible bucket, and serves queries through the Model Context Protocol and
Prometheus- and Loki-compatible HTTP APIs. It ships no user interface.

> [!WARNING]
> Pulso is early software. Logs and metrics work end to end. Native alert
> evaluation, Slack delivery, live resource hints and rule history are experimental.
> Event-time retention is experimental and off by default. Grafana migration
> compatibility and traces are not implemented yet. Expect breaking changes between
> releases and read the release notes before upgrading.

## Running Pulso

- [Deployment](deployment.md): install the Helm chart or run the container,
  expose it safely, connect collectors, and query data.
- [Configuration](configuration.md): every environment variable, tenants and
  tokens, object storage requirements, and metrics compaction.
- [Querying](querying.md): the Model Context Protocol tools, the Prometheus and
  Loki query APIs, the supported query subset, and query limits.
- [Alerting](alerting.md): experimental native evaluation, dedicated credentials,
  rule management and modification history, with explicit compatibility limits.
- [Retention](retention.md): experimental event-time retention for logs and
  metrics, its irreversible rollout, policy changes, limits, and recovery.
- [Ingest limits](ingest-limits.md): per-request budgets for records,
  attributes, and payload sizes, and how to tune them.
- [Self-monitoring](self-monitoring.md): the `/metrics` endpoint, what each
  metric means, and how to scrape it without exposing it.
- [Storage costs](storage-costs.md): request and retained-byte cost drivers,
  Tigris pricing examples, and avoiding small-object amplification.

## Understanding Pulso

- [Architecture](architecture.md): the storage model, coordination through
  object storage, the query path, and the Model Context Protocol interface.
  Read it before contributing a design change.

## Getting help

Report bugs and ask questions in
[GitHub issues](https://github.com/tuist/pulso/issues).
