# Pulso documentation

Pulso is a headless observability backend. It accepts logs and metrics over the
protocols your collectors already speak, stores them as Parquet in an
S3-compatible bucket, and serves queries through the Model Context Protocol and
Prometheus- and Loki-compatible HTTP APIs. It ships no user interface.

> [!WARNING]
> Pulso is early software. Logs and metrics work end to end; traces, alerting,
> and data retention are not implemented yet. Expect breaking changes between
> releases and read the release notes before upgrading.

## Running Pulso

- [Deployment](deployment.md): install the Helm chart or run the container,
  expose it safely, connect collectors, and query data.
- [Configuration](configuration.md): every environment variable, tenants and
  tokens, object storage requirements, and metrics compaction.
- [Querying](querying.md): the Model Context Protocol tools, the Prometheus and
  Loki query APIs, the supported query subset, and query limits.
- [Ingest limits](ingest-limits.md): per-request budgets for records,
  attributes, and payload sizes, and how to tune them.
- [Self-monitoring](self-monitoring.md): the `/metrics` endpoint, what each
  metric means, and how to scrape it without exposing it.

## Understanding Pulso

- [Architecture](architecture.md): the storage model, coordination through
  object storage, the query path, and the Model Context Protocol interface.
  Read it before contributing a design change.

## Getting help

Report bugs and ask questions in
[GitHub issues](https://github.com/tuist/pulso/issues).
