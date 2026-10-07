# Deploying Pulso

Pulso runs as a stateless container in front of an S3-compatible bucket. The
bucket holds every acknowledged record; the container keeps nothing on disk
that cannot be thrown away. Each release publishes a container image and a
Helm chart with the same version:

- Image: `ghcr.io/tuist/pulso:<version>`
- Chart: `oci://ghcr.io/tuist/charts/pulso`, version `<version>`

Pick a version from the [releases page](https://github.com/tuist/pulso/releases).

> [!IMPORTANT]
> Run one Pulso node per bucket for now. Multi-node clustering is not wired into
> the release yet. Rolling updates briefly run two nodes; conditional writes in
> object storage keep that safe, but do not scale out permanently.

## Before you start

You need:

- A Kubernetes cluster with Helm 3.8 or later, or any host that runs containers.
- A dedicated bucket on storage that supports conditional writes, plus an access
  key that can list, read, write, and delete objects in it. See
  [object storage](configuration.md#object-storage).
- One bearer token per tenant. See [tenants and tokens](configuration.md#tenants-and-tokens).

## Install with Helm

Create a values file. Keep secrets out of version control, or use
[an existing Secret](#using-an-existing-secret) instead:

```yaml
# pulso-values.yaml
storage:
  bucket: pulso-telemetry
  region: us-east-1
  # endpoint: https://<account>.r2.cloudflarestorage.com
  accessKeyId: AKIA...
  secretAccessKey: ...

tenantTokens:
  production: "sha256$<digest of the production token>"
```

Install it:

```sh
helm install pulso oci://ghcr.io/tuist/charts/pulso \
  --version <version> \
  --namespace observability --create-namespace \
  --values pulso-values.yaml
```

The chart checks required values and digest formats while rendering, so a
mistake fails the install instead of producing a crash-looping pod. Check that the
node reached storage:

```sh
kubectl -n observability port-forward svc/pulso 4000:4000
curl -s localhost:4000/readyz
# {"status":"ok","checks":{"manifest":"ok","query_workers":"ok","storage":"ok"}}
```

[`charts/pulso/values.yaml`](../charts/pulso/values.yaml) documents every value.
The ones you are most likely to change:

| Value | Purpose |
| --- | --- |
| `storage.*` | Bucket, region, endpoint, and credentials. |
| `tenantTokens` | Tenant name to token digest. |
| `existingSecret` | Use a Secret you manage instead of chart-rendered credentials. |
| `ingress.*` | Expose ingestion and queries outside the cluster. |
| `serviceMonitor.enabled` | Create a Prometheus Operator `ServiceMonitor` for self-monitoring. |
| `metricsCompaction.enabled` | Merge small metrics segments. Read [the prerequisites](configuration.md#metrics-compaction) first. |
| `ingestLimits` | Per-request ingest budgets, keyed by setting name, such as `max_records`. |
| `ingestBatching.flushIntervalMs` | Optional concurrent unkeyed ingest coalescing window (`0..1000` ms); `0` disables it. Read [the tradeoffs](configuration.md#ingest-coalescing). |
| `mcp.allowedOrigins` | Browser origins allowed to call the Model Context Protocol endpoint. |
| `alerting.principals` | Dedicated alert principal identities, hashed credentials and capabilities; see [alerting](alerting.md). |
| `alerting.evaluationEnabled` | Opt-in experimental native evaluation, default `false`. Grafana execution remains unsupported; native delivery has a separate opt-in. |
| `alerting.notificationsEnabled` | Opt-in native Slack delivery, default `false`; not Grafana routing/template parity. |
| `alerting.notificationTargets` | Public target descriptors; reference webhook environment secrets supplied through `extraEnv`, never inline URLs. |
| `alerting.pollIntervalMs` | Durable rule discovery interval (`1000..60000` ms), default `5000`; benchmark read cost before lowering it. |
| `resources` | CPU and memory. The defaults are starting points, not measured capacity. |

### Using an existing secret

To manage credentials with a secret store or an operator, create a Secret with
these keys and set `existingSecret` to its name:

```sh
kubectl -n observability create secret generic pulso-credentials \
  --from-literal=SECRET_KEY_BASE="$(openssl rand -base64 48)" \
  --from-literal=PULSO_TENANT_TOKENS='{"production":"sha256$<digest>"}' \
  --from-literal=PULSO_S3_ACCESS_KEY_ID=AKIA... \
  --from-literal=PULSO_S3_SECRET_ACCESS_KEY=...
```

The chart then ignores `secretKeyBase`, `tenantTokens`, and the storage
credentials in values. Pods do not restart automatically when an external Secret
changes; run `kubectl rollout restart deployment/pulso` after updating it.

## Networking and TLS

Pulso serves plain HTTP on port 4000 and expects TLS to terminate in front of
it, at an ingress controller, load balancer, or service mesh. In-cluster
collectors can talk to the Service directly.

The same listener serves ingestion, queries, the Model Context Protocol, health
probes, and the unauthenticated self-monitoring endpoint `/metrics`. Never expose
the whole listener publicly. The chart's ingress routes only these paths:

| Path | Type | Serves |
| --- | --- | --- |
| `/v1/logs` | Exact | OpenTelemetry logs |
| `/loki/api/v1` | Prefix | Loki push and queries |
| `/api/v1` | Prefix | Prometheus remote write and queries |
| `/mcp` | Exact | Model Context Protocol |

Requests for any other path, including `/metrics`, `/healthz`, and `/readyz`,
never reach Pulso through that ingress. If you write your own proxy rules, keep
an allowlist like this, and make sure the proxy normalizes paths before matching
so encoded or doubled slashes cannot reach `/metrics`. See
[self-monitoring](self-monitoring.md#exposure-and-independent-collection) for
details.

Example with ingress-nginx and cert-manager:

```yaml
ingress:
  enabled: true
  className: nginx
  host: pulso.example.com
  annotations:
    cert-manager.io/cluster-issuer: letsencrypt
    nginx.ingress.kubernetes.io/proxy-body-size: 5m
  tls:
    - secretName: pulso-tls
      hosts: [pulso.example.com]
```

Raise the proxy's request body limit to at least 4 MiB, the largest body Pulso
accepts. Many proxies default to 1 MiB, which rejects large collector batches
before they reach Pulso.

## Probes, shutdown, and upgrades

- **Liveness** (`GET /healthz`) answers as long as the node can serve HTTP.
- **Readiness** (`GET /readyz`) returns `503` until a background check can list the
  bucket. The check runs every 15 seconds, so readiness never adds storage
  requests per probe. A wrong bucket name or credentials keep the pod unready;
  the reason is logged.
- **Shutdown**: the chart waits `preStopSleepSeconds` (5 by default) so load
  balancers stop routing to the pod, then gives Pulso the rest of
  `terminationGracePeriodSeconds` (30 by default) to finish in-flight requests.
  Pulso only acknowledges a batch after it is durable in the bucket, so
  collectors retry anything cut off mid-request.
- **Compatible upgrades** roll out one new pod before stopping the old one.
  Read release notes first. Crossing the stale-sample compatibility boundary
  below requires a non-overlapping upgrade, not the default rolling strategy.
  Do not mix incompatible readers, writers, or compactors.

The pod runs as an unprivileged user with a read-only root filesystem. The only
writable path is an `emptyDir` at `/tmp` for runtime scratch files.

### Stale-sample storage compatibility

When upgrading from a release that rejected stale markers, NaN, and infinities,
**do not use the default rolling update**. New writers persist those values
immediately, while old readers and compactors cannot decode them. Collectors
send stale markers automatically when series disappear, so asking collectors
not to send them is not a safe rollout strategy.

For the chart's supported single-node deployment, use `Recreate` for this upgrade:

```yaml
strategy:
  type: Recreate
  rollingUpdate: null # Removes the chart's default rolling-update settings.
```

Render the chart first and verify that `strategy` has only `type: Recreate`.
Then upgrade to the new image/chart. The old pod must finish termination before
the new pod starts. If other processes write, read, or compact the same bucket,
stop them too before starting any new-version writer. Keep collector queues and
the reference destination running; ingestion is unavailable during the upgrade,
and collectors must retry unacknowledged requests within their retry horizon.
Verify readiness, finite and stale-sample queries, and independent monitoring
before expanding traffic. Compatible subsequent upgrades can restore the rolling
strategy.

**Rollback is not a simple image rollback after the first non-finite write.**
Retain a compatible Pulso version to read the existing bucket, and roll forward
with a fix or route collection/query traffic back to the reference destination.
Do not delete live objects or restore an older manifest to hide incompatible
records: that can lose acknowledged data. Returning the bucket to an older data
contract requires a separately validated data migration or a pre-upgrade backup
with an explicitly accepted loss/backfill procedure. Record this boundary in the
release's upgrade notes.

## Sending telemetry

Every request carries the tenant's bearer token and, for HTTP ingestion, the
`X-Scope-OrgID` header. The receivers speak existing protocols, so collectors
need only a destination change.

| Signal | Endpoint | Formats |
| --- | --- | --- |
| Logs | `POST /loki/api/v1/push` | Loki protobuf (Snappy) or JSON, optionally gzip |
| Logs | `POST /v1/logs` | OpenTelemetry Protocol over HTTP, JSON encoding only, optionally gzip |
| Metrics | `POST /api/v1/write` | Prometheus remote write 1.0 |

Traces and OpenTelemetry metrics are not supported yet.

[Grafana Alloy](https://grafana.com/docs/alloy/latest/) example, using the
in-cluster Service:

```alloy
loki.write "pulso" {
  endpoint {
    url          = "http://pulso.observability.svc:4000/loki/api/v1/push"
    tenant_id    = "production"
    bearer_token = sys.env("PULSO_TOKEN")
  }
}

prometheus.remote_write "pulso" {
  endpoint {
    url          = "http://pulso.observability.svc:4000/api/v1/write"
    headers      = { "X-Scope-OrgID" = "production" }
    bearer_token = sys.env("PULSO_TOKEN")
  }
}
```

OpenTelemetry Collector example for logs:

```yaml
exporters:
  otlphttp/pulso:
    logs_endpoint: http://pulso.observability.svc:4000/v1/logs
    encoding: json
    headers:
      Authorization: Bearer ${env:PULSO_TOKEN}
      X-Scope-OrgID: production
```

Pulso rejects a whole batch with `413` when it exceeds the
[ingest limits](ingest-limits.md); shrink the collector's batch size if that
happens. When adding Pulso next to an existing backend, give each destination
its own retry queue so a problem with one does not hold back the other.

## Querying

- **Model Context Protocol**: point an agent at `https://<host>/mcp` with the
  bearer token. Pulso implements the stateless `2026-07-28` revision of the
  protocol over Streamable HTTP; clients that only speak older revisions receive
  `400` with the supported version.
- **Prometheus API**: `GET|POST /api/v1/query` and `/api/v1/query_range`, with
  the same token and tenant header. Grafana can use it as a Prometheus data
  source for the [supported query subset](querying.md#prometheus-api).
- **Loki API**: `/loki/api/v1/query`, `query_range`, `labels`, and
  `label/<name>/values`.

## Monitoring Pulso

Scrape `GET /metrics` on every node from a monitoring system other than Pulso.
Set `serviceMonitor.enabled=true` if you run the Prometheus Operator. See
[self-monitoring](self-monitoring.md) for the metrics and suggested queries.

## Running without Kubernetes

The image runs anywhere containers do. Pass the [configuration](configuration.md)
as environment variables:

```sh
docker run -d --name pulso -p 127.0.0.1:4000:4000 \
  -e SECRET_KEY_BASE="$(openssl rand -base64 48)" \
  -e PULSO_TENANT_TOKENS='{"production":"sha256$<digest>"}' \
  -e PULSO_S3_BUCKET=pulso-telemetry \
  -e PULSO_S3_REGION=us-east-1 \
  -e PULSO_S3_ACCESS_KEY_ID=AKIA... \
  -e PULSO_S3_SECRET_ACCESS_KEY=... \
  ghcr.io/tuist/pulso:<version>
```

Put a TLS-terminating proxy in front of it with the same path allowlist described
in [Networking and TLS](#networking-and-tls).
