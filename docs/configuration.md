# Configuration

A Pulso release reads its configuration from environment variables when it
boots. Missing or invalid required values stop the node at startup with a
message naming the variable, rather than failing later on traffic. The
[Helm chart](deployment.md) sets these variables from its values; this page
describes what each one means.

## Required

| Variable | Meaning |
| --- | --- |
| `SECRET_KEY_BASE` | Random secret of at least 64 characters, used by the web framework. Generate one with `openssl rand -base64 48`. The chart generates and keeps one for you. |
| `PULSO_TENANT_TOKENS` | JSON object mapping each tenant name to the SHA-256 digest of its bearer token. See [Tenants and tokens](#tenants-and-tokens). |
| `PULSO_S3_BUCKET` | Bucket that stores every acknowledged record. |
| `PULSO_S3_REGION` | Region passed to the S3 client. Providers that ignore regions still need a value, such as `auto` for Cloudflare R2. |
| `PULSO_S3_ACCESS_KEY_ID` | Access key for the bucket. |
| `PULSO_S3_SECRET_ACCESS_KEY` | Secret key for the bucket. |

Pulso only uses the static credentials above. It does not read instance
profiles, workload identity, or other ambient cloud credentials.

## Optional

| Variable | Default | Meaning |
| --- | --- | --- |
| `PORT` | `4000` | HTTP listener port. |
| `PULSO_S3_ENDPOINT` | Amazon S3 | URL of an S3-compatible endpoint, for example `https://<account>.r2.cloudflarestorage.com`. |
| `PULSO_S3_ALLOW_HTTP` | `false` | Allow a plain-HTTP storage endpoint. Use only for storage on a private network. |
| `PULSO_METRICS_COMPACTION_ENABLED` | `false` | Merge small metrics segments in the background. Read [Metrics compaction](#metrics-compaction) first. |
| `PULSO_ALERTING_PRINCIPALS_JSON` | `[]` | Dedicated alerting principal array with `tenant`, `id`, `type`, `token_hash` and `capabilities`; at most 256 entries. See [alerting credentials](alerting.md#configure-credentials-and-evaluation). |
| `PULSO_ALERTING_EVALUATION_ENABLED` | `false` | Enable experimental native threshold evaluation. Grafana execution remains unsupported; native Slack delivery has a separate opt-in. |
| `PULSO_ALERTING_NOTIFICATIONS_ENABLED` | `false` | Enable native Slack delivery from committed bounded outboxes. See [delivery semantics](alerting.md#native-slack-delivery). |
| `PULSO_ALERTING_NOTIFICATION_TARGETS_JSON` | `[]` | Operator-provisioned target descriptors (`tenant`, `id`, `type: slack_webhook`, `secret_env`), at most 256. Webhook URLs must live in the referenced environment secrets, not in descriptors. |
| `PULSO_ALERTING_POLL_INTERVAL_MS` | `5000` | Native evaluator discovery interval, integer `1000..60000`; measure object-store read cost before reducing it. |
| `PULSO_MCP_ALLOWED_ORIGINS` | none | Comma-separated browser origins allowed to call `POST /mcp`, such as `https://agent.example.com`. Requests that carry no `Origin` header, which is the case for most agents and servers, are always accepted; any other origin receives `403`. |
| `PULSO_INGEST_FLUSH_INTERVAL_MS` | `0` | Optional coalescing window for concurrent appends without an idempotency key; integer `0..1000`, where `0` disables it. See [Ingest coalescing](#ingest-coalescing). |
| `PULSO_INGEST_MAX_*` | see [ingest limits](ingest-limits.md) | Per-request record and attribute budgets for ingestion. |
| `PULSO_RETENTION_ENABLED` | `false` | Run the experimental retention and cleanup worker even with both durations at `0`; it also runs whenever a duration is positive. See [retention](retention.md). |
| `PULSO_LOGS_RETENTION_DAYS` | `0` | Log event-time retention in whole days, `0..3650`. `0` stops advancing the floor; without earlier enforcement, logs are kept indefinitely. |
| `PULSO_METRICS_RETENTION_DAYS` | `0` | Metric event-time retention in whole days, `0..3650`. `0` stops advancing the floor; without earlier enforcement, metrics are kept indefinitely. |
| `PULSO_RETENTION_MODE` | `observe` | `observe` previews, `enforce` converts manifests and expires data (irreversible), `paused` stops all retention work. |
| `PULSO_RETENTION_DELETE_GRACE_MS` | `3600000` | Grace between committed expiry and deletion, `60000..2592000000`. |
| `PULSO_RETENTION_INTERVAL_MS` | `30000` | Retention maintenance cadence, `1000..3600000`. |
| `PULSO_RETENTION_DELETE_LIMIT` | `512` | Maximum DELETE attempts per node per `PULSO_RETENTION_INTERVAL_MS`, `1..512`, shared by retention and compaction cleanup of retention-managed prefixes. At most four run at once. |
| `PULSO_RETENTION_TIMEOUT_MS` | `30000` | Deadline for one retention operation, `1000..600000`. |
| `PULSO_RETENTION_MIGRATION_TIMEOUT_MS` | `600000` | Deadline for converting one existing manifest during enforcement, `30000..3600000`. Normal maintenance keeps `PULSO_RETENTION_TIMEOUT_MS`. |
| `PULSO_RETENTION_FUTURE_SKEW_MS` | `600000` | Maximum record time ahead of the node clock on retention-managed prefixes, `0..86400000`. |
| `PULSO_RETENTION_SWEEP_HORIZON_DAYS` | twice the recorded days plus two | Days of expired partitions revisited by orphan sweeps, `1..7302`. |
| `PHX_HOST` | `example.com` | Host name used when Pulso generates absolute URLs. Pulso does not currently emit any, so this can stay unset. |

Boolean variables accept `true`, `1`, or `yes`; anything else is false.

## Tenants and tokens

Every ingest and query request belongs to a tenant. HTTP requests name it in the
`X-Scope-OrgID` header, the convention Loki, Mimir, and Cortex clients already
support; requests without the header use the tenant `default`. Model Context
Protocol tools take the tenant as an argument instead. Tenant names use letters,
digits, `_`, `.`, and `-`, up to 128 characters.

Requests authenticate with `Authorization: Bearer <token>`. Pulso stores only the
SHA-256 digest of each token, so `PULSO_TENANT_TOKENS` never contains a usable
credential:

```sh
TOKEN=$(openssl rand -hex 32)
DIGEST=$(printf '%s' "$TOKEN" | shasum -a 256 | awk '{print $1}')
echo "{\"production\": \"sha256\$$DIGEST\"}"
```

Give the plaintext token to the collectors and agents for that tenant and put
the digest in `PULSO_TENANT_TOKENS`. A tenant that is not listed is rejected,
and so is a malformed digest, so a blank value cannot switch authentication off.

These ingest/query credentials give each tenant exactly one token. Alerting uses
separately configured per-principal credentials; a tenant-shared token does not
automatically grant alert administration. See [alerting](alerting.md).

Each tenant has exactly one ingest/query token. To rotate it, update the collectors and the
digest together, or briefly route the collectors through a second tenant name
while they roll over. Changing the variable requires a restart.

Data is isolated per tenant: a token for one tenant cannot read or write
another. Tenants are not a quota mechanism; [ingest limits](ingest-limits.md)
and query budgets apply to the whole node.

## Object storage

Pulso needs an S3-compatible bucket that supports conditional writes
(`If-None-Match: *` on create and `If-Match: <etag>` on replace). Amazon S3,
Cloudflare R2, MinIO, and RustFS support them; check your provider's
documentation before using another. Pulso refuses to report ready until it can list the bucket.

The credentials need these permissions on the bucket and its objects:

- `s3:ListBucket`
- `s3:GetObject`
- `s3:PutObject`
- `s3:DeleteObject`, used by metrics compaction to remove merged segments and by
  retention to remove expired data

An Amazon S3 policy for a bucket named `pulso-telemetry`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::pulso-telemetry"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::pulso-telemetry/*"
    }
  ]
}
```

Use a dedicated bucket. Pulso writes under `tenants/` and expects nothing else to
modify that prefix. Its readiness check lists the `.pulso/` prefix. Do not configure
bucket lifecycle rules that expire objects under `tenants/`: Pulso tracks live
objects in per-tenant manifests, so an object that disappears underneath it makes
queries fail. Use Pulso's own [retention](retention.md) to expire data instead.

## Ingest coalescing

Set `PULSO_INGEST_FLUSH_INTERVAL_MS=50` (chart `ingestBatching.flushIntervalMs: 50`)
to let concurrent unkeyed requests for the same tenant and signal share a Parquet
segment and manifest publication. It is disabled by default. Keyed requests
always use the original direct path, preserving their retry fingerprints.
Sequential producers cannot share a flush, so they gain no request savings.

Each node-local buffer reserves at most 128 callers, 100,000 rows, and 10 MiB of
estimated input term bytes, including work already executing. Oversized requests
and overflow go through the original unbuffered path, not an unbounded queue.
These bounds are not node-wide ingest rate limits or exact heap-memory ceilings;
encoding and the request processes need additional memory. Idle buffers terminate
after 30 seconds. Buffers for different tenants, signals, or storage configurations
are isolated.

The window adds up to its configured delay before storage I/O. Every request
still waits for the shared segment PUT and manifest conditional write before
acknowledgment. No local WAL is introduced; a node crash before acknowledgment
requires collector retry. Unkeyed delivery remains at-least-once: a lost response
and retry can duplicate data, just as on the unbuffered path. A shared storage
failure fails all requests in that flush; it never falls back to individual writes
after an ambiguous publication. Invalid encodings are isolated before upload.

Enable this on a test deployment first. Compare provider PUT counts, received and
acknowledged records, failures, node memory, and delivery latency. Larger source
batches and low concurrency may already make coalescing unnecessary. Disable it
by setting the window back to `0` and restarting nodes; stored data needs no
migration.

## Metrics compaction

With `PULSO_METRICS_COMPACTION_ENABLED=true`, Pulso merges small metrics segments
from the same hour into larger ones and deletes the originals after a one-hour
grace period. This reduces the number of objects queries have to read. Logs are
not compacted yet.

Before enabling it:

- **Run the same Pulso version everywhere that writes to the bucket.** Older
  versions do not understand the metadata compaction records, and mixing them
  after compaction has started is unsupported.
- **Turn on bucket versioning or back up the manifests.** After compaction, the
  per-tenant manifest is the only record of which objects are live. If it is lost,
  Pulso returns an error rather than guessing, and recovery means restoring a
  previous manifest version.
- **Keep node clocks synchronized.** The deletion grace period assumes clocks
  agree to well within an hour.

## Retention

By default Pulso keeps data indefinitely. Experimental event-time retention
expires logs and metrics after a configured number of days and deletes their
objects itself. Enforcing it converts manifests to a paged format that older
Pulso releases cannot read, and expired data cannot be restored. Read
[retention](retention.md) for the rollout procedure, modes, policy changes,
capacity limits and recovery before setting any `PULSO_RETENTION_*` variable.

## Fixed limits

Some limits are not configurable through the environment yet. Each node admits
four concurrent queries, at most two per tenant, and bounds each query's
duration, memory, and scanned data. Requests are capped at 4 MiB on the wire and
16 MiB after decompression. Requests over these limits fail with an explicit
error rather than a partial result.
