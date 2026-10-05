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
| `PULSO_MCP_ALLOWED_ORIGINS` | none | Comma-separated browser origins allowed to call `POST /mcp`, such as `https://agent.example.com`. Requests that carry no `Origin` header, which is the case for most agents and servers, are always accepted; any other origin receives `403`. |
| `PULSO_INGEST_MAX_*` | see [ingest limits](ingest-limits.md) | Per-request record and attribute budgets for ingestion. |
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

Each tenant has exactly one token. To rotate it, update the collectors and the
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
- `s3:DeleteObject`, used by metrics compaction to remove merged segments

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
objects in per-tenant manifests and does not yet implement retention, so an
object that disappears underneath it makes queries fail.

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

## Fixed limits

Some limits are not configurable through the environment yet. Each node admits
four concurrent queries, at most two per tenant, and bounds each query's
duration, memory, and scanned data. Requests are capped at 4 MiB on the wire and
16 MiB after decompression. Requests over these limits fail with an explicit
error rather than a partial result.
