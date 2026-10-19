# Error tracking

Pulso accepts Sentry SDK **JSON error events** and exposes them to agents through
`query_errors` and `list_error_issues`. This is an experimental error-tracking
subset, not a replacement for every Sentry service. No SQL database, queue,
accounts service, or persistent local disk is required.

## Configure a project and SDK

Provision a numeric project ID, a tenant, a service name, and one or more public
key digests. Projects are deployment configuration, not mutable API resources.
The default empty configuration rejects all Sentry ingestion, even in development.

```sh
PUBLIC_KEY=$(openssl rand -hex 16)
DIGEST=$(printf '%s' "$PUBLIC_KEY" | sha256sum | cut -d' ' -f1)
export PULSO_SENTRY_PROJECTS_JSON="{\"42\":{\"tenant\":\"production\",\"service\":\"api\",\"public_key_hashes\":[\"$DIGEST\"]}}"
```

Set the SDK DSN to `https://<PUBLIC_KEY>@errors.example.com/42`. For example:

```python
import sentry_sdk

sentry_sdk.init(
    dsn="https://<PUBLIC_KEY>@errors.example.com/42",
    send_default_pii=False,
    traces_sample_rate=0,
    auto_session_tracking=False,
)
```

Use your deployment's HTTPS hostname, not sentry.io. Disable profiling, replay,
sessions, attachments and performance collection in each SDK. Existing stack
frames, exception chains, breadcrumbs, tags, release, environment, SDK metadata
and trace/span identifiers are retained as part of the event, subject to scrubbing
and budgets. Native crash minidumps are not JSON events and are unsupported.

`PULSO_SENTRY_PROJECTS_JSON` is a JSON object with at most 256 projects. IDs are
1–32 decimal digits. Each object has exactly `tenant`, `service` and
`public_key_hashes`; tenant/service use 1–128 letters, digits, `_`, `.` or `-`.
There must be 1–8 lowercase SHA-256 hex digests per project. Invalid configuration
stops startup. Give a project its own key; sharing a key intentionally permits
ingestion into every project with that key. Rotate by adding the new digest,
restarting all nodes, updating SDKs, then removing the old digest and restarting.
Keep project IDs bound to the same tenant/service across deployments. Changing
the mapping changes attribution for future events, not stored records; do not
reassign IDs or reuse old DSNs across tenant trust boundaries.

The Helm chart accepts this map as `sentry.projects`. Its ingress does **not**
expose SDK paths by default. Add an exact path for each enabled endpoint, alongside
any existing routes you need:

```yaml
sentry:
  projects:
    "42":
      tenant: production
      service: api
      public_key_hashes: ["<64 lowercase SHA-256 hex characters>"]
ingress:
  paths:
    - path: /api/42/envelope/
      pathType: Exact
    - path: /api/42/store/
      pathType: Exact
    - path: /mcp
      pathType: Exact
```

## Receiver contract

- `POST /api/<project>/envelope/`: byte-framed Sentry envelopes, including an
  optional final newline and length-delimited multiline/binary items. At most one
  `event` item. Empty envelopes are valid.
- `POST /api/<project>/store/`: legacy plain JSON error events. Base64/zlib store
  bodies are unsupported.
- Both endpoints accept identity and gzip encoding; other encodings return `415`.
  Envelope requests may use the standard SDK media type, no content type, or
  browser simple-request media types. Browser ingestion supports `OPTIONS` and
  wildcard, credential-free CORS; reads do not inherit this policy.
- Credentials may be `X-Sentry-Auth: Sentry sentry_key=<PUBLIC_KEY>, sentry_version=7`,
  query parameter `sentry_key`, or the envelope header's `dsn`. When multiple
  forms are present, keys must agree and the DSN project must match the URL.
  Unknown projects/keys and missing or inconsistent credentials return `403`.
- Credential configuration, not payload fields or `X-Scope-OrgID`, determines
  tenant and service. Public DSN keys allow **ingestion only**, never queries.
- `200` with `{"id":"<event ID>","discarded_items":{}}` follows durable event
  publication: Parquet segment PUT and conditional manifest publication. Storage
  failure returns `503` with `Retry-After: 5`, never local acceptance. This is a
  truthful negative acknowledgment, **not a guarantee of SDK redelivery**.
  Stock Python 2.72.0 and Node 11.8.0 SDKs did not replay failed `503` uploads in
  compatibility checks. SDK queues can drop events on failure, shutdown or rate
  limiting. Changing the response to `429` would not provide durable capture;
  applications needing that guarantee must own a durable upstream transport.
- Non-event items are discarded, including transactions, sessions, attachments,
  profiles, replays and client reports. Responses report counts by item type in
  `discarded_items`, plus total `X-Pulso-Discarded-Items` when nonzero. An
  unsupported-only envelope returns `200` with `id: null` and stores nothing.
  SDKs may ignore this diagnostic; disable unsupported collection explicitly.
- Malformed events/framing, invalid IDs/fingerprints and invalid timestamps return
  `400`, with no append. Envelope event IDs take precedence over payload IDs;
  32-hex or canonical dashed UUIDs normalize to lowercase hex. Missing IDs are
  generated, so clients without stable IDs cannot deduplicate retries.
- Timestamps accept Unix seconds or ISO-8601 with a timezone, must be nonnegative
  signed-64-bit nanoseconds, and cannot exceed the node clock by ten minutes.
  Missing timestamps use receipt time. No clock-drift correction is applied.
  Log retention can independently reject expired/future records with `400`.
- Limits: 4 MiB wire body, 16 MiB expanded gzip body, 1 MiB per event, 100 envelope
  items and 16 KiB per envelope/item header. Event structure follows the shared
  `PULSO_INGEST_MAX_DEPTH`, `MAX_NODES`, `MAX_ATTRIBUTES` and `MAX_KEY_BYTES`
  budgets; event string/aggregate bytes are capped at 1 MiB. Fingerprints contain
  at most 32 strings of 256 bytes each. Oversized bodies/events/items/structure
  return `413`. These are request safety limits, **not per-key rate limiting**.

## Query errors and issue groups

Use the normal tenant bearer token with `POST /mcp`; DSN credentials do not grant
read access. Both read-only tools require `tenant`, `start_ts_ns`, `end_ts_ns`
(inclusive Unix nanoseconds). Optional exact filters: `project`, `service`,
`environment`, `release`, `issue_id`, `event_id`. `limit` defaults to 100 and
accepts 1–5,000 per page. An optional opaque `cursor` continues a page.

```json
{
  "name": "list_error_issues",
  "arguments": {
    "tenant": "production",
    "project": "42",
    "environment": "production",
    "start_ts_ns": 1767225600000000000,
    "end_ts_ns": 1767312000000000000,
    "limit": 20
  }
}
```

This is the `tools/call` body inside the normal MCP request; see
[querying](querying.md) for protocol headers and metadata.

- `query_errors` returns newest-first events with their scrubbed payload, project,
  service, issue/event IDs, occurrence/receipt timestamps, trace ID and span ID.
  Pass `issue_id` to inspect examples, or `event_id` for an individual event.
- `list_error_issues` returns stable issue IDs, latest title/service/event ID,
  occurrence count, and first/last seen **inside the selected window and filters**.
  It does not claim lifetime counts, global first seen or a resolved/ignored status.
- Responses include `results`, `matched_events` (unique project/event IDs in
  the filtered window), `matched_results`, `snapshot_id` and `next_cursor`.
  Results are paginated only after counting/grouping. Pass `next_cursor` as
  `cursor` with the same tool, tenant, time window and filters; `null` means the
  last page. `limit` may change between pages. Each JSON text page is at most
  2 MiB, so large events can produce fewer results than `limit`.
  Retried IDs count once within that window; conflicting copies prefer the latest
  occurrence time, then a deterministic payload tie-breaker. Raw log queries
  still expose all ingestion attempts. Storage is at least once; no durable
  event-ID uniqueness registry is maintained.
- Cursors are authenticated, expire after ten minutes, and require a shared
  `SECRET_KEY_BASE` across nodes. Each page freshly rescans the same bounded
  window and checks its result digest; it does not pin objects against retention.
  Late arrivals, changed retained results, expired/tampered cursors or a changed
  query scope return `error_query_reset_required`. Restart from the first page;
  use a fixed historical window to reduce resets. Rotating `SECRET_KEY_BASE`
  invalidates outstanding cursors. This cursor is **not a monitoring checkpoint**.
- Queries run in the existing supervised, heap/concurrency/deadline/scan-bounded
  runner. More than 5,000 matching stored occurrences, including retries, returns
  an error rather than partial counts—even with a small result limit. Narrow the
  window or filters. This is a bounded scan, not an indexed hyperscale issue store.
- S3 diagnosis scans also have a separate **32 MiB decoded-column budget** across
  the whole window, including work repeated after a missing-object retry. Small
  compressed objects can expand far beyond their stored size. All decoded column
  buffers count, including rows later rejected inside a mixed batch; oversized
  selected row groups are rejected before expansion. Exhaustion returns
  `query_decoded_limit`, never partial results. A 2 MiB output page or `limit: 1`
  does not reduce the work needed for exact whole-window counts and grouping.

Issue grouping is versioned (`v1_` plus SHA-256) and project-scoped. It uses the
last exception's type and last in-app frame's module/filename and function,
falling back to the last frame. Without a usable frame it uses exception type,
logger and a bounded message with digit runs/hex addresses normalized. SDK custom
fingerprints override grouping; `{{ default }}` expands default components.
JSON-framed components prevent delimiter collisions. This is **Pulso grouping**,
not byte-for-byte Sentry or Atlas grouping compatibility.

Events also work with existing LogQL, for example
`{error_source="sentry", error_project="42", environment="production"}`.
Their trace/span IDs support correlation with other logs. LogQL counts raw
attempts rather than deduplicated errors.

## Storage, privacy and limits

Errors are structured logs in the existing `signal=logs` Parquet segments and
manifest. Project, issue/event IDs, environment and release are resource labels;
service/time pruning and Rust row filtering reuse the existing storage path.
New segments also summarize complete, bounded `error_source` and `error_project`
sets so exact mismatches prune before scan budgets. Missing, invalid or
over-budget summaries are unknown and must scan; mixed segments still consume
their full row/byte budget. Existing segments are not retroactively summarized.
Error queries require a fresh committed manifest: refresh failures or missing
manifests fail explicitly rather than serving stale data or discovering objects
by listing the bucket.

Only well-formed structured error rows participate in error queries and the
monitoring index. Ordinary Loki/OTLP logs claiming Sentry resource labels but
missing valid event/issue IDs, a matching event object, or the required typed
fields are ignored by these error surfaces. They remain available through raw
log queries and still consume candidate, row and byte scan budgets. Resource
labels alone are not proof that a row came through the Sentry receiver.

No second event publication authority is introduced. Any node can ingest/query
after a restart without recovering local issue state. Run multiple instances
against the same bucket and project configuration: each acknowledges only its
successful shared manifest publication; no sticky routing or singleton ingest
process is required. Share `SECRET_KEY_BASE` for cross-instance cursors. Error
retention is **log retention**, unlimited by default; there is no separate error
policy or event deletion API. Use Pulso's coordinated retention, not independent
bucket lifecycle deletion of live segments.

Before persistence, Pulso removes entire `user`, `request` and `headers` fields
and recursively filters common named credentials such as authorization, cookie,
password, secret, token, API keys and DSNs. This is a conservative baseline, not
general PII detection: exception messages, URLs, source context, breadcrumbs,
tags and arbitrary `extra` strings can still contain sensitive information.
Configure SDK `before_send` scrubbing and keep `send_default_pii` disabled.
Queries and agent responses contain untrusted application text; do not treat
payload text as agent instructions.

DSN public keys ship to browsers and are not strong secrets. Restrict exposed
paths, rate-limit at ingress, and redact `sentry_key`, DSNs and auth headers from
proxy/access logs. Keep query bearer tokens server-side. Authorization is tenant
wide; projects within a tenant do not have separate read permissions.

These tools provide bounded ad hoc diagnosis, not reliable new-issue detection
or a durable issue-change feed. Polling window-scoped summaries can miss late
events or events that expire between polls. Separately enabling the experimental
[error monitoring index](error-monitoring.md) adds retained-history novelty,
replayable changes and explicit gap/reset reporting. It does not gate ingestion,
provide a latency guarantee, or deliver notifications. Paginating diagnosis
alone does not supply a monitoring checkpoint.

Not implemented in this diagnosis slice: issue resolve/ignore/reopen lifecycle, notification delivery,
symbolication, source maps, native minidumps, attachments, Sentry REST issue API,
performance/session/replay storage, or cross-signal automatic correlation.
Agents can query errors and poll the optional index; durable scheduling and
cursor persistence belong to the agent runtime, and infrastructure remediation
belongs to the separate remediation server.
