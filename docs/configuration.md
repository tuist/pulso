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
| `PULSO_TENANT_TOKENS` | JSON object mapping each tenant name to the SHA-256 digest of its bearer token. See [Tenants and tokens](#tenants-and-tokens). May be empty (`{}`) only when `PULSO_WORKLOAD_IDENTITY_JSON` configures [workload identity](#workload-identity) or the [tenant registry](tenants.md#tenant-registry) is enabled. |
| `PULSO_S3_BUCKET` | Bucket that stores every acknowledged record. |
| `PULSO_S3_REGION` | Region passed to the S3 client. Providers that ignore regions still need a value, such as `auto` for Cloudflare R2. |
| `PULSO_S3_ACCESS_KEY_ID` | Access key for the bucket. |
| `PULSO_S3_SECRET_ACCESS_KEY` | Secret key for the bucket. |

For object storage, Pulso only uses the static credentials above. It does not
read instance profiles, cloud workload identity, or other ambient cloud credentials.

## Optional

| Variable | Default | Meaning |
| --- | --- | --- |
| `PORT` | `4000` | HTTP listener port. |
| `PULSO_OTLP_GRPC_PORT` | unset | Enables the [OTLP/gRPC receiver](otlp-grpc.md) on this port (conventionally `4317`). Unset keeps it disabled. |
| `PULSO_LOKI_TAIL_MAX_PER_TENANT` | `10` | Atomic concurrent [Loki live-tail](querying.md#live-tail) admission per tenant on each instance, including history preparation. Further tails return HTTP 429; connection termination releases its slot. Not a deployment-wide limit. |
| `PULSO_LOKI_TAIL_POLL_INTERVAL_MS` | `1000` | How often an instance with open [live tails](querying.md#live-tail) reads a tenant's log manifest for pushes other instances received (100 to 60000). |
| `PULSO_OTLP_GRPC_IP` | `0.0.0.0` | Address the OTLP/gRPC receiver binds. |
| `PULSO_OTLP_GRPC_MAX_MESSAGE_BYTES` | `16777216` | Largest decompressed OTLP/gRPC message; larger messages fail permanently. |
| `PULSO_OTLP_GRPC_MAX_IN_FLIGHT` | `64` | Admitted OTLP/gRPC calls receiving/ingesting per instance; further calls wait within their deadline (HTTP/2 backpressure). |
| `PULSO_OTLP_GRPC_TIMEOUT_MS` | `30000` | End-to-end OTLP/gRPC deadline including admission wait, body completion and storage; expires as retryable `UNAVAILABLE`. A BEAM-handler watchdog fires this interval plus 1 second after handoff; it does not cancel already-running native/provider work. |
| `PULSO_S3_ENDPOINT` | Amazon S3 | URL of an S3-compatible endpoint, for example `https://<account>.r2.cloudflarestorage.com`. |
| `PULSO_S3_ALLOW_HTTP` | `false` | Allow a plain-HTTP storage endpoint. Use only for storage on a private network. |
| `PULSO_METRICS_FOOTER_PRUNING_ENABLED` | `false` | Optional bounded suffix-GET exclusion for ordinary metric queries. Requires strong ETags, suffix Range and GET If-Match; may add GETs/earlier scan rejection. See [provider prerequisites and bounds](metric-footer-pruning.md). |
| `PULSO_MANIFEST_FORMAT` | `paged` | Manifest format for prefixes without enforced retention. `paged` keeps the conditionally written root bounded and converts flat manifests on their next write, so ingest cost and manifest memory no longer grow with history. `flat` keeps writing flat lists; use it while older releases still write to the bucket. Read [Manifest format](#manifest-format) first. |
| `PULSO_METRICS_COMPACTION_ENABLED` | `false` | Merge small metrics segments in the background. Read [Metrics compaction](#metrics-compaction) first. |
| `PULSO_TEMPO_SEARCH_ENABLED` | `false` | Opt in to experimental Tempo `/api/search`, resource/span/instrumentation/event/link tag discovery (names/values optionally filtered on parent-span strings; filtered names are complete conservative hints, not matching-key proofs) and corresponding MCP tools. Complete-context search may use publication-proven exact resource.service.name candidate-source pruning plus conservative trace-ID context selection; unknown/legacy sources still read, without time clipping. Retained catalog metadata/page/heap bounds can bind before the hard 8192-reference ceiling; actual fetched inputs cumulatively cap at 1024 / 32 MiB / 1,000,000 raw rows, plus 65,536 matching-or-legacy-unknown IDs and independent output bounds. Index-probe exhaustion reads conservatively and can fail late; unread irrelevant objects are not scrubbed. Requires metadata2/export3/search5/discovery12, tuple `{2,1,3,5,12}`, on every serving artifact. Representative postings/capacity/client acceptance remains pending. Discovery does not use this service/ID plan. Discovery also caps requested-owner-plus-attribute work at 2,000,000 per scan attempt before filtering, lowering flat-scope capacity as attribute counts grow. Search always returns the most recent matching traces, so `with (most_recent=true)` is optional (Grafana omits it); bounded same-span negated-regex/status/int64 OR/span-duration and unscoped span-first expressions use full-scope admission without the service planner and an additional 20M prefilter work ceiling per scan attempt; see [expression semantics](trace-query-expressions.md). Static intrinsic names/seven v2 enum-or-empty vocabularies require the same gate/admission/auth but read no storage and prove no stored presence. Default names combine all five namespaces plus static v2 suggestions under one 2M-work/5000-scope-name-pair budget (20 intrinsic reservations apply even for v1 flattening), with service.name hints even unfiltered and without original attrs; storage errors fail whole rather than emitting partial suggestions. None values combine resource/span/instrumentation originals, not all name scopes. Bounded resource/span string filters select one parent before three-owner union/work, a complete extension rather than Tempo's ignored-query behavior. Same-key equality, nonstring/nested/intrinsic filters reject; values retain original types, matching complex targets fail, and legacy/corruption fail even on nonmatches. Filtered-none needs query-aware unscoped execution or bounded reference, plus separate explicit windowed-filtered-none support; old unfiltered/window callbacks cannot silently ignore the new mode. Floats (including non-finite classes) and seven stored intrinsic strings and event:timeSinceStart exact signed int nanosecond offsets read bounded data, accepting same-parent string filters; event-time filters are a disclosed complete extension versus upstream's empty path. Effective event time0 may yield negative offsets; no events invent no values. Invalid requested event times fail even on nonmatches and parent+all event attrs charge before filtering. New explicit event-time and separate windowed-event-time execution is required, never old string/window callbacks. These are distinct from storage-free static vocabulary. Paired positive discovery time bounds select retention-visible immutable segment envelopes, not span starts; all raw header/provider costs still apply, no indexed I/O gain or cross-window dictionary cache. Compaction can broaden suggestions, excluded payloads are not integrity-scrubbed. Matching capability-12 artifacts are required on every serving instance; flat/old adapters reject unsupported windows. Root name/service dictionaries select parent trace IDs then recover complete retained context, including roots outside predicate/window. Missing/ambiguous roots or missing original string root service fail409 whole, never default/skip. In-flight children-before-root and retention-straddling/multi-root traces can make broad/recent autocomplete unusable; this is a strict default-off extension, not transparent replacement. Root S3 input is the whole retained tenant, capped1024 objects/32MiB compressed/1M raw rows regardless of window/matches; no service/ID planner, local replay decodes all sources without another GET and cross-checks header/body rows. 65,536 candidates/16MiB bookkeeping,2M resource/span/link owner+attr work plus normal dictionary/result limits; dedicated root/window support required. Canonical trace:duration also measures complete retained max-end/min-start, without requiring roots. It may grow during ingest or shrink with retention, not exporter duration. Unfiltered emits int nanoseconds; any supported parent filter emits Go-style duration strings (including µs); both sort lexically. Do not cache across filter/type changes. Root capture/input/replay limits remain; duration work is2 resource/span owners+attrs,65,536 groups/80B per-summary charge within16MiB charged bookkeeping (not total native memory),5000 values/independent1MiB dictionaries/results. Broad nanosecond dictionaries commonly fail422. Dedicated duration/window support fences old root/string callbacks. Static duration/span:duration/traceDuration empty vocabularies remain unchanged. Zero/partial discovery windows, stored span-duration intrinsics, exact upstream distinct-omission/dynamic dedicated-column parity, full TraceQL and deployed-client parity remain unsupported. HTTP metadata search accepts omitted bounds or paired zeros for all retained context with unchanged ceilings; partial search windows reject. See [Tempo search](traces.md#experimental-tempo-search). |
| `PULSO_LOG_COMPACTION_ENABLED` | `false` | Merge small log segments natively in the background. Read [Log compaction](#log-compaction) first. |
| `PULSO_TRACE_COMPACTION_ENABLED` | `false` | Experimental native trace merges and retirement cleanup. Requires all readers/writers/compactors upgraded and bucket versioning; see [trace compaction](traces.md#optional-native-compaction). |
| `PULSO_SENTRY_PROJECTS_JSON` | `{}` | Ingest-only Sentry project map: numeric ID to `tenant`, `service`, `public_key_hashes` (1–8 lowercase SHA-256 digests), optional `allowed_domains` (up to 32 patterns of 1–256 UTF-8 bytes). Omitted origin policy is unrestricted; `[]` rejects Origin-bearing requests but permits authenticated SDKs without Origin. At most 256 projects. Empty disables ingestion. See [error tracking](error-tracking.md). |
| `PULSO_ERROR_PROJECTION_ENABLED` | `false` | Enable the experimental S3-derived issue index, bounded error-rate detector and replayable change feed. Independent of ingestion and diagnosis; see [error monitoring](error-monitoring.md). |
| `PULSO_ERROR_HISTORY_DAYS` | `1` | Derived error history in whole days, integer `1..365`. Newness and counts are limited to this horizon and raw log retention. Changing it rebaselines the index. |
| `PULSO_ERROR_PROJECTION_INTERVAL_MS` | `5000` | Derived-index worker cadence, integer `1000..60000`. Each pass visits one configured project and at most 32 new segments; catchup and freshness depend on project count and storage latency. |
| `PULSO_WORKLOAD_IDENTITY_JSON` | `[]` | Trusted Kubernetes ServiceAccount token issuers with inline or URL-fetched keys and subject bindings. See [Workload identity](#workload-identity). |
| `PULSO_ALERTING_PRINCIPALS_JSON` | `[]` | Dedicated alerting principal array with `tenant`, `id`, `type`, `token_hash` and `capabilities`; at most 256 entries. See [alerting credentials](alerting.md#configure-credentials-and-evaluation). |
| `PULSO_ALERTING_EVALUATION_ENABLED` | `false` | Enable experimental native threshold/recording evaluation and durable recording outbox draining. Grafana execution remains unsupported; native Slack delivery has a separate opt-in. |
| `PULSO_ALERTING_NOTIFICATIONS_ENABLED` | `false` | Enable native Slack delivery from committed bounded outboxes. See [delivery semantics](alerting.md#native-slack-delivery). |
| `PULSO_ALERTING_NOTIFICATION_TARGETS_JSON` | `[]` | Operator-provisioned target descriptors (`tenant`, `id`, `type: slack_webhook`, `secret_env`), at most 256. Webhook URLs must live in the referenced environment secrets, not in descriptors. |
| `PULSO_ALERTING_SOURCE_BINDINGS_JSON` | `[]` | Explicit read-only imported graph mappings (`tenant`, original datasource `uid`, `type: native_promql` or `native_logql`), at most 256 unique pairs; no URLs. Same-tenant native Prometheus instant or metric-producing LogQL instant/range previews only. Does not enable imports or prove residency; see [graph previews](alerting.md#preview-an-imported-expression-graph). |
| `PULSO_ALERTING_POLL_INTERVAL_MS` | `5000` | Native evaluator discovery interval, integer `1000..60000`; measure object-store read cost before reducing it. |
| `PULSO_MCP_ALLOWED_ORIGINS` | none | Comma-separated browser origins allowed to call `POST /mcp`, such as `https://agent.example.com`. Requests that carry no `Origin` header, which is the case for most agents and servers, are always accepted; any other origin receives `403`. |
| `PULSO_MEMORY_BUDGET_MB` | container memory limit | Memory budget, `256..1048576` MiB, that sizes ingest and query slots when they are not set explicitly. Production releases read the container's cgroup limit when it is unset; without a limit the fixed defaults below apply. See [Node memory envelope](#node-memory-envelope). |
| `PULSO_MEMORY_PRESSURE_PERCENT` | `85` | Share of the memory limit, `0..99`, at which the node sheds memory: new interactive queries get a retryable overload error, and idle manifest roots are evicted, until resident memory has stayed 10 points below it for 10 s. `0` disables it. The limit is `PULSO_MEMORY_BUDGET_MB`, else the container's cgroup limit (re-read every 15 s). See [Memory pressure](scaling.md#memory-pressure). |
| `PULSO_MANIFEST_CACHE_MAX_BYTES` | `134217728` | Byte bound on encoded manifest roots cached per node, `1048576..68719476736`. Least recently used roots above it are evicted and reloaded on next use; the most recently used root is always kept. |
| `PULSO_INGEST_MAX_CONCURRENT_REQUESTS` | derived, else `32` | Node-local bound on ingest requests processed at once over HTTP and gRPC, `1..100000`, shared between signals; derived from the memory budget when unset. Excess requests get a retryable `429` (gRPC `RESOURCE_EXHAUSTED`) with `Retry-After: 1`. See [Ingest admission](#ingest-admission). |
| `PULSO_INGEST_RESERVED_REQUESTS_PER_SIGNAL` | an eighth of the slots, at least 1 | Slots that metrics, logs and traces each keep for themselves, `0..PULSO_INGEST_MAX_CONCURRENT_REQUESTS/3`. The rest are shared. See [Ingest admission](#ingest-admission). |
| `PULSO_INGEST_MAX_PENDING_BYTES` | `67108864` | Estimated memory of coalesced requests that have released their slot while they wait for durable publication, `0..17179869184`; `0` disables the handoff. See [Ingest admission](#ingest-admission). |
| `PULSO_TENANT_LIMITS_JSON` | `{}` | Per-tenant ingest concurrency, ingest byte rate and query concurrency, per instance. See [per-tenant limits](scaling.md#per-tenant-limits). |
| `PULSO_ADMIN_TOKENS` | `[]` | JSON array of `sha256$<hex>` digests of admin Bearer tokens for `/api/v1/admin`; empty hides the admin API. See [admin API](tenants.md#admin-api). |
| `PULSO_TENANT_REGISTRY_ENABLED` | `false` | Runtime tenant provisioning, suspension and deletion through the admin API, stored in the bucket. Needs S3 storage. See [tenant registry](tenants.md#tenant-registry). |
| `PULSO_TENANT_REGISTRY_CACHE_TTL_MS` | `30000` | How long instances cache a tenant's record, `1000..300000`; state changes reach every instance within it. |
| `PULSO_TENANT_REGISTRY_MAX_LOOKUPS_PER_SECOND` | `100` | Bucket reads per instance per second for uncached tenants, `1..100000`; beyond it, uncached tenants get a retryable `503`. |
| `PULSO_TENANT_DELETION_GRACE_MS` | `600000` | Wait between a deletion request and the purge of the tenant's data, `60000..2592000000`, at least twice the cache TTL. See [deletion](tenants.md#deletion). |
| `PULSO_TENANT_PURGE_INTERVAL_MS` | `60000` | How often maintenance instances purge deleted tenants, `1000..3600000`. |
| `PULSO_USAGE_ENABLED` | `false` | Per-tenant usage metering written to `usage/v1/` in the bucket. Needs S3 storage. See [usage metering](tenants.md#usage-metering). |
| `PULSO_USAGE_FLUSH_INTERVAL_MS` | `60000` | How often each instance writes its usage, `1000..3600000`; a crash can lose at most this much usage. |
| `PULSO_ROLES` | `ingest,query,maintenance` | Comma-separated work this instance takes on. Requests for a role it does not run get a retryable `503`; background workers (compaction, retention, alerting, error projection, reliability) run only with `maintenance`. See [scaling](scaling.md#separate-pools). |
| `DNS_CLUSTER_QUERY` | none | DNS name whose A records list the other instances, to join one Erlang cluster for rendezvous ownership. Set `RELEASE_DISTRIBUTION=name`, `RELEASE_NODE=pulso@<pod IP>` and a shared `RELEASE_COOKIE` too; the chart does this with `clustering.enabled`. |
| `PULSO_INGEST_FLUSH_INTERVAL_MS` | `0` | Optional coalescing window for concurrent appends without an idempotency key; integer `0..1000`, where `0` disables it. See [Ingest coalescing](#ingest-coalescing). |
| `PULSO_INGEST_MAX_*` | see [ingest limits](ingest-limits.md) | Per-request source-record, attribute and request-node budgets for ingestion. OTLP distributions also cap expanded float samples at `PULSO_INGEST_MAX_RECORDS`; the separate fixed 16 MiB repeated-source metadata ceiling is unchanged. |
| `PULSO_INGEST_MAX_OTLP_NODES` | `262144` | Request-wide log/metric JSON-shaped node ceiling, independent of per-set nodes. Applies to JSON/protobuf logs and metrics; see [OTLP logs](logs.md) and [OTLP metrics](metrics.md). |
| `PULSO_RETENTION_ENABLED` | `false` | Run the experimental retention and cleanup worker even with all durations at `0`; it also runs whenever a duration is positive. See [retention](retention.md). |
| `PULSO_LOGS_RETENTION_DAYS` | `0` | Log event-time retention in whole days, `0..3650`. `0` stops advancing the floor; without earlier enforcement, logs are kept indefinitely. |
| `PULSO_METRICS_RETENTION_DAYS` | `0` | Metric event-time retention in whole days, `0..3650`. `0` stops advancing the floor; without earlier enforcement, metrics are kept indefinitely. |
| `PULSO_TRACES_RETENTION_DAYS` | `0` | Trace retention by span start in whole days, `0..3650`. Uses the same irreversible floor, paged catalogs, grace and cleanup as other signals. |
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

## Reliability assessments

Experimental periodic model assessments are independently opt-in using
`PULSO_RELIABILITY_ENABLED`, an explicit `PULSO_RELIABILITY_TENANTS_JSON` enrollment,
a secret `PULSO_RELIABILITY_OPENAI_API_KEY` and an operator-approved
`PULSO_RELIABILITY_PRICE_VALID_UNTIL` ISO date. Assessment, estimated object work
and external investigation have separate per-tenant monthly allowances. See the
[complete variable table, defaults, disclosure policy and limits](reliability.md#enable-assessments).
The chart's `reliability` block sets non-secret fields; Secret-backed `extraEnv`
supplies credentials. This does not change raw retention or ingest durability.

## Tenants and tokens

Every ingest and query request belongs to a tenant. Sentry SDK ingestion maps
project credentials to tenant/service instead of trusting tenant headers; its
public keys never grant read access. Other HTTP requests name the tenant in the
`X-Scope-OrgID` header, the convention Loki, Mimir, and Cortex clients already
support; requests without the header use the tenant `default`, unless their
credential comes from a [trusted issuer](issuer-credentials.md), which names the
tenant itself. Model Context Protocol tools take the tenant as an argument
instead. Tenant names use letters, digits, `_`, `.`, and `-`, up to 128
characters.

For a hosted deployment with many tenants, configure `PULSO_ISSUER_JSON`
instead of per-tenant tokens: see [issuer credentials](issuer-credentials.md).

Requests authenticate with `Authorization: Bearer <token>`, or with HTTP Basic
credentials whose username is the tenant and whose password is the token, the
convention Grafana Cloud clients, Alloy destinations and Grafana datasources
use. When a Basic request has no `X-Scope-OrgID` header, its username names the
tenant; Basic credentials never authorize a different tenant. Pulso stores only the
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

## Workload identity

Services running in Kubernetes can authenticate with a projected
ServiceAccount token instead of a tenant token. They send it as
`Authorization: Bearer <jwt>` on every request. Pulso verifies the token
against the issuing cluster's public signing keys. You either pin those keys
inline, which suits other clusters because their JWKS endpoint never has to
be reachable, or let Pulso fetch them from a JWKS URL, which suits its own
cluster (see [JWKS URL key sources](#jwks-url-key-sources)).

`PULSO_WORKLOAD_IDENTITY_JSON` is a JSON array of up to 64 trust entries:

| Field | Meaning |
| --- | --- |
| `name` | Unique label for the entry, used in logs. |
| `issuer` | Exact `iss` claim, for example `https://kubernetes.default.svc.cluster.local`. |
| `audiences` | Accepted `aud` values. A token must carry at least one. |
| `max_token_ttl_seconds` | Optional, `600..86400`, default `3600`. Tokens whose `exp - iat` is longer are rejected. |
| `keys` | `{"type": "inline", "jwks": ...}`: the issuing cluster's public signing keys, as a JWKS object or as the JWKS document in a JSON string (the verbatim output of `/openid/v1/jwks`). Or `{"type": "url", ...}`, described in [JWKS URL key sources](#jwks-url-key-sources). |
| `bindings` | Up to 1024 objects `{"subject", "tenant", "capabilities"}`. `subject` is an exact `system:serviceaccount:<namespace>:<name>`; `capabilities` is `["read"]`, `["ingest"]` or both. |

```json
[
  {
    "name": "atlas-production",
    "issuer": "https://kubernetes.default.svc.cluster.local",
    "audiences": ["pulso.tuist.dev"],
    "max_token_ttl_seconds": 3600,
    "keys": {"type": "inline", "jwks": {"keys": [
      {"kty": "RSA", "alg": "RS256", "use": "sig", "kid": "<kid>", "n": "<modulus>", "e": "AQAB"}
    ]}},
    "bindings": [
      {"subject": "system:serviceaccount:atlas-production:atlas", "tenant": "tuist", "capabilities": ["read"]}
    ]
  }
]
```

`read` covers every query API and the MCP endpoint. `ingest` covers the
push and write APIs (OTLP over HTTP and gRPC, Loki push, and Prometheus remote
write). Tenant tokens keep working alongside workload identity and still grant
both capabilities. Alerting administration keeps its own
[principals](alerting.md).

Many clusters share kubeadm's default issuer string, so Pulso does not treat
the issuer as proof of origin. It identifies the trusted cluster by the pinned
key that verifies the signature, then checks that entry's issuer, audience
and bindings. Inline key IDs must be unique across entries. Pulso accepts a
token only when all of these hold:

* The header uses `RS256`, names a `kid` that matches a pinned or fetched
  key, and has no `crit` parameter. `none`, HMAC and every other algorithm are
  rejected.
* The signature verifies against exactly one such key. A key that verifies for
  two entries, such as one pinned inline and also served by a URL, is
  ambiguous and rejected. Pinned keys must be RSA, with a
  modulus of at least 2048 bits and an odd exponent of at least 3. Keys that
  include private material are refused at startup.
* `iss` matches exactly, and `aud` contains an accepted audience.
* `exp` and `iat` are integers, and so is `nbf` if present. The token has not
  expired and is already valid, with 60 seconds of clock skew. Its lifetime,
  `exp - iat`, is within `max_token_ttl_seconds`.
* `sub` is a ServiceAccount subject whose namespace and name match the
  `kubernetes.io` claims, and a binding names that subject.

A token that fails any check gets `401 invalid_token`. A valid token with no
binding for the requested tenant and capability gets `403 forbidden`. A token
that no key verifies while a URL source has no usable keys gets
`503 auth_unavailable` (gRPC `UNAVAILABLE`), so clients retry instead of
treating their credential as bad. Pulso
logs the reason and the trust entry name, never the token. Invalid
configuration stops the node at startup.

Verification is offline, so Pulso cannot revoke a token. Deleting the pod or
ServiceAccount does not invalidate a token that has already been issued: a
leaked token stays valid until its `exp`, plus the skew. Keep
`expirationSeconds` and `max_token_ttl_seconds` short. To cut off a principal
immediately, remove its binding and restart Pulso.

Bearer values shaped like a JWT, three base64url segments whose header is a
JSON object with `alg`, go to workload identity whenever trust entries are
configured. Other values go to tenant-token checks. Random tenant tokens never
have this shape.

Read the issuing cluster's keys with a credential that may access the
ServiceAccount issuer discovery endpoint:

```sh
kubectl --context <issuing-cluster> get --raw /openid/v1/jwks
```

Configure the workload with a projected token for Pulso's audience:

```yaml
volumes:
  - name: pulso-token
    projected:
      sources:
        - serviceAccountToken:
            audience: pulso.tuist.dev
            expirationSeconds: 3600
            path: token
```

The kubelet rotates the file before expiry, so read it again for each request
or when it changes. When the issuing cluster rotates its signing key, add the
new key next to the old one, roll Pulso, and remove the old key once tokens
signed with it have expired. Pasting the JWKS verbatim works only while every
key in it is RSA. A cluster that also publishes EC keys fails startup, so pin
only its RSA keys.

### JWKS URL key sources

Workloads in Pulso's own cluster, such as Grafana Alloy, can be verified
against keys Pulso fetches from the API server instead of keys pinned in
values. Kubernetes key rotation then needs no Pulso change.

```json
{
  "name": "local-cluster",
  "issuer": "https://kubernetes.default.svc.cluster.local",
  "audiences": ["pulso.tuist.dev"],
  "keys": {
    "type": "url",
    "url": "https://kubernetes.default.svc/openid/v1/jwks",
    "ca_file": "/var/run/secrets/pulso/kubernetes-api/ca.crt",
    "token_file": "/var/run/secrets/pulso/kubernetes-api/token",
    "refresh_interval_seconds": 300
  },
  "bindings": [
    {"subject": "system:serviceaccount:monitoring:alloy", "tenant": "tuist", "capabilities": ["ingest"]}
  ]
}
```

| Field | Meaning |
| --- | --- |
| `url` | `https` JWKS URL. Redirects are not followed. |
| `ca_file` | Optional PEM CA bundle that must have signed the server's certificate. Without it, the system trust store is used. |
| `token_file` | Optional file whose contents Pulso sends as `Authorization: Bearer` on each fetch. Pulso reads it again on every fetch, so rotated tokens are picked up. |
| `refresh_interval_seconds` | Optional, `60..3600`, default `300`. |

Pulso checks at startup that the URL is `https` and that `ca_file` and
`token_file` exist, but it does not need the URL to be reachable. Each
instance fetches the keys itself and caches them in memory:

* It fetches once at startup and then every `refresh_interval_seconds`, with a
  5-second timeout and a 1 MiB response limit.
* Fetched keys stay usable for three refresh intervals after the last
  successful fetch. A short issuer outage does not reject traffic, and a key
  the issuer stops publishing stops verifying within that time.
* A token with an unknown `kid` triggers an immediate refresh, so new signing
  keys work at once. These refreshes happen at most once every 30 seconds per
  source, so forged tokens cannot flood the issuer.
* Keys Pulso cannot use, such as EC keys, are skipped. A document with no
  usable RSA key counts as a failed fetch.
* Failed fetches are logged with the trust entry name and reason, never the
  credential.

On Kubernetes, the API server's `/openid/v1/jwks` needs an authenticated
caller. The default `system:service-account-issuer-discovery` binding allows
any ServiceAccount, including Pulso's. Set
`workloadIdentity.kubernetesApiToken.enabled` in the Helm chart to mount a
projected API token and the cluster CA at the paths above. That leaves
`serviceAccount.automount` off. Use `https://kubernetes.default.svc` rather
than the issuer URL, because the API server certificate covers that name.

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

The provider must honor conditional creation (`If-None-Match: *`) for keyed
segments as well as manifest conditional writes. New S3 nodes require matching
native object-store API `{1,1}` (retention, keyed publication). Upgrade/drain every
writer, including recording evaluators; local startup cannot fence older writers
in another pool. See [keyed ingestion](keyed-ingestion.md) before changing writer
versions or retrying a permanent HTTP409 byte conflict.

Use a dedicated bucket. Pulso writes under `tenants/` and expects nothing else to
modify that prefix. Its readiness check lists the `.pulso/` prefix. Do not configure
bucket lifecycle rules that expire objects under `tenants/`: Pulso tracks live
objects in per-tenant manifests, so an object that disappears underneath it makes
queries fail. Use Pulso's own [retention](retention.md) to expire data instead.

## Node memory envelope

Each node bounds its memory with admission slots rather than relying on load
staying low. When `PULSO_MEMORY_BUDGET_MB` is set, or a production release runs
under a container memory limit, Pulso derives the slots from that budget:

- 256 MiB is reserved for the runtime, caches and manifests;
- half of the rest backs ingest slots at 32 MiB each (2 to 256 slots);
- the other half backs query workers: interactive slots plus one alerting and
  one reliability worker, each with a heap ceiling of up to 128 MiB (1 to 64
  interactive slots).

| Memory limit | Ingest slots | Interactive query slots | Query heap |
| --- | --- | --- | --- |
| 512 MiB | 4 | 2 | 32 MiB |
| 1 GiB | 12 | 3 | 76 MiB |
| 2 GiB | 28 | 5 | 128 MiB |
| 4 GiB | 60 | 13 | 128 MiB |

`PULSO_INGEST_MAX_CONCURRENT_REQUESTS`, `PULSO_QUERY_MAX_CONCURRENT` and
`PULSO_QUERY_MAX_HEAP_MB` override individual values. Requests beyond the slots
get retryable overload errors, counted by `pulso_ingest_admission_total` (ingest)
and `pulso_admission_rejections_total` (queries and tenant limits),
and the slot gauges show saturation for autoscaling; see [scaling](scaling.md).
Flat manifests (`PULSO_MANIFEST_FORMAT=flat`) still grow with history and are
not covered by the reserve; see the [sizing formula](scaling.md#sizing-formula)
and the `pulso_memory_bytes` breakdown on `/metrics`.

## Ingest memory

Per-request limits cap one request (4 MiB on the wire, 16 MiB decompressed, plus
record and attribute budgets), but exporters send in parallel, and replay their
retry queues in bursts after a restart. `PULSO_INGEST_MAX_CONCURRENT_REQUESTS`
(chart `ingestBatching.maxConcurrentRequests`) bounds how many ingest requests a
node decodes and appends at once; further requests get a retryable `429` with
`Retry-After: 1`, which Grafana Alloy, the Collector and Prometheus honor. It is
a per-instance bound, not a cluster limit: add instances to add capacity.

Budget roughly 32 MiB per slot for worst-case requests (compressed and
decompressed bodies plus decoded records); typical exporter batches use far less.
Under a container memory limit the slot count is derived from it (see
[Node memory envelope](#node-memory-envelope)); without one the default is 32,
which a 1 GiB node only survives for typical batches. With
[ingest coalescing](#ingest-coalescing) enabled, add
`PULSO_INGEST_MAX_PENDING_BYTES` (64 MiB by default) for requests that wait for
publication without a slot.

### Ingest admission

Slots are shared fairly between three signal classes: metrics (remote write,
OTLP metrics), logs (Loki push, OTLP logs, Sentry) and traces (OTLP traces).
Each class keeps `PULSO_INGEST_RESERVED_REQUESTS_PER_SIGNAL` slots (chart
`ingestBatching.reservedRequestsPerSignal`, by default an eighth of the slots
and at least one when there are three or more)
that the others cannot take. The remaining slots are a shared pool that any class
can borrow from. A remote-write replay after an outage can therefore use its
reserve plus the whole pool and receive `429`s beyond that, while Loki pushes and
trace exports still get their reserved slots. Loki's `loki.write` drops a batch
after its retries run out, so do not set the reserve to `0` unless logs have
another path. With 32 slots the default is 4 reserved per signal and 20 shared,
so a single signal can use up to 24 slots; the 12 slots derived for a 1 GiB
limit give 1 reserved per signal and 9 shared.

With coalescing enabled, a request holds its slot only while it is decoded and
queued. Once its records are in the coalescing buffer, it moves to a node-wide
pending budget (`PULSO_INGEST_MAX_PENDING_BYTES`, chart
`ingestBatching.maxPendingBytes`) and waits there for the durable write. Its
charge is the memory the waiting request actually retains plus the buffer's copy.
When that budget is full, new requests keep their slots while they wait, so memory
stays bounded by the slots plus the budget. This only changes how long a request
holds a slot, not when it is acknowledged; see
[Ingest coalescing](#ingest-coalescing).

Within each signal, slots are also shared between tenants, identified by the
`X-Scope-OrgID` header or the Basic auth username (each Sentry project counts
separately). A tenant is active for a signal while it holds a slot or has
requested one in the last 5 seconds. Each active tenant can always use its equal
share of the signal's slots (reserve plus shared pool). A tenant above that share
is admitted only while at least one slot stays free for every other active
tenant. A tenant replaying a backlog therefore gets `429`s for the slots another
tenant needs, and keeps the rest of the capacity when it is alone. The tenant is
read before authentication, so a client can choose its key; requests still fail
authentication afterwards, and each node tracks at most 1,024 tenants per signal,
with any further tenants sharing one bucket. Tenant shares apply to admission
slots, not to the pending budget.

Watch `pulso_ingest_admission_total{signal,outcome}` (`admitted`, `rejected`,
`tenant_limited`, `handed_off`), `pulso_ingest_admission_in_flight{signal}`,
`pulso_ingest_admission_active_tenants{signal}` and
`pulso_ingest_admission_pending_bytes`; see [self-monitoring](self-monitoring.md).
`rejected` counts every refusal; `tenant_limited` is the part refused while free
slots were kept for other tenants. A steady `rejected` rate for one signal means
its exporters retry: add instances, raise the slots together with memory, or
raise that signal's reserve.

Manifests are the other memory consumer that grows. With retention disabled,
each signal's manifest is a flat list with one entry per append, kept forever.
Every publication rewrites it (reusing each unchanged entry's encoding, which
keeps about one more manifest's worth of bytes in memory per tenant and signal)
and every query decodes it, so memory and CPU grow with history (roughly 1 MiB of encoded manifest per hour per signal at about two
appends a second, more for traces). Pulso logs a warning once a flat manifest
passes 20,000 segments. For a long-running deployment, enable
[retention](retention.md) so manifests become bounded pages, enable compaction to
reduce segment counts, and consider [ingest coalescing](#ingest-coalescing).

## Ingest coalescing

Set `PULSO_INGEST_FLUSH_INTERVAL_MS=20` (chart `ingestBatching.flushIntervalMs: 20`)
to let concurrent unkeyed requests for the same tenant and signal share a Parquet
segment and manifest publication. It is disabled by default. Keyed requests
always use the original direct path, preserving their retry fingerprints.
Sequential producers cannot share a flush, so they gain no request savings.

Flushes are pipelined. A batch is flushed when its window expires or when it
reaches a quarter of the buffer bounds, whichever comes first. Up to 16 flushes
per tenant and signal upload their segments at the same time, and the manifest
owner folds every segment that finished uploading into the next conditional
manifest write. A node therefore never waits for one flush's manifest write
before it starts the next upload.

Manifest writes for one tenant and signal happen one at a time, so a segment
that is ready while a write is in flight waits for it. The window therefore
adapts: while a manifest write is in flight, the buffer keeps collecting until
its encode and PUT would finish just before that write ends (at most 2 s), then
flushes in time for the next one. Requests that would have waited anyway now
share a segment, so fewer segments and manifest entries are written without
later acknowledgments. The configured window applies when no manifest write is
in flight. Use 20 ms: a longer fixed window only adds latency when the manifest
owner is idle, and at a few requests per second per signal it rarely catches a
second request. In a staging-shaped load test (4 Loki pushes/s from 8 senders,
1.5 remote writes/s, 0.5 trace exports/s, flat manifests of 44,000 log and
25,000 metric segments, S3 at 50–90 ms plus 45 MB/s), a 20 ms adaptive window
wrote about a third fewer log segments than a fixed 20 ms window (18% fewer
segments overall) at the same latency. Fixed windows of 100 and 250 ms merged
less or similar and added 80 to 230 ms to remote-write and trace
acknowledgments.

Each node-local buffer reserves at most 512 callers, 400,000 rows, and 32 MiB of
estimated input term bytes, including work already executing. Oversized requests
and overflow go through the original unbuffered path, not an unbounded queue.
These bounds are per buffer, not node-wide; node memory is bounded by the
admission slots and the pending budget (see [Ingest admission](#ingest-admission)).
Idle buffers terminate after 30 seconds. Buffers for different tenants, signals,
or storage configurations are isolated.

The window adds up to its configured delay before storage I/O. Every request
still waits for the shared segment PUT and manifest conditional write before
acknowledgment, exactly as without coalescing: an acknowledged request is in a
segment object referenced by a committed manifest. Releasing the admission slot
while it waits does not change that. No local WAL is introduced; a node crash before acknowledgment
requires collector retry. Unkeyed delivery remains at-least-once: a lost response
and retry can duplicate data, just as on the unbuffered path. A shared storage
failure fails all requests in that flush; it never falls back to individual writes
after an ambiguous publication. Invalid encodings are isolated before upload.

Enable this on a test deployment first. Compare provider PUT counts, received and
acknowledged records, failures, node memory, and delivery latency. Larger source
batches and low concurrency may already make coalescing unnecessary. Disable it
by setting the window back to `0` and restarting nodes; stored data needs no
migration.

## Manifest format

Every tenant and signal has one `manifest.json` that lists its segments and is
rewritten with a conditional write on every ingest publication. Releases before
format 4 kept that list flat, so each write downloaded, re-encoded and uploaded
the whole history: ingest latency and node memory grew with the age of a
deployment even at constant traffic, unless retention removed old data.

With the default `PULSO_MANIFEST_FORMAT=paged`, the manifest is a small root
that keeps only recently published segments inline and references older ones
through immutable pages under `.../signal=<s>/journal/`. An append rewrites only
the root, plus one page every 512 segments. Its cost and the memory a node uses
to cache manifests stay flat as history grows. Queries read the pages they need
through a bounded 16 MiB page cache. Prefixes with enforced retention keep using
the retention format.

Existing flat manifests convert automatically on their next ingest write. A
conversion of tens of thousands of segments writes about one page per 512 segments
before the root and is a one-time cost of a few seconds.

**Upgrade every node before paged writes start.** Releases before format 4 do not
read paged manifests. They reject them and fail the request (clients retry)
rather than reading or overwriting them, so mixing versions cannot lose data, but
older nodes cannot serve converted prefixes. For a single replica or a quick
rollout, upgrade normally: ingest landing on the old pod during the overlap gets
retryable errors. To avoid even that, roll out with `PULSO_MANIFEST_FORMAT=flat`
(chart: `manifestFormat: flat`), then switch back to `paged` once no older pod
remains. Setting `flat` again later does not convert paged manifests back, and
rolling back to an older release after conversion is not supported.

Paged manifests hold the same metadata as flat ones, including retirement
records, so the total metadata in the bucket still grows with history. Retention
is what removes old data; paging only stops ingest from paying for it on every
write. Watch `pulso_manifest_commit_duration_seconds` and
`pulso_manifest_roots` (see [self-monitoring](self-monitoring.md)).

## Metrics compaction

With `PULSO_METRICS_COMPACTION_ENABLED=true`, Pulso merges small metrics segments
from the same hour into larger ones and deletes the originals after a one-hour
grace period. This reduces the number of objects queries have to read.

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

Without compaction nothing bounds the number of metric segments: every
remote-write batch stays its own object forever, with or without retention.
Remote write shards series across batches, so almost every segment of a window
holds some series of any given metric and metric-name pruning cannot skip them.
At 55 segments a minute a 20-minute range already exceeds the default
1,024-segment budget, and an hour reads about 3,300 objects.

Compaction merges up to 32 small segments (1 MiB or less, at most 8 MiB and
100,000 rows per merge) from the same hour, and merges the results again while
they stay small, so an hour converges to a little more than its row count
divided by 100,000 segments (9.8 segments an hour at 720,000 rows an hour in
the benchmark below). Each maintenance pass (every minute) runs up to 32 merges per
tenant, so it keeps up with ingest and works through existing history four
manifest pages at a time.

## Long dashboard and alert ranges

Compacted segments hold all series of their hour, so a query over a window
reads every stored row of that window, whatever its matchers. The row budget,
not the segment budget, then decides how long a range can be. Measured with
`test/pulso/promql/query_scale_bench_test.exs` on 3,000 series scraped every
15 s (720,000 rows an hour), 18 ms per object GET and four segments of
prefetch on one node:

| History | Query | Segments read | Time |
| --- | --- | --- | --- |
| 6 h uncompacted, 18,720 segments | any 1 h range | over 1,024 | refused (`max_scan_segments`) |
| same, segment budget raised | 1 h range | 2,020 of about 3,300 | timed out at 10 s |
| 6 h compacted, 59 segments | 1 h range, default budgets | 12 | 0.1 s |
| same | 6 h dashboards, `and on`, ratio | 59 | 0.4 to 0.6 s with raised budgets |
| same | 6 h `rate` over 400 series | 59 | 2.8 s with raised budgets |
| 7 days compacted, 1,690 segments | 24 h dashboards | 241 | 1.4 to 2.2 s |
| same | `increase(x[1d])` | 240 | 2.0 s |
| same | `[3d:30m]` subquery | 717 | 4.6 s |
| same | `[7d:5m]` subquery | 1,681 | 11.4 s |
| same | `offset 168h` over `[1h]` | 20 | 0.2 s |

To serve 1-hour to 24-hour dashboards and 1-hour to 7-day alert windows:

1. Turn on bucket versioning, then enable metrics compaction
   (`metricsCompaction.enabled: true`), after every writer runs the same release.
2. Raise the scan budgets to cover the longest window: rows an hour times hours
   (17 million rows for a day and 121 million for a week at the rate above) and
   segments an hour times hours (about 10 an hour once compacted). A query
   over a budget reports what it needed, for example `max_scan_rows = 1000000,
   needed 17352000`, which sizes the budget for your own ingest rate.
3. Raise `PULSO_QUERY_MAX_SAMPLES`, `PULSO_QUERY_MAX_RESULT_POINTS`,
   `PULSO_QUERY_MAX_MATERIALIZED_SAMPLES`, `PULSO_QUERY_MAX_WORK` and
   `PULSO_QUERY_MAX_HEAP_MB` for
   ranges over hundreds of series (see [Query budgets](#query-budgets)), and give the
   container enough memory for (`PULSO_QUERY_MAX_CONCURRENT` + 2) query heaps.
4. Raise `PULSO_QUERY_TIMEOUT_MS` above the slowest window you need, below
   Grafana's 30 s datasource timeout. A 7-day window took 11 s above.

```yaml
metricsCompaction:
  enabled: true
resources:
  limits:
    memory: 6Gi
extraEnv:
  - {name: PULSO_QUERY_MAX_SCAN_ROWS, value: "200000000"}
  - {name: PULSO_QUERY_MAX_SCAN_SEGMENTS, value: "4096"}
  - {name: PULSO_QUERY_MAX_SCAN_BYTES, value: "4294967296"}
  - {name: PULSO_QUERY_MAX_SAMPLES, value: "1000000"}
  - {name: PULSO_QUERY_MAX_RESULT_POINTS, value: "1000000"}
  - {name: PULSO_QUERY_MAX_MATERIALIZED_SAMPLES, value: "1000000"}
  - {name: PULSO_QUERY_MAX_WORK, value: "50000000"}
  - {name: PULSO_QUERY_MAX_HEAP_MB, value: "512"}
  - {name: PULSO_QUERY_MAX_CONCURRENT, value: "3"}
  - {name: PULSO_QUERY_TIMEOUT_MS, value: "25000"}
  - {name: PULSO_QUERY_PREFETCH, value: "8"}
```

Compaction catches up on existing history gradually; until it has, long
ranges over older hours still fail on the segment budget, and the error says
so. Long windows read every row of the window on every evaluation, so a 7-day
alert evaluated every minute reads the whole week each minute. Pulso has no
downsampled rollups or per-series index yet; prefer recording rules or shorter
windows where they answer the same question.

## Log compaction

With `PULSO_LOG_COMPACTION_ENABLED=true` (chart: `logCompaction.enabled`),
Pulso merges small log segments from the same hour the same way, with the same
grace period, ownership among instances and prerequisites as metrics
compaction above. Without it, every log push stays its own object, so a query
over hours of frequently pushed logs reads thousands of objects.

Merges run natively without converting records: each merge reads at most 32
segments, 8 MiB of input and 100,000 rows, keeps every record and its typed
metadata exactly, and writes the same layout ingestion writes, ordered by service
and time. A merge that would exceed its row or 32 MiB string ceiling is skipped
rather than truncated. Older segments without typed OTLP metadata merge
unchanged. Stream discovery, queries, keyed retries and retention read compacted
segments like any other.

Each maintenance pass (every minute) keeps merging a tenant's segments while
merges make progress, up to 32 merges per signal, so compaction keeps up with
shippers that push every second instead of falling behind one merge a minute.

## Retention

By default Pulso keeps data indefinitely. Experimental event-time retention
expires logs and metrics after a configured number of days and deletes their
objects itself. Enforcing it converts manifests to a paged format that older
Pulso releases cannot read, and expired data cannot be restored. Read
[retention](retention.md) for the rollout procedure, modes, policy changes,
capacity limits and recovery before setting any `PULSO_RETENTION_*` variable.

## Query budgets

Each query is bounded by budgets shared by PromQL and LogQL. Set them with
these variables (through `extraEnv` in the Helm chart):

| Variable | Default | Meaning |
| --- | --- | --- |
| `PULSO_QUERY_MAX_SAMPLES` | `100000` | Samples a PromQL query may select. |
| `PULSO_QUERY_MAX_LOG_SAMPLES` | `5000000` | Per-line samples a LogQL metric query may retain (one per line its pipeline keeps). |
| `PULSO_QUERY_MAX_WORK` | `5000000` | Evaluation work units per query. |
| `PULSO_QUERY_MAX_RESULT_POINTS` | `100000` | Points a PromQL query may return, also the points each selector's intermediate series may hold: a range of 721 steps fits about 138 series. |
| `PULSO_QUERY_MAX_MATERIALIZED_SAMPLES` | `100000` | Points PromQL subqueries may materialize per query. |
| `PULSO_QUERY_MAX_SCAN_SEGMENTS` | `1024` | Stored segment objects a query may read, `1..1000000`. Every segment costs one object GET, so this mostly bounds query latency. A PromQL query reads each candidate segment once, however many selectors need it, and charges this budget once per segment. |
| `PULSO_QUERY_MAX_SCAN_ROWS` | `1000000` | Stored rows a query may read, counted from each segment's footer, including rows its matchers then discard. |
| `PULSO_QUERY_MAX_SCAN_BYTES` | `134217728` | Stored bytes a query may read. |
| `PULSO_QUERY_TIMEOUT_MS` | `10000` | Wall-clock limit for one query, including object-store reads (1000 to 300000). Queries past it fail with a timeout. Keep it below the client's own timeout (Grafana's datasource default is 30 s). |
| `PULSO_QUERY_MAX_CONCURRENT` | derived, else `3` | Interactive queries (HTTP and MCP) a node runs at once, `1..64`; more wait for a slot or fail as overloaded. Native alert evaluation and reliability callbacks run one each outside these slots. |
| `PULSO_QUERY_MAX_HEAP_MB` | derived, else `128` | Process heap ceiling of one query worker, `16..16384`. A query past it fails with a resource-limit error. Off-heap segment reads (up to the prefetch depth) and native accumulators come on top, so size memory for about (`PULSO_QUERY_MAX_CONCURRENT` + 2) × this plus prefetched segments, and lower both on small nodes. |
| `PULSO_QUERY_PREFETCH` | `4` | Segments fetched ahead of the one being processed, hiding object-store latency. A query that scans many segments is latency-bound: with 25 ms per request, scanning 1,000 segments took about 25 s at 1, 5.8 s at 4 and 2.8 s at 8, with no more requests or memory. Larger values can read up to that many minus one segments more when a query stops early or exhausts a budget, and hold that many segments in memory per query. Log scans that cannot stop early (metric queries and unlimited log queries) read further ahead, up to 32 segments, while the segments they read keep at most 16 MiB in flight; a value of `1` disables this. |

Log metric queries stream: they hold one decoded chunk of a segment plus
about 40 bytes per line their pipeline keeps (17 once packed), so their memory
grows with the kept lines, not the log bytes. `PULSO_QUERY_MAX_LOG_SAMPLES`
bounds those lines and is enforced while the query reads, and every selected
line also counts as work and every stored row against the scan budgets. Raise
the log sample, work and scan-row budgets together for high log volume and
size memory for four concurrent queries. PromQL
queries still materialize their samples and remain bounded by each query's
memory ceiling as well. A query over a budget fails with HTTP 422 rather than
returning a partial result.

The error names the limit, the value that applied and, when known, what the
query needed, for example `max_scan_segments=1024 exceeded: candidates in the
time range total at least 1025 segments; narrow the time range or selector, or
raise PULSO_QUERY_MAX_SCAN_SEGMENTS`. The Prometheus HTTP API returns it in
`error` (prefixed with `query exceeded `) and as a `limit` object (`reason`,
`limit`, `value`, `observed`, `env`); MCP returns `:query_execution_limit: ` (or
`:query_scan_limit: ` for log and trace tools) followed by the same text. In alert batches the value
is the source's share of the batch's scan budget. A timeout answers at
`PULSO_QUERY_TIMEOUT_MS` even if the query's worker is still inside a native
call; that worker keeps its query slot until it stops.

PromQL memory grows with the samples a query selects. Each segment's decoded
rows are folded into per-series time/value points as it is read, but evaluation
peaks at about 450 bytes of worker heap per selected sample: in our benchmark a
6-hour range over 400 series (576,000 samples) needed between 192 and 256 MiB.
The default 128 MiB heap fits the default 100,000 samples with headroom. Raise
`PULSO_QUERY_MAX_SAMPLES` and `PULSO_QUERY_MAX_HEAP_MB` together, about 512 MiB
per million samples.

## Fixed limits

Each node admits `PULSO_QUERY_MAX_CONCURRENT` interactive queries (three by
default), at most two per tenant, and bounds each query's duration and memory. Requests are capped at 4 MiB on the wire and
16 MiB after decompression. Requests over these limits fail with an explicit
error rather than a partial result.
