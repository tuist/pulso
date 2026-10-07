# Alerting

Pulso currently implements an **experimental native threshold evaluator**, durable
rule management, configuration-change history, and per-rule event replay. These
features share object storage; restarting evaluators does not erase pending,
firing, or keep-firing state.

Native rules support **opt-in Slack webhook delivery** and live MCP resource
update hints. Grafana routing/grouping, OnCall and Atlas webhook compatibility
remain unimplemented. Do not transfer production paging responsibility to Pulso.
Grafana definitions can be
validated and stored losslessly, but remain disabled and cannot execute yet.
Import success is not query, template, lifecycle, or notification compatibility.

## Configure credentials and evaluation

Alerting does not accept the existing tenant-shared token as an administrative
identity. Configure dedicated principals in `PULSO_ALERTING_PRINCIPALS_JSON`:

```json
[
  {
    "tenant": "production",
    "id": "operator",
    "type": "human",
    "token_hash": "<64 lowercase hexadecimal SHA-256 characters>",
    "capabilities": [
      "alert:read", "alert:rules:write", "alert:audit:read",
      "alert:preview", "alert:evaluate", "alert:import"
    ]
  }
]
```

Generate a random bearer token and hash it offline. Store only the hash in
configuration; give the plaintext token to its owner. Principal types are
`human`, `agent`, and `service`. History identifies the credential holder. A
shared service token does not identify a person behind that service, and
caller-supplied identity assertions are not accepted.

| Capability | Permission |
| --- | --- |
| `alert:read` | Read authorized configurations, current state and events. |
| `alert:rules:write` | Create, replace, disable, delete and restore rules. |
| `alert:audit:read` | Read changes and historical snapshots. |
| `alert:preview` | Read-only native preview; also requires `alert:read`. |
| `alert:evaluate` | Commit native evaluations; also requires `alert:read`. |
| `alert:import` | Validate imports and access imported Grafana content. |

Import administration also needs rule-write permission. Imported configuration
and the edit that removes it retain the import classification. Generic read or
audit permission does not expose those historical definitions. Credentials for
existing signal-query and ingest APIs remain configured separately through
`PULSO_TENANT_TOKENS`; alerting principals do not automatically grant that access.

Automatic evaluation is opt-in:

- `PULSO_ALERTING_EVALUATION_ENABLED=true`, default `false`.
- `PULSO_ALERTING_POLL_INTERVAL_MS=5000`, range `1000..60000`.

The Helm equivalents are `alerting.principals`, `alerting.evaluationEnabled`, and
`alerting.pollIntervalMs`. Deploy identical principal configuration, object-store
identity, and `SECRET_KEY_BASE` across nodes. Revocation requires updating and
restarting all affected nodes. Keep clocks synchronized. Workers discover durable
rules and use live cluster membership to reduce duplicate work; conditional
writes, not ownership, enforce correctness.

Evaluation queries have a dedicated background slot rather than consuming the
interactive reservation. Query budgets and per-tenant limits still apply. Polling
many rule heads can be costly; benchmark the interval before enabling evaluation
at scale. No local durable queue or database is introduced.

## Create a native rule

HTTP calls use `Authorization: Bearer <token>` and `X-Scope-OrgID`. Tenant and rule
IDs are at most 128 characters, using letters, digits, `.`, `_`, and `-`; `.` and
`..` alone are not valid IDs. All request bodies are JSON.

`POST /api/v1/alerting/rules/backup-failed`:

```json
{
  "operation_id": "<new UUID>",
  "reason": "Watch for backup failures",
  "rule": {
    "kind": "promql_threshold",
    "name": "Backup failed",
    "query": "max(backup_failed)",
    "threshold": {"op": "gt", "value": 0},
    "enabled": true,
    "cadence_ms": 60000,
    "for_ms": 300000,
    "keep_firing_ms": 0,
    "labels": {"severity": "warning"}
  }
}
```

Native queries use the existing supported [Prometheus query subset](querying.md)
and must return an instant vector. Threshold operators are `gt`, `lt`, and `eq`.
Rules default to disabled, a 60-second cadence, and zero pending/keep-firing
holds. Cadence is `1000..86400000` milliseconds; holds are `0..604800000`.
Custom labels are bounded; `__name__` and `alertname` are reserved. Up to eight
operator-provisioned `notification_targets` may be referenced by ID. The rule name
becomes `alertname`. Unknown native configuration fields are rejected.

Eligible ticks follow a UTC cadence grid. Pending and keep-firing timers advance
only at successful eligible evaluations, never by an independent timer. Retrigger
preserves the original firing start. In a nonempty result, missing active series
are treated as false predicates. A wholly empty result sets `no_data` and keeps
previous instances/timers. Source errors also keep state and report `error`.
Admission/resource deferral and query overrun skip publication, not a healthy
empty result. These are native semantics, **not Grafana compatibility claims**.

Every configuration replacement resets native lifecycle state and records
resolution of previous instances. Deleted rules have durable tombstones.
Re-creation uses a fresh generation and preserves audit ancestry. Restore always
creates a new disabled revision; enabling it requires a separate explicit edit.

## Management, preview and state

| Method/path under `/api/v1/alerting` | Operation |
| --- | --- |
| `GET /rules` | List authorized current rules. |
| `GET /rules/:id` | Read current configuration and revision. |
| `POST /rules/:id` | Create, or explicitly re-create a deleted rule. |
| `PUT /rules/:id` | Replace configuration. |
| `DELETE /rules/:id` | Publish a tombstone. |
| `POST /rules/:id/preview` | Preview native results without writes. |
| `POST /rules/:id/evaluate` | Commit an eligible native evaluation. |
| `GET /rules/:id/state` | Read durable instances, health and evaluation revision. |
| `GET /rules/:id/changes` | Read the first newest-first change page. |
| `POST /rules/:id/changes` | Continue change pagination with a body cursor. |
| `GET /rules/:id/revisions/:revision` | Read a historical revision by digest. |
| `POST /rules/:id/revisions/:revision` | Read using a body `revision_cursor`. |
| `POST /rules/:id/restore` | Restore a historical configuration as a new revision. |
| `POST /rules/:id/events` | Page committed lifecycle/health events. |
| `POST /import/preview` | Validate a Grafana definition array without writes. |

Mutations require a nonempty bounded `reason`, a fresh `operation_id`, and the
current `expected_revision` except for first creation. HTTP `If-Match` can carry
the same revision returned in `ETag`. If both are supplied, they must agree.
Exact retries reuse their operation ID and unchanged request. Changed payloads
or a rebase onto a different configuration revision require a new ID.
Reasons are untrusted context; do not put credentials or sensitive incident data
in rule names, labels, queries, or reasons.

The rule head publishes the configuration pointer and its audit record together.
Immutable candidates that lose conditional publication never become history or
fires. After an uncertain conditional response, Pulso reads back the committed
authority. Unresolved outcomes return an availability/ambiguity error; do not
assume that means the operation definitely failed. Recent mutation receipts are
bounded to 128 entries, independently of evaluation ticks. Bounded ancestry
resolution can still return ambiguity; retain operation IDs and inspect history
rather than blindly creating another operation.

Successful requests return `200`; malformed input returns `400`, missing/denied
credentials `401`/`403`, missing rules `404`, conflicts or cursor reset `409`,
disabled/unsupported execution `422`, and unavailable/indeterminate publication
`503`.

## History and replay

A full immutable revision snapshot is the audit record, including parent,
verified principal, reason and operation identity. Change listing pages backwards
from a frozen committed tip. Edits during pagination do not move that tip. Use
`next_cursor` in a POST body or tool arguments, never a URL. This is not
chronological forward replay or an automatic-action feed.

Each change includes an encrypted `revision_cursor`. Supply it with `revision`
when reading/restoring an old snapshot to avoid walking from the newest head.
A bare historical digest lookup scans at most 400 revisions, returning a scan
limit rather than a false not-found result. Page changes to obtain a fresh
revision cursor for older snapshots. Restore additionally needs current
`expected_revision`, `operation_id`, and `reason`.

Event pages read forward from an encrypted per-rule cursor. They retain up to
64 inline references and 32 sealed pages of 64 events: at most 2,111 readable
transitions while the current tail is open. The actual floor advances when the
oldest page is dropped. This is a count bound, not a guaranteed number of days.
Below-floor positions require explicit resynchronization. A cursor omitted from
a fresh request starts at the earliest retained event, not at current state.

Cursors are principal/view/rule-bound, expire after ten minutes, and derive their
key from `SECRET_KEY_BASE`. Changing permissions or rotating that secret requires
an explicit earliest-retained restart. Deduplicate by stable event IDs, not only
an old sequence high-water mark: newly authorized older events may become visible.
Current-state reconciliation is separate from claiming transitions were processed.

Audit snapshots and unreferenced candidates are not garbage-collected in this
initial implementation. Do not apply a bucket lifecycle expiry to the alerting
namespace. Event replay pruning does not authorize deleting longer-lived audit
snapshots or entire generation prefixes. Retention cleanup and operational cost
controls remain follow-up work.

## Model Context Protocol tools

The existing stateless `POST /mcp` transport exposes:

- `list_alert_rules`, `get_alert_rule`, `create_alert_rule`, `update_alert_rule`,
  `delete_alert_rule`;
- `list_alert_rule_changes`, `get_alert_rule_revision`,
  `restore_alert_rule_revision`;
- `get_alert_state`, `read_alert_events`, `preview_alert_rule`,
  `evaluate_alert_rule`, and `validate_alert_import`.

Calls take `tenant` and, where applicable, `id`, with the same mutation and cursor
fields as HTTP. Mutation annotations are distinct from read-only query tools;
authentication and capability checks happen before execution. Preview does not
write state or send notifications. Rule administration does not grant infrastructure
remediation. Silences, acknowledgements and dynamic notification-configuration APIs remain
unsupported. Targets are provisioned by the operator, not by model-generated URLs.

## Native Slack delivery

Configure public target descriptors in `PULSO_ALERTING_NOTIFICATION_TARGETS_JSON`:

```json
[{"tenant":"production","id":"slack-prod","type":"slack_webhook","secret_env":"PULSO_ALERTING_SLACK_PROD_URL"}]
```

Set `PULSO_ALERTING_SLACK_PROD_URL` to the HTTPS incoming-webhook URL through your
secret manager or a Kubernetes Secret, not a rule body or values file. The webhook
URL is never stored in alert history or events. Pulso disables HTTP redirects and
does not expose transport error details. Changing a target's descriptor requires
a new target ID; rotate a secret under the same reference only when preserving
its logical destination. Retain old descriptors/secrets until their work drains.

Add `"notification_targets":["slack-prod"]` to the native rule and enable
`PULSO_ALERTING_NOTIFICATIONS_ENABLED=true` separately from evaluation. Both
features default to disabled. Provisioning a target alone never sends a message.
Only committed firing/retrigger/resolution events enqueue native notifications;
pending disappearance does not send a phantom resolution. Health is available
through state/resources, not a Grafana-style DatasourceError notification yet.

Each target has a durable bounded outbox and a leased sender claim. Event replay
pruning cannot drop queued payload references. Failed sends retain work for retry
after the 30-second lease expires. Successful sends advance delivery progress by
conditional head write. Restarted or competing senders may duplicate an external
message after an uncertain response or lease expiration: delivery is at-least-once,
not Slack exactly-once or guaranteed remote ordering. Producer text uses Slack
`plain_text`, not executable instructions, markup or mention parsing.

Outboxes hold at most 64 events per target and 16 live/retiring bindings per rule.
Full backlogs reject additional transition publication rather than silently losing
notifications. Configuration changes that must publish resolution notifications
may also wait for backlog drainage. Stop automatic evaluation through its enable
flag if delivery is unavailable; monitor current state and the object-store costs.
This initial delivery is one native event per message, not Grafana notification
policy grouping, repeat intervals, escalation or template parity.

## Live resources and subscriptions

`resources/list`, `resources/templates/list` and `resources/read` expose authorized
resources at `pulso://alerts/tenants/<tenant>/rules/<id>/{state,events,changes}`.
Reads are private and uncached (`ttlMs: 0`). State resources contain current
instances; events and changes contain committed replay/audit frontiers. A
checkpoint is not proof that events were processed. Drain the history tools with
your own body cursor and durable event-ID deduplication.

Use standard `subscriptions/listen` with a `resourceSubscriptions` array. The POST
response is actually streamed: acknowledgement first, then
`notifications/resources/updated` hints carrying the subscription ID. There are
no GET streams, protocol sessions, or `Last-Event-ID` replay. Hints may coalesce;
durable tool replay is the continuity mechanism.

Subscriptions poll committed authority heads once per second, refresh credential
and classification checks on every poll, and close gracefully on revocation or a
view change. They last at most ten minutes, accept at most 32 resource URIs and
have local admission limits of 64 streams globally and two per principal.
Reconnection requires a new authenticated POST and replay from your persisted
cursor. Infrastructure actions still require a separate authorized server and
human approval; a resource update is never action authorization.

## Grafana import

`POST /api/v1/alerting/import/preview` accepts `{"rules": [<definitions>]}`.
It validates identities and expression references/cycles and returns a disabled
`grafana` wrapper for each complete original definition, with explicit
`executable: false` and migration blockers. No partial import writes occur.
Persist approved wrappers individually with the normal create operation.

The checked-in inventory test covers all 115 definitions, including four
recordings, two paused rules and 13 nonzero keep-firing settings. Original query
graphs, unknown fields, recording metadata and template defects are retained,
not silently repaired or dropped. Grafana execution, external adapters, recording
publication, notification rendering/routing and migration fixtures remain future
work. No imported rule may be enabled until those gates are implemented.
