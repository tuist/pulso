# Ingest request limits

Pulso bounds individual ingest requests independently of rate limiting or queue
admission. These limits apply to OTLP/HTTP JSON logs (`/v1/logs`), Loki JSON and
Snappy protobuf (`/loki/api/v1/push`), and Prometheus remote write v1
(`/api/v1/write`). They are conservative safety defaults, not measured capacity
or a guarantee that every request within the limits can be served concurrently.

## Defaults and configuration

Configure `Pulso.IngestLimits` through Elixir configuration or the optional
runtime environment variables below. Environment variables override only the
specified settings. Values must be integers in `1..2147483647`; zero and
`infinity` do not disable a limit. Invalid environment values fail at startup.

| Setting | Default | Environment variable |
| --- | ---: | --- |
| `max_records` | 10,000 | `PULSO_INGEST_MAX_RECORDS` |
| `max_attributes` | 128 | `PULSO_INGEST_MAX_ATTRIBUTES` |
| `max_key_bytes` | 256 | `PULSO_INGEST_MAX_KEY_BYTES` |
| `max_value_bytes` | 16,384 (16 KiB) | `PULSO_INGEST_MAX_VALUE_BYTES` |
| `max_attribute_bytes` | 65,536 (64 KiB) | `PULSO_INGEST_MAX_ATTRIBUTE_BYTES` |
| `max_depth` | 16 | `PULSO_INGEST_MAX_DEPTH` |
| `max_nodes` | 1,024 | `PULSO_INGEST_MAX_NODES` |

For example, in `config/config.exs`:

```elixir
config :pulso, Pulso.IngestLimits,
  max_records: 5_000,
  max_attributes: 64,
  max_attribute_bytes: 32_768
```

Or set `PULSO_INGEST_MAX_RECORDS=5000` for a release. Changing these settings does
not change the existing default body caps: 4 MiB on the wire and 16 MiB
expanded. Uncompressed JSON also has the 4 MiB parser cap. Gzip expansion is incremental and
cumulatively bounded; Snappy checks the declared expanded length before
allocating its output buffer. Measure memory and decode cost before raising
limits; these are node-wide settings, not per-tenant quotas.

## What is counted

- **Records:** supplied OTLP `logRecords`, Loki `values`/entries, or remote-write
  samples, accumulated across the entire request. Malformed or subsequently
  rejected records still count. Empty containers also consume a separate work
  budget: `max_records` Loki streams or remote-write series, or
  `2 × max_records` combined OTLP resource and scope groups (each OTLP record
  can need both groups). This prevents empty-container floods.
- **Attributes:** each OTLP resource, scope, record attribute list, and nested
  key-value list; each Loki stream label set and entry metadata set; each
  remote-write series label set. The count is per set, not per request. Supplied
  duplicate entries count before last-wins normalization in OTLP and protobuf.
  JSON object duplicate keys have already been collapsed by the JSON parser.
  Loki `trace_id`/`span_id` metadata counts even though decoding promotes it to
  dedicated fields. Remote-write `__name__` counts as a label.
- **Bytes:** UTF-8 key/value bytes, not character counts. Each attribute set has
  an aggregate key/value budget including nested values. Strings contribute
  their byte length and non-string scalar values contribute eight bytes.
  OTLP protocol wrapper names are not attribute keys and do not consume this
  budget; `intValue` and base64 `bytesValue` strings are measured as supplied.
  Loki protobuf label values are measured after Go unescaping; the label parser
  also has a wire-size ceiling of `8 × max_attribute_bytes + 8 × max_attributes`
  to bound allocation on malformed or excessively padded label strings.
- **Structure:** JSON attribute trees have bounded depth and visited nodes
  (containers and scalar values; OTLP attribute keys also count as nodes).
  The root is depth zero. Each attribute set has its own node budget. OTLP
  structured bodies have the same structure limits; ordinary log messages do
  not receive the smaller attribute-value or aggregate-attribute byte caps.
  Protobuf labels and metadata are flat, so depth/node limits do not apply.

## Rejection and authentication

A request exceeding the record/container budget returns HTTP **413** with
`{"error":"too_many_records"}`. Attribute or structure failures return **413**
with `{"error":"attributes_too_large"}`. No records from that request are
appended, and no success or partial-success envelope is returned. Body-cap
failures keep their existing transport behavior (`payload_too_large` for
protobuf, parser errors for JSON). Existing in-budget
malformed-record rejection and partial-success behavior is unchanged.

Tenant validation and authentication precede these semantic checks. Snappy
protobuf preflight runs after one bounded decompression, before allocating
record/sample collections, deduplicating labels, hashing series, or constructing
Erlang record terms. JSON preflight runs on the bounded parsed body before OTLP
AnyValue conversion or record expansion. JSON parsing and gzip expansion still
happen before controller authentication, under their body-size caps; this change
does not add a pre-parse authentication gate.

Collectors must reduce oversized batches or attributes rather than blindly
retrying them unchanged. This does not establish the actual collectors' retry
behavior, per-tenant fairness, ingest concurrency, or bounded queueing. Those
require separate rollout verification and admission/backpressure work.
