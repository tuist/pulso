# Actual SDK/Collector bridge evidence

These are **first-party harness captures driving real upstream components**,
not an independent implementation of SDK/Collector behavior or a deployed-client
acceptance claim. Ordinary tests replay checked-in data with Elixir/Rust only.
They do not download, probe for, build or execute Go or a Collector.

## Producers and boundaries

- Go 1.25.10, `go.opentelemetry.io/otel` / SDK / gRPC trace exporter **1.43.0**.
  Full module pins/checksums: `../sdk-bridge.go.mod.txt`,
  `../sdk-bridge.go.sum.txt`; captured `go version -m` is `sdk-buildinfo.txt`.
- Producer source: `../produce_sdk_bridge.go.txt`. It uses two real tracer
  providers/resources, SDK batch processors, gRPC exporters and an actual
  loopback HTTP request with W3C `TraceContext` injection/extraction. IDs and
  source timestamps are deterministic; RPC/batch ordering is scheduler-dependent.
  Explicit tiny sampler/queue/batch/attribute/event/link limits are not defaults.
- **Core** Collector **0.137.0**, official release (not contrib/Alloy):
  `https://github.com/open-telemetry/opentelemetry-collector-releases/releases/tag/v0.137.0`.
  Darwin-arm64 archive `otelcol_0.137.0_darwin_arm64.tar.gz` SHA-256:
  `56a4535b97b9dcb168bbb46d77d926a2ec9de6d02a5262416d7101eb691dd76f`,
  matched against the release's published checksum manifest.
  Extracted binary SHA-256:
  `1747a6d1a058684e610d2a508e10185788cd66e1f802f547b320e81660110bae`;
  `--version`: `otelcol version 0.137.0`. This checksum match is not a signature-
  verification or vulnerability/security recommendation. Other platforms must
  verify their own artifacts; do not compare Linux binaries to this checksum.
- Passing profile: no Collector processors/re-batching or sending queue; gzip
  `otlphttp` exporter with static tenant/auth headers and bounded retries. Actual
  Pulso HTTP server, native Parquet/S3 client against an **owned HTTP S3 stub**,
  manifest-supervision restart, and MCP reads. No real provider/node restart,
  TLS, deployed SDK/Alloy, persistent queue or capacity acceptance.

The producer's gRPC unary interceptor deterministically marshals the export
**request message** before invoking gRPC. `sdk-request.protobuf` is not literal
gRPC packet bytes, HTTP/2 frames or compressed transport capture. Collector
requests are actual HTTP bodies captured at the loopback proxy after gzip
inflation. Concatenating repeated-resource export request messages preserves the
whole supplied span list; it is not trace-ID reconciliation or SDK deduplication.

## Frozen artifacts

`SDKBridgeTest` checks these SHA-256 values before replay:

| File | SHA-256 |
| --- | --- |
| `sdk-request.protobuf` | `1ca97ee4617977b66ab1af855513272d444355224c88a00f9ad74cede66b311d` |
| `collector-request.protobuf` | `38b8d8b31c4eb5b8626c13d4394fb029011cde4d65e8ddd97e00a8283a92a913` |
| `sdk-oracle.json` | `cf52697ccfa9b85c7dc978a2cae310506132441073afb37c4f1f72264fa15f0d` |
| `sdk-buildinfo.txt` | `42e934d97ebccf053ecb7896b5cfc5680417f55f1538352471b1ad8bcfdf28f1` |
| `split-collector-request.protobuf` | `b78065e716a77c276da3c44c1dad5beae97c494bdaed9189c5356ab5004b4236` |
| `partial-producer.protobuf` | `977747b0c64211e2b07187a7d5c333ead8cb542ec35e12707247e185c54ce967` |
| `partial-collector.protobuf` | `19cdc88fac71f448c59140a510ac07c8e67eeae7f002abaf213f93baab971bfe` |
| `partial-result.json` | `a85bb10c2fe3983a53c315cdb3ac52f4f45e347a3167a31b5ff9bddc3126a1fe` |

`sdk-oracle.json` snapshots the SDK `ReadOnlySpan` API with a small first-party
attribute converter. Float values are decimal/class strings to avoid losing
signed zero in the oracle's JSON; replay normalizes them and separately asserts
projection and typed scalar/array zero bits. Int64 above 2^53, flags, statuses,
resource/scope schemas, typed metadata, events, links and drops are checked.
SDK request → Collector → stored records agree for every normalized field.
Native Parquet compaction/assembly and four Tempo export formats also replay.

There are two deliberately retained losses:

1. SDK 1.43 snapshots have link tracestate `vendor=opaque`, but its export request
   message already omits that field. The Collector/Pulso preserve what was sent;
   no tracestate is fabricated. Upstream source inspected: `opentelemetry-go`,
   tag `exporters/otlp/otlptrace/v1.43.0`,
   `exporters/otlp/otlptrace/internal/tracetransform/span.go`, function `links`:
   the constructed protobuf link omits `TraceState`.
2. `split-collector-request.protobuf` comes from the same SDK request bytes, with
   Collector 0.137 batch processing instead: `send_batch_size: 2`,
   `send_batch_max_size: 2`, `timeout: 100ms`. Mixed frontend/worker resources
   split so the database span loses both resource and scope schema URLs. The
   other normalized fields and IDs agree. Replay asserts the precise loss and
   that storage doesn't invent the missing values. A different batch order may
   choose another split span; no universal byte-reproduction claim.

The partial case deliberately constructs one valid and one all-zero-trace-ID
span using **public OTLP protobuf types**, not an ordinary SDK. Collector forwards
both, Pulso stores the valid span and reports `rejectedSpans=1`. The live test
observes one HTTP 200 attempt, no whole-batch retry, and a successful upstream
gRPC response with empty `partial_success:{}` rather than the backend count.
Fixture replay alone does not establish network retry behavior.

The live harness additionally injects **pre-publication** HTTP 429/503, observes
unchanged decompressed retry messages, and checks actual Pulso 401/413 permanence.
Two static pipelines have the same trace/span IDs but different tenant resources
and credentials; unauthorized MCP access fails. This does not authenticate the
Collector's incoming gRPC clients or demonstrate a public multi-tenant gateway.
SDK process exit means capture completed, not delivery succeeded: export and
flush errors are recorded/asserted separately. A background failure may not be
returned by a later flush.

## Optional local reproduction

Use directly built binaries on Unix. Do not pass `go run` or a wrapper that forks
without `exec`: subprocess cleanup signals only the owned direct OS pid. Port
handoff has a reservation race (reserve ephemeral ports, release, Collector binds);
a conflict fails readiness rather than attaching to an unrelated server. Rerun
with fresh ports on that failure. Logs are bounded to 64 KiB and evidence requests
to 64 (exhaustion fails, never truncates). Child environments clear known OTel,
gRPC, proxy and Go runtime settings and fix GOMAXPROCS=2, without changing the
parent/test environment. This is not an OS sandbox. Captured credentials are
synthetic; request logs redact authorization and no secret-bearing config/log is
frozen here.

```sh
# Run from the Pulso root; Go/dependency downloads are optional producer tooling.
work=$(mktemp -d)
cp test/fixtures/traces/produce_sdk_bridge.go.txt "$work/main.go"
cp test/fixtures/traces/sdk-bridge.go.mod.txt "$work/go.mod"
cp test/fixtures/traces/sdk-bridge.go.sum.txt "$work/go.sum"
(cd "$work" && GOTOOLCHAIN=local go build -mod=readonly -o "$work/trace-sdk" .)

# Supply a separately downloaded/verified core Collector 0.137.0 binary.
PULSO_TEST_OTELCOL=/absolute/path/to/otelcol \
PULSO_TEST_TRACE_SDK="$work/trace-sdk" ERL_FLAGS='+S 4:4' \
  mix test test/pulso_web/controllers/collector_bridge_live_test.exs \
    --include collector_bridge --max-cases 4 --trace
```

A missing binary path is an explicit failure after opt-in, not a skip. The tag is
excluded by default **even when `PULSO_INTEGRATION=1`**; normal CI's S3 integration
job must not require these tools. Generated SDK oracles/request messages,
Collector requests/delivery logs and config stay in the test-owned tmp directory;
the successful test prints its path. Reproduction never refreshes golden files.
Inspect semantic differences before deliberately updating captures/checksums.

Source SHA-256:

- Producer `3d64e6f315152d9d95223637a4f53e3ce9de1e6fceeff8fa0c70d1801e6ff082`.
- Module `6d5f7aa51683242a7d2f3e7b7e60d8b634231d8767777bd97ccda2e92aae2cdb`.
- Checksums `3c0e4cc1e2fe9edf04b373eaa2fc0928559602c05f5c29ab0134be7a54b86001`.

No upstream implementation is vendored. The workload still needs actual
Rust/Elixir SDKs, deployment/Alloy/TLS, queues/crashes/ambiguous acknowledgments,
representative batches/volume, provider/native pressure, recovery and authorized
shadow/cutover acceptance. See [operator contract](../../../../docs/trace-collector-bridge.md).
