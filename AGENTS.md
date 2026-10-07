# Pulso

A headless, open-source observability backend: unified logs/metrics/traces access, built-in alerting, and a native Model Context Protocol (MCP) interface, built on Elixir/OTP with a Rust hot path for columnar data.

Pulso is agent-native from day one. It does not ship a UI. Consumers are humans through their own dashboards and, first-class, AI agents through MCP.

## Read this before any non-trivial design work

**[`docs/architecture.md`](./docs/architecture.md)** — source of truth for Pulso's architecture. Load it before you reason about ingest, storage, coordination, alerting, the Elixir/Rust split, or the MCP boundary. If you're about to add a database, a WAL, a leader-election protocol, or any cluster-visible mutable state, read the "Core bets" and "What Pulso deliberately does not have" sections first.

## Where the project is right now

Early scaffolding. In place:

- Phoenix 1.8 headless app (no HTML, no assets, no Ecto)
- `Pulso.Loki` — read-only Loki HTTP client wrapping `query_range`
- `Pulso.MCP` — stateless MCP `2026-07-28` dispatcher (`server/discover`, `tools/list`, `tools/call`, `subscriptions/listen`); no `initialize` handshake, sessions, or `ping`
- `Pulso.MCP.Tools` — read-only query tools plus capability-scoped alert management/state/history tools under `Pulso.Alerting.Tools`; infrastructure remediation is never part of this registry
- `PulsoWeb.MCPController` at `POST /mcp` — Streamable HTTP transport: one message per POST (no batches), `202` for notifications, mirrored-header validation (`PulsoWeb.MCPHeaders`), `405` for GET/DELETE; `PulsoWeb.MCPRequestGate` rejects disallowed `Origin` headers (`403`) and non-JSON POST bodies (`415`) before body parsing
- `PulsoWeb.OTLPController` at `POST /v1/logs` — OTLP/HTTP JSON logs ingest
- `PulsoWeb.LokiController` at `POST /loki/api/v1/push` — Loki push ingest, JSON and Snappy-compressed protobuf (decoded in Rust by `Pulso.Codec.NIF`)
- `PulsoWeb.RemoteWriteController` at `POST /api/v1/write` — Prometheus remote_write v1 ingest (Snappy-compressed protobuf, hand-decoded in Rust). Full receiver contract per the Prometheus spec; see `lib/pulso_web/controllers/remote_write_controller.ex` for the header/status-code rules.
- `PulsoWeb.CompressedBodyReader` — gzip-aware Plug.Parsers body reader, so JSON receivers accept compressed bodies
- `Pulso.Storage` — signal-generic behaviour (`append(signal, tenant, records, opts)` / `query(signal, tenant, opts)` with `signal :: :logs | :metrics`), with an in-memory adapter for tests and `Pulso.Storage.S3` for dev/prod. S3 objects are Apache Parquet segments (logs via `Pulso.Codec.NIF.{encode,decode}_log_segment_parquet`, metrics via `encode_metric_segment_parquet` / `decode_metric_segment_parquet`), keyed as `tenants/<t>/v4/signal=<s>/date=<Y-m-d>/hour=<H>/<min_ts>-<max_ts>-<suffix>.parquet`, sorted by `(service, timestamp_ns)` for logs and `(series_id, timestamp_ns)` for metrics, coordinated per `(tenant, signal)` through an S3-CAS manifest (`Pulso.Storage.S3.Manifest`, `Pulso.Storage.S3.ManifestOwner`, `Pulso.Storage.S3.ManifestCache`).
- `native/pulso_codec/src/stable_hash.rs` — Pulso's port of Prometheus's `labels.StableHash` (xxhash64 over `name<0xff>value<0xff>…` across labels sorted by name). Byte-exact with the Go reference; conformance-tested in-crate.
- `Pulso.PromQL` — bounded [Prometheus Query Language](https://prometheus.io/docs/prometheus/latest/querying/basics/) subset: float selectors/range functions, grouped aggregations, scalar/vector arithmetic, comparisons, set operators and matching, classic histogram quantiles, clamps, label replacement, ranking/sorting, rounding, time functions, and positive offsets. Stale markers and IEEE non-finite values survive remote write, Parquet, and compaction. Native histograms and exemplars remain unsupported and are explicitly counted as rejected. Exposed by `query_promql` and `/api/v1/query{,_range}`.
- Metrics manifests carry complete, bounded metric-name and low-cardinality label-value sets for exact-match pruning, dictionary-deduplicated with independent budgets. Log manifests carry bounded nonempty promoted-service sets. Unknown summaries are always scanned.

- `Pulso.Storage.S3.CompactionWorker` and `MetricsCompactor` provide opt-in metrics compaction, rendezvous ownership among live eligible workers, durable manifest-based tenant discovery, and restart-safe retirement cleanup. See the metrics compaction section in `docs/architecture.md`.
- `Pulso.Storage.S3.AppendBuffer` provides opt-in bounded node-local coalescing of unkeyed appends before segment upload. Keyed requests and overflow remain direct; every acknowledgment still waits for segment PUT and manifest CAS. `PULSO_INGEST_FLUSH_INTERVAL_MS` defaults to `0` (disabled). This is not rendezvous-owned ingest forwarding or node-wide admission.

- `Pulso.Alerting` — experimental object-backed rule management, immutable revision audit history, per-rule event replay, and opt-in native Prometheus threshold evaluation. Grafana wrappers preserve complete original definitions but remain disabled; native Slack outboxes and request-scoped live resource subscriptions are experimental; Grafana notification-policy parity is not implemented. See `docs/alerting.md` for current semantics and limits.

Not yet built: full Grafana alert/notification compatibility, Mimir/Tempo clients, sidecar indexes (bloom filters, posting lists, stats — label postings are the first follow-up on the metrics path), traces signal, OTLP/HTTP metrics (`/v1/metrics`), remediation surface, HITL wiring, ingest forwarding and ownership. Follow-up priority: label posting indexes, OpenTelemetry metrics ingestion, then alert evaluation.

## Design bet

Grafana's Loki/Mimir/Tempo (and the VictoriaMetrics stack) are already headless, API-only storage — but they're three systems coordinated externally, and existing "AI-ready" MCP servers on top are thin read-only wrappers bolted on after the fact.

Pulso's bet: build the ingest, storage-facade, alerting, and MCP-interface layer as one coherent system, on a runtime (BEAM/OTP) whose concurrency and supervision model fits this problem shape unusually well.

**v1 shape**: headless nodes with object storage as the source of truth, immutable Parquet segments, and conditional manifest writes. Query tools, protocol-compatible ingestion, and future alerting share the same storage path. `docs/architecture.md` is authoritative for this design.

## Why Elixir/OTP

- **Per-process resource isolation** — each stream/tenant/query can be its own process with an independent heap and `max_heap_size` cap. Kill a runaway unit of work without taking down the node.
- **Scheduler fairness** — preemptive, reduction-based scheduling; one expensive query cannot easily starve the rest.
- **Backpressure-aware ingestion** — GenStage/Broadway for demand-driven pipelines. Process mailboxes are unbounded by default, so this must be designed for deliberately.
- **Self-healing alerting** — each alert rule as a supervised process; "let it crash and restart" maps directly onto rule-evaluation correctness.
- **Clustering without external coordinators** — membership discovery and rendezvous hashing assign expected owners; object-storage conditional writes enforce correctness during ownership changes.
- **Phoenix PubSub** — distributed pub/sub for cross-signal correlation and alert fan-out.

## Known constraints to design around

- Default distributed Erlang is a full mesh, doesn't scale cleanly past ~100–200 nodes without partitioned topologies.
- Ownership is an optimization, not a correctness guarantee. Alert fires must use conditional object creation, as described in `docs/architecture.md`; do not add leader election or consensus services.
- Binary sub-references can pin large buffers in memory — `:binary.copy/1` discipline is needed when parsing large payloads and keeping small slices.
- Distributed Erlang's cookie auth is weak by default — cluster must stay inside a VPC, or use TLS distribution.

## MCP read/write boundary (load-bearing rule)

Pulso deliberately splits its MCP surface by risk class:

| Tier | Where it lives | Examples |
|---|---|---|
| Read-only diagnosis | Pulso's MCP server (this repo) | query logs/metrics/traces, correlate signals, fetch alert context |
| Write within Pulso | Pulso's MCP server (this repo) | silence/ack alert, attach annotation |
| Infrastructure remediation | **Separate MCP server, not this repo** | restart service, roll back deploy, scale |

**Do not** add infrastructure remediation tools to `Pulso.MCP.Tools`. Their blast radius must be contained by construction, not by hoping the agent behaves. Anything destructive should be gated through the human-in-the-loop pause that the agent runtime (e.g. Google AX) provides natively.

## Repo layout

```
lib/
  pulso/
    application.ex        # OTP supervision root
    loki.ex               # Read-only Loki HTTP client
    mcp.ex                # JSON-RPC dispatcher (public MCP entry point)
    mcp/
      tools.ex            # Query and capability-scoped alert tool registry
  pulso_web/
    controllers/
      mcp_controller.ex   # POST /mcp — thin JSON-RPC transport
    endpoint.ex
    router.ex
    telemetry.ex
config/                   # Standard Phoenix config; Loki base_url lives here
charts/pulso/             # Helm chart, published with each release
docs/                     # User-facing documentation; docs/README.md is the index
```

Storage backend URLs are read from `config :pulso, Pulso.Loki, base_url: ...` and analogous config keys for future backends. Never hardcode.

## Conventions

- **HTTP client**: use `Req`. Never `HTTPoison`, `Tesla`, `:httpc`, or `Finch` directly.
- **JSON**: use `Pulso.JSON` (Rust fast path that defers to Elixir's built-in `JSON` for anything it cannot match exactly, so behavior and errors are `JSON`'s), or `JSON` itself where the native code is not loaded yet (`config/runtime.exs`); never `Jason`. Phoenix's `:json_library` is set to `Pulso.JSON` in `config/config.exs`, and a Credo rule (`Credo.Check.Warning.ForbiddenModule`) fails CI on any direct `Jason.*` reference. Jason may still appear as a transitive dep of `phoenix` or a dev dep, but no code in `lib/`, `config/`, or `test/` may call it.
- **Rust fast paths** must be semantically identical to an Elixir reference implementation: when the Rust side cannot guarantee the same result it returns `:fallback` and the Elixir code runs. Tests compare the two on randomized input and assert the Rust path actually answered. The one exception is the Parquet log-segment codec (`Pulso.Codec.NIF.{encode,decode}_log_segment_parquet`): Parquet is not a fast path for a pre-existing Elixir behaviour, so there is no reference implementation and no Elixir fallback — a `:fallback` there is a hard error (`{:error, {:encode_failed | :decode_failed, _}}`), mirroring `Pulso.ObjectStore.NIF`, which has no Elixir S3 client to fall back to. Round-trip tests fuzz `encode → decode → encode → decode` and assert idempotency from the first decode onwards.
- **New backends** go under `Pulso.<Backend>` (e.g. `Pulso.Mimir`, `Pulso.Tempo`), with the same read-only-first shape as `Pulso.Loki`. Every read function must accept a `:base_url` override in opts.
- **MCP tools** live in `Pulso.MCP.Tools`. Each tool has an `inputSchema`, and its `call/2` clause returns `{:ok, [content_block]}` or `{:error, reason}`. Content blocks follow the MCP shape: `%{"type" => "text", "text" => "..."}`.
- **Alerting**: current native evaluation runs in bounded supervised work with rendezvous ownership and object-store conditional head publication. Full revisions are their audit records; stale candidates never become fires. Preserve the distinction between the implemented native slice and unimplemented Grafana/delivery semantics. Configuration changes currently reset native lifecycle; imported Grafana rules cannot be enabled.
- **Ingestion** (when added): pull-based via Broadway/GenStage. No unbounded process mailboxes.
- **Naming**: predicate functions end in `?`, not `is_` (see Elixir guidelines below).
- **Rust NIF distribution**: the NIF crates under `native/` (`pulso_object_store`, `pulso_codec`) ship via `rustler_precompiled`. Every `v*` tag triggers `.github/workflows/release.yml`, which builds artifacts for each crate and the target triples in its `lib/pulso/*/nif.ex` module and attaches them to the matching GitHub Release. Downstream consumers install without a Cargo toolchain. Local dev keeps compiling from source (`PULSO_NIF_FORCE_BUILD=true` is the default); unset it to opt into the precompiled path.
- **Memory copies across the NIF boundary**: minimize them. GET streams the S3 body into a Rustler `NewBinary` allocated on the Erlang heap (one copy total, no Rust-side intermediate). PUT does not copy: the Erlang binary is saved into a process-independent `OwnedEnv` and wrapped with `Bytes::from_owner`, so every clone reqwest's retry layer makes shares an owner that keeps the binary alive (a lifetime-extended slice would be unsound for exactly that reason). S3 clients are cached per config so connections are reused. The codec (`pulso_codec`) writes encoder output straight into an Erlang binary and never copies it at the end. The Loki decoder decompresses once into a `NewBinary` and returns every string as a sub-binary of it, and the JSON and segment decoders return strings over 64 bytes as sub-binaries of their input, so retained decoded data pins its source buffer: `:binary.copy/1` anything kept past the request. The Parquet segment decoder allocates one Erlang binary per string column per batch (the arena) and returns every row's string field as a sub-binary of it — a batch of N rows costs 7 fresh binaries per column, not 7 × N. It skips UTF-8 revalidation on Arrow's `StringBuilder` output on encode (the JSON encoder is documented to emit valid UTF-8 already), so per-row string-column encode cost is a single memcpy. The Parquet reader still copies the whole input blob into a `Bytes` for `ChunkReader` ownership — one memcpy per read per segment, revisit when the query path is hot.

## Documentation

User-facing documentation lives in `docs/`, with [`docs/README.md`](./docs/README.md) as the index. Keep it current in the same change that alters behavior: new or renamed environment variables, endpoints, limits, chart values, storage requirements, or operational procedures must update the relevant page (and the index when a page is added).

- Write for people self-hosting Pulso: what to configure, what to expose, how to operate and upgrade it, and what is not supported yet.
- Leave out internal details that do not help an operator: module names, implementation history, review notes, and anything specific to a particular organization's deployment, rollout, or companion services. Contributor-facing design belongs in `docs/architecture.md`; rollout planning belongs in `plans/`.
- The Helm chart in `charts/pulso` ships with every release alongside the container image, at the same version. Keep `charts/pulso/values.yaml` comments, `docs/deployment.md`, and `docs/configuration.md` in sync with `config/runtime.exs`.

## Development workflow

- **Pull request titles must follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/)**: `type(scope): summary` or `type: summary`. Use an appropriate type (`feat`, `fix`, `docs`, `chore`, `refactor`, `test`, `perf`, `build`, `ci`, or `style`) and a concise imperative summary. For example: `docs(alerting): plan stateless alerting and change history`. Conventional commit messages do not replace this requirement: validate the PR title itself before creating or updating a pull request.
- `mix setup` — fetch deps
- `mix compile --warnings-as-errors` — must be clean before merge
- `mix test` or `mix test test/path/to_test.exs`
- `mix precommit` — runs compile-with-warnings-as-errors, unused-deps check, formatter, tests
- The Elixir/Erlang toolchain is pinned in `mise.toml`; run `mise install` to match.

## Related resources

- Design brief covering Pulso's rationale, alerting model, and agent-integration tiers lives in the project notes (paste from the Claude Desktop session), not in-repo yet.
- Grafana Loki HTTP API — the shape `Pulso.Loki` targets.
- Model Context Protocol spec — `Pulso.MCP` targets the stateless 2026-07-28 protocol version only. Legacy initialize-based clients (2025-11-25 and earlier) receive a `400` naming the supported version.

---

The sections below are auto-managed framework rules. Preserve the markers.

<!-- usage-rules-start -->

<!-- phoenix:elixir-start -->
## Elixir guidelines

- Elixir lists **do not support index based access via the access syntax**

  **Never do this (invalid)**:

      i = 0
      mylist = ["blue", "green"]
      mylist[i]

  Instead, **always** use `Enum.at`, pattern matching, or `List` for index based list access, ie:

      i = 0
      mylist = ["blue", "green"]
      Enum.at(mylist, i)

- Elixir variables are immutable, but can be rebound, so for block expressions like `if`, `case`, `cond`, etc
  you *must* bind the result of the expression to a variable if you want to use it and you CANNOT rebind the result inside the expression, ie:

      # INVALID: we are rebinding inside the `if` and the result never gets assigned
      if connected?(socket) do
        socket = assign(socket, :val, val)
      end

      # VALID: we rebind the result of the `if` to a new variable
      socket =
        if connected?(socket) do
          assign(socket, :val, val)
        end

- **Never** nest multiple modules in the same file as it can cause cyclic dependencies and compilation errors
- **Never** use map access syntax (`changeset[:field]`) on structs as they do not implement the Access behaviour by default. For regular structs, you **must** access the fields directly, such as `my_struct.field` or use higher level APIs that are available on the struct if they exist, `Ecto.Changeset.get_field/2` for changesets
- Elixir's standard library has everything necessary for date and time manipulation. Familiarize yourself with the common `Time`, `Date`, `DateTime`, and `Calendar` interfaces by accessing their documentation as necessary. **Never** install additional dependencies unless asked or for date/time parsing (which you can use the `date_time_parser` package)
- Don't use `String.to_atom/1` on user input (memory leak risk)
- Predicate function names should not start with `is_` and should end in a question mark. Names like `is_thing` should be reserved for guards
- Elixir's builtin OTP primitives like `DynamicSupervisor` and `Registry`, require names in the child spec, such as `{DynamicSupervisor, name: MyApp.MyDynamicSup}`, then you can use `DynamicSupervisor.start_child(MyApp.MyDynamicSup, child_spec)`
- Use `Task.async_stream(collection, callback, options)` for concurrent enumeration with back-pressure. The majority of times you will want to pass `timeout: :infinity` as option

## Mix guidelines

- Read the docs and options before using tasks (by using `mix help task_name`)
- To debug test failures, run tests in a specific file with `mix test test/my_test.exs` or run all previously failed tests with `mix test --failed`
- `mix deps.clean --all` is **almost never needed**. **Avoid** using it unless you have good reason

## Test guidelines

- **Always use `start_supervised!/1`** to start processes in tests as it guarantees cleanup between tests
- **Avoid** `Process.sleep/1` and `Process.alive?/1` in tests
  - Instead of sleeping to wait for a process to finish, **always** use `Process.monitor/1` and assert on the DOWN message:

      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}

   - Instead of sleeping to synchronize before the next call, **always** use `_ = :sys.get_state/1` to ensure the process has handled prior messages
<!-- phoenix:elixir-end -->

<!-- phoenix:phoenix-start -->
## Phoenix guidelines

- Remember Phoenix router `scope` blocks include an optional alias which is prefixed for all routes within the scope. **Always** be mindful of this when creating routes within a scope to avoid duplicate module prefixes.

- You **never** need to create your own `alias` for route definitions! The `scope` provides the alias, ie:

      scope "/admin", AppWeb.Admin do
        pipe_through :browser

        live "/users", UserLive, :index
      end

  the UserLive route would point to the `AppWeb.Admin.UserLive` module

- `Phoenix.View` no longer is needed or included with Phoenix, don't use it
<!-- phoenix:phoenix-end -->

<!-- usage-rules-end -->
