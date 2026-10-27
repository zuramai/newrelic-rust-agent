# newrelic-rust-agent

[![crates.io](https://img.shields.io/crates/v/newrelic-rust-agent.svg)](https://crates.io/crates/newrelic-rust-agent)
[![docs.rs](https://docs.rs/newrelic-rust-agent/badge.svg)](https://docs.rs/newrelic-rust-agent)
[![license](https://img.shields.io/crates/l/newrelic-rust-agent.svg)](LICENSE)

A native [New Relic](https://newrelic.com) APM agent for Rust.

## Why

Exporting OpenTelemetry (OTLP) to New Relic gives you distributed traces, but the service is
not a full **APM** entity. The Transactions tab, Apdex, the APM error rate, transaction traces
and the "web transactions time" charts stay empty.

This crate talks to New Relic's **collector protocol (v17)** directly, the same protocol the
official Go, Java and Python agents use. Your Rust service shows up under
**APM & Services** with:

- **Transactions**: throughput, response time, Apdex, and the slowest transaction traces.
- **Errors**: error rate, the Errors inbox (error events and traced errors).
- **Distributed tracing**: W3C `traceparent`/`tracestate` plus the `newrelic` header,
  interoperable with every other New Relic agent. Service maps work too.
- **Logs in context**: `tracing` events forwarded to New Relic Logs, linked to trace and span.
- **Custom events and metrics**.

It plugs into the ecosystem you already use. Existing `#[tracing::instrument]` spans become
segments, a tower layer creates one transaction per request (axum, tonic, hyper), and a
`reqwest-middleware` middleware times outbound calls.

## Cargo features

| Feature | Default | Enables |
|---|---|---|
| `tracing` | yes | `NewRelicLayer` (spans → transactions/segments, events → logs) and `NewRelicSpanExt` |
| `tower` | no | `TransactionLayer`: one web transaction per HTTP/gRPC request (axum, tonic, hyper) |
| `axum` | no | `tower` plus route-template naming via axum's `MatchedPath` (`GET /users/{id}`) |
| `reqwest` | no | `NewRelicMiddleware` for `reqwest-middleware`: External segments plus outbound trace headers |

```toml
[dependencies]
newrelic-rust-agent = { version = "0.1", features = ["axum", "reqwest"] }
```

The crate requires a Tokio runtime. Its MSRV is 1.88.

## Quick start

```sh
export NEW_RELIC_LICENSE_KEY=<your 40-character ingest license key>
export NEW_RELIC_APP_NAME=my-service
```

```rust
use std::time::Duration;
use newrelic_rust_agent::{Agent, NewRelicLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let agent = Agent::from_env()?; // connects in the background

    tracing_subscriber::registry()
        .with(NewRelicLayer::new(agent.clone()))
        // Filter console output per layer, so New Relic still sees INFO spans.
        .with(tracing_subscriber::fmt::layer().with_filter(
            // An unset RUST_LOG would otherwise hide everything below ERROR.
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        ))
        .init();

    run_app().await;

    agent.shutdown(Duration::from_secs(10)).await; // flush buffered data
    Ok(())
}

async fn run_app() { /* your server or job */ }
```

Data is harvested every 60 seconds; events may be sent more often if the server asks for it.
Expect the service to appear in New Relic one or two minutes after the first request.

> **Filters:** apply `EnvFilter` (or any level filter) **per layer**, as in the snippet above.
> A global filter that disables `INFO` spans also hides them from `NewRelicLayer`, and then
> segments and request linking stop working.

## Configuration

| Environment variable | Builder method | Meaning |
|---|---|---|
| `NEW_RELIC_LICENSE_KEY` | `.license_key(..)` | **Required.** 40-character ingest license key. |
| `NEW_RELIC_APP_NAME` | `.app_name(..)` | **Required.** Application name. Up to 3 names separated by `;` (for example `svc;svc-all`) also report into the rollup apps. |
| `NEW_RELIC_ENABLED` | `.enabled(..)` | `true`/`false`/`1`/`0`, default `true`. When disabled nothing is validated and everything is a no-op. |
| `NEW_RELIC_HOST` | `.host(..)` | Collector host override. A bare host uses HTTPS; a full URL such as `http://127.0.0.1:8080` is used as-is (proxies, tests). |

The region comes from the license key. An EU key (`eu01xx…`) talks to
`collector.eu01.nr-data.net`, a US key to `collector.newrelic.com`. Builder values take
precedence over environment variables:

```rust
let agent = Agent::builder()
    .app_name("face-service")
    .license_key(std::env::var("NR_KEY")?)
    .build()?;
```

## Instrumenting code with `tracing`

With `NewRelicLayer` installed, every span created **inside a transaction** becomes a segment
named `Custom/{span name}`, and its fields become span attributes. Existing code needs no
changes:

```rust
#[tracing::instrument(skip_all, err)]
async fn detect_face(image: &[u8]) -> Result<Faces, Error> {
    let tensor = preprocess(image).await?; // Custom/preprocess, nested
    run_model(tensor).await                // Custom/run_model, nested
}
```

By default only spans at `INFO` or more severe are reported (`#[instrument]` uses `INFO`). This
keeps the `DEBUG`/`TRACE` spans of hyper, h2 and similar crates out of your traces. Change it
with `NewRelicLayer::new(agent).with_span_level(Level::DEBUG)`.

**Background transactions.** A span with a `newrelic.transaction` field starts a
transaction when there is no enclosing one. The value `"web"` makes it a web transaction; any
other value makes it a background (`OtherTransaction`) transaction. The transaction ends when
the span closes.

```rust
#[tracing::instrument(fields(newrelic.transaction = "other"))]
async fn nightly_sync() { /* … */ }        // OtherTransaction/Rust/nightly_sync
```

**Errors.** An `ERROR` event inside a segment (for example from `#[instrument(err)]`) sets
`error.class` and `error.message` on that span. It does **not** mark the transaction as failed,
so errors you handle do not inflate the error rate. A transaction counts as failed when one of
these happens:

- HTTP status ≥ 500,
- a failing gRPC status (see below),
- an explicit `txn.notice_error(&err)` or `txn.notice_error_with(NoticedError { .. })`.

## axum

```rust
use axum::{routing::get, Extension, Router};
use newrelic_rust_agent::{Transaction, TransactionLayer};

let app = Router::new()
    .route("/users/{id}", get(get_user))
    .route("/checkout", get(checkout))
    .layer(TransactionLayer::new(agent.clone()).ignore_path("/health"));

async fn checkout(Extension(txn): Extension<Transaction>) -> &'static str {
    txn.add_attribute("cart.items", 3);
    "ok"
}
```

- Transactions are named `"{METHOD} {route template}"`, for example
  `WebTransaction/Rust/GET /users/{id}`. Apply the layer with `Router::layer` so axum's
  `MatchedPath` is available. Without the `axum` feature, or with a different axum major
  version, the name falls back to the bare method (`GET`). Rename the transaction with
  `txn.set_name(..)` in that case.
- `Transaction` and `SegmentContext` are inserted into the request extensions.
- Inbound distributed-trace headers are accepted automatically.

## tonic (gRPC)

tonic servers accept any tower layer, so no extra feature is needed beyond `tower`:

```rust
tonic::transport::Server::builder()
    .layer(TransactionLayer::new(agent.clone()).ignore_path("/grpc.health.v1.Health/Check"))
    .add_service(FaceServiceServer::new(svc))
    .serve(addr)
    .await?;
```

Transactions are named after the gRPC method, for example
`WebTransaction/Rust/face.FaceService/Detect`. The `grpc-status` code is recorded as the
`grpcStatusCode` attribute. These codes count as errors, with the decoded `grpc-message` as the
message:

| Code | Name | Error? |
|---|---|---|
| 0 | OK | no |
| 1, 3, 5–11, 16 | Cancelled, InvalidArgument, NotFound, … | no (client-side faults) |
| 2 | Unknown | **yes** |
| 4 | DeadlineExceeded | **yes** |
| 12 | Unimplemented | **yes** |
| 13 | Internal | **yes** |
| 14 | Unavailable | **yes** |
| 15 | DataLoss | **yes** |

## Outbound HTTP with reqwest

```rust
let client = reqwest_middleware::ClientBuilder::new(reqwest::Client::new())
    .with(newrelic_rust_agent::NewRelicMiddleware)
    .build();

client.get("https://api.example.com/v1/items").send().await?;
```

Each request inside a transaction becomes an External segment
(`External/api.example.com/http/GET`) and carries `traceparent`, `tracestate` and `newrelic`
headers. The middleware finds the parent in one of two places:

- the current `tracing` span (with the `tracing` feature),
- an explicit context: `.with_extension(txn.context())`.

Requests made outside any transaction pass through untouched.

## Explicit API (no `tracing`)

```rust
use newrelic_rust_agent::{TransactionKind, TransportType};

let txn = agent.start_transaction("consume order", TransactionKind::Other);
txn.accept_distributed_trace_headers(&incoming_headers, TransportType::Http);

let seg = txn.start_segment("validate");          // Custom/validate
let child = seg.start_segment("load-rules");      // nested
child.end();
seg.end();                                         // or let it drop

let ext = txn.start_external_segment(&http::Method::POST, &"https://payments/charge".parse()?);
ext.insert_distributed_trace_headers(&mut outgoing_headers);
// ... send the request ...
ext.set_status_code(200);
ext.end();

txn.add_attribute("order.id", 1234);
txn.end(); // also ends when the last clone is dropped
```

`Segment`s end when dropped. Bind them to a variable for as long as the work runs.
`SegmentContext` (from `txn.context()` or `seg.context()`) is `Clone + Send`; pass it to
spawned tasks to parent their segments correctly. Inside a `tracing` span, get the current
context with `tracing::Span::current().newrelic_context()`. Link an existing span to a
transaction with `span.set_newrelic_transaction(&txn)`.

## Custom events, custom metrics and logs

```rust
use newrelic_rust_agent::AttributeValue;

agent.record_custom_event("FaceMatch", [
    ("score", AttributeValue::from(0.93)),
    ("model", AttributeValue::from("v4")),
])?;
agent.record_custom_metric("model/latency_ms", 41.0); // reported as Custom/model/latency_ms
```

Query custom events with NRQL: `SELECT * FROM FaceMatch`.

**Log forwarding** is on by default at `INFO`. Each `tracing` event at that level or more
severe is sent to New Relic Logs:

- `message` is the log message,
- the other event fields become attributes,
- events inside a transaction carry `trace.id` and `span.id`, so logs-in-context works.

Change the level with `.with_log_forwarding(Some(Level::WARN))`, or disable forwarding with
`.with_log_forwarding(None)`. Records can also be sent directly with `agent.record_log(..)`
and `txn.record_log(..)`.

## Shutdown and short-lived jobs

`agent.shutdown(timeout)` sends everything still buffered and stops the background task. If the
first connect is still in flight, shutdown waits for it, up to `timeout`, so data from very
short runs is not lost. For jobs that make outbound calls, call
`agent.wait_for_connection(Duration::from_secs(10)).await` first: distributed-trace headers
are only produced once connected.

## Migrating from the Go agent / `vtelemetry`

| Go (`vtelemetry` / go-agent) | Rust |
|---|---|
| `newrelic.NewApplication(ConfigAppName, ConfigLicense, ConfigDistributedTracerEnabled(true))` | `Agent::from_env()` (distributed tracing is always on) |
| Echo/HTTP middleware naming `METHOD + " " + path` | `TransactionLayer` (+ `axum` feature for route templates) |
| gRPC interceptor | `TransactionLayer` on `tonic::transport::Server::builder()` |
| `vtelemetry.StartTx(ctx, tracer, name)` + `defer span.End()` | `#[tracing::instrument]` |
| `txn.StartSegment(name)` / `seg.End()` | `txn.start_segment(name)` / drop or `.end()` |
| `newrelic.NewRoundTripper` | `NewRelicMiddleware` |
| `txn.AddAttribute(k, v)` | `Transaction::add_attribute` |
| `txn.NoticeError(err)` | `Transaction::notice_error` |
| `app.RecordCustomEvent` / `RecordCustomMetric` | `Agent::record_custom_event` / `record_custom_metric` |
| zerolog New Relic hook (`RecordLog`) and log decoration | log forwarding in `NewRelicLayer` |
| `app.Shutdown(10 * time.Second)` | `agent.shutdown(Duration::from_secs(10)).await` |

## Performance notes

- Spans outside any transaction cost one registry lookup and an ancestor walk. Nothing is
  recorded for them.
- Segment names come from `&'static str` span metadata, so starting a segment does not
  allocate a name.
- Each transaction takes one mutex for its own state. Metrics, events and spans are computed
  once at transaction end, and the shared harvest buffer is locked a single time.
- Unsampled transactions build no span events. Sampling follows New Relic's adaptive sampler
  (10 traces per minute by default, as configured by the server).
- Payloads are gzip-compressed and sent from a background task. Request handling never waits
  on the network.

## Limitations

- No datastore segments, slow SQL traces, browser monitoring, or legacy CAT.
- Server-side naming rules (`url_rules`, `transaction_name_rules`, …) are not applied. Name
  transactions with route templates instead.
- New Relic's collector rejects unknown agent languages (`"rust"` gets `503` on connect), so
  the agent identifies itself as `language: "c"` (the C SDK). Metric names still use
  `WebTransaction/Rust/…` and `OtherTransaction/Rust/…`.
- No high-security mode, security policies (LASP) or infinite tracing.
- Errors carry no stack traces.

## License

Licensed under the [Apache License, Version 2.0](LICENSE). Parts of the protocol logic
(adaptive sampler, payload layouts) follow the Apache-2.0 licensed
[New Relic Go agent](https://github.com/newrelic/go-agent).
