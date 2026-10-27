//! Collector protocol contract tests against a fake collector.

mod common;

use std::time::Duration;

use common::{bodies, events, fake_collector, has_metric, metric_names};
use newrelic_rust_agent::TransactionKind;

#[tokio::test]
async fn shutdown_sends_web_transaction_metrics() {
    let (server, agent) = fake_collector().await;
    agent
        .start_transaction("GET /x", TransactionKind::Web)
        .end();
    agent.shutdown(Duration::from_secs(5)).await;
    let names = metric_names(&server).await;
    assert!(has_metric(&names, "WebTransaction/Rust/GET /x", None));
    assert!(has_metric(&names, "Apdex", None));
    assert!(has_metric(&names, "Instance/Reporting", None));
}

#[tokio::test]
async fn shutdown_sends_transaction_event_with_dt_intrinsics() {
    let (server, agent) = fake_collector().await;
    agent
        .start_transaction("GET /x", TransactionKind::Web)
        .end();
    agent.shutdown(Duration::from_secs(5)).await;
    let events = events(&server, "analytic_event_data").await;
    assert_eq!(events.len(), 1);
    let intrinsics = &events[0][0];
    assert_eq!(intrinsics["name"], "WebTransaction/Rust/GET /x");
    for key in ["guid", "traceId", "priority", "sampled"] {
        assert!(intrinsics.get(key).is_some(), "missing {key}");
    }
}

#[tokio::test]
async fn harvest_payloads_carry_the_run_id_and_negotiated_reservoir_size() {
    let (server, agent) = fake_collector().await;
    agent.start_transaction("job", TransactionKind::Other).end();
    agent.shutdown(Duration::from_secs(5)).await;
    let body = &bodies(&server, "analytic_event_data").await[0];
    assert_eq!(body[0], "run-1");
    assert_eq!(body[1]["reservoir_size"], 833);
}

#[tokio::test]
async fn connect_payload_uses_accepted_language_and_app_name() {
    let (server, agent) = fake_collector().await;
    agent.shutdown(Duration::from_secs(5)).await;
    let connect = &bodies(&server, "connect").await[0][0];
    assert_eq!(connect["language"], "c");
    assert_eq!(connect["app_name"], serde_json::json!(["test-app"]));
    assert_eq!(connect["identifier"], "test-app");
}

#[tokio::test]
async fn custom_events_and_metrics_are_harvested() {
    let (server, agent) = fake_collector().await;
    agent
        .record_custom_event("JobRun", [("items", 42)])
        .unwrap();
    agent.record_custom_metric("job/items", 42.0);
    agent.shutdown(Duration::from_secs(5)).await;
    let events = events(&server, "custom_event_data").await;
    assert_eq!(events[0][0]["type"], "JobRun");
    assert_eq!(events[0][1]["items"], 42);
    assert!(has_metric(
        &metric_names(&server).await,
        "Custom/job/items",
        None
    ));
}

#[tokio::test]
async fn record_custom_event_rejects_invalid_type() {
    let (_server, agent) = fake_collector().await;
    let err = agent
        .record_custom_event("bad-type!", [("a", 1)])
        .unwrap_err();
    assert!(matches!(
        err,
        newrelic_rust_agent::Error::InvalidEventType(_)
    ));
}

#[tokio::test]
async fn errors_produce_error_events_and_traced_errors() {
    let (server, agent) = fake_collector().await;
    let txn = agent.start_transaction("GET /fail", TransactionKind::Web);
    txn.set_status_code(503);
    txn.end();
    agent.shutdown(Duration::from_secs(5)).await;
    let error_events = events(&server, "error_event_data").await;
    assert_eq!(error_events[0][0]["error.class"], "503");
    assert_eq!(error_events[0][0]["error.message"], "Service Unavailable");
    let traced = &bodies(&server, "error_data").await[0];
    assert_eq!(traced[1][0][1], "WebTransaction/Rust/GET /fail");
    assert!(has_metric(
        &metric_names(&server).await,
        "Errors/allWeb",
        None
    ));
}

#[tokio::test]
async fn ignored_transaction_records_nothing() {
    let (server, agent) = fake_collector().await;
    let txn = agent.start_transaction("GET /health", TransactionKind::Web);
    txn.ignore();
    txn.end();
    agent.shutdown(Duration::from_secs(5)).await;
    assert!(events(&server, "analytic_event_data").await.is_empty());
    assert!(!has_metric(
        &metric_names(&server).await,
        "WebTransaction/Rust/GET /health",
        None
    ));
}

#[tokio::test]
async fn segments_produce_scoped_metrics_and_span_events() {
    let (server, agent) = fake_collector().await;
    let txn = agent.start_transaction("job", TransactionKind::Other);
    let outer = txn.start_segment("outer");
    outer.start_segment("inner").end();
    outer.end();
    txn.end();
    agent.shutdown(Duration::from_secs(5)).await;
    let names = metric_names(&server).await;
    assert!(has_metric(
        &names,
        "Custom/inner",
        Some("OtherTransaction/Rust/job")
    ));
    let spans = events(&server, "span_event_data").await;
    let span_names: Vec<_> = spans
        .iter()
        .map(|s| s[0]["name"].as_str().unwrap().to_owned())
        .collect();
    assert!(span_names.contains(&"Custom/outer".to_owned()));
    assert!(span_names.contains(&"OtherTransaction/Rust/job".to_owned()));
}

#[tokio::test]
async fn inbound_distributed_trace_continues_the_callers_trace() {
    let (server, agent) = fake_collector().await;
    let upstream = agent.start_transaction("caller", TransactionKind::Other);
    let mut headers = http::HeaderMap::new();
    upstream.insert_distributed_trace_headers(&mut headers);
    let downstream = agent.start_transaction("GET /callee", TransactionKind::Web);
    downstream.accept_distributed_trace_headers(&headers, newrelic_rust_agent::TransportType::Http);
    assert_eq!(downstream.trace_id(), upstream.trace_id());
    downstream.end();
    upstream.end();
    agent.shutdown(Duration::from_secs(5)).await;
    let names = metric_names(&server).await;
    assert!(has_metric(
        &names,
        "DurationByCaller/App/1/2/HTTP/all",
        None
    ));
    assert!(has_metric(
        &names,
        "TransportDuration/App/1/2/HTTP/allWeb",
        None
    ));
}

#[tokio::test]
async fn disabled_agent_is_a_no_op() {
    let agent = newrelic_rust_agent::Agent::disabled();
    let txn = agent.start_transaction("x", TransactionKind::Web);
    assert!(txn.trace_id().is_none());
    assert!(!agent.wait_for_connection(Duration::from_millis(10)).await);
    agent.shutdown(Duration::from_secs(1)).await;
}
