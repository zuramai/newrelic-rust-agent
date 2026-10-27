//! `NewRelicLayer` turns `tracing` spans into transactions/segments and events into logs.
#![cfg(feature = "tracing")]

mod common;

use std::time::Duration;

use common::{bodies, events, fake_collector, has_metric, metric_names};
use newrelic_rust_agent::{NewRelicLayer, NewRelicSpanExt, TransactionKind};
use tracing_subscriber::layer::SubscriberExt as _;

#[tracing::instrument(fields(newrelic.transaction = "other"))]
async fn nightly_job() {
    load_rows(3).await;
    tracing::info!(rows = 3, "loaded rows");
}

#[tracing::instrument]
async fn load_rows(count: u64) {
    tokio::time::sleep(Duration::from_millis(5)).await;
}

#[tracing::instrument(fields(newrelic.transaction = "other"))]
async fn job_with_debug_span() {
    debug_wrapper().await;
}

#[tracing::instrument(level = "debug")]
async fn debug_wrapper() {
    load_rows(1).await;
}

#[tracing::instrument(err)]
async fn failing_step() -> Result<(), std::io::Error> {
    Err(std::io::Error::other("disk on fire"))
}

#[tracing::instrument(fields(newrelic.transaction = "web"))]
async fn handled_error_request() {
    let _ = failing_step().await;
}

#[tokio::test]
async fn instrumented_root_becomes_transaction_with_nested_segment() {
    let (server, agent) = fake_collector().await;
    let subscriber = tracing_subscriber::registry().with(NewRelicLayer::new(agent.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);
    nightly_job().await;
    agent.shutdown(Duration::from_secs(5)).await;
    let names = metric_names(&server).await;
    assert!(has_metric(
        &names,
        "OtherTransaction/Rust/nightly_job",
        None
    ));
    assert!(has_metric(
        &names,
        "Custom/load_rows",
        Some("OtherTransaction/Rust/nightly_job")
    ));
}

#[tokio::test]
async fn debug_spans_are_skipped_and_children_attach_to_reported_ancestor() {
    let (server, agent) = fake_collector().await;
    let subscriber = tracing_subscriber::registry().with(NewRelicLayer::new(agent.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);
    job_with_debug_span().await;
    agent.shutdown(Duration::from_secs(5)).await;
    let names = metric_names(&server).await;
    assert!(!has_metric(&names, "Custom/debug_wrapper", None));
    let spans = events(&server, "span_event_data").await;
    let root = spans
        .iter()
        .find(|s| s[0]["nr.entryPoint"] == true)
        .unwrap();
    let load = spans
        .iter()
        .find(|s| s[0]["name"] == "Custom/load_rows")
        .unwrap();
    assert_eq!(load[0]["parentId"], root[0]["guid"]);
}

#[tokio::test]
async fn span_fields_become_segment_attributes() {
    let (server, agent) = fake_collector().await;
    let subscriber = tracing_subscriber::registry().with(NewRelicLayer::new(agent.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);
    nightly_job().await;
    agent.shutdown(Duration::from_secs(5)).await;
    let spans = events(&server, "span_event_data").await;
    let load = spans
        .iter()
        .find(|s| s[0]["name"] == "Custom/load_rows")
        .unwrap();
    assert_eq!(load[1]["count"], 3);
}

#[tokio::test]
async fn info_event_inside_transaction_is_forwarded_with_trace_id() {
    let (server, agent) = fake_collector().await;
    let subscriber = tracing_subscriber::registry().with(NewRelicLayer::new(agent.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);
    nightly_job().await;
    agent.shutdown(Duration::from_secs(5)).await;
    let batch = &bodies(&server, "log_event_data").await[0][0];
    let log = batch["logs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|l| l["message"] == "loaded rows")
        .unwrap();
    assert_eq!(log["level"], "INFO");
    assert_eq!(log["attributes"]["rows"], 3);
    assert!(log["trace.id"].as_str().is_some_and(|t| t.len() == 32));
    assert_eq!(batch["common"]["attributes"]["entity.guid"], "guid-1");
}

#[tokio::test]
async fn debug_events_are_not_forwarded_by_default() {
    let (server, agent) = fake_collector().await;
    let subscriber = tracing_subscriber::registry().with(NewRelicLayer::new(agent.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);
    tracing::debug!("noise");
    agent.shutdown(Duration::from_secs(5)).await;
    assert!(bodies(&server, "log_event_data").await.is_empty());
}

#[tokio::test]
async fn handled_error_event_annotates_span_without_failing_transaction() {
    let (server, agent) = fake_collector().await;
    let subscriber = tracing_subscriber::registry()
        .with(NewRelicLayer::new(agent.clone()).with_log_forwarding(None));
    let _guard = tracing::subscriber::set_default(subscriber);
    handled_error_request().await;
    agent.shutdown(Duration::from_secs(5)).await;
    let names = metric_names(&server).await;
    assert!(has_metric(
        &names,
        "WebTransaction/Rust/handled_error_request",
        None
    ));
    assert!(!has_metric(&names, "Errors/allWeb", None));
    let spans = events(&server, "span_event_data").await;
    let step = spans
        .iter()
        .find(|s| s[0]["name"] == "Custom/failing_step")
        .unwrap();
    assert_eq!(step[2]["error.message"], "disk on fire");
}

#[tokio::test]
async fn span_ext_links_explicit_transaction_to_span() {
    let (server, agent) = fake_collector().await;
    let subscriber = tracing_subscriber::registry().with(NewRelicLayer::new(agent.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);
    let txn = agent.start_transaction("explicit", TransactionKind::Other);
    let span = tracing::info_span!("wrapper");
    span.set_newrelic_transaction(&txn);
    assert_eq!(
        span.newrelic_transaction().and_then(|t| t.trace_id()),
        txn.trace_id()
    );
    {
        let _enter = span.enter();
        tracing::info_span!("child").in_scope(|| {});
    }
    drop(span);
    txn.end();
    agent.shutdown(Duration::from_secs(5)).await;
    assert!(has_metric(
        &metric_names(&server).await,
        "Custom/child",
        Some("OtherTransaction/Rust/explicit")
    ));
}
