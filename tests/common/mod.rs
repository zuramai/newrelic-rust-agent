//! Fake New Relic collector for integration tests.
#![allow(
    dead_code,
    reason = "each test binary uses a different subset of helpers"
)]
#![expect(
    clippy::unwrap_used,
    reason = "test fixtures panic on malformed collector payloads"
)]

use std::io::Read as _;
use std::time::Duration;

use newrelic_rust_agent::Agent;
use serde_json::{Value, json};
use wiremock::matchers::{method, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

pub async fn fake_collector() -> (MockServer, Agent) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(query_param("method", "preconnect"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"return_value": {"redirect_host": "ignored"}})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(query_param("method", "connect"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"return_value": {
                "agent_run_id": "run-1", "entity_guid": "guid-1", "account_id": "1",
                "trusted_account_key": "1", "primary_application_id": "2", "apdex_t": 0.5,
                "sampling_target": 10, "sampling_target_period_in_seconds": 60,
                "event_harvest_config": {"report_period_ms": 5000, "harvest_limits": {
                    "analytic_event_data": 833, "custom_event_data": 2500, "log_event_data": 833,
                    "error_event_data": 8, "span_event_data": 166}}
            }})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"return_value": null})))
        .mount(&server)
        .await;
    let agent = Agent::builder()
        .license_key("a".repeat(40))
        .app_name("test-app")
        .host(server.uri())
        .build()
        .unwrap();
    assert!(agent.wait_for_connection(Duration::from_secs(5)).await);
    (server, agent)
}

/// Gunzipped JSON bodies received for a collector method.
pub async fn bodies(server: &MockServer, collector_method: &str) -> Vec<Value> {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .iter()
        .filter(|r| {
            r.url
                .query_pairs()
                .any(|(k, v)| k == "method" && v == collector_method)
        })
        .map(|r| {
            let mut json = String::new();
            flate2::read::GzDecoder::new(r.body.as_slice())
                .read_to_string(&mut json)
                .unwrap();
            serde_json::from_str(&json).unwrap()
        })
        .collect()
}

/// Metric names (with optional scope) in all `metric_data` payloads.
pub async fn metric_names(server: &MockServer) -> Vec<(String, Option<String>)> {
    bodies(server, "metric_data")
        .await
        .iter()
        .flat_map(|b| b[3].as_array().cloned().unwrap_or_default())
        .map(|row| {
            (
                row[0]["name"].as_str().unwrap().to_owned(),
                row[0]["scope"].as_str().map(str::to_owned),
            )
        })
        .collect()
}

pub fn has_metric(names: &[(String, Option<String>)], name: &str, scope: Option<&str>) -> bool {
    names
        .iter()
        .any(|(n, s)| n == name && s.as_deref() == scope)
}

/// All events of an event endpoint (`[run_id, info, [events]]`).
pub async fn events(server: &MockServer, collector_method: &str) -> Vec<Value> {
    bodies(server, collector_method)
        .await
        .iter()
        .flat_map(|b| b[2].as_array().cloned().unwrap_or_default())
        .collect()
}
