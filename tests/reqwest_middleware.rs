//! `NewRelicMiddleware` records External segments and propagates trace context.
#![cfg(all(feature = "reqwest", feature = "tracing"))]

mod common;

use std::time::Duration;

use common::{events, fake_collector, has_metric, metric_names};
use newrelic_rust_agent::{NewRelicLayer, NewRelicMiddleware, NewRelicSpanExt, TransactionKind};
use tracing_subscriber::layer::SubscriberExt as _;
use wiremock::matchers::path;
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client() -> reqwest_middleware::ClientWithMiddleware {
    reqwest_middleware::ClientBuilder::new(reqwest::Client::new())
        .with(NewRelicMiddleware)
        .build()
}

fn is_traceparent(v: &str) -> bool {
    let parts: Vec<&str> = v.split('-').collect();
    let hex = |s: &str, n: usize| {
        s.len() == n
            && s.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    };
    parts.len() == 4
        && parts[0] == "00"
        && hex(parts[1], 32)
        && hex(parts[2], 16)
        && (parts[3] == "00" || parts[3] == "01")
}

#[tokio::test]
async fn outbound_call_gets_traceparent_and_external_metric() {
    let (collector, agent) = fake_collector().await;
    let upstream = MockServer::start().await;
    Mock::given(path("/thing"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&upstream)
        .await;
    let txn = agent.start_transaction("job", TransactionKind::Other);
    client()
        .get(format!("{}/thing?secret=1", upstream.uri()))
        .with_extension(txn.context())
        .send()
        .await
        .unwrap();
    let trace_id = txn.trace_id().unwrap();
    txn.end();
    agent.shutdown(Duration::from_secs(5)).await;

    let received = upstream.received_requests().await.unwrap();
    let tp = received[0]
        .headers
        .get("traceparent")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(is_traceparent(tp), "{tp}");
    assert_eq!(tp.split('-').nth(1), Some(trace_id.as_str()));

    let port = upstream.address().port();
    let host = format!("127.0.0.1:{port}");
    let names = metric_names(&collector).await;
    assert!(has_metric(&names, &format!("External/{host}/all"), None));
    assert!(has_metric(&names, "External/allOther", None));
    assert!(has_metric(
        &names,
        &format!("External/{host}/http/GET"),
        Some("OtherTransaction/Rust/job")
    ));

    let spans = events(&collector, "span_event_data").await;
    let ext = spans.iter().find(|s| s[0]["category"] == "http").unwrap();
    assert_eq!(ext[2]["http.url"], format!("http://{host}/thing"));
    assert_eq!(ext[2]["http.statusCode"], 204);
    assert_eq!(ext[0]["span.kind"], "client");
}

#[tokio::test]
async fn outbound_call_uses_current_tracing_span_context() {
    let (_collector, agent) = fake_collector().await;
    let upstream = MockServer::start().await;
    Mock::given(path("/thing"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&upstream)
        .await;
    let subscriber = tracing_subscriber::registry().with(NewRelicLayer::new(agent.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);
    let txn = agent.start_transaction("job", TransactionKind::Other);
    let span = tracing::info_span!("request");
    span.set_newrelic_transaction(&txn);
    {
        use tracing::Instrument as _;
        client()
            .get(format!("{}/thing", upstream.uri()))
            .send()
            .instrument(span.clone())
            .await
            .unwrap();
    }
    drop(span);
    let trace_id = txn.trace_id().unwrap();
    txn.end();
    agent.shutdown(Duration::from_secs(5)).await;
    let received = upstream.received_requests().await.unwrap();
    let tp = received[0]
        .headers
        .get("traceparent")
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(tp.split('-').nth(1), Some(trace_id.as_str()));
    assert!(received[0].headers.get("newrelic").is_some());
    assert!(
        received[0]
            .headers
            .get("tracestate")
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("1@nr=0-0-1-2-")
    );
}

#[tokio::test]
async fn outbound_call_outside_transaction_is_untouched() {
    let (_collector, agent) = fake_collector().await;
    let upstream = MockServer::start().await;
    Mock::given(path("/thing"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&upstream)
        .await;
    client()
        .get(format!("{}/thing", upstream.uri()))
        .send()
        .await
        .unwrap();
    agent.shutdown(Duration::from_secs(5)).await;
    let received = upstream.received_requests().await.unwrap();
    assert!(received[0].headers.get("traceparent").is_none());
}
