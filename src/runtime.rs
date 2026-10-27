//! Background task: connect, harvest periodically, reconnect, final harvest on shutdown.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::sync::watch;
use tokio::time::{Instant, MissedTickBehavior};

use crate::agent::{AgentInner, lock};
use crate::collector::connect::{ConnectOutcome, RunConfig, connect};
use crate::collector::{Collector, Outcome};
use crate::harvest::metrics::{MetricData, MetricKey};
use crate::harvest::payload;
use crate::harvest::reservoir::Reservoir;

const TARGET: &str = "newrelic_rust_agent";
/// Connect retry delays in seconds; the last value repeats forever.
const BACKOFF_SECS: [u64; 6] = [15, 15, 30, 60, 120, 300];

/// What the harvest loop does next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    Continue,
    Reconnect,
    Shutdown,
}

impl Step {
    fn worst(self, other: Self) -> Self {
        match (self, other) {
            (Self::Shutdown, _) | (_, Self::Shutdown) => Self::Shutdown,
            (Self::Reconnect, _) | (_, Self::Reconnect) => Self::Reconnect,
            _ => Self::Continue,
        }
    }
}

pub(crate) async fn run(inner: Arc<AgentInner>, mut shutdown: watch::Receiver<bool>) {
    let collector = match Collector::new(inner.config.license_key.clone()) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(target: TARGET, error = %e, "cannot build HTTP client; agent disabled");
            return;
        }
    };
    loop {
        let Some(run) = connect_loop(&collector, &inner, &mut shutdown).await else {
            return;
        };
        match harvest_loop(&collector, &inner, &run, &mut shutdown).await {
            Step::Reconnect if *shutdown.borrow() => {
                inner.clear_run();
                return;
            }
            Step::Reconnect => {
                inner.clear_run();
                tracing::info!(target: TARGET, "collector requested reconnect");
            }
            Step::Shutdown => {
                inner.clear_run();
                return;
            }
            Step::Continue => return,
        }
    }
}

/// Connects with backoff. `None` means stop (shutdown requested or collector said 410).
///
/// A connect attempt is never cancelled by shutdown: data recorded before the first connect
/// is only delivered if that attempt completes (bounded by [`crate::Agent::shutdown`]'s
/// timeout). Shutdown only interrupts the backoff sleep between attempts.
async fn connect_loop(
    collector: &Collector,
    inner: &AgentInner,
    shutdown: &mut watch::Receiver<bool>,
) -> Option<Arc<RunConfig>> {
    let mut attempt = 0usize;
    loop {
        let outcome = connect(collector, &inner.config, &inner.hostname).await;
        match outcome {
            Ok(ConnectOutcome::Connected(run)) => {
                let run: Arc<RunConfig> = Arc::from(run);
                inner.install_run(Arc::clone(&run));
                tracing::info!(
                    target: TARGET,
                    run_id = %run.run_id,
                    entity_guid = %run.entity_guid,
                    "connected to New Relic"
                );
                return Some(run);
            }
            Ok(ConnectOutcome::Shutdown) => {
                tracing::error!(target: TARGET, "collector answered 410 Gone; agent stops reporting");
                inner.clear_run();
                return None;
            }
            Ok(ConnectOutcome::Retry) => {}
            Err(e) => tracing::warn!(target: TARGET, error = %e, "connect failed"),
        }
        if *shutdown.borrow() {
            return None;
        }
        let delay = Duration::from_secs(BACKOFF_SECS[attempt.min(BACKOFF_SECS.len() - 1)]);
        attempt += 1;
        tracing::debug!(target: TARGET, ?delay, "retrying connect");
        tokio::select! {
            () = tokio::time::sleep(delay) => {}
            _ = shutdown.changed() => return None,
        }
    }
}

async fn harvest_loop(
    collector: &Collector,
    inner: &AgentInner,
    run: &RunConfig,
    shutdown: &mut watch::Receiver<bool>,
) -> Step {
    let mut tick = tokio::time::interval(Duration::from_secs(1).min(inner.slow_period));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let start = Instant::now();
    let mut next_events = start + run.event_report_period;
    let mut next_slow = start + inner.slow_period;
    loop {
        if *shutdown.borrow() {
            let step = harvest_events(collector, inner, run).await;
            return step.worst(harvest_slow(collector, inner, run).await);
        }
        tokio::select! {
            _ = tick.tick() => {}
            _ = shutdown.changed() => continue,
        }
        let now = Instant::now();
        if now >= next_events {
            next_events = now + run.event_report_period;
            let step = harvest_events(collector, inner, run).await;
            if step != Step::Continue {
                return step;
            }
        }
        if now >= next_slow {
            next_slow = now + inner.slow_period;
            let step = harvest_slow(collector, inner, run).await;
            if step != Step::Continue {
                return step;
            }
        }
    }
}

async fn send(
    collector: &Collector,
    run: &RunConfig,
    method: &'static str,
    json: serde_json::Result<Vec<u8>>,
) -> Outcome {
    let json = match json {
        Ok(j) => j,
        Err(e) => {
            tracing::warn!(target: TARGET, method, error = %e, "serializing payload failed");
            return Outcome::Discard;
        }
    };
    match collector.invoke(&run.call(method), &json).await {
        Ok(o) => o,
        Err(e) => {
            tracing::warn!(target: TARGET, method, error = %e, "sending payload failed");
            Outcome::Discard
        }
    }
}

/// Sends one reservoir. On success or discard it is emptied; on retry it is kept so the
/// caller merges it back.
async fn send_reservoir<T>(
    collector: &Collector,
    run: &RunConfig,
    method: &'static str,
    enabled: bool,
    r: &mut Reservoir<T>,
    to_json: impl FnOnce(&Reservoir<T>) -> serde_json::Result<Vec<u8>>,
) -> Step {
    if r.is_empty() || !enabled {
        r.take();
        return Step::Continue;
    }
    let outcome = send(collector, run, method, to_json(r)).await;
    match outcome {
        Outcome::Retry => Step::Continue,
        Outcome::Ok(_) | Outcome::Discard => {
            r.take();
            Step::Continue
        }
        Outcome::Reconnect => Step::Reconnect,
        Outcome::Shutdown => Step::Shutdown,
    }
}

async fn harvest_events(collector: &Collector, inner: &AgentInner, run: &RunConfig) -> Step {
    let mut ev = lock(&inner.harvest).take_events();
    let id = run.run_id.as_str();
    let mut step = send_reservoir(
        collector,
        run,
        "custom_event_data",
        run.collect_custom_events,
        &mut ev.custom_events,
        |r| payload::events_json(id, r),
    )
    .await;
    if step == Step::Continue {
        step = send_reservoir(
            collector,
            run,
            "log_event_data",
            true,
            &mut ev.log_events,
            |r| {
                payload::logs_json(
                    &run.entity_guid,
                    &inner.config.identifier,
                    &inner.hostname,
                    r,
                )
            },
        )
        .await;
    }
    if step == Step::Continue {
        step = send_reservoir(
            collector,
            run,
            "error_event_data",
            run.collect_error_events,
            &mut ev.error_events,
            |r| payload::events_json(id, r),
        )
        .await;
    }
    if step == Step::Continue {
        step = send_reservoir(
            collector,
            run,
            "span_event_data",
            run.collect_span_events,
            &mut ev.span_events,
            |r| payload::events_json(id, r),
        )
        .await;
    }
    if step == Step::Continue {
        step = send_reservoir(
            collector,
            run,
            "analytic_event_data",
            run.collect_analytics_events,
            &mut ev.txn_events,
            |r| payload::events_json(id, r),
        )
        .await;
    }
    if step == Step::Continue {
        // Only reservoirs that hit a retryable failure are still populated.
        lock(&inner.harvest).merge_events(ev);
    }
    step
}

async fn harvest_slow(collector: &Collector, inner: &AgentInner, run: &RunConfig) -> Step {
    let slow = {
        let mut h = lock(&inner.harvest);
        h.add_metric(
            MetricKey::unscoped("Instance/Reporting"),
            MetricData::count(1.0),
            true,
        );
        h.take_slow(SystemTime::now())
    };
    let metrics_json = slow.metrics.json(&run.run_id, SystemTime::now());
    match send(collector, run, "metric_data", metrics_json).await {
        Outcome::Ok(_) | Outcome::Discard => {}
        Outcome::Retry => lock(&inner.harvest).merge_metrics(slow.metrics),
        Outcome::Reconnect => return Step::Reconnect,
        Outcome::Shutdown => return Step::Shutdown,
    }
    if !slow.traced_errors.is_empty() && run.collect_errors {
        let json = payload::errors_json(&run.run_id, &slow.traced_errors);
        match send(collector, run, "error_data", json).await {
            Outcome::Reconnect => return Step::Reconnect,
            Outcome::Shutdown => return Step::Shutdown,
            _ => {}
        }
    }
    if let Some(trace) = slow.trace.filter(|_| run.collect_traces) {
        let json = payload::trace_json(&run.run_id, &trace);
        match send(collector, run, "transaction_sample_data", json).await {
            Outcome::Reconnect => return Step::Reconnect,
            Outcome::Shutdown => return Step::Shutdown,
            _ => {}
        }
    }
    Step::Continue
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Agent, TransactionKind};
    use wiremock::matchers::{method, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn connect_reply() -> serde_json::Value {
        serde_json::json!({"return_value": {
            "agent_run_id": "run-1", "account_id": "1", "trusted_account_key": "1",
            "primary_application_id": "2", "entity_guid": "guid-1"
        }})
    }

    async fn requests_for(server: &MockServer, m: &str) -> usize {
        server
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|r| r.url.query_pairs().any(|(k, v)| k == "method" && v == m))
            .count()
    }

    async fn mount_handshake(server: &MockServer) {
        Mock::given(method("POST"))
            .and(query_param("method", "preconnect"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(
                    serde_json::json!({"return_value": {"redirect_host": "ignored"}}),
                ),
            )
            .mount(server)
            .await;
        Mock::given(method("POST"))
            .and(query_param("method", "connect"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(connect_reply())
                    .set_delay(Duration::from_millis(100)),
            )
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn shutdown_during_first_connect_still_delivers_buffered_data() {
        let server = MockServer::start().await;
        mount_handshake(&server).await;
        let agent = Agent::builder()
            .license_key("a".repeat(40))
            .app_name("test-app")
            .host(server.uri())
            .build()
            .unwrap();
        agent.start_transaction("job", TransactionKind::Other).end();
        agent.shutdown(Duration::from_secs(5)).await;
        assert_eq!(requests_for(&server, "metric_data").await, 1);
        assert_eq!(requests_for(&server, "analytic_event_data").await, 1);
    }

    #[tokio::test]
    async fn run_reconnects_when_metric_data_returns_401() {
        let server = MockServer::start().await;
        mount_handshake(&server).await;
        Mock::given(method("POST"))
            .and(query_param("method", "metric_data"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;
        let agent = Agent::builder()
            .license_key("a".repeat(40))
            .app_name("test-app")
            .host(server.uri())
            .slow_harvest_period(Duration::from_millis(200))
            .build()
            .unwrap();
        assert!(agent.wait_for_connection(Duration::from_secs(5)).await);
        agent.start_transaction("job", TransactionKind::Other).end();
        let deadline = Instant::now() + Duration::from_secs(5);
        while requests_for(&server, "connect").await < 2 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(requests_for(&server, "connect").await >= 2);
        agent.shutdown(Duration::from_secs(5)).await;
    }

    #[test]
    fn step_worst_prefers_shutdown_then_reconnect() {
        assert_eq!(Step::Reconnect.worst(Step::Shutdown), Step::Shutdown);
        assert_eq!(Step::Continue.worst(Step::Reconnect), Step::Reconnect);
    }
}
