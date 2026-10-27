//! `preconnect` + `connect` handshake and the resulting run configuration.

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize};

use super::{Call, Collector, CollectorError, DEFAULT_MAX_PAYLOAD, Outcome};
use crate::config::Config;
use crate::harvest::{
    DEFAULT_CUSTOM_EVENTS, DEFAULT_ERROR_EVENTS, DEFAULT_LOG_EVENTS, DEFAULT_SPAN_EVENTS,
    DEFAULT_TXN_EVENTS, Limits,
};

/// Value of the connect payload's `language` field.
///
/// The collector only accepts languages of official agents: `"rust"` is answered with
/// `503 {}` on every connect. `"c"` (the C SDK) is accepted and has no language-specific
/// server-side behavior.
pub(crate) const LANGUAGE: &str = "c";
const DEFAULT_REPORT_PERIOD: Duration = Duration::from_secs(60);

/// Server-side settings for one agent run. [`RunConfig::default`] is the placeholder used
/// before connect: apdex 0.5, everything collected, no account (so no DT headers).
#[derive(Debug, Clone)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "mirrors the collector's independent collect_* flags"
)]
pub(crate) struct RunConfig {
    /// Collector base URL after preconnect redirection.
    pub(crate) host_base: String,
    pub(crate) run_id: String,
    pub(crate) request_headers_map: HashMap<String, String>,
    pub(crate) max_payload_size_in_bytes: usize,
    pub(crate) entity_guid: String,
    pub(crate) account_id: String,
    pub(crate) trusted_account_key: String,
    pub(crate) primary_application_id: String,
    pub(crate) apdex_t: f64,
    pub(crate) web_transactions_apdex: HashMap<String, f64>,
    pub(crate) sampling_target: u64,
    pub(crate) sampling_target_period: Duration,
    pub(crate) collect_analytics_events: bool,
    pub(crate) collect_custom_events: bool,
    pub(crate) collect_traces: bool,
    pub(crate) collect_errors: bool,
    pub(crate) collect_error_events: bool,
    pub(crate) collect_span_events: bool,
    pub(crate) event_report_period: Duration,
    pub(crate) limits: Limits,
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            host_base: String::new(),
            run_id: String::new(),
            request_headers_map: HashMap::new(),
            max_payload_size_in_bytes: DEFAULT_MAX_PAYLOAD,
            entity_guid: String::new(),
            account_id: String::new(),
            trusted_account_key: String::new(),
            primary_application_id: String::new(),
            apdex_t: 0.5,
            web_transactions_apdex: HashMap::new(),
            sampling_target: 10,
            sampling_target_period: DEFAULT_REPORT_PERIOD,
            collect_analytics_events: true,
            collect_custom_events: true,
            collect_traces: true,
            collect_errors: true,
            collect_error_events: true,
            collect_span_events: true,
            event_report_period: DEFAULT_REPORT_PERIOD,
            limits: Limits::default(),
        }
    }
}

impl RunConfig {
    pub(crate) fn call(&self, method: &'static str) -> Call<'_> {
        Call {
            base: &self.host_base,
            method,
            run_id: Some(&self.run_id),
            headers: &self.request_headers_map,
            max_payload: self.max_payload_size_in_bytes,
        }
    }
}

#[derive(Serialize)]
struct ConnectPayload<'a> {
    pid: u32,
    language: &'static str,
    agent_version: &'static str,
    host: &'a str,
    settings: serde_json::Map<String, serde_json::Value>,
    metadata: serde_json::Map<String, serde_json::Value>,
    app_name: &'a [String],
    identifier: &'a str,
    high_security: bool,
    environment: [[&'static str; 2]; 2],
    utilization: Utilization<'a>,
    event_harvest_config: EventHarvestConfigOut,
}

#[derive(Serialize)]
struct Utilization<'a> {
    metadata_version: u8,
    logical_processors: usize,
    hostname: &'a str,
}

#[derive(Serialize)]
struct EventHarvestConfigOut {
    report_period_ms: u64,
    harvest_limits: HarvestLimitsOut,
}

#[derive(Serialize)]
#[expect(
    clippy::struct_field_names,
    reason = "field names are the collector's wire keys"
)]
struct HarvestLimitsOut {
    analytic_event_data: usize,
    custom_event_data: usize,
    log_event_data: usize,
    error_event_data: usize,
    span_event_data: usize,
}

pub(crate) fn connect_payload(config: &Config, hostname: &str) -> serde_json::Result<Vec<u8>> {
    let payload = ConnectPayload {
        pid: std::process::id(),
        language: LANGUAGE,
        agent_version: env!("CARGO_PKG_VERSION"),
        host: hostname,
        settings: serde_json::Map::new(),
        metadata: serde_json::Map::new(),
        app_name: &config.app_names,
        identifier: &config.identifier,
        high_security: false,
        environment: [
            ["OS", std::env::consts::OS],
            ["Arch", std::env::consts::ARCH],
        ],
        utilization: Utilization {
            metadata_version: 5,
            logical_processors: std::thread::available_parallelism().map_or(1, usize::from),
            hostname,
        },
        event_harvest_config: EventHarvestConfigOut {
            report_period_ms: 60_000,
            harvest_limits: HarvestLimitsOut {
                analytic_event_data: DEFAULT_TXN_EVENTS,
                custom_event_data: DEFAULT_CUSTOM_EVENTS,
                log_event_data: DEFAULT_LOG_EVENTS,
                error_event_data: DEFAULT_ERROR_EVENTS,
                span_event_data: DEFAULT_SPAN_EVENTS,
            },
        },
    };
    serde_json::to_vec(&[payload])
}

#[derive(Deserialize)]
struct Envelope<T> {
    return_value: Option<T>,
}

#[derive(Deserialize, Default)]
struct PreconnectReply {
    #[serde(default)]
    redirect_host: String,
}

fn default_true() -> bool {
    true
}

/// Accepts a JSON string or number (the collector is not consistent about ids).
fn string_or_number<'de, D: Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    Ok(match serde_json::Value::deserialize(d)? {
        serde_json::Value::String(s) => s,
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    })
}

#[derive(Deserialize, Default)]
#[expect(
    clippy::struct_field_names,
    reason = "field names are the collector's wire keys"
)]
struct HarvestLimitsIn {
    analytic_event_data: Option<usize>,
    custom_event_data: Option<usize>,
    log_event_data: Option<usize>,
    error_event_data: Option<usize>,
    span_event_data: Option<usize>,
}

#[derive(Deserialize, Default)]
struct EventHarvestConfigIn {
    report_period_ms: Option<i64>,
    #[serde(default)]
    harvest_limits: HarvestLimitsIn,
}

#[derive(Deserialize, Default)]
struct SpanHarvestConfigIn {
    harvest_limit: Option<usize>,
}

#[derive(Deserialize)]
struct Message {
    #[serde(default)]
    message: String,
    #[serde(default)]
    level: String,
}

#[derive(Deserialize)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "mirrors the collector's independent collect_* flags"
)]
struct ConnectReply {
    #[serde(default, deserialize_with = "string_or_number")]
    agent_run_id: String,
    #[serde(default)]
    request_headers_map: HashMap<String, String>,
    max_payload_size_in_bytes: Option<usize>,
    #[serde(default, deserialize_with = "string_or_number")]
    entity_guid: String,
    #[serde(default, deserialize_with = "string_or_number")]
    account_id: String,
    #[serde(default, deserialize_with = "string_or_number")]
    trusted_account_key: String,
    #[serde(default, deserialize_with = "string_or_number")]
    primary_application_id: String,
    apdex_t: Option<f64>,
    #[serde(default)]
    web_transactions_apdex: HashMap<String, f64>,
    sampling_target: Option<u64>,
    sampling_target_period_in_seconds: Option<u64>,
    #[serde(default = "default_true")]
    collect_analytics_events: bool,
    #[serde(default = "default_true")]
    collect_custom_events: bool,
    #[serde(default = "default_true")]
    collect_traces: bool,
    #[serde(default = "default_true")]
    collect_errors: bool,
    #[serde(default = "default_true")]
    collect_error_events: bool,
    #[serde(default = "default_true")]
    collect_span_events: bool,
    #[serde(default)]
    event_harvest_config: EventHarvestConfigIn,
    #[serde(default)]
    span_event_harvest_config: SpanHarvestConfigIn,
    #[serde(default)]
    messages: Vec<Message>,
}

impl ConnectReply {
    fn into_run(self, host_base: String) -> Result<(RunConfig, Vec<Message>), CollectorError> {
        if self.agent_run_id.is_empty() {
            return Err(CollectorError::MissingRunId);
        }
        let defaults = RunConfig::default();
        let report_period = self
            .event_harvest_config
            .report_period_ms
            .and_then(|ms| u64::try_from(ms).ok())
            .filter(|ms| *ms > 0)
            .map_or(DEFAULT_REPORT_PERIOD, Duration::from_millis);
        let limits_in = self.event_harvest_config.harvest_limits;
        let run = RunConfig {
            host_base,
            run_id: self.agent_run_id,
            request_headers_map: self.request_headers_map,
            max_payload_size_in_bytes: self
                .max_payload_size_in_bytes
                .filter(|n| *n > 0)
                .unwrap_or(DEFAULT_MAX_PAYLOAD),
            entity_guid: self.entity_guid,
            account_id: self.account_id,
            trusted_account_key: self.trusted_account_key,
            primary_application_id: self.primary_application_id,
            apdex_t: self
                .apdex_t
                .filter(|t| *t > 0.0)
                .unwrap_or(defaults.apdex_t),
            web_transactions_apdex: self.web_transactions_apdex,
            sampling_target: self.sampling_target.unwrap_or(defaults.sampling_target),
            sampling_target_period: self
                .sampling_target_period_in_seconds
                .filter(|s| *s > 0)
                .map_or(defaults.sampling_target_period, Duration::from_secs),
            collect_analytics_events: self.collect_analytics_events,
            collect_custom_events: self.collect_custom_events,
            collect_traces: self.collect_traces,
            collect_errors: self.collect_errors,
            collect_error_events: self.collect_error_events,
            collect_span_events: self.collect_span_events,
            event_report_period: report_period,
            limits: Limits {
                txn_events: limits_in.analytic_event_data,
                span_events: self
                    .span_event_harvest_config
                    .harvest_limit
                    .or(limits_in.span_event_data),
                error_events: limits_in.error_event_data,
                custom_events: limits_in.custom_event_data,
                log_events: limits_in.log_event_data,
            },
        };
        Ok((run, self.messages))
    }
}

/// Result of a connect attempt.
pub(crate) enum ConnectOutcome {
    Connected(Box<RunConfig>),
    /// Transient failure; retry with backoff.
    Retry,
    /// The collector answered 410: stop forever.
    Shutdown,
}

/// Runs `preconnect` then `connect`.
pub(crate) async fn connect(
    collector: &Collector,
    config: &Config,
    hostname: &str,
) -> Result<ConnectOutcome, CollectorError> {
    let (base, explicit) = config.collector_base();
    let no_headers = HashMap::new();
    let pre = Call {
        base: &base,
        method: "preconnect",
        run_id: None,
        headers: &no_headers,
        max_payload: DEFAULT_MAX_PAYLOAD,
    };
    let body = match collector
        .invoke(&pre, br#"[{"high_security":false}]"#)
        .await?
    {
        Outcome::Ok(body) => body,
        Outcome::Shutdown => return Ok(ConnectOutcome::Shutdown),
        _ => return Ok(ConnectOutcome::Retry),
    };
    let redirect = serde_json::from_slice::<Envelope<PreconnectReply>>(&body)?
        .return_value
        .unwrap_or_default()
        .redirect_host;
    let host_base = if explicit || redirect.is_empty() {
        base
    } else {
        format!("https://{redirect}")
    };
    let call = Call {
        base: &host_base,
        method: "connect",
        run_id: None,
        headers: &no_headers,
        max_payload: DEFAULT_MAX_PAYLOAD,
    };
    let body = match collector
        .invoke(&call, &connect_payload(config, hostname)?)
        .await?
    {
        Outcome::Ok(body) => body,
        Outcome::Shutdown => return Ok(ConnectOutcome::Shutdown),
        _ => return Ok(ConnectOutcome::Retry),
    };
    let reply = serde_json::from_slice::<Envelope<ConnectReply>>(&body)?
        .return_value
        .ok_or(CollectorError::MissingRunId)?;
    let (run, messages) = reply.into_run(host_base)?;
    for m in messages {
        tracing::info!(target: "newrelic_rust_agent", level = %m.level, "collector message: {}", m.message);
    }
    Ok(ConnectOutcome::Connected(Box::new(run)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse(v: &serde_json::Value) -> RunConfig {
        let reply: ConnectReply = serde_json::from_value(v.clone()).unwrap();
        reply.into_run("https://h".to_owned()).unwrap().0
    }

    #[test]
    fn into_run_applies_defaults_for_missing_fields() {
        let run = parse(&json!({"agent_run_id": "r"}));
        assert!((run.apdex_t - 0.5).abs() < f64::EPSILON);
        assert!(run.collect_span_events);
        assert_eq!(run.event_report_period, Duration::from_secs(60));
        assert_eq!(run.limits, Limits::default());
    }

    #[test]
    fn into_run_prefers_span_event_harvest_config_limit() {
        let run = parse(&json!({
            "agent_run_id": "r",
            "event_harvest_config": {"report_period_ms": 5000, "harvest_limits": {"span_event_data": 1}},
            "span_event_harvest_config": {"harvest_limit": 2}
        }));
        assert_eq!(run.limits.span_events, Some(2));
        assert_eq!(run.event_report_period, Duration::from_secs(5));
    }

    #[test]
    fn into_run_accepts_numeric_account_ids() {
        let run =
            parse(&json!({"agent_run_id": "r", "account_id": 123, "trusted_account_key": "9"}));
        assert_eq!(run.account_id, "123");
        assert_eq!(run.trusted_account_key, "9");
    }

    #[test]
    fn into_run_rejects_missing_run_id() {
        let reply: ConnectReply = serde_json::from_value(json!({})).unwrap();
        assert!(matches!(
            reply.into_run(String::new()),
            Err(CollectorError::MissingRunId)
        ));
    }
}
