//! Wire shapes for every harvest endpoint.

use std::borrow::Cow;

use serde::Serialize;
use serde::ser::SerializeTuple as _;

use crate::attributes::Attributes;
use crate::harvest::reservoir::Reservoir;

/// An analytics event: `[intrinsics, userAttributes, agentAttributes]`. Used for
/// `Transaction`, `Span`, `TransactionError` and custom events.
#[derive(Clone, Debug, Default)]
pub(crate) struct Event {
    pub(crate) intrinsics: Attributes,
    pub(crate) user: Attributes,
    pub(crate) agent: Attributes,
}

impl Serialize for Event {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        (&self.intrinsics, &self.user, &self.agent).serialize(s)
    }
}

/// A forwarded log record.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct LogEvent {
    pub(crate) level: Cow<'static, str>,
    pub(crate) message: String,
    pub(crate) timestamp: u64,
    #[serde(rename = "trace.id", skip_serializing_if = "Option::is_none")]
    pub(crate) trace_id: Option<String>,
    #[serde(rename = "span.id", skip_serializing_if = "Option::is_none")]
    pub(crate) span_id: Option<String>,
    #[serde(skip_serializing_if = "Attributes::is_empty")]
    pub(crate) attributes: Attributes,
}

/// A traced error (`error_data` row).
#[derive(Clone, Debug)]
pub(crate) struct TracedError {
    pub(crate) timestamp_ms: f64,
    pub(crate) txn_name: String,
    pub(crate) message: String,
    pub(crate) class: String,
    pub(crate) params: TraceParams,
    pub(crate) txn_guid: String,
}

/// `{"agentAttributes", "userAttributes", "intrinsics"}` block shared by traced errors and
/// transaction traces.
#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct TraceParams {
    #[serde(rename = "agentAttributes")]
    pub(crate) agent: Attributes,
    #[serde(rename = "userAttributes")]
    pub(crate) user: Attributes,
    pub(crate) intrinsics: Attributes,
}

impl Serialize for TracedError {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        (
            self.timestamp_ms,
            &self.txn_name,
            &self.message,
            &self.class,
            &self.params,
            &self.txn_guid,
        )
            .serialize(s)
    }
}

/// A transaction-trace node: `[rel_start_ms, rel_stop_ms, name, {}, [children]]`.
#[derive(Clone, Debug)]
pub(crate) struct TraceNode {
    pub(crate) start_ms: u64,
    pub(crate) stop_ms: u64,
    pub(crate) name: String,
    pub(crate) children: Vec<TraceNode>,
}

struct EmptyObject;

impl Serialize for EmptyObject {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap as _;
        s.serialize_map(Some(0))?.end()
    }
}

impl Serialize for TraceNode {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        (
            self.start_ms,
            self.stop_ms,
            &self.name,
            EmptyObject,
            &self.children,
        )
            .serialize(s)
    }
}

/// A transaction trace (`transaction_sample_data` row).
#[derive(Clone, Debug)]
pub(crate) struct TxnTrace {
    pub(crate) start_ms: f64,
    pub(crate) duration_ms: f64,
    pub(crate) name: String,
    pub(crate) request_uri: Option<String>,
    /// The `ROOT` node.
    pub(crate) root: TraceNode,
    pub(crate) params: TraceParams,
    pub(crate) trace_id: String,
}

struct TraceData<'a>(&'a TxnTrace);

impl Serialize for TraceData<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let t = self.0;
        (t.start_ms, EmptyObject, EmptyObject, &t.root, &t.params).serialize(s)
    }
}

impl Serialize for TxnTrace {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut t = s.serialize_tuple(10)?;
        t.serialize_element(&self.start_ms)?;
        t.serialize_element(&self.duration_ms)?;
        t.serialize_element(&self.name)?;
        t.serialize_element(&self.request_uri)?;
        t.serialize_element(&TraceData(self))?;
        t.serialize_element(&self.trace_id)?;
        t.serialize_element(&())?;
        t.serialize_element(&false)?;
        t.serialize_element(&())?;
        t.serialize_element("")?;
        t.end()
    }
}

#[derive(Serialize)]
struct ReservoirInfo {
    reservoir_size: usize,
    events_seen: u64,
}

struct Items<'a, T>(&'a Reservoir<T>);

impl<T: Serialize> Serialize for Items<'_, T> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_seq(self.0.items())
    }
}

/// `[run_id, {"reservoir_size", "events_seen"}, [events…]]`
pub(crate) fn events_json(run_id: &str, events: &Reservoir<Event>) -> serde_json::Result<Vec<u8>> {
    serde_json::to_vec(&(
        run_id,
        ReservoirInfo {
            reservoir_size: events.cap(),
            events_seen: events.seen(),
        },
        Items(events),
    ))
}

/// `[run_id, [traced errors…]]`
pub(crate) fn errors_json(run_id: &str, errors: &[TracedError]) -> serde_json::Result<Vec<u8>> {
    serde_json::to_vec(&(run_id, errors))
}

/// `[run_id, [trace]]`
pub(crate) fn trace_json(run_id: &str, trace: &TxnTrace) -> serde_json::Result<Vec<u8>> {
    serde_json::to_vec(&(run_id, [trace]))
}

#[derive(Serialize)]
struct LogCommon<'a> {
    attributes: LogCommonAttrs<'a>,
}

#[derive(Serialize)]
struct LogCommonAttrs<'a> {
    #[serde(rename = "entity.guid")]
    entity_guid: &'a str,
    #[serde(rename = "entity.name")]
    entity_name: &'a str,
    hostname: &'a str,
}

#[derive(Serialize)]
struct LogBatch<'a> {
    common: LogCommon<'a>,
    logs: Items<'a, LogEvent>,
}

/// `[{"common": {"attributes": {…}}, "logs": [...]}]`; no run id.
pub(crate) fn logs_json(
    entity_guid: &str,
    entity_name: &str,
    hostname: &str,
    logs: &Reservoir<LogEvent>,
) -> serde_json::Result<Vec<u8>> {
    serde_json::to_vec(&[LogBatch {
        common: LogCommon {
            attributes: LogCommonAttrs {
                entity_guid,
                entity_name,
                hostname,
            },
        },
        logs: Items(logs),
    }])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::Priority;
    use serde_json::{Value, json};

    fn to_value(bytes: serde_json::Result<Vec<u8>>) -> Value {
        serde_json::from_slice(&bytes.unwrap()).unwrap()
    }

    #[test]
    fn events_json_wraps_events_with_reservoir_info() {
        let mut r = Reservoir::new(5);
        let mut e = Event::default();
        e.intrinsics.push("type", "Transaction");
        e.user.push("u", 1);
        r.push(Priority::new(0.5), e);
        assert_eq!(
            to_value(events_json("run", &r)),
            json!(["run", {"reservoir_size": 5, "events_seen": 1}, [[{"type": "Transaction"}, {"u": 1}, {}]]])
        );
    }

    #[test]
    fn errors_json_matches_traced_error_shape() {
        let mut params = TraceParams::default();
        params.intrinsics.push("guid", "g");
        let err = TracedError {
            timestamp_ms: 1000.0,
            txn_name: "WebTransaction/Rust/GET".to_owned(),
            message: "boom".to_owned(),
            class: "500".to_owned(),
            params,
            txn_guid: "g".to_owned(),
        };
        assert_eq!(
            to_value(errors_json("run", &[err])),
            json!(["run", [[1000.0, "WebTransaction/Rust/GET", "boom", "500",
                {"agentAttributes": {}, "userAttributes": {}, "intrinsics": {"guid": "g"}}, "g"]]])
        );
    }

    #[test]
    fn logs_json_omits_missing_trace_context() {
        let mut r = Reservoir::new(5);
        r.push(
            Priority::new(0.5),
            LogEvent {
                level: "INFO".into(),
                message: "hi".to_owned(),
                timestamp: 7,
                trace_id: None,
                span_id: None,
                attributes: Attributes::default(),
            },
        );
        assert_eq!(
            to_value(logs_json("guid", "app", "host", &r)),
            json!([{"common": {"attributes": {"entity.guid": "guid", "entity.name": "app", "hostname": "host"}},
                    "logs": [{"level": "INFO", "message": "hi", "timestamp": 7}]}])
        );
    }

    #[test]
    fn trace_json_matches_collector_layout() {
        let trace = TxnTrace {
            start_ms: 1000.0,
            duration_ms: 20.0,
            name: "WebTransaction/Rust/GET".to_owned(),
            request_uri: Some("/x".to_owned()),
            root: TraceNode {
                start_ms: 0,
                stop_ms: 20,
                name: "ROOT".to_owned(),
                children: vec![TraceNode {
                    start_ms: 0,
                    stop_ms: 20,
                    name: "WebTransaction/Rust/GET".to_owned(),
                    children: vec![],
                }],
            },
            params: TraceParams::default(),
            trace_id: "t".to_owned(),
        };
        assert_eq!(
            to_value(trace_json("run", &trace)),
            json!(["run", [[1000.0, 20.0, "WebTransaction/Rust/GET", "/x",
                [1000.0, {}, {}, [0, 20, "ROOT", {}, [[0, 20, "WebTransaction/Rust/GET", {}, []]]],
                 {"agentAttributes": {}, "userAttributes": {}, "intrinsics": {}}],
                "t", null, false, null, ""]]])
        );
    }
}
