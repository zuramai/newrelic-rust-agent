//! Distributed tracing headers: W3C `traceparent`/`tracestate` and New Relic's `newrelic`.

pub(crate) mod sampler;

use std::fmt;

use base64::Engine as _;
use http::{HeaderMap, HeaderValue};

use crate::ids::{Priority, SpanId, TraceId};

/// How a distributed-trace payload travelled between services.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum TransportType {
    /// HTTP or HTTP/2 (including gRPC).
    Http,
    /// Unknown transport.
    #[default]
    Unknown,
}

impl TransportType {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Http => "HTTP",
            Self::Unknown => "Unknown",
        }
    }
}

impl fmt::Display for TransportType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A validated inbound distributed-trace payload.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct InboundPayload {
    pub(crate) trace_id: TraceId,
    pub(crate) parent_span_id: Option<SpanId>,
    pub(crate) parent_type: Option<String>,
    pub(crate) account: Option<String>,
    pub(crate) app: Option<String>,
    pub(crate) txn_id: Option<String>,
    pub(crate) sampled: Option<bool>,
    pub(crate) priority: Option<f32>,
    pub(crate) timestamp_ms: Option<u64>,
    pub(crate) transport: TransportType,
}

impl InboundPayload {
    /// Whether New Relic trace info (from `tracestate` or `newrelic`) is present.
    pub(crate) fn has_nr_info(&self) -> bool {
        self.parent_type.is_some() && self.account.is_some() && self.app.is_some()
    }
}

/// Parses inbound headers; W3C headers take precedence over the `newrelic` header.
pub(crate) fn parse_inbound(
    headers: &HeaderMap,
    trusted_account_key: &str,
    transport: TransportType,
) -> Option<InboundPayload> {
    if let Some(tp) = headers.get("traceparent") {
        // The traceparent sampled flag is not trusted: without New Relic tracestate the local
        // adaptive sampler decides, as in the Go agent's default remote-parent policy.
        let (trace_id, parent) = parse_traceparent(tp.to_str().ok()?)?;
        let mut payload = InboundPayload {
            trace_id,
            parent_span_id: Some(parent),
            parent_type: None,
            account: None,
            app: None,
            txn_id: None,
            sampled: None,
            priority: None,
            timestamp_ms: None,
            transport,
        };
        let nr_entry = headers
            .get_all("tracestate")
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(','))
            .find_map(|member| {
                let (key, value) = member.trim().split_once('=')?;
                (key.strip_suffix("@nr")? == trusted_account_key).then_some(value)
            });
        if let Some(entry) = nr_entry.and_then(parse_nr_tracestate) {
            payload.parent_type = Some(entry.parent_type.to_owned());
            payload.account = Some(entry.account.to_owned());
            payload.app = Some(entry.app.to_owned());
            payload.txn_id = entry.txn_id.map(str::to_owned);
            payload.sampled = entry.sampled;
            payload.priority = entry.priority;
            payload.timestamp_ms = Some(entry.timestamp_ms);
        }
        return Some(payload);
    }
    let raw = headers.get("newrelic")?.to_str().ok()?.trim();
    parse_newrelic_header(raw, trusted_account_key, transport)
}

/// `^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})(-.*)?$`
fn parse_traceparent(v: &str) -> Option<(TraceId, SpanId)> {
    let mut parts = v.trim().splitn(5, '-');
    let version = parts.next()?;
    let trace = parts.next()?;
    let parent = parts.next()?;
    let flags = parts.next()?;
    let rest = parts.next();
    let is_hex2 = |s: &str| {
        s.len() == 2
            && s.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    };
    if !is_hex2(version) || !is_hex2(flags) || version == "ff" {
        return None;
    }
    if version == "00" && rest.is_some() {
        return None;
    }
    let trace_id = TraceId::parse_hex(trace)?;
    let parent = SpanId::parse_hex(parent)?;
    Some((trace_id, parent))
}

struct NrTracestate<'a> {
    parent_type: &'static str,
    account: &'a str,
    app: &'a str,
    txn_id: Option<&'a str>,
    sampled: Option<bool>,
    priority: Option<f32>,
    timestamp_ms: u64,
}

/// `version-type-account-app-spanId-txnId-sampled-priority-timestampMs`
fn parse_nr_tracestate(value: &str) -> Option<NrTracestate<'_>> {
    let fields: Vec<&str> = value.split('-').collect();
    if fields.len() < 9 {
        return None;
    }
    let parent_type = match fields[1] {
        "0" => "App",
        "1" => "Browser",
        "2" => "Mobile",
        _ => return None,
    };
    if fields[2].is_empty() || fields[3].is_empty() {
        return None;
    }
    let sampled = match fields[6] {
        "1" => Some(true),
        "0" => Some(false),
        _ => None,
    };
    Some(NrTracestate {
        parent_type,
        account: fields[2],
        app: fields[3],
        txn_id: Some(fields[5]).filter(|s| !s.is_empty()),
        sampled,
        priority: fields[7].parse().ok(),
        timestamp_ms: fields[8].parse().ok()?,
    })
}

#[derive(serde::Deserialize)]
struct NrHeader {
    v: [u32; 2],
    d: NrHeaderData,
}

#[derive(serde::Deserialize)]
struct NrHeaderData {
    ty: String,
    ac: String,
    ap: String,
    tx: Option<String>,
    id: Option<String>,
    tr: String,
    pr: Option<f32>,
    sa: Option<bool>,
    ti: u64,
    tk: Option<String>,
}

fn parse_newrelic_header(
    raw: &str,
    trusted_account_key: &str,
    transport: TransportType,
) -> Option<InboundPayload> {
    let json = if raw.starts_with('{') {
        raw.as_bytes().to_vec()
    } else {
        base64::engine::general_purpose::STANDARD.decode(raw).ok()?
    };
    let header: NrHeader = serde_json::from_slice(&json).ok()?;
    if header.v[0] > 0 {
        return None;
    }
    let d = header.d;
    if d.tx.is_none() && d.id.is_none() {
        return None;
    }
    if d.tk.as_deref().unwrap_or(&d.ac) != trusted_account_key {
        return None;
    }
    // The `newrelic` header may carry a 16-hex trace id from older agents; left-pad it.
    let trace_id = TraceId::parse_hex(&format!("{:0>32}", d.tr))?;
    Some(InboundPayload {
        trace_id,
        parent_span_id: d.id.as_deref().and_then(SpanId::parse_hex),
        parent_type: Some(d.ty),
        account: Some(d.ac),
        app: Some(d.ap),
        txn_id: d.tx,
        sampled: d.sa,
        priority: d.pr,
        timestamp_ms: Some(d.ti),
        transport,
    })
}

/// Everything needed to write outbound DT headers.
pub(crate) struct OutboundContext<'a> {
    pub(crate) trace_id: TraceId,
    pub(crate) span_id: SpanId,
    pub(crate) txn_guid: SpanId,
    pub(crate) sampled: bool,
    pub(crate) priority: Priority,
    pub(crate) account_id: &'a str,
    pub(crate) app_id: &'a str,
    pub(crate) trusted_account_key: &'a str,
    pub(crate) timestamp_ms: u64,
}

#[derive(serde::Serialize)]
struct OutHeader<'a> {
    v: [u8; 2],
    d: OutHeaderData<'a>,
}

#[derive(serde::Serialize)]
struct OutHeaderData<'a> {
    ty: &'static str,
    ap: &'a str,
    ac: &'a str,
    tx: String,
    id: String,
    tr: String,
    pr: Priority,
    sa: bool,
    ti: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    tk: Option<&'a str>,
}

/// Writes `traceparent`, `tracestate` and `newrelic`. No-op before connect (no account).
pub(crate) fn outbound_headers(out: &mut HeaderMap, ctx: &OutboundContext<'_>) {
    if ctx.account_id.is_empty() || ctx.trusted_account_key.is_empty() {
        return;
    }
    let flag = if ctx.sampled { "01" } else { "00" };
    let traceparent = format!("00-{}-{}-{flag}", ctx.trace_id, ctx.span_id);
    let tracestate = format!(
        "{}@nr=0-0-{}-{}-{}-{}-{}-{}-{}",
        ctx.trusted_account_key,
        ctx.account_id,
        ctx.app_id,
        ctx.span_id,
        ctx.txn_guid,
        u8::from(ctx.sampled),
        ctx.priority.to_tracestate(),
        ctx.timestamp_ms,
    );
    let nr = OutHeader {
        v: [0, 1],
        d: OutHeaderData {
            ty: "App",
            ap: ctx.app_id,
            ac: ctx.account_id,
            tx: ctx.txn_guid.to_string(),
            id: ctx.span_id.to_string(),
            tr: ctx.trace_id.to_string(),
            pr: ctx.priority,
            sa: ctx.sampled,
            ti: ctx.timestamp_ms,
            tk: (ctx.trusted_account_key != ctx.account_id).then_some(ctx.trusted_account_key),
        },
    };
    let Ok(json) = serde_json::to_vec(&nr) else {
        return;
    };
    let newrelic = base64::engine::general_purpose::STANDARD.encode(json);
    for (name, value) in [
        ("traceparent", traceparent),
        ("tracestate", tracestate),
        ("newrelic", newrelic),
    ] {
        if let Ok(v) = HeaderValue::from_str(&value) {
            out.insert(name, v);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outbound(tk: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        outbound_headers(
            &mut h,
            &OutboundContext {
                trace_id: TraceId(0x1234),
                span_id: SpanId(0xab),
                txn_guid: SpanId(0xcd),
                sampled: true,
                priority: Priority::new(1.5),
                account_id: "11",
                app_id: "22",
                trusted_account_key: tk,
                timestamp_ms: 1_700_000_000_000,
            },
        );
        h
    }

    #[test]
    fn parse_inbound_round_trips_outbound_w3c_headers() {
        let p = parse_inbound(&outbound("11"), "11", TransportType::Http).unwrap();
        assert_eq!(p.trace_id, TraceId(0x1234));
        assert_eq!(p.parent_span_id, Some(SpanId(0xab)));
        assert_eq!(p.txn_id.as_deref(), Some("00000000000000cd"));
        assert_eq!(p.sampled, Some(true));
        assert_eq!(p.priority, Some(1.5));
        assert_eq!(p.account.as_deref(), Some("11"));
        assert_eq!(p.parent_type.as_deref(), Some("App"));
    }

    #[test]
    fn parse_inbound_round_trips_newrelic_header_alone() {
        let mut h = outbound("11");
        h.remove("traceparent");
        let p = parse_inbound(&h, "11", TransportType::Http).unwrap();
        assert_eq!(p.trace_id, TraceId(0x1234));
        assert_eq!(p.timestamp_ms, Some(1_700_000_000_000));
    }

    #[test]
    fn parse_inbound_rejects_all_zero_traceparent() {
        let mut h = HeaderMap::new();
        h.insert(
            "traceparent",
            HeaderValue::from_static("00-00000000000000000000000000000000-00000000000000ab-01"),
        );
        assert!(parse_inbound(&h, "11", TransportType::Http).is_none());
    }

    #[test]
    fn parse_inbound_rejects_newrelic_header_from_untrusted_account() {
        let mut h = outbound("99");
        h.remove("traceparent");
        assert!(parse_inbound(&h, "11", TransportType::Http).is_none());
    }

    #[test]
    fn parse_inbound_keeps_traceparent_ids_when_tracestate_untrusted() {
        let p = parse_inbound(&outbound("99"), "11", TransportType::Http).unwrap();
        assert_eq!(p.trace_id, TraceId(0x1234));
        assert!(!p.has_nr_info());
        assert_eq!(p.sampled, None);
    }

    #[test]
    fn outbound_headers_omit_tk_when_trusted_key_equals_account() {
        let h = outbound("11");
        let raw = h.get("newrelic").unwrap().to_str().unwrap();
        let json = base64::engine::general_purpose::STANDARD
            .decode(raw)
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&json).unwrap();
        assert!(v["d"].get("tk").is_none());
    }

    #[test]
    fn outbound_headers_write_nothing_before_connect() {
        let mut h = HeaderMap::new();
        outbound_headers(
            &mut h,
            &OutboundContext {
                trace_id: TraceId(1),
                span_id: SpanId(1),
                txn_guid: SpanId(1),
                sampled: false,
                priority: Priority::new(0.1),
                account_id: "",
                app_id: "",
                trusted_account_key: "",
                timestamp_ms: 0,
            },
        );
        assert!(h.is_empty());
    }
}
