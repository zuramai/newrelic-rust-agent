//! Transactions: the unit New Relic reports in the Transactions tab.

pub(crate) mod finish;
mod segment;

use std::borrow::Cow;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

pub(crate) use segment::SegmentId;
pub use segment::{ExternalSegment, Segment, SegmentContext};

use crate::agent::{AgentInner, lock};
use crate::attributes::{AttributeValue, Attributes, truncate_utf8};
use crate::dt::{InboundPayload, OutboundContext, TransportType, outbound_headers, parse_inbound};
use crate::harvest::payload::LogEvent;
use crate::ids::{Priority, SpanId, TraceId, txn_guid};

/// Maximum segments recorded per transaction; later segments are no-ops.
pub(crate) const MAX_SEGMENTS: usize = 2000;
/// Maximum errors recorded per transaction.
const MAX_TXN_ERRORS: usize = 5;
/// Outbound payloads beyond this count are marked unsampled.
const MAX_OUTBOUND_PAYLOADS: u8 = 35;
/// Maximum bytes kept of a forwarded log message.
pub(crate) const MAX_LOG_MESSAGE_BYTES: usize = 32_768;

/// Whether a transaction is a web request or background work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionKind {
    /// A request handled by a server: reported under `WebTransaction/Rust/…` with Apdex.
    Web,
    /// Background work (jobs, consumers, CLI runs): reported under `OtherTransaction/Rust/…`.
    Other,
}

/// An error recorded on a transaction with [`Transaction::notice_error_with`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NoticedError {
    /// Error class shown in the Errors inbox (for example a type name or status code).
    pub class: String,
    /// Error message.
    pub message: String,
    /// Expected errors are reported but do not affect error rate or Apdex.
    pub expected: bool,
}

/// A log line to forward to New Relic Logs.
#[derive(Clone, Debug)]
pub struct LogRecord {
    /// Severity, for example `"INFO"` or `"ERROR"`.
    pub level: Cow<'static, str>,
    /// The message; truncated to 32 KiB.
    pub message: String,
    /// When the record was produced.
    pub timestamp: SystemTime,
    /// Extra attributes attached to the log line.
    pub attributes: Vec<(Cow<'static, str>, AttributeValue)>,
}

#[derive(Debug)]
pub(crate) struct NoticedErrorAt {
    pub(crate) err: NoticedError,
    pub(crate) at: SystemTime,
    pub(crate) span: SpanId,
}

#[derive(Debug)]
pub(crate) enum SegmentKind {
    Custom,
    External {
        host: String,
        method: String,
        url: String,
        status: Option<u16>,
    },
}

#[derive(Debug)]
pub(crate) struct SegmentRecord {
    pub(crate) parent: Option<SegmentId>,
    pub(crate) name: Cow<'static, str>,
    pub(crate) kind: SegmentKind,
    pub(crate) span_id: SpanId,
    pub(crate) start: Instant,
    pub(crate) end: Option<Instant>,
    pub(crate) attrs: Attributes,
    pub(crate) agent_attrs: Attributes,
}

/// Mutable transaction state; `None` in [`TxnShared::state`] once ended.
#[derive(Debug)]
pub(crate) struct TxnState {
    pub(crate) kind: TransactionKind,
    pub(crate) name: Cow<'static, str>,
    pub(crate) start_wall: SystemTime,
    pub(crate) start: Instant,
    pub(crate) ignored: bool,
    pub(crate) trace_id: TraceId,
    pub(crate) txn_guid: SpanId,
    pub(crate) root_span: SpanId,
    pub(crate) priority: Priority,
    pub(crate) sampled: Option<bool>,
    pub(crate) inbound: Option<InboundPayload>,
    pub(crate) transport_duration: Option<Duration>,
    pub(crate) outbound_created: bool,
    pub(crate) payloads_created: u8,
    pub(crate) user_attrs: Attributes,
    pub(crate) agent_attrs: Attributes,
    /// Agent attributes that only go on the root span event.
    pub(crate) root_span_attrs: Attributes,
    pub(crate) errors: Vec<NoticedErrorAt>,
    pub(crate) status_code: Option<u16>,
    status_error_noticed: bool,
    pub(crate) request_uri: Option<String>,
    pub(crate) segments: Vec<SegmentRecord>,
}

impl TxnState {
    pub(crate) fn new(name: Cow<'static, str>, kind: TransactionKind) -> Self {
        let trace_id = TraceId::random();
        Self {
            kind,
            name,
            start_wall: SystemTime::now(),
            start: Instant::now(),
            ignored: false,
            trace_id,
            txn_guid: txn_guid(trace_id),
            root_span: SpanId::random(),
            priority: Priority::random(),
            sampled: None,
            inbound: None,
            transport_duration: None,
            outbound_created: false,
            payloads_created: 0,
            user_attrs: Attributes::default(),
            agent_attrs: Attributes::default(),
            root_span_attrs: Attributes::default(),
            errors: Vec::new(),
            status_code: None,
            status_error_noticed: false,
            request_uri: None,
            segments: Vec::new(),
        }
    }

    pub(crate) fn span_of(&self, seg: Option<SegmentId>) -> SpanId {
        seg.and_then(|id| self.segments.get(id.index()))
            .map_or(self.root_span, |s| s.span_id)
    }

    pub(crate) fn push_error(&mut self, err: NoticedError) {
        if self.errors.len() < MAX_TXN_ERRORS {
            self.errors.push(NoticedErrorAt {
                err,
                at: SystemTime::now(),
                span: self.root_span,
            });
        }
    }
}

pub(crate) struct TxnShared {
    pub(crate) agent: Arc<AgentInner>,
    pub(crate) state: Mutex<Option<TxnState>>,
}

impl Drop for TxnShared {
    fn drop(&mut self) {
        let state = self
            .state
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(state) = state {
            record_finished(&self.agent, state, Instant::now());
        }
    }
}

/// Decides sampling once per transaction (inbound decision wins over the local sampler).
pub(crate) fn ensure_sampled(agent: &AgentInner, s: &mut TxnState) -> bool {
    if let Some(sampled) = s.sampled {
        return sampled;
    }
    if let Some(inbound) = &s.inbound {
        if let Some(p) = inbound.priority {
            s.priority = Priority::new(p);
        }
        if let Some(sampled) = inbound.sampled {
            s.sampled = Some(sampled);
            return sampled;
        }
    }
    let sampled = lock(&agent.sampler).compute_sampled(s.priority.0, Instant::now());
    if sampled {
        s.priority = Priority::new(s.priority.0 + 1.0);
    }
    s.sampled = Some(sampled);
    sampled
}

/// Converts a finished transaction and records it. Must be called without holding the
/// transaction lock.
fn record_finished(agent: &AgentInner, mut state: TxnState, end: Instant) {
    if state.ignored {
        return;
    }
    ensure_sampled(agent, &mut state);
    let run = agent.run_snapshot();
    let finished = finish::finish(&state, end, &run);
    lock(&agent.harvest).absorb(finished);
}

pub(crate) fn unix_ms(t: SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

pub(crate) fn log_event(
    record: LogRecord,
    trace_id: Option<String>,
    span_id: Option<String>,
) -> LogEvent {
    let mut message = record.message;
    let keep = truncate_utf8(&message, MAX_LOG_MESSAGE_BYTES).len();
    message.truncate(keep);
    let mut attributes = Attributes::with_capacity(record.attributes.len());
    for (k, v) in record.attributes {
        attributes.insert(k, v);
    }
    LogEvent {
        level: record.level,
        message,
        timestamp: unix_ms(record.timestamp),
        trace_id,
        span_id,
        attributes,
    }
}

/// A New Relic transaction. Cheap to clone; all clones refer to the same transaction.
///
/// The transaction ends when [`end`](Self::end) is called or when the last clone (including
/// those held by segments) is dropped. A transaction from a disabled agent is a no-op.
#[derive(Clone)]
pub struct Transaction {
    pub(crate) inner: Option<Arc<TxnShared>>,
}

impl fmt::Debug for Transaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transaction")
            .field("enabled", &self.inner.is_some())
            .finish_non_exhaustive()
    }
}

impl Transaction {
    pub(crate) fn new(
        agent: Arc<AgentInner>,
        name: Cow<'static, str>,
        kind: TransactionKind,
    ) -> Self {
        Self {
            inner: Some(Arc::new(TxnShared {
                agent,
                state: Mutex::new(Some(TxnState::new(name, kind))),
            })),
        }
    }

    /// A transaction that records nothing.
    pub(crate) fn noop() -> Self {
        Self { inner: None }
    }

    /// Runs `f` on the live state; `None` when no-op or already ended.
    pub(crate) fn with_state<R>(&self, f: impl FnOnce(&mut TxnState) -> R) -> Option<R> {
        let shared = self.inner.as_ref()?;
        let mut guard = lock(&shared.state);
        guard.as_mut().map(f)
    }

    pub(crate) fn agent(&self) -> Option<&Arc<AgentInner>> {
        self.inner.as_ref().map(|s| &s.agent)
    }

    /// Renames the transaction. The final metric name is `WebTransaction/Rust/{name}` or
    /// `OtherTransaction/Rust/{name}`; keep names low-cardinality (route templates, not raw
    /// paths).
    pub fn set_name(&self, name: impl Into<Cow<'static, str>>) {
        let name = name.into();
        self.with_state(|s| s.name = name);
    }

    /// Adds a custom attribute to the transaction event, its errors and its root span.
    pub fn add_attribute(
        &self,
        key: impl Into<Cow<'static, str>>,
        value: impl Into<AttributeValue>,
    ) {
        let (key, value) = (key.into(), value.into());
        self.with_state(|s| s.user_attrs.insert(key, value));
    }

    #[cfg(feature = "tower")]
    pub(crate) fn set_agent_attribute(&self, key: &'static str, value: impl Into<AttributeValue>) {
        let value = value.into();
        self.with_state(|s| s.agent_attrs.set(key, value));
    }

    /// Records request attributes (`request.method`, `request.uri`, selected headers).
    pub fn set_web_request<B>(&self, req: &http::Request<B>) {
        let header = |name: http::header::HeaderName| {
            req.headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(|v| truncate_utf8(v, 255).to_owned())
        };
        let method = req.method().as_str().to_owned();
        let path = req.uri().path().to_owned();
        let host = header(http::header::HOST).or_else(|| req.uri().host().map(str::to_owned));
        let accept = header(http::header::ACCEPT);
        let content_type = header(http::header::CONTENT_TYPE);
        let content_length =
            header(http::header::CONTENT_LENGTH).and_then(|v| v.parse::<i64>().ok());
        self.with_state(|s| {
            let a = &mut s.agent_attrs;
            a.set("request.method", method);
            a.set("request.uri", path.clone());
            if let Some(v) = host {
                a.set("request.headers.host", v);
            }
            if let Some(v) = accept {
                a.set("request.headers.accept", v);
            }
            if let Some(v) = content_type {
                a.set("request.headers.contentType", v);
            }
            if let Some(v) = content_length {
                a.set("request.headers.contentLength", v);
            }
            s.request_uri = Some(path);
        });
    }

    /// Records the response status. Statuses `>= 500` record a transaction error (once).
    pub fn set_status_code(&self, status: u16) {
        self.with_state(|s| {
            s.agent_attrs.set("http.statusCode", status);
            s.agent_attrs.set("httpResponseCode", status.to_string());
            s.status_code = Some(status);
            if status >= 500 && !s.status_error_noticed {
                s.status_error_noticed = true;
                let message = http::StatusCode::from_u16(status)
                    .ok()
                    .and_then(|c| c.canonical_reason())
                    .unwrap_or("HTTP Error")
                    .to_owned();
                s.push_error(NoticedError {
                    class: status.to_string(),
                    message,
                    expected: false,
                });
            }
        });
    }

    /// Records an unexpected error. The class is the error's type name.
    pub fn notice_error<E: std::error::Error + ?Sized>(&self, err: &E) {
        let type_name = std::any::type_name::<E>();
        let class = if type_name.starts_with("dyn ") {
            "Error".to_owned()
        } else {
            type_name.to_owned()
        };
        self.notice_error_with(NoticedError {
            class,
            message: err.to_string(),
            expected: false,
        });
    }

    /// Records an error with an explicit class, message and expectation.
    pub fn notice_error_with(&self, err: NoticedError) {
        self.with_state(|s| s.push_error(err));
    }

    /// The root context; segments started from it are direct children of the transaction.
    #[must_use]
    pub fn context(&self) -> SegmentContext {
        SegmentContext::new(self.clone(), None)
    }

    /// Starts a segment directly under the transaction.
    pub fn start_segment(&self, name: impl Into<Cow<'static, str>>) -> Segment {
        self.context().start_segment(name)
    }

    /// Starts an External (outbound HTTP) segment directly under the transaction.
    pub fn start_external_segment(
        &self,
        method: &http::Method,
        uri: &http::Uri,
    ) -> ExternalSegment {
        self.context().start_external_segment(method, uri)
    }

    /// Accepts inbound distributed-trace headers (`traceparent`/`tracestate` or `newrelic`).
    ///
    /// Ignored before the agent connects, after outbound headers were created, or when headers
    /// were already accepted.
    pub fn accept_distributed_trace_headers(
        &self,
        headers: &http::HeaderMap,
        transport: TransportType,
    ) {
        let Some(agent) = self.agent() else { return };
        let run = agent.run_snapshot();
        if run.trusted_account_key.is_empty() {
            return;
        }
        let Some(payload) = parse_inbound(headers, &run.trusted_account_key, transport) else {
            return;
        };
        self.with_state(|s| {
            if s.outbound_created || s.inbound.is_some() {
                return;
            }
            s.trace_id = payload.trace_id;
            s.transport_duration = payload.timestamp_ms.map(|ts| {
                let sent = SystemTime::UNIX_EPOCH + Duration::from_millis(ts);
                s.start_wall.duration_since(sent).unwrap_or(Duration::ZERO)
            });
            s.inbound = Some(payload);
        });
    }

    /// Writes outbound distributed-trace headers for a call made directly from the transaction.
    pub fn insert_distributed_trace_headers(&self, headers: &mut http::HeaderMap) {
        self.insert_dt_headers(None, headers);
    }

    pub(crate) fn insert_dt_headers(&self, seg: Option<SegmentId>, headers: &mut http::HeaderMap) {
        let Some(agent) = self.agent() else { return };
        let run = agent.run_snapshot();
        if run.account_id.is_empty() || run.trusted_account_key.is_empty() {
            return;
        }
        let ids = self.with_state(|s| {
            s.outbound_created = true;
            let sampled = ensure_sampled(agent, s);
            s.payloads_created = s.payloads_created.saturating_add(1);
            (
                s.trace_id,
                s.span_of(seg),
                s.txn_guid,
                sampled && s.payloads_created <= MAX_OUTBOUND_PAYLOADS,
                s.priority,
            )
        });
        let Some((trace_id, span_id, guid, sampled, priority)) = ids else {
            return;
        };
        outbound_headers(
            headers,
            &OutboundContext {
                trace_id,
                span_id,
                txn_guid: guid,
                sampled,
                priority,
                account_id: &run.account_id,
                app_id: &run.primary_application_id,
                trusted_account_key: &run.trusted_account_key,
                timestamp_ms: unix_ms(SystemTime::now()),
            },
        );
    }

    /// The current trace id (32 lowercase hex digits), or `None` for a no-op/ended transaction.
    #[must_use]
    pub fn trace_id(&self) -> Option<String> {
        self.with_state(|s| s.trace_id.to_string())
    }

    /// Discards the transaction: nothing is recorded when it ends.
    pub fn ignore(&self) {
        self.with_state(|s| s.ignored = true);
    }

    /// Ends the transaction and records it. Idempotent; segments still open are closed at
    /// this instant.
    pub fn end(&self) {
        let end = Instant::now();
        let Some(shared) = &self.inner else { return };
        let state = lock(&shared.state).take();
        if let Some(state) = state {
            record_finished(&shared.agent, state, end);
        }
    }

    /// Forwards a log record linked to this transaction (`trace.id`, `span.id`).
    pub fn record_log(&self, record: LogRecord) {
        self.record_log_at(None, record);
    }

    pub(crate) fn record_log_at(&self, seg: Option<SegmentId>, record: LogRecord) {
        let Some(agent) = self.agent() else { return };
        let ctx = self.with_state(|s| {
            ensure_sampled(agent, s);
            (s.trace_id, s.span_of(seg), s.priority)
        });
        let event = match ctx {
            Some((trace, span, priority)) => (
                priority,
                log_event(record, Some(trace.to_string()), Some(span.to_string())),
            ),
            None => (Priority::random(), log_event(record, None, None)),
        };
        lock(&agent.harvest).push_log(event.0, event.1);
    }
}
