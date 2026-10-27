//! `tracing` integration: spans become transactions/segments, events become forwarded logs.

use std::any::TypeId;
use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;
use std::time::SystemTime;

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Dispatch, Event, Level, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::{LookupSpan, SpanRef};

use crate::Agent;
use crate::attributes::AttributeValue;
use crate::transaction::{LogRecord, Segment, SegmentContext, Transaction, TransactionKind};

/// Span field that turns a root span into a transaction. Its value selects the kind:
/// `"web"` → [`TransactionKind::Web`], anything else → [`TransactionKind::Other`].
const TXN_FIELD: &str = "newrelic.transaction";
const OWN_TARGET: &str = "newrelic_rust_agent";

/// New Relic state stored in a span's extensions.
enum NrSpan {
    /// The span owns (or carries) a transaction.
    Root { txn: Transaction, owned: bool },
    /// The span is a segment of an enclosing transaction.
    Segment(Segment),
}

impl NrSpan {
    fn context(&self) -> SegmentContext {
        match self {
            Self::Root { txn, .. } => txn.context(),
            Self::Segment(seg) => seg.context(),
        }
    }
}

/// Type-erased access to span extensions, exposed through `Layer::downcast_raw` so
/// [`NewRelicSpanExt`] works with any subscriber stack (pattern from `tracing-opentelemetry`).
struct WithContext {
    set_txn: fn(&Dispatch, &Id, Transaction),
    get_ctx: fn(&Dispatch, &Id) -> Option<SegmentContext>,
}

/// A [`tracing_subscriber::Layer`] reporting spans and events to New Relic.
///
/// * A span inside a transaction becomes a segment (`Custom/{span name}`), so existing
///   `#[tracing::instrument]` code is traced without changes. Span fields become segment
///   attributes. Only spans at `INFO` or more severe are reported by default
///   ([`with_span_level`](Self::with_span_level)).
/// * A span with a `newrelic.transaction` field and no enclosing transaction starts a
///   transaction named after the span: `"web"` makes a web transaction, any other value a
///   background (`OtherTransaction`) one. It ends when the span closes.
/// * `ERROR` events annotate the enclosing segment's span with `error.class` (the event target)
///   and `error.message` without failing the transaction; record transaction errors with
///   [`Transaction::notice_error`] or HTTP/gRPC status codes.
/// * Events at or above the forwarding level (default `INFO`) are forwarded to New Relic Logs,
///   linked to the enclosing trace/span when there is one.
pub struct NewRelicLayer<S> {
    agent: Agent,
    log_level: Option<Level>,
    span_level: Level,
    with_context: WithContext,
    _subscriber: PhantomData<fn(S)>,
}

impl<S> fmt::Debug for NewRelicLayer<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NewRelicLayer")
            .field("agent", &self.agent)
            .field("log_level", &self.log_level)
            .field("span_level", &self.span_level)
            .finish_non_exhaustive()
    }
}

impl<S> NewRelicLayer<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    /// Creates the layer. Log forwarding is enabled at `INFO`.
    #[must_use]
    pub fn new(agent: Agent) -> Self {
        Self {
            agent,
            log_level: Some(Level::INFO),
            span_level: Level::INFO,
            with_context: WithContext {
                set_txn: Self::set_txn,
                get_ctx: Self::get_ctx,
            },
            _subscriber: PhantomData,
        }
    }

    /// Only spans at `level` or more severe become segments/transactions (default `INFO`,
    /// the level `#[tracing::instrument]` uses). Keeps `DEBUG`/`TRACE` spans of libraries
    /// such as hyper or h2 out of traces. Spans below the level are skipped: their children
    /// attach to the nearest reported ancestor.
    #[must_use]
    pub fn with_span_level(mut self, level: Level) -> Self {
        self.span_level = level;
        self
    }

    /// Forwards events at `level` or more severe; `None` disables log forwarding.
    #[must_use]
    pub fn with_log_forwarding(mut self, level: Option<Level>) -> Self {
        self.log_level = level;
        self
    }

    fn set_txn(dispatch: &Dispatch, id: &Id, txn: Transaction) {
        let Some(subscriber) = dispatch.downcast_ref::<S>() else {
            return;
        };
        let Some(span) = subscriber.span(id) else {
            return;
        };
        let previous = span
            .extensions_mut()
            .replace(NrSpan::Root { txn, owned: false });
        // Dropped after the extensions lock is released.
        drop(previous);
    }

    fn get_ctx(dispatch: &Dispatch, id: &Id) -> Option<SegmentContext> {
        let subscriber = dispatch.downcast_ref::<S>()?;
        let span = subscriber.span(id)?;
        nearest_context(&span)
    }
}

/// The `NrSpan` context of `span` or its nearest ancestor that has one.
fn nearest_context<'a, R: LookupSpan<'a>>(span: &SpanRef<'a, R>) -> Option<SegmentContext> {
    span.scope()
        .find_map(|s| s.extensions().get::<NrSpan>().map(NrSpan::context))
}

/// Collects span/event fields as attributes.
#[derive(Default)]
struct FieldVisitor {
    attrs: Vec<(Cow<'static, str>, AttributeValue)>,
    message: Option<String>,
    error: Option<String>,
    txn_kind: Option<String>,
}

impl FieldVisitor {
    fn push(&mut self, field: &Field, value: AttributeValue) {
        match field.name() {
            TXN_FIELD => {
                if let AttributeValue::String(s) = value {
                    self.txn_kind = Some(s);
                }
            }
            "message" => {
                if let AttributeValue::String(s) = value {
                    self.message = Some(s);
                }
            }
            "error" => {
                if let AttributeValue::String(s) = &value {
                    self.error = Some(s.clone());
                }
                self.attrs.push((Cow::Borrowed("error"), value));
            }
            name if name.starts_with("log.") => {}
            name => self.attrs.push((Cow::Borrowed(name), value)),
        }
    }
}

impl Visit for FieldVisitor {
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.push(field, value.into());
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.push(field, value.into());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.push(field, value.into());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.push(field, value.into());
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.push(field, value.into());
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.push(field, value.to_string().into());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.push(field, format!("{value:?}").into());
    }
}

impl<S> Layer<S> for NewRelicLayer<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let meta = attrs.metadata();
        if *meta.level() > self.span_level {
            return;
        }
        let Some(span) = ctx.span(id) else { return };
        let parent_ctx = span.parent().as_ref().and_then(nearest_context);
        let mut fields = FieldVisitor::default();
        let nr = if let Some(parent) = parent_ctx {
            attrs.record(&mut fields);
            NrSpan::Segment(parent.start_segment(meta.name()))
        } else if meta.fields().field(TXN_FIELD).is_some() {
            if !self.agent.is_enabled() {
                return;
            }
            attrs.record(&mut fields);
            let kind = match fields.txn_kind.as_deref() {
                Some(k) if k.eq_ignore_ascii_case("web") => TransactionKind::Web,
                _ => TransactionKind::Other,
            };
            NrSpan::Root {
                txn: self.agent.start_transaction(meta.name(), kind),
                owned: true,
            }
        } else {
            return;
        };
        let ctx = nr.context();
        for (k, v) in fields.attrs {
            ctx.add_attribute(k, v);
        }
        span.extensions_mut().insert(nr);
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let Some(nr_ctx) = span.extensions().get::<NrSpan>().map(NrSpan::context) else {
            return;
        };
        let mut fields = FieldVisitor::default();
        values.record(&mut fields);
        for (k, v) in fields.attrs {
            nr_ctx.add_attribute(k, v);
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let meta = event.metadata();
        let level = *meta.level();
        let forward = self.log_level.is_some_and(|max| level <= max)
            && !meta.target().starts_with(OWN_TARGET);
        let is_error = level == Level::ERROR;
        if !forward && !is_error {
            return;
        }
        let nr_ctx = ctx.event_span(event).as_ref().and_then(nearest_context);
        if !forward && nr_ctx.is_none() {
            return;
        }
        let mut fields = FieldVisitor::default();
        event.record(&mut fields);
        if is_error && let Some(c) = &nr_ctx {
            let message = fields
                .error
                .as_deref()
                .or(fields.message.as_deref())
                .unwrap_or_default();
            c.annotate_error(meta.target(), message);
        }
        if forward {
            let record = LogRecord {
                level: Cow::Borrowed(level.as_str()),
                message: fields.message.unwrap_or_default(),
                timestamp: SystemTime::now(),
                attributes: fields.attrs,
            };
            match nr_ctx {
                Some(c) => c.record_log(record),
                None => self.agent.record_log(record),
            }
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let nr = span.extensions_mut().remove::<NrSpan>();
        match nr {
            Some(NrSpan::Root { txn, owned: true }) => txn.end(),
            // Segments end on drop; borrowed roots are ended by their owner.
            other => drop(other),
        }
    }

    // SAFETY: the returned pointers point at `self` and `self.with_context`, which live as
    // long as the layer, and are only cast back to the `TypeId` they were requested for. This
    // is the contract `tracing_subscriber::Layer::downcast_raw` requires.
    #[expect(
        unsafe_code,
        reason = "required by tracing_subscriber::Layer::downcast_raw to expose span-extension access; mirrors tracing-opentelemetry"
    )]
    unsafe fn downcast_raw(&self, id: TypeId) -> Option<*const ()> {
        if id == TypeId::of::<Self>() {
            Some(std::ptr::from_ref(self).cast())
        } else if id == TypeId::of::<WithContext>() {
            Some(std::ptr::from_ref(&self.with_context).cast())
        } else {
            None
        }
    }
}

/// Links `tracing` spans with New Relic transactions.
pub trait NewRelicSpanExt {
    /// Attaches `txn` to this span: spans created inside it become segments of `txn`.
    /// The span does not end the transaction when it closes.
    fn set_newrelic_transaction(&self, txn: &Transaction);

    /// The transaction this span belongs to, if any.
    fn newrelic_transaction(&self) -> Option<Transaction>;

    /// The segment context of this span (or its nearest ancestor), for starting segments or
    /// writing distributed-trace headers manually.
    fn newrelic_context(&self) -> Option<SegmentContext>;
}

impl NewRelicSpanExt for tracing::Span {
    fn set_newrelic_transaction(&self, txn: &Transaction) {
        let txn = txn.clone();
        let mut txn = Some(txn);
        self.with_subscriber(|(id, dispatch)| {
            if let (Some(wc), Some(txn)) = (dispatch.downcast_ref::<WithContext>(), txn.take()) {
                (wc.set_txn)(dispatch, id, txn);
            }
        });
    }

    fn newrelic_transaction(&self) -> Option<Transaction> {
        self.newrelic_context().map(|c| c.transaction().clone())
    }

    fn newrelic_context(&self) -> Option<SegmentContext> {
        self.with_subscriber(|(id, dispatch)| {
            dispatch
                .downcast_ref::<WithContext>()
                .and_then(|wc| (wc.get_ctx)(dispatch, id))
        })
        .flatten()
    }
}
