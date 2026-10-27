//! Segments: timed units of work inside a transaction.

use std::borrow::Cow;
use std::fmt;
use std::time::Instant;

use super::{MAX_SEGMENTS, SegmentKind, SegmentRecord, Transaction};
use crate::attributes::{AttributeValue, Attributes};
use crate::ids::SpanId;

/// Index of a segment inside its transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SegmentId(pub(super) u32);

impl SegmentId {
    pub(crate) fn index(self) -> usize {
        self.0 as usize
    }
}

/// A position in a transaction's segment tree: new segments become children of it.
///
/// Obtain one from [`Transaction::context`], [`Segment::context`], the request extensions
/// populated by the tower layer, or `NewRelicSpanExt::newrelic_context`.
#[derive(Clone)]
pub struct SegmentContext {
    txn: Transaction,
    parent: Option<SegmentId>,
}

impl fmt::Debug for SegmentContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SegmentContext")
            .field("txn", &self.txn)
            .field("parent", &self.parent)
            .finish()
    }
}

impl SegmentContext {
    pub(crate) fn new(txn: Transaction, parent: Option<SegmentId>) -> Self {
        Self { txn, parent }
    }

    /// The transaction this context belongs to.
    #[must_use]
    pub fn transaction(&self) -> &Transaction {
        &self.txn
    }

    fn push(&self, name: Cow<'static, str>, kind: SegmentKind) -> Option<SegmentId> {
        let parent = self.parent;
        self.txn
            .with_state(|s| {
                if s.segments.len() >= MAX_SEGMENTS {
                    return None;
                }
                let id = SegmentId(u32::try_from(s.segments.len()).ok()?);
                s.segments.push(SegmentRecord {
                    parent,
                    name,
                    kind,
                    span_id: SpanId::random(),
                    start: Instant::now(),
                    end: None,
                    attrs: Attributes::default(),
                    agent_attrs: Attributes::default(),
                });
                Some(id)
            })
            .flatten()
    }

    /// Starts a custom segment, reported as `Custom/{name}`.
    pub fn start_segment(&self, name: impl Into<Cow<'static, str>>) -> Segment {
        let id = self.push(name.into(), SegmentKind::Custom);
        Segment {
            ctx: self.clone(),
            id,
        }
    }

    /// Starts an External segment for an outbound HTTP call, reported as
    /// `External/{host}/http/{METHOD}`.
    pub fn start_external_segment(
        &self,
        method: &http::Method,
        uri: &http::Uri,
    ) -> ExternalSegment {
        let host = match (uri.host(), uri.port_u16()) {
            (Some(h), Some(p)) => format!("{h}:{p}"),
            (Some(h), None) => h.to_owned(),
            (None, _) => "unknown".to_owned(),
        };
        // Built from host/port so credentials in the authority never leak; no query string.
        let url = match uri.scheme_str() {
            Some(scheme) if uri.host().is_some() => format!("{scheme}://{host}{}", uri.path()),
            _ => uri.path().to_owned(),
        };
        let method = method.as_str().to_owned();
        let name = format!("External/{host}/http/{method}");
        let id = self.push(
            name.into(),
            SegmentKind::External {
                host,
                method,
                url,
                status: None,
            },
        );
        ExternalSegment {
            segment: Segment {
                ctx: self.clone(),
                id,
            },
        }
    }

    /// Writes outbound distributed-trace headers whose parent span is this context.
    pub fn insert_distributed_trace_headers(&self, headers: &mut http::HeaderMap) {
        self.txn.insert_dt_headers(self.parent, headers);
    }

    #[cfg(feature = "tracing")]
    /// Sets `error.class`/`error.message` on this context's span without failing the
    /// transaction.
    pub(crate) fn annotate_error(&self, class: &str, message: &str) {
        let parent = self.parent;
        let (class, message) = (class.to_owned(), message.to_owned());
        self.txn.with_state(|s| {
            let attrs = match parent.and_then(|id| s.segments.get_mut(id.index())) {
                Some(seg) => &mut seg.agent_attrs,
                None => &mut s.root_span_attrs,
            };
            attrs.set("error.class", class);
            attrs.set("error.message", message);
        });
    }

    #[cfg(feature = "tracing")]
    /// Adds a user attribute to this context's segment, or to the transaction at the root.
    pub(crate) fn add_attribute(&self, key: Cow<'static, str>, value: AttributeValue) {
        let parent = self.parent;
        self.txn.with_state(
            |s| match parent.and_then(|id| s.segments.get_mut(id.index())) {
                Some(seg) => seg.attrs.insert(key, value),
                None => s.user_attrs.insert(key, value),
            },
        );
    }

    #[cfg(feature = "tracing")]
    /// Forwards a log linked to this context's span.
    pub(crate) fn record_log(&self, record: super::LogRecord) {
        self.txn.record_log_at(self.parent, record);
    }
}

/// A timed segment. Ends when [`end`](Self::end) is called or when dropped.
///
/// If the transaction already reached its 2000-segment limit (or is a no-op), the segment
/// records nothing; children started from it attach to its parent.
#[must_use = "a segment ends when dropped; bind it to a variable for the duration of the work"]
pub struct Segment {
    ctx: SegmentContext,
    id: Option<SegmentId>,
}

impl fmt::Debug for Segment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Segment")
            .field("ctx", &self.ctx)
            .field("id", &self.id)
            .finish()
    }
}

impl Segment {
    /// A context whose new segments are children of this one.
    #[must_use]
    pub fn context(&self) -> SegmentContext {
        SegmentContext::new(self.ctx.txn.clone(), self.id.or(self.ctx.parent))
    }

    /// Starts a child segment.
    pub fn start_segment(&self, name: impl Into<Cow<'static, str>>) -> Segment {
        self.context().start_segment(name)
    }

    /// Adds a custom attribute to this segment's span event.
    pub fn add_attribute(
        &self,
        key: impl Into<Cow<'static, str>>,
        value: impl Into<AttributeValue>,
    ) {
        let Some(id) = self.id else { return };
        let (key, value) = (key.into(), value.into());
        self.ctx.txn.with_state(|s| {
            if let Some(seg) = s.segments.get_mut(id.index()) {
                seg.attrs.insert(key, value);
            }
        });
    }

    /// Ends the segment now.
    pub fn end(self) {
        drop(self);
    }

    fn with_record(&self, f: impl FnOnce(&mut SegmentRecord)) {
        let Some(id) = self.id else { return };
        self.ctx.txn.with_state(|s| {
            if let Some(seg) = s.segments.get_mut(id.index()) {
                f(seg);
            }
        });
    }
}

impl Drop for Segment {
    fn drop(&mut self) {
        let now = Instant::now();
        self.with_record(|seg| {
            if seg.end.is_none() {
                seg.end = Some(now);
            }
        });
    }
}

/// A segment timing an outbound HTTP call. Ends when [`end`](Self::end) is called or when
/// dropped.
#[derive(Debug)]
#[must_use = "a segment ends when dropped; bind it to a variable for the duration of the call"]
pub struct ExternalSegment {
    segment: Segment,
}

impl ExternalSegment {
    /// Records the response status code.
    pub fn set_status_code(&self, status: u16) {
        self.segment.with_record(|seg| {
            if let SegmentKind::External { status: s, .. } = &mut seg.kind {
                *s = Some(status);
            }
        });
    }

    /// Writes outbound distributed-trace headers whose parent span is this segment.
    pub fn insert_distributed_trace_headers(&self, headers: &mut http::HeaderMap) {
        self.segment
            .context()
            .insert_distributed_trace_headers(headers);
    }

    /// A context whose new segments are children of this one.
    #[must_use]
    pub fn context(&self) -> SegmentContext {
        self.segment.context()
    }

    /// Ends the segment now.
    pub fn end(self) {
        drop(self);
    }
}
