//! Data buffered between harvests.

pub(crate) mod metrics;
pub(crate) mod payload;
pub(crate) mod reservoir;

use std::time::SystemTime;

use crate::ids::Priority;
use crate::transaction::finish::FinishedTxn;
use metrics::{MetricData, MetricKey, MetricTable};
use payload::{Event, LogEvent, TracedError, TxnTrace};
use reservoir::Reservoir;

pub(crate) const DEFAULT_TXN_EVENTS: usize = 10_000;
pub(crate) const DEFAULT_SPAN_EVENTS: usize = 2_000;
pub(crate) const DEFAULT_ERROR_EVENTS: usize = 100;
pub(crate) const DEFAULT_CUSTOM_EVENTS: usize = 30_000;
pub(crate) const DEFAULT_LOG_EVENTS: usize = 10_000;
const MAX_TRACED_ERRORS: usize = 20;

/// Reservoir caps negotiated at connect. `None` keeps the local default.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[expect(clippy::struct_field_names, reason = "one field per event type")]
pub(crate) struct Limits {
    pub(crate) txn_events: Option<usize>,
    pub(crate) span_events: Option<usize>,
    pub(crate) error_events: Option<usize>,
    pub(crate) custom_events: Option<usize>,
    pub(crate) log_events: Option<usize>,
}

/// Event data sent on the (configurable, default 60 s) event cycle.
#[derive(Debug)]
#[expect(clippy::struct_field_names, reason = "one field per event type")]
pub(crate) struct EventPayloads {
    pub(crate) txn_events: Reservoir<Event>,
    pub(crate) span_events: Reservoir<Event>,
    pub(crate) error_events: Reservoir<Event>,
    pub(crate) custom_events: Reservoir<Event>,
    pub(crate) log_events: Reservoir<LogEvent>,
}

/// Data sent on the fixed 60 s cycle.
#[derive(Debug)]
pub(crate) struct SlowPayloads {
    pub(crate) metrics: MetricTable,
    pub(crate) traced_errors: Vec<TracedError>,
    pub(crate) trace: Option<TxnTrace>,
}

#[derive(Debug)]
pub(crate) struct Harvest {
    pub(crate) metrics: MetricTable,
    pub(crate) events: EventPayloads,
    pub(crate) traced_errors: Vec<TracedError>,
    pub(crate) trace: Option<TxnTrace>,
}

impl EventPayloads {
    fn new() -> Self {
        Self {
            txn_events: Reservoir::new(DEFAULT_TXN_EVENTS),
            span_events: Reservoir::new(DEFAULT_SPAN_EVENTS),
            error_events: Reservoir::new(DEFAULT_ERROR_EVENTS),
            custom_events: Reservoir::new(DEFAULT_CUSTOM_EVENTS),
            log_events: Reservoir::new(DEFAULT_LOG_EVENTS),
        }
    }
}

impl Harvest {
    pub(crate) fn new(now: SystemTime) -> Self {
        Self {
            metrics: MetricTable::new(now),
            events: EventPayloads::new(),
            traced_errors: Vec::new(),
            trace: None,
        }
    }

    pub(crate) fn add_metric(&mut self, key: MetricKey, data: MetricData, forced: bool) {
        self.metrics.add(key, data, forced);
    }

    pub(crate) fn take_events(&mut self) -> EventPayloads {
        EventPayloads {
            txn_events: self.events.txn_events.take(),
            span_events: self.events.span_events.take(),
            error_events: self.events.error_events.take(),
            custom_events: self.events.custom_events.take(),
            log_events: self.events.log_events.take(),
        }
    }

    pub(crate) fn take_slow(&mut self, now: SystemTime) -> SlowPayloads {
        SlowPayloads {
            metrics: std::mem::replace(&mut self.metrics, MetricTable::new(now)),
            traced_errors: std::mem::take(&mut self.traced_errors),
            trace: self.trace.take(),
        }
    }

    pub(crate) fn merge_events(&mut self, e: EventPayloads) {
        self.events.txn_events.merge(e.txn_events);
        self.events.span_events.merge(e.span_events);
        self.events.error_events.merge(e.error_events);
        self.events.custom_events.merge(e.custom_events);
        self.events.log_events.merge(e.log_events);
    }

    pub(crate) fn merge_metrics(&mut self, m: MetricTable) {
        self.metrics.merge_failed(m);
    }

    pub(crate) fn apply_limits(&mut self, l: &Limits) {
        let e = &mut self.events;
        e.txn_events
            .set_cap(l.txn_events.unwrap_or(DEFAULT_TXN_EVENTS));
        e.span_events
            .set_cap(l.span_events.unwrap_or(DEFAULT_SPAN_EVENTS));
        e.error_events
            .set_cap(l.error_events.unwrap_or(DEFAULT_ERROR_EVENTS));
        e.custom_events
            .set_cap(l.custom_events.unwrap_or(DEFAULT_CUSTOM_EVENTS));
        e.log_events
            .set_cap(l.log_events.unwrap_or(DEFAULT_LOG_EVENTS));
    }

    pub(crate) fn push_log(&mut self, priority: Priority, log: LogEvent) {
        self.events.log_events.push(priority, log);
    }

    pub(crate) fn push_custom_event(&mut self, priority: Priority, event: Event) {
        self.events.custom_events.push(priority, event);
    }

    /// Records everything produced by one finished transaction.
    pub(crate) fn absorb(&mut self, f: FinishedTxn) {
        for (key, data, forced) in f.metrics {
            self.metrics.add(key, data, forced);
        }
        if let Some(e) = f.txn_event {
            self.events.txn_events.push(f.priority, e);
        }
        for e in f.span_events {
            self.events.span_events.push(f.priority, e);
        }
        for e in f.error_events {
            self.events.error_events.push(f.priority, e);
        }
        for e in f.traced_errors {
            if self.traced_errors.len() < MAX_TRACED_ERRORS {
                self.traced_errors.push(e);
            }
        }
        if let Some(t) = f.trace {
            let slower = self
                .trace
                .as_ref()
                .is_none_or(|cur| t.duration_ms > cur.duration_ms);
            if slower {
                self.trace = Some(t);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn take_events_keeps_negotiated_caps() {
        let mut h = Harvest::new(SystemTime::UNIX_EPOCH);
        h.apply_limits(&Limits {
            span_events: Some(7),
            ..Limits::default()
        });
        let _ = h.take_events();
        assert_eq!(h.events.span_events.cap(), 7);
        assert_eq!(h.events.txn_events.cap(), DEFAULT_TXN_EVENTS);
    }
}
