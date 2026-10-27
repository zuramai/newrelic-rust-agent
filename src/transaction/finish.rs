//! Pure conversion of a finished transaction into harvest data.

use std::borrow::Cow;
use std::time::{Duration, Instant, SystemTime};

use super::{SegmentKind, SegmentRecord, TransactionKind, TxnState, unix_ms};
use crate::attributes::Attributes;
use crate::collector::connect::RunConfig;
use crate::harvest::metrics::{MetricData, MetricKey};
use crate::harvest::payload::{Event, TraceNode, TraceParams, TracedError, TxnTrace};
use crate::ids::Priority;

/// Segments shorter than this are left out of transaction traces.
const TRACE_SEGMENT_THRESHOLD: Duration = Duration::from_millis(2);
/// Maximum segment nodes in one transaction trace.
const MAX_TRACE_NODES: usize = 256;
/// Maximum span events per transaction (root included).
const MAX_SPAN_EVENTS: usize = 2000;

/// Everything one transaction contributes to the harvest.
#[derive(Debug, Default)]
pub(crate) struct FinishedTxn {
    pub(crate) priority: Priority,
    pub(crate) metrics: Vec<(MetricKey, MetricData, bool)>,
    pub(crate) txn_event: Option<Event>,
    pub(crate) span_events: Vec<Event>,
    pub(crate) error_events: Vec<Event>,
    pub(crate) traced_errors: Vec<TracedError>,
    pub(crate) trace: Option<TxnTrace>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ApdexZone {
    Satisfying,
    Tolerating,
    Failing,
}

impl ApdexZone {
    fn as_str(self) -> &'static str {
        match self {
            Self::Satisfying => "S",
            Self::Tolerating => "T",
            Self::Failing => "F",
        }
    }

    fn metric(self, threshold: f64) -> MetricData {
        match self {
            Self::Satisfying => MetricData::apdex(1.0, 0.0, 0.0, threshold),
            Self::Tolerating => MetricData::apdex(0.0, 1.0, 0.0, threshold),
            Self::Failing => MetricData::apdex(0.0, 0.0, 1.0, threshold),
        }
    }
}

/// A segment clipped to the transaction, with derived timings.
struct Seg<'a> {
    rec: &'a SegmentRecord,
    start: Instant,
    end: Instant,
    exclusive: Duration,
    metric: Cow<'static, str>,
}

impl Seg<'_> {
    fn duration(&self) -> Duration {
        self.end.saturating_duration_since(self.start)
    }
}

/// Length of the union of `intervals` clipped to `[lo, hi]`.
fn union_len(lo: Instant, hi: Instant, intervals: &mut [(Instant, Instant)]) -> Duration {
    intervals.sort_unstable_by_key(|i| i.0);
    let mut total = Duration::ZERO;
    let mut cur: Option<(Instant, Instant)> = None;
    for &(s, e) in intervals.iter() {
        let (s, e) = (s.max(lo), e.min(hi));
        if e <= s {
            continue;
        }
        cur = match cur {
            Some((cs, ce)) if s <= ce => Some((cs, ce.max(e))),
            Some((cs, ce)) => {
                total += ce - cs;
                Some((s, e))
            }
            None => Some((s, e)),
        };
    }
    if let Some((cs, ce)) = cur {
        total += ce - cs;
    }
    total
}

fn caller(s: &TxnState) -> String {
    let unknown = "Unknown";
    match &s.inbound {
        Some(p) => format!(
            "{}/{}/{}/{}",
            p.parent_type.as_deref().unwrap_or(unknown),
            p.account.as_deref().unwrap_or(unknown),
            p.app.as_deref().unwrap_or(unknown),
            p.transport.as_str()
        ),
        None => "Unknown/Unknown/Unknown/Unknown".to_owned(),
    }
}

fn wall_at(s: &TxnState, t: Instant) -> SystemTime {
    s.start_wall + t.saturating_duration_since(s.start)
}

#[expect(
    clippy::too_many_lines,
    reason = "single linear pass mirroring the collector's per-transaction data set"
)]
pub(crate) fn finish(s: &TxnState, end: Instant, run: &RunConfig) -> FinishedTxn {
    let duration = end.saturating_duration_since(s.start);
    let is_web = s.kind == TransactionKind::Web;
    let prefix = if is_web {
        "WebTransaction/Rust"
    } else {
        "OtherTransaction/Rust"
    };
    let sep = if s.name.starts_with('/') { "" } else { "/" };
    let final_name = format!("{prefix}{sep}{}", s.name);
    // `rest` drops the first path element: "WebTransaction/Rust/GET" -> "Rust/GET".
    let rest = final_name
        .split_once('/')
        .map_or(final_name.as_str(), |(_, r)| r);
    let sampled = s.sampled.unwrap_or(false);
    let rollup_suffix = if is_web { "allWeb" } else { "allOther" };

    // --- Segment timing ------------------------------------------------------------------
    let segs: Vec<Seg<'_>> = s
        .segments
        .iter()
        .map(|rec| {
            let start = rec.start.clamp(s.start, end);
            let seg_end = rec.end.unwrap_or(end).clamp(start, end);
            let metric = match &rec.kind {
                SegmentKind::Custom => Cow::Owned(format!("Custom/{}", rec.name)),
                SegmentKind::External { .. } => rec.name.clone(),
            };
            Seg {
                rec,
                start,
                end: seg_end,
                exclusive: Duration::ZERO,
                metric,
            }
        })
        .collect();
    let mut children: Vec<Vec<usize>> = vec![Vec::new(); segs.len()];
    let mut root_children = Vec::new();
    for (i, seg) in segs.iter().enumerate() {
        match seg.rec.parent.map(super::SegmentId::index) {
            Some(p) if p < i => children[p].push(i),
            _ => root_children.push(i),
        }
    }
    let mut segs = segs;
    let mut scratch = Vec::new();
    for i in 0..segs.len() {
        scratch.clear();
        scratch.extend(children[i].iter().map(|&c| (segs[c].start, segs[c].end)));
        let covered = union_len(segs[i].start, segs[i].end, &mut scratch);
        segs[i].exclusive = segs[i].duration().saturating_sub(covered);
    }
    scratch.clear();
    scratch.extend(root_children.iter().map(|&c| (segs[c].start, segs[c].end)));
    let root_exclusive = duration.saturating_sub(union_len(s.start, end, &mut scratch));
    let total_time = root_exclusive + segs.iter().map(|g| g.exclusive).sum::<Duration>();

    // --- Errors and Apdex ----------------------------------------------------------------
    let has_unexpected = s.errors.iter().any(|e| !e.err.expected);
    let has_expected_only = !s.errors.is_empty() && !has_unexpected;
    let apdex_t = run
        .web_transactions_apdex
        .get(&final_name)
        .copied()
        .unwrap_or(run.apdex_t);
    let zone = is_web.then(|| {
        let d = duration.as_secs_f64();
        if has_unexpected {
            ApdexZone::Failing
        } else if d <= apdex_t {
            ApdexZone::Satisfying
        } else if d <= 4.0 * apdex_t {
            ApdexZone::Tolerating
        } else {
            ApdexZone::Failing
        }
    });

    // --- Metrics -------------------------------------------------------------------------
    let mut metrics: Vec<(MetricKey, MetricData, bool)> = Vec::with_capacity(16 + segs.len() * 2);
    let mut add = |name: Cow<'static, str>, scope: Option<&str>, data: MetricData, forced: bool| {
        let key = match scope {
            Some(sc) => MetricKey::scoped(name, sc),
            None => MetricKey::unscoped(name),
        };
        metrics.push((key, data, forced));
    };
    let dur = MetricData::from_duration(duration, Duration::ZERO);
    let tt = MetricData::value(total_time.as_secs_f64());
    if is_web {
        add("HttpDispatcher".into(), None, dur, true);
        add("WebTransaction".into(), None, dur, true);
        add(final_name.clone().into(), None, dur, true);
        add("WebTransactionTotalTime".into(), None, tt, true);
        add(
            format!("WebTransactionTotalTime/{rest}").into(),
            None,
            tt,
            false,
        );
    } else {
        add(final_name.clone().into(), None, dur, true);
        add("OtherTransaction/all".into(), None, dur, true);
        add("OtherTransactionTotalTime".into(), None, tt, true);
        add(
            format!("OtherTransactionTotalTime/{rest}").into(),
            None,
            tt,
            false,
        );
    }
    if let Some(zone) = zone {
        add("Apdex".into(), None, zone.metric(apdex_t), true);
        add(
            format!("Apdex/{rest}").into(),
            None,
            zone.metric(apdex_t),
            false,
        );
    }
    if has_unexpected {
        let one = MetricData::count(1.0);
        add("Errors/all".into(), None, one, true);
        add(format!("Errors/{rollup_suffix}").into(), None, one, true);
        add(format!("Errors/{final_name}").into(), None, one, true);
    } else if has_expected_only {
        add(
            "ErrorsExpected/all".into(),
            None,
            MetricData::count(1.0),
            true,
        );
    }
    let mut external_count: i64 = 0;
    let mut external_duration = Duration::ZERO;
    for seg in &segs {
        let data = MetricData::from_duration(seg.duration(), seg.exclusive);
        match &seg.rec.kind {
            SegmentKind::Custom => {
                add(seg.metric.clone(), None, data, false);
                add(seg.metric.clone(), Some(&final_name), data, false);
            }
            SegmentKind::External { host, .. } => {
                external_count += 1;
                external_duration += seg.duration();
                add("External/all".into(), None, data, true);
                add(format!("External/{rollup_suffix}").into(), None, data, true);
                add(format!("External/{host}/all").into(), None, data, false);
                add(seg.metric.clone(), Some(&final_name), data, false);
            }
        }
    }
    let caller = caller(s);
    add(
        format!("DurationByCaller/{caller}/all").into(),
        None,
        dur,
        false,
    );
    add(
        format!("DurationByCaller/{caller}/{rollup_suffix}").into(),
        None,
        dur,
        false,
    );
    if let (Some(_), Some(td)) = (&s.inbound, s.transport_duration) {
        let data = MetricData::from_duration(td, td);
        add(
            format!("TransportDuration/{caller}/all").into(),
            None,
            data,
            false,
        );
        add(
            format!("TransportDuration/{caller}/{rollup_suffix}").into(),
            None,
            data,
            false,
        );
    }
    if has_unexpected {
        let one = MetricData::count(1.0);
        add(
            format!("ErrorsByCaller/{caller}/all").into(),
            None,
            one,
            false,
        );
        add(
            format!("ErrorsByCaller/{caller}/{rollup_suffix}").into(),
            None,
            one,
            false,
        );
    }

    // --- Shared intrinsics ---------------------------------------------------------------
    let trace_id = s.trace_id.to_string();
    let guid = s.txn_guid.to_string();
    let push_shared = |i: &mut Attributes| {
        i.push("duration", duration.as_secs_f64());
        if external_count > 0 {
            i.push("externalCallCount", external_count);
            i.push("externalDuration", external_duration.as_secs_f64());
        }
        if let Some(p) = &s.inbound {
            if p.has_nr_info() {
                i.push("parent.type", p.parent_type.clone().unwrap_or_default());
                i.push("parent.app", p.app.clone().unwrap_or_default());
                i.push("parent.account", p.account.clone().unwrap_or_default());
                i.push(
                    "parent.transportDuration",
                    s.transport_duration.unwrap_or_default().as_secs_f64(),
                );
            }
            i.push("parent.transportType", p.transport.as_str());
        }
        i.push("guid", guid.clone());
        i.push("traceId", trace_id.clone());
        i.push("priority", f64::from(s.priority.0));
        i.push("sampled", sampled);
    };

    let txn_event = run.collect_analytics_events.then(|| {
        let mut i = Attributes::with_capacity(20);
        i.push("type", "Transaction");
        i.push("name", final_name.clone());
        i.push("timestamp", ms_int(s.start_wall));
        if let Some(zone) = zone {
            i.push("nr.apdexPerfZone", zone.as_str());
        }
        i.push("error", has_unexpected);
        push_shared(&mut i);
        i.push("totalTime", total_time.as_secs_f64());
        if let Some(p) = &s.inbound {
            if let Some(tx) = &p.txn_id {
                i.push("parentId", tx.clone());
            }
            if let Some(span) = p.parent_span_id {
                i.push("parentSpanId", span.to_string());
            }
        }
        Event {
            intrinsics: i,
            user: s.user_attrs.clone(),
            agent: s.agent_attrs.clone(),
        }
    });

    let error_events: Vec<Event> = if run.collect_error_events {
        s.errors
            .iter()
            .map(|e| {
                let mut i = Attributes::with_capacity(20);
                i.push("type", "TransactionError");
                i.push("error.class", e.err.class.clone());
                i.push("error.message", e.err.message.clone());
                i.push("timestamp", ms_int(e.at));
                i.push("transactionName", final_name.clone());
                i.push("spanId", e.span.to_string());
                if e.err.expected {
                    i.push("error.expected", true);
                }
                push_shared(&mut i);
                Event {
                    intrinsics: i,
                    user: s.user_attrs.clone(),
                    agent: s.agent_attrs.clone(),
                }
            })
            .collect()
    } else {
        Vec::new()
    };

    let trace_intrinsics = |expected: bool| {
        let mut i = Attributes::with_capacity(6);
        i.push("totalTime", total_time.as_secs_f64());
        i.push("guid", guid.clone());
        i.push("traceId", trace_id.clone());
        i.push("priority", f64::from(s.priority.0));
        i.push("sampled", sampled);
        if expected {
            i.push("error.expected", true);
        }
        i
    };

    let traced_errors: Vec<TracedError> = if run.collect_errors {
        s.errors
            .iter()
            .map(|e| TracedError {
                timestamp_ms: ms_float(e.at),
                txn_name: final_name.clone(),
                message: e.err.message.clone(),
                class: e.err.class.clone(),
                params: TraceParams {
                    agent: s.agent_attrs.clone(),
                    user: s.user_attrs.clone(),
                    intrinsics: trace_intrinsics(e.err.expected),
                },
                txn_guid: guid.clone(),
            })
            .collect()
    } else {
        Vec::new()
    };

    // --- Span events ---------------------------------------------------------------------
    let span_events = if sampled && run.collect_span_events {
        let mut out = Vec::with_capacity((segs.len() + 1).min(MAX_SPAN_EVENTS));
        let span_base = |i: &mut Attributes,
                         guid: String,
                         parent: Option<String>,
                         start: SystemTime,
                         d: Duration,
                         name: String| {
            i.push("type", "Span");
            i.push("traceId", trace_id.clone());
            i.push("guid", guid);
            if let Some(p) = parent {
                i.push("parentId", p);
            }
            i.push("transactionId", s.txn_guid.to_string());
            i.push("sampled", sampled);
            i.push("priority", f64::from(s.priority.0));
            i.push("timestamp", ms_int(start));
            i.push("duration", d.as_secs_f64());
            i.push("name", name);
        };
        let mut root = Attributes::with_capacity(14);
        span_base(
            &mut root,
            s.root_span.to_string(),
            s.inbound
                .as_ref()
                .and_then(|p| p.parent_span_id)
                .map(|id| id.to_string()),
            s.start_wall,
            duration,
            final_name.clone(),
        );
        root.push("category", "generic");
        root.push("nr.entryPoint", true);
        let mut root_agent = s.agent_attrs.clone();
        root_agent.set("transaction.name", final_name.clone());
        if let Some(e) = s.errors.first() {
            root_agent.set("error.class", e.err.class.clone());
            root_agent.set("error.message", e.err.message.clone());
        }
        root_agent.extend_from(&s.root_span_attrs);
        out.push(Event {
            intrinsics: root,
            user: s.user_attrs.clone(),
            agent: root_agent,
        });
        for seg in segs.iter().take(MAX_SPAN_EVENTS - 1) {
            let parent = s.span_of(seg.rec.parent);
            let mut i = Attributes::with_capacity(14);
            span_base(
                &mut i,
                seg.rec.span_id.to_string(),
                Some(parent.to_string()),
                wall_at(s, seg.start),
                seg.duration(),
                seg.metric.to_string(),
            );
            let mut agent = seg.rec.agent_attrs.clone();
            match &seg.rec.kind {
                SegmentKind::Custom => i.push("category", "generic"),
                SegmentKind::External {
                    method,
                    url,
                    status,
                    ..
                } => {
                    i.push("category", "http");
                    i.push("component", "http");
                    i.push("span.kind", "client");
                    agent.set("http.url", url.clone());
                    agent.set("http.method", method.clone());
                    if let Some(st) = status {
                        agent.set("http.statusCode", *st);
                    }
                }
            }
            out.push(Event {
                intrinsics: i,
                user: seg.rec.attrs.clone(),
                agent,
            });
        }
        out
    } else {
        Vec::new()
    };

    // --- Transaction trace ---------------------------------------------------------------
    let trace = (run.collect_traces && duration.as_secs_f64() >= 4.0 * apdex_t).then(|| {
        let dur_ms = ms_rel(duration);
        let nodes = trace_nodes(s, &segs);
        TxnTrace {
            start_ms: ms_float(s.start_wall),
            duration_ms: duration.as_secs_f64() * 1000.0,
            name: final_name.clone(),
            request_uri: s.request_uri.clone(),
            root: TraceNode {
                start_ms: 0,
                stop_ms: dur_ms,
                name: "ROOT".to_owned(),
                children: vec![TraceNode {
                    start_ms: 0,
                    stop_ms: dur_ms,
                    name: final_name.clone(),
                    children: nodes,
                }],
            },
            params: TraceParams {
                agent: s.agent_attrs.clone(),
                user: s.user_attrs.clone(),
                intrinsics: trace_intrinsics(false),
            },
            trace_id: trace_id.clone(),
        }
    });

    FinishedTxn {
        priority: s.priority,
        metrics,
        txn_event,
        span_events,
        error_events,
        traced_errors,
        trace,
    }
}

/// Builds trace nodes for the longest segments over the threshold, re-parenting segments whose
/// ancestors were dropped onto the nearest kept ancestor.
fn trace_nodes(s: &TxnState, segs: &[Seg<'_>]) -> Vec<TraceNode> {
    let mut eligible: Vec<usize> = (0..segs.len())
        .filter(|&i| segs[i].duration() >= TRACE_SEGMENT_THRESHOLD)
        .collect();
    eligible.sort_by_key(|&i| std::cmp::Reverse(segs[i].duration()));
    eligible.truncate(MAX_TRACE_NODES);
    let mut kept = vec![false; segs.len()];
    for &i in &eligible {
        kept[i] = true;
    }
    let kept_parent = |mut i: usize| -> Option<usize> {
        loop {
            let p = segs[i].rec.parent.map(super::SegmentId::index)?;
            if p >= i {
                return None;
            }
            if kept[p] {
                return Some(p);
            }
            i = p;
        }
    };
    let mut kids: Vec<Vec<usize>> = vec![Vec::new(); segs.len()];
    let mut top = Vec::new();
    let mut ordered = eligible;
    ordered.sort_by_key(|&i| segs[i].start);
    for i in ordered {
        match kept_parent(i) {
            Some(p) => kids[p].push(i),
            None => top.push(i),
        }
    }
    top.iter().map(|&i| build(s, segs, &kids, i)).collect()
}

fn build(s: &TxnState, segs: &[Seg<'_>], kids: &[Vec<usize>], i: usize) -> TraceNode {
    TraceNode {
        start_ms: ms_rel(segs[i].start.saturating_duration_since(s.start)),
        stop_ms: ms_rel(segs[i].end.saturating_duration_since(s.start)),
        name: segs[i].metric.to_string(),
        children: kids[i].iter().map(|&c| build(s, segs, kids, c)).collect(),
    }
}

fn ms_int(t: SystemTime) -> u64 {
    unix_ms(t)
}

fn ms_float(t: SystemTime) -> f64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64() * 1000.0)
}

fn ms_rel(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attributes::AttributeValue;
    use crate::ids::SpanId;
    use crate::transaction::{NoticedError, SegmentId};

    fn state(kind: TransactionKind, name: &'static str) -> TxnState {
        let mut s = TxnState::new(name.into(), kind);
        s.sampled = Some(true);
        s
    }

    fn seg(s: &mut TxnState, parent: Option<usize>, name: &'static str, from_ms: u64, to_ms: u64) {
        s.segments.push(SegmentRecord {
            parent: parent.map(|p| SegmentId(u32::try_from(p).unwrap())),
            name: name.into(),
            kind: SegmentKind::Custom,
            span_id: SpanId::random(),
            start: s.start + Duration::from_millis(from_ms),
            end: Some(s.start + Duration::from_millis(to_ms)),
            attrs: Attributes::default(),
            agent_attrs: Attributes::default(),
        });
    }

    fn metric<'a>(f: &'a FinishedTxn, name: &str, scope: Option<&str>) -> Option<&'a MetricData> {
        f.metrics
            .iter()
            .find(|(k, _, _)| k.name == name && k.scope.as_deref() == scope)
            .map(|(_, d, _)| d)
    }

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    #[test]
    fn finish_subtracts_union_of_overlapping_children_from_exclusive_time() {
        let mut s = state(TransactionKind::Other, "job");
        seg(&mut s, None, "parent", 0, 100);
        seg(&mut s, Some(0), "a", 10, 50);
        seg(&mut s, Some(0), "b", 30, 70);
        let end = s.start + Duration::from_millis(100);
        let f = finish(&s, end, &RunConfig::default());
        let parent = metric(&f, "Custom/parent", None).unwrap();
        // children cover 10..70 = 60ms, so exclusive = 40ms.
        assert!(close(parent.exclusive, 0.040));
    }

    #[test]
    fn finish_emits_web_metric_set() {
        let s = state(TransactionKind::Web, "GET /users/{id}");
        let end = s.start + Duration::from_millis(120);
        let f = finish(&s, end, &RunConfig::default());
        for name in [
            "HttpDispatcher",
            "WebTransaction",
            "WebTransaction/Rust/GET /users/{id}",
            "WebTransactionTotalTime",
            "WebTransactionTotalTime/Rust/GET /users/{id}",
            "Apdex",
            "Apdex/Rust/GET /users/{id}",
            "DurationByCaller/Unknown/Unknown/Unknown/Unknown/all",
            "DurationByCaller/Unknown/Unknown/Unknown/Unknown/allWeb",
        ] {
            assert_eq!(metric(&f, name, None).map(|m| m.count), Some(1.0), "{name}");
        }
        assert!(metric(&f, "Errors/all", None).is_none());
        let apdex = metric(&f, "Apdex", None).unwrap();
        assert_eq!((apdex.count, apdex.total, apdex.exclusive), (1.0, 0.0, 0.0));
    }

    #[test]
    fn finish_marks_apdex_failing_when_unexpected_error_noticed() {
        let mut s = state(TransactionKind::Web, "GET");
        s.push_error(NoticedError {
            class: "500".to_owned(),
            message: "boom".to_owned(),
            expected: false,
        });
        let end = s.start + Duration::from_millis(1);
        let f = finish(&s, end, &RunConfig::default());
        assert!(close(metric(&f, "Apdex", None).unwrap().exclusive, 1.0));
        let event = f.txn_event.unwrap();
        assert_eq!(
            event.intrinsics.get("nr.apdexPerfZone"),
            Some(&AttributeValue::String("F".to_owned()))
        );
    }

    #[test]
    fn finish_emits_no_span_events_when_unsampled() {
        let mut s = state(TransactionKind::Other, "job");
        s.sampled = Some(false);
        seg(&mut s, None, "a", 0, 5);
        let end = s.start + Duration::from_millis(10);
        assert!(
            finish(&s, end, &RunConfig::default())
                .span_events
                .is_empty()
        );
    }

    #[test]
    fn finish_emits_errors_all_web_only_for_unexpected_errors() {
        let mut s = state(TransactionKind::Web, "GET");
        s.push_error(NoticedError {
            class: "E".to_owned(),
            message: "m".to_owned(),
            expected: true,
        });
        let end = s.start + Duration::from_millis(1);
        let f = finish(&s, end, &RunConfig::default());
        assert!(metric(&f, "Errors/allWeb", None).is_none());
        assert!(metric(&f, "ErrorsExpected/all", None).is_some());
        assert_eq!(f.error_events.len(), 1);
    }

    #[test]
    fn finish_links_segment_spans_to_parent_span() {
        let mut s = state(TransactionKind::Other, "job");
        seg(&mut s, None, "a", 0, 5);
        seg(&mut s, Some(0), "b", 1, 2);
        let root = s.root_span.to_string();
        let a = s.segments[0].span_id.to_string();
        let end = s.start + Duration::from_millis(10);
        let f = finish(&s, end, &RunConfig::default());
        let parent_of = |i: usize| f.span_events[i].intrinsics.get("parentId").cloned();
        assert_eq!(parent_of(1), Some(AttributeValue::String(root)));
        assert_eq!(parent_of(2), Some(AttributeValue::String(a)));
    }

    #[test]
    fn finish_builds_trace_only_for_slow_transactions_and_skips_short_segments() {
        let mut s = state(TransactionKind::Other, "job");
        seg(&mut s, None, "slow", 0, 1500);
        seg(&mut s, Some(0), "tiny", 10, 11);
        seg(&mut s, Some(1), "grandchild", 10, 11);
        let end = s.start + Duration::from_millis(2500);
        let f = finish(&s, end, &RunConfig::default());
        let trace = f.trace.unwrap();
        let txn_node = &trace.root.children[0];
        assert_eq!(txn_node.children.len(), 1);
        assert_eq!(txn_node.children[0].name, "Custom/slow");
        assert!(txn_node.children[0].children.is_empty());
    }

    #[test]
    fn finish_skips_trace_for_fast_transactions() {
        let s = state(TransactionKind::Other, "job");
        let end = s.start + Duration::from_millis(10);
        assert!(finish(&s, end, &RunConfig::default()).trace.is_none());
    }
}
