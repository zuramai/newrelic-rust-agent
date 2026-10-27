//! Aggregated timeslice metrics (`metric_data`).

use std::borrow::Cow;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use serde::ser::{SerializeSeq as _, SerializeTuple as _};

/// Maximum distinct unforced metrics per harvest.
const MAX_METRICS: usize = 2000;
/// After this many failed sends a metric table is dropped instead of merged back.
const MAX_FAILED_HARVESTS: u8 = 5;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct MetricKey {
    pub(crate) name: Cow<'static, str>,
    pub(crate) scope: Option<String>,
}

impl MetricKey {
    pub(crate) fn unscoped(name: impl Into<Cow<'static, str>>) -> Self {
        Self {
            name: name.into(),
            scope: None,
        }
    }

    pub(crate) fn scoped(name: impl Into<Cow<'static, str>>, scope: &str) -> Self {
        Self {
            name: name.into(),
            scope: Some(scope.to_owned()),
        }
    }
}

/// `[count, total, exclusive, min, max, sum_of_squares]`; apdex metrics reuse the first three
/// slots as satisfied/tolerating/frustrated counts and min/max as the apdex threshold.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct MetricData {
    pub(crate) count: f64,
    pub(crate) total: f64,
    pub(crate) exclusive: f64,
    pub(crate) min: f64,
    pub(crate) max: f64,
    pub(crate) sum_sq: f64,
    apdex: bool,
}

impl MetricData {
    pub(crate) fn from_duration(d: Duration, exclusive: Duration) -> Self {
        let t = d.as_secs_f64();
        Self {
            count: 1.0,
            total: t,
            exclusive: exclusive.as_secs_f64(),
            min: t,
            max: t,
            sum_sq: t * t,
            apdex: false,
        }
    }

    pub(crate) fn count(n: f64) -> Self {
        Self {
            count: n,
            total: 0.0,
            exclusive: 0.0,
            min: 0.0,
            max: 0.0,
            sum_sq: 0.0,
            apdex: false,
        }
    }

    pub(crate) fn value(v: f64) -> Self {
        Self {
            count: 1.0,
            total: v,
            exclusive: v,
            min: v,
            max: v,
            sum_sq: v * v,
            apdex: false,
        }
    }

    pub(crate) fn apdex(satisfied: f64, tolerating: f64, failing: f64, threshold: f64) -> Self {
        Self {
            count: satisfied,
            total: tolerating,
            exclusive: failing,
            min: threshold,
            max: threshold,
            sum_sq: 0.0,
            apdex: true,
        }
    }

    pub(crate) fn merge(&mut self, other: &Self) {
        if self.apdex {
            self.count += other.count;
            self.total += other.total;
            self.exclusive += other.exclusive;
            return;
        }
        if self.count == 0.0 {
            self.min = other.min;
            self.max = other.max;
        } else if other.count != 0.0 {
            self.min = self.min.min(other.min);
            self.max = self.max.max(other.max);
        }
        self.count += other.count;
        self.total += other.total;
        self.exclusive += other.exclusive;
        self.sum_sq += other.sum_sq;
    }
}

impl serde::Serialize for MetricData {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut t = s.serialize_tuple(6)?;
        for v in [
            self.count,
            self.total,
            self.exclusive,
            self.min,
            self.max,
            self.sum_sq,
        ] {
            t.serialize_element(&v)?;
        }
        t.end()
    }
}

#[derive(Debug)]
pub(crate) struct MetricTable {
    map: HashMap<MetricKey, MetricData>,
    pub(crate) start: SystemTime,
    failed_harvests: u8,
}

impl MetricTable {
    pub(crate) fn new(start: SystemTime) -> Self {
        Self {
            map: HashMap::new(),
            start,
            failed_harvests: 0,
        }
    }

    pub(crate) fn add(&mut self, key: MetricKey, data: MetricData, forced: bool) {
        if let Some(existing) = self.map.get_mut(&key) {
            existing.merge(&data);
        } else if forced || self.map.len() < MAX_METRICS {
            self.map.insert(key, data);
        }
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn get(&self, name: &str, scope: Option<&str>) -> Option<&MetricData> {
        self.map.get(&MetricKey {
            name: Cow::Owned(name.to_owned()),
            scope: scope.map(str::to_owned),
        })
    }

    /// Merges a table whose send failed back in. Returns without merging once the table has
    /// failed [`MAX_FAILED_HARVESTS`] times so a dead collector cannot grow memory forever.
    pub(crate) fn merge_failed(&mut self, mut other: Self) {
        other.failed_harvests += 1;
        if other.failed_harvests >= MAX_FAILED_HARVESTS {
            return;
        }
        self.start = self.start.min(other.start);
        self.failed_harvests = self.failed_harvests.max(other.failed_harvests);
        for (k, v) in other.map {
            // Merged-back data is forced so it is never silently dropped by the cap.
            self.add(k, v, true);
        }
    }

    pub(crate) fn json(&self, run_id: &str, end: SystemTime) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(&MetricPayload {
            run_id,
            start: unix_secs(self.start),
            end: unix_secs(end),
            table: self,
        })
    }
}

pub(crate) fn unix_secs(t: SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

struct MetricPayload<'a> {
    run_id: &'a str,
    start: u64,
    end: u64,
    table: &'a MetricTable,
}

#[derive(serde::Serialize)]
struct KeyJson<'a> {
    name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    scope: Option<&'a str>,
}

struct Rows<'a>(&'a HashMap<MetricKey, MetricData>);

impl serde::Serialize for Rows<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.0.len()))?;
        for (k, v) in self.0 {
            let key = KeyJson {
                name: &k.name,
                scope: k.scope.as_deref(),
            };
            seq.serialize_element(&(key, v))?;
        }
        seq.end()
    }
}

impl serde::Serialize for MetricPayload<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut t = s.serialize_tuple(4)?;
        t.serialize_element(self.run_id)?;
        t.serialize_element(&self.start)?;
        t.serialize_element(&self.end)?;
        t.serialize_element(&Rows(&self.table.map))?;
        t.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn json_matches_collector_shape_with_and_without_scope() {
        let start = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let mut t = MetricTable::new(start);
        t.add(
            MetricKey::scoped("Custom/x", "WebTransaction/Rust/GET"),
            MetricData::from_duration(Duration::from_secs(2), Duration::from_secs(1)),
            false,
        );
        let v: serde_json::Value =
            serde_json::from_slice(&t.json("run", start + Duration::from_secs(60)).unwrap())
                .unwrap();
        assert_eq!(
            v,
            json!(["run", 100, 160, [[{"name": "Custom/x", "scope": "WebTransaction/Rust/GET"}, [1.0, 2.0, 1.0, 2.0, 2.0, 4.0]]]])
        );
        let mut t = MetricTable::new(start);
        t.add(
            MetricKey::unscoped("Instance/Reporting"),
            MetricData::count(1.0),
            true,
        );
        let v: serde_json::Value = serde_json::from_slice(&t.json("run", start).unwrap()).unwrap();
        assert_eq!(v[3][0][0], json!({"name": "Instance/Reporting"}));
    }

    #[test]
    fn merge_keeps_min_and_max_across_samples() {
        let mut a = MetricData::from_duration(Duration::from_secs(1), Duration::ZERO);
        a.merge(&MetricData::from_duration(
            Duration::from_secs(3),
            Duration::ZERO,
        ));
        assert_eq!(
            (a.count, a.total, a.min, a.max, a.sum_sq),
            (2.0, 4.0, 1.0, 3.0, 10.0)
        );
    }

    #[test]
    fn merge_sums_apdex_zones_and_keeps_threshold() {
        let mut a = MetricData::apdex(1.0, 0.0, 0.0, 0.5);
        a.merge(&MetricData::apdex(0.0, 0.0, 1.0, 0.5));
        assert_eq!(
            (a.count, a.total, a.exclusive, a.min, a.max),
            (1.0, 0.0, 1.0, 0.5, 0.5)
        );
    }

    #[test]
    fn add_drops_unforced_metrics_beyond_cap() {
        let mut t = MetricTable::new(SystemTime::UNIX_EPOCH);
        for i in 0..MAX_METRICS {
            t.add(
                MetricKey::unscoped(format!("m{i}")),
                MetricData::count(1.0),
                false,
            );
        }
        t.add(MetricKey::unscoped("extra"), MetricData::count(1.0), false);
        t.add(MetricKey::unscoped("forced"), MetricData::count(1.0), true);
        assert!(t.get("extra", None).is_none());
        assert!(t.get("forced", None).is_some());
    }

    #[test]
    fn merge_failed_drops_table_after_five_failures() {
        let mut current = MetricTable::new(SystemTime::UNIX_EPOCH);
        let mut failed = MetricTable::new(SystemTime::UNIX_EPOCH);
        failed.add(MetricKey::unscoped("m"), MetricData::count(1.0), true);
        failed.failed_harvests = MAX_FAILED_HARVESTS - 1;
        current.merge_failed(failed);
        assert!(current.is_empty());
    }
}
