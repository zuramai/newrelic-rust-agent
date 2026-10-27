//! Trace/span identifiers and sampling priority.

use std::cmp::Ordering;
use std::fmt;

/// 128-bit W3C trace id. Never zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct TraceId(pub(crate) u128);

/// 64-bit span id. Never zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct SpanId(pub(crate) u64);

impl TraceId {
    pub(crate) fn random() -> Self {
        loop {
            let v: u128 = rand::random();
            if v != 0 {
                return Self(v);
            }
        }
    }

    pub(crate) fn parse_hex(s: &str) -> Option<Self> {
        parse_lower_hex(s, 32).map(Self)
    }
}

impl SpanId {
    pub(crate) fn random() -> Self {
        loop {
            let v: u64 = rand::random();
            if v != 0 {
                return Self(v);
            }
        }
    }

    pub(crate) fn parse_hex(s: &str) -> Option<Self> {
        parse_lower_hex(s, 16)
            .and_then(|v| u64::try_from(v).ok())
            .map(Self)
    }
}

/// Parses exactly `len` lowercase hex digits into a non-zero value.
fn parse_lower_hex(s: &str, len: usize) -> Option<u128> {
    if s.len() != len || !s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return None;
    }
    u128::from_str_radix(s, 16).ok().filter(|v| *v != 0)
}

impl fmt::Display for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

impl fmt::Display for SpanId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// Transaction guid: the first 16 hex digits of the locally generated trace id.
pub(crate) fn txn_guid(trace: TraceId) -> SpanId {
    let hi = u64::try_from(trace.0 >> 64).unwrap_or(0);
    // A zero upper half is possible for a random trace id; fall back to the lower half.
    let lo = u64::try_from(trace.0 & u128::from(u64::MAX)).unwrap_or(0);
    let id = if hi == 0 { lo } else { hi };
    SpanId(id)
}

/// Sampling priority in `[0, 2)`; `+1.0` once a transaction is sampled.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct Priority(pub(crate) f32);

impl Priority {
    pub(crate) fn random() -> Self {
        Self::new(rand::random::<f32>())
    }

    /// Rounds to 6 decimals, the precision New Relic uses on the wire.
    pub(crate) fn new(v: f32) -> Self {
        Self((v * 1e6).round() / 1e6)
    }

    pub(crate) fn to_tracestate(self) -> String {
        let s = format!("{:.6}", self.0);
        let s = s.trim_end_matches('0');
        s.trim_end_matches('.').to_owned()
    }
}

impl Eq for Priority {}

impl PartialOrd for Priority {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Priority {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl serde::Serialize for Priority {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_f32(self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn priority_to_tracestate_trims_trailing_zeros() {
        assert_eq!(Priority::new(0.5).to_tracestate(), "0.5");
    }

    #[test]
    fn priority_to_tracestate_keeps_six_decimals() {
        assert_eq!(Priority::new(1.234_567).to_tracestate(), "1.234567");
    }

    #[test]
    fn trace_id_parse_hex_rejects_all_zero() {
        assert!(TraceId::parse_hex(&"0".repeat(32)).is_none());
    }

    #[test]
    fn trace_id_parse_hex_rejects_uppercase() {
        assert!(TraceId::parse_hex(&"A".repeat(32)).is_none());
    }

    #[test]
    fn span_id_display_is_zero_padded() {
        assert_eq!(SpanId(0xab).to_string(), "00000000000000ab");
    }

    #[test]
    fn txn_guid_uses_upper_half_of_trace_id() {
        let trace = TraceId::parse_hex("0123456789abcdef1111111111111111").unwrap();
        assert_eq!(txn_guid(trace).to_string(), "0123456789abcdef");
    }
}
