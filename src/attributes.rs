//! Attribute values and bounded attribute maps.

use std::borrow::Cow;

use serde::ser::SerializeMap as _;

/// Maximum number of bytes kept for attribute keys and string values.
pub(crate) const MAX_ATTR_BYTES: usize = 255;
/// Maximum number of user attributes per transaction, segment or custom event.
pub(crate) const MAX_USER_ATTRS: usize = 64;

/// A value attached to a transaction, segment, custom event or log record.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(untagged)]
pub enum AttributeValue {
    /// A string, truncated to 255 bytes when recorded.
    String(String),
    /// A boolean.
    Bool(bool),
    /// A signed integer.
    Int(i64),
    /// A floating point number.
    Float(f64),
}

impl From<&str> for AttributeValue {
    fn from(v: &str) -> Self {
        Self::String(v.to_owned())
    }
}

impl From<String> for AttributeValue {
    fn from(v: String) -> Self {
        Self::String(v)
    }
}

impl From<Cow<'_, str>> for AttributeValue {
    fn from(v: Cow<'_, str>) -> Self {
        Self::String(v.into_owned())
    }
}

impl From<bool> for AttributeValue {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

impl From<i32> for AttributeValue {
    fn from(v: i32) -> Self {
        Self::Int(i64::from(v))
    }
}

impl From<i64> for AttributeValue {
    fn from(v: i64) -> Self {
        Self::Int(v)
    }
}

impl From<u32> for AttributeValue {
    fn from(v: u32) -> Self {
        Self::Int(i64::from(v))
    }
}

impl From<u16> for AttributeValue {
    fn from(v: u16) -> Self {
        Self::Int(i64::from(v))
    }
}

impl From<u64> for AttributeValue {
    fn from(v: u64) -> Self {
        #[expect(
            clippy::cast_precision_loss,
            reason = "values beyond i64::MAX are reported as floats"
        )]
        i64::try_from(v).map_or(Self::Float(v as f64), Self::Int)
    }
}

impl From<f32> for AttributeValue {
    fn from(v: f32) -> Self {
        Self::Float(f64::from(v))
    }
}

impl From<f64> for AttributeValue {
    fn from(v: f64) -> Self {
        Self::Float(v)
    }
}

/// Ordered key/value list serialized as a JSON object.
///
/// [`Attributes::insert`] applies New Relic's user-attribute limits; [`Attributes::push`] is for
/// agent-controlled keys (intrinsics, agent attributes) and skips them.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Attributes(Vec<(Cow<'static, str>, AttributeValue)>);

impl Attributes {
    pub(crate) fn with_capacity(n: usize) -> Self {
        Self(Vec::with_capacity(n))
    }

    /// Appends without limit checks. Callers own key uniqueness.
    pub(crate) fn push(
        &mut self,
        key: impl Into<Cow<'static, str>>,
        value: impl Into<AttributeValue>,
    ) {
        self.0.push((key.into(), value.into()));
    }

    /// Inserts or replaces a user attribute, enforcing key/value/count limits.
    pub(crate) fn insert(&mut self, key: Cow<'static, str>, value: AttributeValue) {
        if key.len() > MAX_ATTR_BYTES {
            return;
        }
        let value = match value {
            AttributeValue::String(mut s) => {
                let keep = truncate_utf8(&s, MAX_ATTR_BYTES).len();
                s.truncate(keep);
                AttributeValue::String(s)
            }
            other => other,
        };
        if let Some(slot) = self.0.iter_mut().find(|(k, _)| *k == key) {
            slot.1 = value;
        } else if self.0.len() < MAX_USER_ATTRS {
            self.0.push((key, value));
        }
    }

    /// Inserts or replaces without limits (agent attributes).
    pub(crate) fn set(&mut self, key: &'static str, value: impl Into<AttributeValue>) {
        let value = value.into();
        if let Some(slot) = self.0.iter_mut().find(|(k, _)| *k == key) {
            slot.1 = value;
        } else {
            self.0.push((Cow::Borrowed(key), value));
        }
    }

    #[cfg(test)]
    pub(crate) fn get(&self, key: &str) -> Option<&AttributeValue> {
        self.0.iter().find(|(k, _)| *k == key).map(|(_, v)| v)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn extend_from(&mut self, other: &Self) {
        for (k, v) in &other.0 {
            if let Some(slot) = self.0.iter_mut().find(|(key, _)| key == k) {
                slot.1 = v.clone();
            } else {
                self.0.push((k.clone(), v.clone()));
            }
        }
    }
}

impl serde::Serialize for Attributes {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut map = s.serialize_map(Some(self.0.len()))?;
        for (k, v) in &self.0 {
            map.serialize_entry(k.as_ref(), v)?;
        }
        map.end()
    }
}

/// Longest prefix of `s` that is at most `max_bytes` long and ends on a char boundary.
pub(crate) fn truncate_utf8(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let end = s
        .char_indices()
        .map(|(i, _)| i)
        .take_while(|i| *i <= max_bytes)
        .last()
        .unwrap_or(0);
    &s[..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_utf8_stops_before_split_multibyte_char() {
        // "é" is two bytes; a 3-byte limit must not split the second "é".
        assert_eq!(truncate_utf8("éé", 3), "é");
    }

    #[test]
    fn truncate_utf8_returns_input_when_short_enough() {
        assert_eq!(truncate_utf8("abc", 3), "abc");
    }

    #[test]
    fn insert_replaces_existing_key() {
        let mut a = Attributes::default();
        a.insert("k".into(), 1.into());
        a.insert("k".into(), 2.into());
        assert_eq!(
            serde_json::to_value(&a).unwrap(),
            serde_json::json!({"k": 2})
        );
    }

    #[test]
    fn insert_drops_attributes_beyond_limit() {
        let mut a = Attributes::default();
        for i in 0..=MAX_USER_ATTRS {
            a.insert(format!("k{i}").into(), true.into());
        }
        assert!(a.get(&format!("k{MAX_USER_ATTRS}")).is_none());
    }

    #[test]
    fn insert_truncates_long_string_values() {
        let mut a = Attributes::default();
        a.insert("k".into(), "x".repeat(300).into());
        assert_eq!(a.get("k"), Some(&AttributeValue::String("x".repeat(255))));
    }

    #[test]
    fn u64_beyond_i64_becomes_float() {
        assert!(matches!(
            AttributeValue::from(u64::MAX),
            AttributeValue::Float(_)
        ));
    }
}
