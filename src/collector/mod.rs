//! HTTP client for New Relic's collector (protocol v17).

pub(crate) mod connect;

use std::collections::HashMap;
use std::io::Write as _;
use std::time::Duration;

use flate2::Compression;
use flate2::write::GzEncoder;

pub(crate) const PROTOCOL_VERSION: &str = "17";
const USER_AGENT: &str = concat!("NewRelic-Rust-Agent/", env!("CARGO_PKG_VERSION"));
pub(crate) const DEFAULT_MAX_PAYLOAD: usize = 1_000_000;

/// What the runtime should do after a collector call.
#[derive(Debug)]
pub(crate) enum Outcome {
    /// Accepted; carries the response body.
    Ok(Vec<u8>),
    /// Transient failure: keep the data for the next harvest.
    Retry,
    /// The run is no longer valid: reconnect.
    Reconnect,
    /// The collector asked the agent to stop for good.
    Shutdown,
    /// Data rejected or too large: drop it.
    Discard,
}

impl Outcome {
    fn from_status(status: u16, body: Vec<u8>) -> Self {
        match status {
            200 | 202 => Self::Ok(body),
            401 | 409 => Self::Reconnect,
            410 => Self::Shutdown,
            408 | 429 | 500 | 503 => Self::Retry,
            _ => Self::Discard,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CollectorError {
    #[error("HTTP client error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("compression failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("connect reply has no agent_run_id")]
    MissingRunId,
    #[error("collector returned {0:?}")]
    Rejected(&'static str),
}

/// One collector call.
pub(crate) struct Call<'a> {
    pub(crate) base: &'a str,
    pub(crate) method: &'static str,
    pub(crate) run_id: Option<&'a str>,
    pub(crate) headers: &'a HashMap<String, String>,
    pub(crate) max_payload: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct Collector {
    http: reqwest::Client,
    license: String,
}

impl Collector {
    pub(crate) fn new(license: String) -> Result<Self, CollectorError> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .user_agent(USER_AGENT)
            .build()?;
        Ok(Self { http, license })
    }

    /// Sends a JSON payload. Network errors map to [`Outcome::Retry`].
    pub(crate) async fn invoke(
        &self,
        call: &Call<'_>,
        json: &[u8],
    ) -> Result<Outcome, CollectorError> {
        let mut gz = GzEncoder::new(
            Vec::with_capacity(json.len() / 4 + 64),
            Compression::default(),
        );
        gz.write_all(json)?;
        let body = gz.finish()?;
        if body.len() > call.max_payload {
            tracing::warn!(
                target: "newrelic_rust_agent",
                method = call.method,
                size = body.len(),
                max = call.max_payload,
                "payload exceeds max_payload_size_in_bytes; discarding"
            );
            return Ok(Outcome::Discard);
        }
        let mut url =
            reqwest::Url::parse(&format!("{}/agent_listener/invoke_raw_method", call.base))
                .map_err(|_| CollectorError::Rejected("invalid collector URL"))?;
        {
            let mut q = url.query_pairs_mut();
            q.append_pair("license_key", &self.license)
                .append_pair("marshal_format", "json")
                .append_pair("method", call.method)
                .append_pair("protocol_version", PROTOCOL_VERSION);
            if let Some(run_id) = call.run_id {
                q.append_pair("run_id", run_id);
            }
        }
        let mut req = self
            .http
            .post(url)
            .header(http::header::CONTENT_TYPE, "application/octet-stream")
            .header(http::header::CONTENT_ENCODING, "gzip")
            .header(http::header::ACCEPT_ENCODING, "identity, deflate");
        for (k, v) in call.headers {
            req = req.header(k.as_str(), v.as_str());
        }
        let resp = match req.body(body).send().await {
            Ok(r) => r,
            Err(e) => {
                // `without_url`: the URL carries the license key and must never be logged.
                let e = e.without_url();
                tracing::warn!(target: "newrelic_rust_agent", method = call.method, error = %e, "collector request failed");
                return Ok(Outcome::Retry);
            }
        };
        let status = resp.status().as_u16();
        let body = match resp.bytes().await {
            Ok(b) => b.to_vec(),
            Err(e) => {
                let e = e.without_url();
                tracing::warn!(target: "newrelic_rust_agent", method = call.method, error = %e, "reading collector response failed");
                return Ok(Outcome::Retry);
            }
        };
        if matches!(status, 200 | 202) {
            // connect replies are large and contain keys; only log their size.
            if call.method == "connect" {
                tracing::debug!(target: "newrelic_rust_agent", method = call.method, status, bytes = body.len(), "collector accepted request");
            } else {
                tracing::debug!(
                    target: "newrelic_rust_agent",
                    method = call.method,
                    status,
                    body = %String::from_utf8_lossy(&body),
                    "collector accepted request"
                );
            }
        } else {
            tracing::warn!(
                target: "newrelic_rust_agent",
                method = call.method,
                status,
                body = %String::from_utf8_lossy(&body),
                "collector rejected request"
            );
        }
        Ok(Outcome::from_status(status, body))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_status_maps_collector_codes() {
        assert!(matches!(Outcome::from_status(202, vec![]), Outcome::Ok(_)));
        assert!(matches!(
            Outcome::from_status(401, vec![]),
            Outcome::Reconnect
        ));
        assert!(matches!(
            Outcome::from_status(409, vec![]),
            Outcome::Reconnect
        ));
        assert!(matches!(
            Outcome::from_status(410, vec![]),
            Outcome::Shutdown
        ));
        assert!(matches!(Outcome::from_status(429, vec![]), Outcome::Retry));
        assert!(matches!(
            Outcome::from_status(413, vec![]),
            Outcome::Discard
        ));
    }
}
