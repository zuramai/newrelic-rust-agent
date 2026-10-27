//! Agent configuration and builder.

use crate::{Agent, Error};

const ENV_LICENSE: &str = "NEW_RELIC_LICENSE_KEY";
const ENV_APP_NAME: &str = "NEW_RELIC_APP_NAME";
const ENV_HOST: &str = "NEW_RELIC_HOST";
const ENV_ENABLED: &str = "NEW_RELIC_ENABLED";

/// Builder for [`Agent`]. Unset fields fall back to environment variables.
///
/// | Setter | Environment variable | Default |
/// |---|---|---|
/// | [`license_key`](Self::license_key) | `NEW_RELIC_LICENSE_KEY` | required |
/// | [`app_name`](Self::app_name) | `NEW_RELIC_APP_NAME` | required |
/// | [`host`](Self::host) | `NEW_RELIC_HOST` | derived from the license region |
/// | [`enabled`](Self::enabled) | `NEW_RELIC_ENABLED` | `true` |
#[derive(Debug, Default, Clone)]
#[must_use]
pub struct AgentBuilder {
    license_key: Option<String>,
    app_name: Option<String>,
    host: Option<String>,
    enabled: Option<bool>,
    /// Overrides the 60 s metric harvest period (crate tests only).
    slow_harvest_period: Option<std::time::Duration>,
}

impl AgentBuilder {
    /// Sets the 40-character New Relic ingest license key.
    pub fn license_key(mut self, key: impl Into<String>) -> Self {
        self.license_key = Some(key.into());
        self
    }

    /// Sets the application name. Up to three names separated by `;` roll data up into
    /// several applications; the first is the primary one.
    pub fn app_name(mut self, name: impl Into<String>) -> Self {
        self.app_name = Some(name.into());
        self
    }

    /// Overrides the collector host.
    ///
    /// A bare host (`collector.eu01.nr-data.net`) is used over HTTPS and may still be redirected
    /// by the collector. A full URL (`http://127.0.0.1:8080`) is used verbatim and never
    /// redirected, which is meant for proxies and tests.
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    #[cfg(test)]
    pub(crate) fn slow_harvest_period(mut self, period: std::time::Duration) -> Self {
        self.slow_harvest_period = Some(period);
        self
    }

    /// Enables or disables the agent. A disabled agent validates nothing and records nothing.
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = Some(enabled);
        self
    }

    /// Validates the configuration and starts the background harvester.
    ///
    /// Connecting to New Relic happens asynchronously; use
    /// [`Agent::wait_for_connection`] to block on it.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] when the license key or app name is missing or malformed, when an
    /// environment variable is invalid, or when called outside a Tokio runtime.
    pub fn build(self) -> Result<Agent, Error> {
        let enabled = match self.enabled {
            Some(v) => v,
            None => match std::env::var(ENV_ENABLED) {
                Ok(v) => parse_bool(&v).ok_or(Error::InvalidEnv {
                    var: ENV_ENABLED,
                    value: v,
                })?,
                Err(_) => true,
            },
        };
        if !enabled {
            return Ok(Agent::disabled());
        }
        let license = self
            .license_key
            .or_else(|| std::env::var(ENV_LICENSE).ok())
            .ok_or(Error::MissingLicenseKey)?;
        let license = license.trim().to_owned();
        if license.len() != 40 {
            return Err(Error::InvalidLicenseKey(license.len()));
        }
        let identifier = self
            .app_name
            .or_else(|| std::env::var(ENV_APP_NAME).ok())
            .map(|n| n.trim().to_owned())
            .filter(|n| !n.is_empty())
            .ok_or(Error::MissingAppName)?;
        let app_names: Vec<String> = identifier
            .split(';')
            .map(str::trim)
            .filter(|n| !n.is_empty())
            .map(str::to_owned)
            .collect();
        if app_names.is_empty() {
            return Err(Error::MissingAppName);
        }
        if app_names.len() > 3 {
            return Err(Error::TooManyAppNames(app_names.len()));
        }
        let host = self
            .host
            .or_else(|| std::env::var(ENV_HOST).ok())
            .map(|h| h.trim().to_owned())
            .filter(|h| !h.is_empty());
        let handle = tokio::runtime::Handle::try_current().map_err(|_| Error::NoTokioRuntime)?;
        Ok(Agent::start(
            Config {
                license_key: license,
                app_names,
                identifier,
                host,
            },
            &handle,
            self.slow_harvest_period
                .unwrap_or(crate::agent::SLOW_HARVEST_PERIOD),
        ))
    }
}

fn parse_bool(v: &str) -> Option<bool> {
    match v.trim().to_ascii_lowercase().as_str() {
        "true" | "1" => Some(true),
        "false" | "0" => Some(false),
        _ => None,
    }
}

/// Validated configuration.
#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub(crate) license_key: String,
    pub(crate) app_names: Vec<String>,
    /// The full, unsplit application name.
    pub(crate) identifier: String,
    pub(crate) host: Option<String>,
}

impl Config {
    /// Base URL for `preconnect` and whether it is explicit (never redirected).
    pub(crate) fn collector_base(&self) -> (String, bool) {
        match &self.host {
            Some(h) if h.contains("://") => (h.trim_end_matches('/').to_owned(), true),
            Some(h) => (format!("https://{h}"), false),
            None => match self.license_key.find('x') {
                Some(idx) if idx >= 1 => (
                    format!("https://collector.{}.nr-data.net", &self.license_key[..idx]),
                    false,
                ),
                _ => ("https://collector.newrelic.com".to_owned(), false),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(license: &str, host: Option<&str>) -> Config {
        Config {
            license_key: license.to_owned(),
            app_names: vec!["a".to_owned()],
            identifier: "a".to_owned(),
            host: host.map(str::to_owned),
        }
    }

    #[test]
    fn collector_base_uses_region_prefix_when_license_has_x() {
        let c = config(&format!("eu01xx{}", "a".repeat(34)), None);
        assert_eq!(
            c.collector_base(),
            ("https://collector.eu01.nr-data.net".to_owned(), false)
        );
    }

    #[test]
    fn collector_base_defaults_to_us_when_license_is_hex() {
        let c = config(&"a".repeat(40), None);
        assert_eq!(
            c.collector_base(),
            ("https://collector.newrelic.com".to_owned(), false)
        );
    }

    #[test]
    fn collector_base_is_explicit_when_host_has_scheme() {
        let c = config(&"a".repeat(40), Some("http://127.0.0.1:1234/"));
        assert_eq!(
            c.collector_base(),
            ("http://127.0.0.1:1234".to_owned(), true)
        );
    }

    #[test]
    fn collector_base_uses_https_for_bare_host() {
        let c = config(&"a".repeat(40), Some("collector.example.com"));
        assert_eq!(
            c.collector_base(),
            ("https://collector.example.com".to_owned(), false)
        );
    }

    #[test]
    fn build_rejects_short_license_key() {
        let err = AgentBuilder::default()
            .license_key("abc")
            .app_name("x")
            .build()
            .unwrap_err();
        assert!(matches!(err, Error::InvalidLicenseKey(3)));
    }

    #[test]
    fn build_rejects_empty_app_name() {
        let err = AgentBuilder::default()
            .license_key("a".repeat(40))
            .app_name("  ")
            .build()
            .unwrap_err();
        assert!(matches!(err, Error::MissingAppName));
    }

    #[test]
    fn build_rejects_more_than_three_app_names() {
        let err = AgentBuilder::default()
            .license_key("a".repeat(40))
            .app_name("a;b;c;d")
            .build()
            .unwrap_err();
        assert!(matches!(err, Error::TooManyAppNames(4)));
    }

    #[test]
    fn build_requires_tokio_runtime() {
        let err = AgentBuilder::default()
            .license_key("a".repeat(40))
            .app_name("a")
            .build()
            .unwrap_err();
        assert!(matches!(err, Error::NoTokioRuntime));
    }

    #[test]
    fn build_returns_disabled_agent_without_validation_when_disabled() {
        let agent = AgentBuilder::default().enabled(false).build().unwrap();
        assert!(!agent.is_enabled());
    }

    #[test]
    fn parse_bool_accepts_case_insensitive_words_and_digits() {
        assert_eq!(parse_bool("TRUE"), Some(true));
        assert_eq!(parse_bool("0"), Some(false));
        assert_eq!(parse_bool("yes"), None);
    }
}
