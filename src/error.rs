//! Public error type.

/// Errors returned by the agent's public API.
///
/// Telemetry delivery never fails the caller: collector/network problems are logged through
/// `tracing` (target `newrelic_rust_agent`) and retried in the background. These variants only
/// cover configuration and input validation.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// No license key was configured and `NEW_RELIC_LICENSE_KEY` is unset.
    #[error("NEW_RELIC_LICENSE_KEY is not set")]
    MissingLicenseKey,
    /// The license key does not have the required 40 characters.
    #[error("license key must be 40 characters, got {0}")]
    InvalidLicenseKey(usize),
    /// No application name was configured and `NEW_RELIC_APP_NAME` is unset or empty.
    #[error("app name is not set; call AgentBuilder::app_name or set NEW_RELIC_APP_NAME")]
    MissingAppName,
    /// More than three `;`-separated application names were given.
    #[error("at most 3 app names separated by ';' are allowed, got {0}")]
    TooManyAppNames(usize),
    /// An environment variable holds a value that cannot be parsed.
    #[error("invalid value {value:?} for {var}")]
    InvalidEnv {
        /// Name of the environment variable.
        var: &'static str,
        /// The rejected value.
        value: String,
    },
    /// [`AgentBuilder::build`](crate::AgentBuilder::build) was called outside a Tokio runtime.
    #[error("Agent must be built inside a Tokio runtime")]
    NoTokioRuntime,
    /// A custom event type contains characters other than `[a-zA-Z0-9:_ ]` or is too long.
    #[error("invalid custom event type {0:?}: must match [a-zA-Z0-9:_ ]+ and be at most 255 bytes")]
    InvalidEventType(String),
}
