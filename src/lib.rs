//! Native [New Relic](https://newrelic.com) APM agent for Rust.
//!
//! The agent speaks New Relic's collector protocol (v17) directly, the same way the official
//! Go/Java/Python agents do, so a Rust service shows up as a real APM entity: the Transactions
//! tab, Apdex, error rate, the Errors inbox, transaction traces, service maps, logs-in-context
//! and distributed tracing all populate.
//!
//! # Quick start
//!
//! ```no_run
//! use newrelic_rust_agent::{Agent, NewRelicLayer};
//! use std::time::Duration;
//! use tracing_subscriber::layer::SubscriberExt as _;
//! use tracing_subscriber::util::SubscriberInitExt as _;
//!
//! # async fn run() -> Result<(), newrelic_rust_agent::Error> {
//! // Reads NEW_RELIC_LICENSE_KEY and NEW_RELIC_APP_NAME.
//! let agent = Agent::from_env()?;
//! tracing_subscriber::registry()
//!     .with(NewRelicLayer::new(agent.clone()))
//!     .init();
//!
//! // Spans created inside a transaction become segments; a span with the
//! // `newrelic.transaction` field starts a transaction.
//! nightly_job().await;
//!
//! agent.shutdown(Duration::from_secs(10)).await;
//! # Ok(())
//! # }
//!
//! #[tracing::instrument(fields(newrelic.transaction = "other"))]
//! async fn nightly_job() {
//!     load_rows().await;
//! }
//!
//! #[tracing::instrument]
//! async fn load_rows() {}
//! ```
//!
//! See the README for the tower/axum/tonic server layer, the reqwest client middleware and the
//! explicit [`Transaction`]/[`Segment`] API.
#![cfg_attr(docsrs, feature(doc_cfg))]

mod agent;
mod attributes;
mod collector;
mod config;
mod dt;
mod error;
mod harvest;
mod ids;
mod integrations;
mod runtime;
mod transaction;

pub use agent::Agent;
pub use attributes::AttributeValue;
pub use config::AgentBuilder;
pub use dt::TransportType;
pub use error::Error;
pub use transaction::{
    ExternalSegment, LogRecord, NoticedError, Segment, SegmentContext, Transaction, TransactionKind,
};

#[cfg(feature = "tracing")]
#[cfg_attr(docsrs, doc(cfg(feature = "tracing")))]
pub use integrations::tracing::{NewRelicLayer, NewRelicSpanExt};

#[cfg(feature = "tower")]
#[cfg_attr(docsrs, doc(cfg(feature = "tower")))]
pub use integrations::tower::{
    ResponseFuture, TransactionBody, TransactionLayer, TransactionService,
};

#[cfg(feature = "reqwest")]
#[cfg_attr(docsrs, doc(cfg(feature = "reqwest")))]
pub use integrations::reqwest::NewRelicMiddleware;
