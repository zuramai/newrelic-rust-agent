//! The [`Agent`] handle.

use std::borrow::Cow;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};
use std::time::{Duration, Instant, SystemTime};

use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::attributes::{AttributeValue, Attributes, MAX_ATTR_BYTES};
use crate::collector::connect::RunConfig;
use crate::config::Config;
use crate::dt::sampler::AdaptiveSampler;
use crate::harvest::Harvest;
use crate::harvest::metrics::{MetricData, MetricKey};
use crate::harvest::payload::Event;
use crate::ids::Priority;
use crate::transaction::{LogRecord, Transaction, TransactionKind, log_event, unix_ms};
use crate::{AgentBuilder, Error};

/// Locks a mutex, recovering the data if another thread panicked while holding it.
pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Default interval of the metric / traced-error / transaction-trace harvest.
pub(crate) const SLOW_HARVEST_PERIOD: Duration = Duration::from_secs(60);

pub(crate) struct AgentInner {
    pub(crate) config: Config,
    run: RwLock<Option<Arc<RunConfig>>>,
    default_run: Arc<RunConfig>,
    pub(crate) sampler: Mutex<AdaptiveSampler>,
    pub(crate) harvest: Mutex<Harvest>,
    pub(crate) hostname: String,
    pub(crate) slow_period: Duration,
    shutdown: watch::Sender<bool>,
    connected: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl AgentInner {
    /// The current run configuration, or the pre-connect placeholder.
    pub(crate) fn run_snapshot(&self) -> Arc<RunConfig> {
        self.run
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .unwrap_or_else(|| Arc::clone(&self.default_run))
    }

    pub(crate) fn connected_run(&self) -> Option<Arc<RunConfig>> {
        self.run
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Installs a new run: sampler and reservoir caps follow the connect reply.
    pub(crate) fn install_run(&self, run: Arc<RunConfig>) {
        *lock(&self.sampler) = AdaptiveSampler::new(
            run.sampling_target,
            run.sampling_target_period,
            Instant::now(),
        );
        lock(&self.harvest).apply_limits(&run.limits);
        *self.run.write().unwrap_or_else(PoisonError::into_inner) = Some(run);
        self.connected.send_replace(true);
    }

    /// Drops the run (reconnect or shutdown); nothing is sampled until the next connect.
    pub(crate) fn clear_run(&self) {
        *self.run.write().unwrap_or_else(PoisonError::into_inner) = None;
        *lock(&self.sampler) = AdaptiveSampler::new(0, Duration::from_secs(60), Instant::now());
        self.connected.send_replace(false);
    }
}

/// Handle to a running New Relic agent. Cheap to clone.
///
/// Build it once at startup with [`Agent::from_env`] or [`Agent::builder`], inside a Tokio
/// runtime. A background task connects to New Relic and harvests data every minute; call
/// [`Agent::shutdown`] before exiting so buffered data is sent.
#[derive(Clone)]
pub struct Agent {
    inner: Option<Arc<AgentInner>>,
}

impl fmt::Debug for Agent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Agent")
            .field("enabled", &self.is_enabled())
            .field("connected", &self.is_connected())
            .finish()
    }
}

impl Agent {
    /// Starts configuring an agent.
    pub fn builder() -> AgentBuilder {
        AgentBuilder::default()
    }

    /// Builds an agent purely from `NEW_RELIC_*` environment variables.
    ///
    /// # Errors
    ///
    /// See [`AgentBuilder::build`].
    pub fn from_env() -> Result<Self, Error> {
        Self::builder().build()
    }

    /// An agent that records nothing; every transaction it starts is a no-op.
    #[must_use]
    pub fn disabled() -> Self {
        Self { inner: None }
    }

    pub(crate) fn start(
        config: Config,
        handle: &tokio::runtime::Handle,
        slow_period: Duration,
    ) -> Self {
        let (shutdown, shutdown_rx) = watch::channel(false);
        let inner = Arc::new(AgentInner {
            config,
            run: RwLock::new(None),
            default_run: Arc::new(RunConfig::default()),
            sampler: Mutex::new(AdaptiveSampler::new(
                0,
                Duration::from_secs(60),
                Instant::now(),
            )),
            harvest: Mutex::new(Harvest::new(SystemTime::now())),
            hostname: gethostname::gethostname()
                .into_string()
                .unwrap_or_else(|_| "unknown".to_owned()),
            slow_period,
            shutdown,
            connected: watch::channel(false).0,
            task: Mutex::new(None),
        });
        let task = handle.spawn(crate::runtime::run(Arc::clone(&inner), shutdown_rx));
        *lock(&inner.task) = Some(task);
        Self { inner: Some(inner) }
    }

    /// Whether this agent records data (false for [`Agent::disabled`]).
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    /// Starts a transaction. Prefer the tower layer or the tracing layer, which do this for
    /// you; use this for work they do not cover.
    #[must_use = "dropping the last handle ends the transaction immediately"]
    pub fn start_transaction(
        &self,
        name: impl Into<Cow<'static, str>>,
        kind: TransactionKind,
    ) -> Transaction {
        match &self.inner {
            Some(inner) => Transaction::new(Arc::clone(inner), name.into(), kind),
            None => Transaction::noop(),
        }
    }

    /// Records a custom event queryable with NRQL (`SELECT * FROM {event_type}`).
    ///
    /// At most 64 attributes are kept; string values are truncated to 255 bytes.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidEventType`] if `event_type` is empty, longer than 255 bytes, or contains
    /// characters other than `[a-zA-Z0-9:_ ]`.
    pub fn record_custom_event<K, V>(
        &self,
        event_type: &str,
        attrs: impl IntoIterator<Item = (K, V)>,
    ) -> Result<(), Error>
    where
        K: Into<Cow<'static, str>>,
        V: Into<AttributeValue>,
    {
        let Some(inner) = &self.inner else {
            return Ok(());
        };
        let valid = !event_type.is_empty()
            && event_type.len() <= MAX_ATTR_BYTES
            && event_type
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, ':' | '_' | ' '));
        if !valid {
            return Err(Error::InvalidEventType(event_type.to_owned()));
        }
        let mut intrinsics = Attributes::with_capacity(2);
        intrinsics.push("type", event_type.to_owned());
        intrinsics.push("timestamp", unix_ms(SystemTime::now()));
        let mut user = Attributes::default();
        for (k, v) in attrs {
            user.insert(k.into(), v.into());
        }
        let event = Event {
            intrinsics,
            user,
            agent: Attributes::default(),
        };
        lock(&inner.harvest).push_custom_event(Priority::random(), event);
        Ok(())
    }

    /// Records a custom metric `Custom/{name}`, aggregated per harvest.
    pub fn record_custom_metric(&self, name: &str, value: f64) {
        let Some(inner) = &self.inner else { return };
        let key = MetricKey::unscoped(format!("Custom/{name}"));
        lock(&inner.harvest).add_metric(key, MetricData::value(value), false);
    }

    /// Forwards a log record not linked to any transaction.
    pub fn record_log(&self, record: LogRecord) {
        let Some(inner) = &self.inner else { return };
        let event = log_event(record, None, None);
        lock(&inner.harvest).push_log(Priority::random(), event);
    }

    /// Whether the agent currently holds a valid connection (agent run) with New Relic.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.inner
            .as_ref()
            .is_some_and(|i| i.connected_run().is_some())
    }

    /// Waits until the agent has connected, up to `timeout`. Returns whether it is connected.
    ///
    /// Useful for short-lived jobs: transactions finished before connecting are still
    /// reported, but distributed-trace headers are only produced once connected.
    pub async fn wait_for_connection(&self, timeout: Duration) -> bool {
        let Some(inner) = &self.inner else {
            return false;
        };
        let mut rx = inner.connected.subscribe();
        tokio::time::timeout(timeout, rx.wait_for(|c| *c))
            .await
            .is_ok_and(|r| r.is_ok())
    }

    /// Sends buffered data and stops the background task, waiting at most `timeout`.
    /// Idempotent; data recorded afterwards is never sent.
    pub async fn shutdown(&self, timeout: Duration) {
        let Some(inner) = &self.inner else { return };
        inner.shutdown.send_replace(true);
        let task = lock(&inner.task).take();
        if let Some(mut task) = task
            && tokio::time::timeout(timeout, &mut task).await.is_err()
        {
            task.abort();
            tracing::warn!(target: "newrelic_rust_agent", "shutdown timed out; buffered data dropped");
        }
    }
}
