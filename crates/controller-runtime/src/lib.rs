//! Typed reconciliation primitives and etcd-backed leader election for Rusternetes controllers.
//!
//! The runtime owns queueing and leadership lifecycle only. Resource controllers must re-read
//! state in `reconcile`; queue events are hints and not durable commands.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use rusternetes_common::ApiError;
use tokio::{
    sync::{watch, Mutex, Notify},
    time::{sleep, Instant},
};

/// A closed Kubernetes resource identity scheduled for reconciliation.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ReconcileKey {
    pub group: String,
    pub version: String,
    pub resource: String,
    pub namespace: Option<String>,
    pub name: String,
}

impl ReconcileKey {
    pub fn core(
        resource: impl Into<String>,
        namespace: Option<impl Into<String>>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            group: String::new(),
            version: "v1".to_owned(),
            resource: resource.into(),
            namespace: namespace.map(Into::into),
            name: name.into(),
        }
    }
}

/// Explicit reconciliation completion or requeue decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconcileResult {
    Done,
    Requeue,
    RequeueAfter(Duration),
}

/// A cancellation-aware typed control loop implementation.
pub trait Reconciler: Send + Sync {
    fn reconcile<'a>(
        &'a self,
        key: ReconcileKey,
        cancelled: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<ReconcileResult, ApiError>> + Send + 'a>>;
}

#[derive(Default)]
struct QueueState {
    pending: VecDeque<ReconcileKey>,
    queued: HashSet<ReconcileKey>,
    retries: HashMap<ReconcileKey, u32>,
    closed: bool,
}

/// Bounded, deduplicating work queue for reconciliation keys.
pub struct WorkQueue {
    capacity: usize,
    retry_base: Duration,
    retry_max: Duration,
    state: Mutex<QueueState>,
    ready: Notify,
}

impl WorkQueue {
    pub fn new(
        capacity: usize,
        retry_base: Duration,
        retry_max: Duration,
    ) -> Result<Self, ApiError> {
        if capacity == 0 || retry_base.is_zero() || retry_max < retry_base {
            return Err(ApiError::BadRequest {
                message: "controller queue requires non-zero capacity and ordered retry durations"
                    .to_owned(),
            });
        }
        Ok(Self {
            capacity,
            retry_base,
            retry_max,
            state: Mutex::new(QueueState::default()),
            ready: Notify::new(),
        })
    }

    /// Enqueues a key once while it is pending; a full queue fails closed instead of growing.
    pub async fn enqueue(&self, key: ReconcileKey) -> Result<bool, ApiError> {
        let mut state = self.state.lock().await;
        if state.closed {
            return Err(ApiError::Internal);
        }
        if state.queued.contains(&key) {
            return Ok(false);
        }
        if state.pending.len() >= self.capacity {
            return Err(ApiError::Internal);
        }
        state.queued.insert(key.clone());
        state.pending.push_back(key);
        drop(state);
        self.ready.notify_one();
        Ok(true)
    }

    /// Waits for one key or returns `None` once shutdown is requested and the queue drains.
    pub async fn pop(&self, cancelled: &mut watch::Receiver<bool>) -> Option<ReconcileKey> {
        loop {
            let notified = self.ready.notified();
            {
                let mut state = self.state.lock().await;
                if let Some(key) = state.pending.pop_front() {
                    state.queued.remove(&key);
                    return Some(key);
                }
                if state.closed || *cancelled.borrow() {
                    return None;
                }
            }
            tokio::select! {
                _ = notified => {}
                changed = cancelled.changed() => {
                    if changed.is_err() || *cancelled.borrow() { return None; }
                }
            }
        }
    }

    pub async fn complete(&self, key: &ReconcileKey) {
        self.state.lock().await.retries.remove(key);
    }

    /// Increments the retry budget and returns a capped exponential retry interval.
    pub async fn next_retry_delay(&self, key: &ReconcileKey) -> Duration {
        let mut state = self.state.lock().await;
        let attempts = state.retries.entry(key.clone()).or_insert(0);
        *attempts = attempts.saturating_add(1);
        self.retry_base
            .checked_mul(2_u32.saturating_pow((*attempts).saturating_sub(1)))
            .unwrap_or(self.retry_max)
            .min(self.retry_max)
    }

    pub async fn close(&self) {
        self.state.lock().await.closed = true;
        self.ready.notify_waiters();
    }
}

/// Runs one reconciliation worker until leadership cancellation or queue shutdown.
pub async fn run_worker(
    queue: Arc<WorkQueue>,
    reconciler: Arc<dyn Reconciler>,
    cancellation: watch::Receiver<bool>,
) {
    let mut cancellation = cancellation;
    while let Some(key) = queue.pop(&mut cancellation).await {
        let result = reconciler
            .reconcile(key.clone(), cancellation.clone())
            .await;
        if *cancellation.borrow() {
            return;
        }
        match result {
            Ok(ReconcileResult::Done) => queue.complete(&key).await,
            Ok(ReconcileResult::Requeue) => {
                let _ = queue.enqueue(key).await;
            }
            Ok(ReconcileResult::RequeueAfter(delay)) => {
                let queue = queue.clone();
                tokio::spawn(async move {
                    sleep(delay).await;
                    let _ = queue.enqueue(key).await;
                });
            }
            Err(_) => {
                let delay = queue.next_retry_delay(&key).await;
                let queue = queue.clone();
                tokio::spawn(async move {
                    sleep(delay).await;
                    let _ = queue.enqueue(key).await;
                });
            }
        }
    }
}

/// A monotonic source suitable for leader-election deadlines and deterministic test substitution.
pub trait Clock: Send + Sync {
    fn now(&self) -> Instant;
}

#[derive(Clone, Debug, Default)]
pub struct TokioClock;

impl Clock for TokioClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn queue_deduplicates_keys_and_releases_after_pop() {
        let queue = WorkQueue::new(2, Duration::from_millis(1), Duration::from_secs(1))
            .expect("queue config valid");
        let key = ReconcileKey::core("pods", Some("default"), "web");
        assert!(queue.enqueue(key.clone()).await.expect("first enqueue"));
        assert!(!queue
            .enqueue(key.clone())
            .await
            .expect("duplicate coalesces"));
        let (sender, mut receiver) = watch::channel(false);
        assert_eq!(queue.pop(&mut receiver).await, Some(key.clone()));
        assert!(queue
            .enqueue(key)
            .await
            .expect("popped key can be requeued"));
        drop(sender);
    }

    #[tokio::test]
    async fn queue_retry_backoff_is_capped() {
        let queue = WorkQueue::new(1, Duration::from_secs(1), Duration::from_secs(4))
            .expect("queue config valid");
        let key = ReconcileKey::core("pods", Some("default"), "web");
        assert_eq!(queue.next_retry_delay(&key).await, Duration::from_secs(1));
        assert_eq!(queue.next_retry_delay(&key).await, Duration::from_secs(2));
        assert_eq!(queue.next_retry_delay(&key).await, Duration::from_secs(4));
        assert_eq!(queue.next_retry_delay(&key).await, Duration::from_secs(4));
    }
}

/// Durable election state persisted under a reserved etcd key.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ElectionRecord {
    pub holder_identity: String,
    pub lease_duration_seconds: u64,
    pub acquire_time_unix_millis: i128,
    pub renew_time_unix_millis: i128,
    pub lease_transitions: u64,
}

impl ElectionRecord {
    fn is_expired(&self, now_unix_millis: i128) -> bool {
        now_unix_millis
            > self.renew_time_unix_millis
                + i128::from(self.lease_duration_seconds).saturating_mul(1_000)
    }
}

/// A stored election record paired with etcd's compare-and-swap revision.
#[derive(Clone, Debug)]
pub struct ObservedElectionRecord {
    pub record: ElectionRecord,
    pub mod_revision: i64,
}

/// Result of one lease acquisition or renewal attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Leadership {
    Acquired,
    Renewed,
    Standby { holder_identity: String },
    Contended,
}

/// Etcd-backed lease record store. `etcd-client` provides the gRPC transport and transactional KV
/// primitives; this type maps them to controller leadership semantics only.
pub struct EtcdLeaseStore {
    client: Arc<Mutex<etcd_client::Client>>,
    key: String,
}

impl EtcdLeaseStore {
    pub async fn connect(
        endpoints: impl IntoIterator<Item = impl AsRef<str>>,
        prefix: &str,
        name: &str,
    ) -> Result<Self, ApiError> {
        if !prefix.starts_with('/')
            || prefix.ends_with('/')
            || prefix.contains('\0')
            || name.is_empty()
            || name.contains('/')
        {
            return Err(ApiError::BadRequest {
                message: "leader-election prefix/name is unsafe for etcd key layout".to_owned(),
            });
        }
        let client = etcd_client::Client::connect(
            endpoints
                .into_iter()
                .map(|endpoint| endpoint.as_ref().to_owned())
                .collect::<Vec<_>>(),
            None,
        )
        .await
        .map_err(|_| ApiError::Internal)?;
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            key: format!("{prefix}/leader-election/{name}"),
        })
    }

    pub async fn observe(&self) -> Result<Option<ObservedElectionRecord>, ApiError> {
        let response = self
            .client
            .lock()
            .await
            .get(self.key.clone(), None)
            .await
            .map_err(|_| ApiError::Internal)?;
        let Some(value) = response.kvs().first() else {
            return Ok(None);
        };
        let record = serde_json::from_slice(value.value()).map_err(|_| ApiError::Internal)?;
        Ok(Some(ObservedElectionRecord {
            record,
            mod_revision: value.mod_revision(),
        }))
    }

    /// Atomically creates, renews, or takes over an expired record. A foreign unexpired record is
    /// read-only standby state; a transaction loss is reported as contention and must be retried.
    pub async fn acquire_or_renew(
        &self,
        identity: &str,
        lease_duration: Duration,
    ) -> Result<Leadership, ApiError> {
        let lease_seconds = lease_duration.as_secs();
        if identity.is_empty() || lease_seconds == 0 {
            return Err(ApiError::BadRequest {
                message: "leader identity and lease duration must be non-empty".to_owned(),
            });
        }
        let now = time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
        let observed = self.observe().await?;
        let (record, compare, outcome) = match observed {
            None => (
                ElectionRecord {
                    holder_identity: identity.to_owned(),
                    lease_duration_seconds: lease_seconds,
                    acquire_time_unix_millis: now,
                    renew_time_unix_millis: now,
                    lease_transitions: 0,
                },
                etcd_client::Compare::version(self.key.clone(), etcd_client::CompareOp::Equal, 0),
                Leadership::Acquired,
            ),
            Some(observed) if observed.record.holder_identity == identity => (
                ElectionRecord {
                    renew_time_unix_millis: now,
                    ..observed.record
                },
                etcd_client::Compare::mod_revision(
                    self.key.clone(),
                    etcd_client::CompareOp::Equal,
                    observed.mod_revision,
                ),
                Leadership::Renewed,
            ),
            Some(observed) if !observed.record.is_expired(now) => {
                return Ok(Leadership::Standby {
                    holder_identity: observed.record.holder_identity,
                })
            }
            Some(observed) => (
                ElectionRecord {
                    holder_identity: identity.to_owned(),
                    lease_duration_seconds: lease_seconds,
                    acquire_time_unix_millis: now,
                    renew_time_unix_millis: now,
                    lease_transitions: observed.record.lease_transitions.saturating_add(1),
                },
                etcd_client::Compare::mod_revision(
                    self.key.clone(),
                    etcd_client::CompareOp::Equal,
                    observed.mod_revision,
                ),
                Leadership::Acquired,
            ),
        };
        let encoded = serde_json::to_vec(&record).map_err(|_| ApiError::Internal)?;
        let transaction =
            etcd_client::Txn::new()
                .when(vec![compare])
                .and_then(vec![etcd_client::TxnOp::put(
                    self.key.clone(),
                    encoded,
                    None,
                )]);
        let response = self
            .client
            .lock()
            .await
            .txn(transaction)
            .await
            .map_err(|_| ApiError::Internal)?;
        if response.succeeded() {
            Ok(outcome)
        } else {
            Ok(Leadership::Contended)
        }
    }
}

/// Validated timing policy for a leader election loop.
#[derive(Clone, Debug)]
pub struct LeaderElectionConfig {
    pub identity: String,
    pub lease_duration: Duration,
    pub renew_deadline: Duration,
    pub retry_period: Duration,
}

impl LeaderElectionConfig {
    pub fn validate(&self) -> Result<(), ApiError> {
        if self.identity.is_empty()
            || self.retry_period.is_zero()
            || self.renew_deadline.is_zero()
            || self.lease_duration <= self.renew_deadline
            || self.renew_deadline < self.retry_period
        {
            return Err(ApiError::BadRequest {
                message: "leader election requires identity and leaseDuration > renewDeadline >= retryPeriod > 0".to_owned(),
            });
        }
        Ok(())
    }
}

/// Runs the supplied controller workers only while this identity can confirm its durable lease.
pub async fn run_leader_controller(
    store: Arc<EtcdLeaseStore>,
    config: LeaderElectionConfig,
    queue: Arc<WorkQueue>,
    reconciler: Arc<dyn Reconciler>,
    workers: usize,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ApiError> {
    config.validate()?;
    if workers == 0 {
        return Err(ApiError::BadRequest {
            message: "controller requires at least one worker".to_owned(),
        });
    }
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        match store
            .acquire_or_renew(&config.identity, config.lease_duration)
            .await?
        {
            Leadership::Acquired | Leadership::Renewed => break,
            Leadership::Standby { .. } | Leadership::Contended => {
                tokio::select! {
                    _ = sleep(config.retry_period) => {}
                    changed = shutdown.changed() => if changed.is_err() || *shutdown.borrow() { return Ok(()); }
                }
            }
        }
    }

    let (cancel_sender, cancel_receiver) = watch::channel(false);
    let mut tasks = Vec::with_capacity(workers);
    for _ in 0..workers {
        tasks.push(tokio::spawn(run_worker(
            queue.clone(),
            reconciler.clone(),
            cancel_receiver.clone(),
        )));
    }
    let mut last_confirmed_renewal = Instant::now();
    loop {
        if *shutdown.borrow() || last_confirmed_renewal.elapsed() >= config.renew_deadline {
            break;
        }
        tokio::select! {
            _ = sleep(config.retry_period) => {}
            changed = shutdown.changed() => if changed.is_err() || *shutdown.borrow() { break; }
        }
        match store
            .acquire_or_renew(&config.identity, config.lease_duration)
            .await?
        {
            Leadership::Renewed | Leadership::Acquired => last_confirmed_renewal = Instant::now(),
            Leadership::Standby { .. } | Leadership::Contended => break,
        }
    }
    let _ = cancel_sender.send(true);
    for task in tasks {
        let _ = task.await;
    }
    Ok(())
}
