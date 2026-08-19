//! Atomic storage and bounded WATCH operations for namespaced ConfigMaps.
//!
//! The store owns all mutable ConfigMap state. HTTP layers never access its map directly;
//! every mutation has one linearization point inside the write lock. Watch history and watcher
//! registration live in the same state owner so a list-then-watch client cannot miss a mutation
//! between replay and registration.

use std::collections::{BTreeMap, VecDeque};

use rusternetes_api_types::{
    ConfigMap, ConfigMapList, ConfigMapWatchEvent, DeleteOptions, FieldSelector, LabelSelector,
};
use rusternetes_common::{ApiError, ResourceReference};
use time::OffsetDateTime;
use tokio::sync::{mpsc, Mutex, RwLock};
use uuid::Uuid;

mod pod_namespace;
pub use pod_namespace::{
    InMemoryNamespaceStore, InMemoryPodStore, NamespaceWatchRequest, NamespaceWatchSubscription,
    PodWatchRequest, PodWatchSubscription,
};

/// Maximum retained ConfigMap events in the single-process history window.
pub const WATCH_HISTORY_CAPACITY: usize = 256;
const WATCHER_CHANNEL_CAPACITY: usize = WATCH_HISTORY_CAPACITY + 1;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ResourceKey {
    namespace: String,
    name: String,
}

impl ResourceKey {
    fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
        }
    }

    fn reference(&self) -> ResourceReference {
        ResourceReference::config_map(self.namespace.clone(), self.name.clone())
    }
}

#[derive(Clone)]
struct HistoryEvent {
    revision: u64,
    resource: ConfigMap,
    event: ConfigMapWatchEvent,
}

struct WatchRegistration {
    namespace: Option<String>,
    label_selector: LabelSelector,
    field_selector: FieldSelector,
    sender: mpsc::Sender<ConfigMapWatchEvent>,
}

impl WatchRegistration {
    fn matches(&self, resource: &ConfigMap) -> bool {
        self.namespace
            .as_deref()
            .is_none_or(|namespace| resource.metadata.namespace.as_deref() == Some(namespace))
            && self.label_selector.matches(&resource.metadata.labels)
            && self.field_selector.matches(resource)
    }
}

#[derive(Default)]
struct StoreState {
    revision: u64,
    next_watcher_id: u64,
    config_maps: BTreeMap<ResourceKey, ConfigMap>,
    history: VecDeque<HistoryEvent>,
    watchers: BTreeMap<u64, WatchRegistration>,
}

impl StoreState {
    fn next_resource_version(&mut self) -> String {
        self.revision = self.revision.checked_add(1).unwrap_or(1);
        self.revision.to_string()
    }

    fn current_resource_version(&self) -> String {
        self.revision.to_string()
    }

    fn allocate_watcher_id(&mut self) -> Result<u64, ApiError> {
        let watcher_id = self.next_watcher_id;
        self.next_watcher_id = self
            .next_watcher_id
            .checked_add(1)
            .ok_or(ApiError::Internal)?;
        Ok(watcher_id)
    }

    fn publish(&mut self, history_event: HistoryEvent) {
        let event = history_event.event.clone();
        let resource = history_event.resource.clone();
        self.history.push_back(history_event);
        if self.history.len() > WATCH_HISTORY_CAPACITY {
            self.history.pop_front();
        }

        let slow_or_closed_watchers = self
            .watchers
            .iter()
            .filter_map(|(watcher_id, watcher)| {
                if !watcher.matches(&resource) {
                    return None;
                }
                match watcher.sender.try_send(event.clone()) {
                    Ok(()) => None,
                    Err(mpsc::error::TrySendError::Full(_))
                    | Err(mpsc::error::TrySendError::Closed(_)) => Some(*watcher_id),
                }
            })
            .collect::<Vec<_>>();
        for watcher_id in slow_or_closed_watchers {
            self.watchers.remove(&watcher_id);
        }
    }
}

/// Result of a successful delete operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteResult {
    pub resource_version: String,
}

/// Selector and resume options for a ConfigMap watch subscription.
#[derive(Clone, Debug, Default)]
pub struct ConfigMapWatchRequest {
    pub namespace: Option<String>,
    pub label_selector: LabelSelector,
    pub field_selector: FieldSelector,
    /// The opaque HTTP resourceVersion requested by the client. `None` starts from now.
    pub resource_version: Option<String>,
    pub allow_bookmarks: bool,
}

/// A live ConfigMap watch subscription. Dropping it schedules registry cleanup without blocking.
pub struct ConfigMapWatchSubscription {
    receiver: mpsc::Receiver<ConfigMapWatchEvent>,
    watcher_id: u64,
    cleanup_sender: mpsc::UnboundedSender<u64>,
}

impl ConfigMapWatchSubscription {
    pub async fn recv(&mut self) -> Option<ConfigMapWatchEvent> {
        self.receiver.recv().await
    }
}

impl Drop for ConfigMapWatchSubscription {
    fn drop(&mut self) {
        let _ = self.cleanup_sender.send(self.watcher_id);
    }
}

/// In-memory single-process ConfigMap storage with bounded watch history.
///
/// One `RwLock` protects the storage's only shared state: objects, revision, bounded history and
/// watcher registrations. Watch event delivery uses `try_send` and never awaits while holding the
/// lock. A separate short-lived mutex only drains cancellation IDs emitted by dropped HTTP streams.
pub struct InMemoryConfigMapStore {
    state: RwLock<StoreState>,
    closed_watchers_sender: mpsc::UnboundedSender<u64>,
    closed_watchers_receiver: Mutex<mpsc::UnboundedReceiver<u64>>,
}

impl Default for InMemoryConfigMapStore {
    fn default() -> Self {
        let (closed_watchers_sender, closed_watchers_receiver) = mpsc::unbounded_channel();
        Self {
            state: RwLock::new(StoreState::default()),
            closed_watchers_sender,
            closed_watchers_receiver: Mutex::new(closed_watchers_receiver),
        }
    }
}

impl InMemoryConfigMapStore {
    pub fn new() -> Self {
        Self::default()
    }

    async fn drain_closed_watchers(&self) {
        let mut receiver = self.closed_watchers_receiver.lock().await;
        let mut watcher_ids = Vec::new();
        while let Ok(watcher_id) = receiver.try_recv() {
            watcher_ids.push(watcher_id);
        }
        drop(receiver);

        if watcher_ids.is_empty() {
            return;
        }
        let mut state = self.state.write().await;
        for watcher_id in watcher_ids {
            state.watchers.remove(&watcher_id);
        }
    }

    pub async fn create(&self, mut resource: ConfigMap) -> Result<ConfigMap, ApiError> {
        self.drain_closed_watchers().await;
        resource.enforce_type_meta()?;
        resource.validate()?;
        let key = ResourceKey::new(
            resource.namespace()?.to_owned(),
            resource.name()?.to_owned(),
        );

        let mut state = self.state.write().await;
        if state.config_maps.contains_key(&key) {
            return Err(ApiError::AlreadyExists {
                resource: key.reference(),
            });
        }
        let resource_version = state.next_resource_version();
        resource.set_create_metadata(
            Uuid::new_v4().to_string(),
            OffsetDateTime::now_utc(),
            resource_version,
        );
        let event = ConfigMapWatchEvent::added(resource.clone());
        let history_event = HistoryEvent {
            revision: state.revision,
            resource: resource.clone(),
            event,
        };
        state.config_maps.insert(key, resource.clone());
        state.publish(history_event);
        Ok(resource)
    }

    pub async fn get(&self, namespace: &str, name: &str) -> Result<ConfigMap, ApiError> {
        let key = ResourceKey::new(namespace, name);
        let state = self.state.read().await;
        state
            .config_maps
            .get(&key)
            .cloned()
            .ok_or_else(|| ApiError::NotFound {
                resource: key.reference(),
            })
    }

    /// Updates a ConfigMap atomically. An explicit resource version is compared with the current
    /// version; an omitted version is deliberately allowed by ConfigMap semantics.
    pub async fn update(&self, mut resource: ConfigMap) -> Result<ConfigMap, ApiError> {
        self.drain_closed_watchers().await;
        resource.enforce_type_meta()?;
        let key = ResourceKey::new(
            resource.namespace()?.to_owned(),
            resource.name()?.to_owned(),
        );
        let requested_version = resource.metadata.resource_version.clone();

        let mut state = self.state.write().await;
        let previous = state
            .config_maps
            .get(&key)
            .cloned()
            .ok_or_else(|| ApiError::NotFound {
                resource: key.reference(),
            })?;

        if let Some(requested_version) = requested_version {
            let current_version = previous
                .metadata
                .resource_version
                .as_deref()
                .unwrap_or_default();
            if requested_version != current_version {
                return Err(ApiError::Conflict {
                    resource: key.reference(),
                });
            }
        }
        resource.validate_update(&previous)?;
        let resource_version = state.next_resource_version();
        resource.preserve_server_metadata_from(&previous, resource_version);
        let event = ConfigMapWatchEvent::modified(resource.clone());
        let history_event = HistoryEvent {
            revision: state.revision,
            resource: resource.clone(),
            event,
        };
        state.config_maps.insert(key, resource.clone());
        state.publish(history_event);
        Ok(resource)
    }

    pub async fn delete(
        &self,
        namespace: &str,
        name: &str,
        options: DeleteOptions,
    ) -> Result<DeleteResult, ApiError> {
        self.drain_closed_watchers().await;
        let key = ResourceKey::new(namespace, name);
        let mut state = self.state.write().await;
        let mut current =
            state
                .config_maps
                .get(&key)
                .cloned()
                .ok_or_else(|| ApiError::NotFound {
                    resource: key.reference(),
                })?;

        if !current.metadata.finalizers.is_empty() {
            return Err(ApiError::BadRequest {
                message: "deleting resources with finalizers is not implemented in phase 1"
                    .to_owned(),
            });
        }

        if let Some(preconditions) = options.preconditions {
            if preconditions.uid.is_some() && preconditions.uid != current.metadata.uid {
                return Err(ApiError::Conflict {
                    resource: key.reference(),
                });
            }
            if preconditions.resource_version.is_some()
                && preconditions.resource_version != current.metadata.resource_version
            {
                return Err(ApiError::Conflict {
                    resource: key.reference(),
                });
            }
        }

        state.config_maps.remove(&key);
        let resource_version = state.next_resource_version();
        current.metadata.resource_version = Some(resource_version.clone());
        let event = ConfigMapWatchEvent::deleted(current.clone());
        let history_event = HistoryEvent {
            revision: state.revision,
            resource: current,
            event,
        };
        state.publish(history_event);
        Ok(DeleteResult { resource_version })
    }

    /// Lists either one namespace or all namespaces from one consistent in-memory snapshot.
    pub async fn list(
        &self,
        namespace: Option<&str>,
        label_selector: &LabelSelector,
        field_selector: &FieldSelector,
    ) -> ConfigMapList {
        let state = self.state.read().await;
        let items = state
            .config_maps
            .iter()
            .filter(|(key, resource)| {
                namespace.is_none_or(|requested| key.namespace == requested)
                    && label_selector.matches(&resource.metadata.labels)
                    && field_selector.matches(resource)
            })
            .map(|(_, resource)| resource.clone())
            .collect();
        ConfigMapList::new(state.current_resource_version(), items)
    }

    /// Registers a bounded subscription after atomically queuing all matching events newer than the
    /// requested resource version. A `410 Expired` error means the caller must perform a fresh LIST.
    pub async fn watch(
        &self,
        request: ConfigMapWatchRequest,
    ) -> Result<ConfigMapWatchSubscription, ApiError> {
        self.drain_closed_watchers().await;
        let requested_revision = parse_resource_version(request.resource_version.as_deref())?;
        let mut state = self.state.write().await;

        if let Some(oldest) = state.history.front() {
            if requested_revision < oldest.revision.saturating_sub(1) {
                return Err(ApiError::ResourceExpired {
                    message: format!(
                        "too old resource version: {requested_revision}; oldest available replay point is {}",
                        oldest.revision.saturating_sub(1)
                    ),
                });
            }
        }

        let (sender, receiver) = mpsc::channel(WATCHER_CHANNEL_CAPACITY);
        for history_event in state
            .history
            .iter()
            .filter(|history_event| history_event.revision > requested_revision)
            .filter(|history_event| {
                request.namespace.as_deref().is_none_or(|namespace| {
                    history_event.resource.metadata.namespace.as_deref() == Some(namespace)
                }) && request
                    .label_selector
                    .matches(&history_event.resource.metadata.labels)
                    && request.field_selector.matches(&history_event.resource)
            })
        {
            sender
                .try_send(history_event.event.clone())
                .map_err(|_| ApiError::Internal)?;
        }
        if request.allow_bookmarks {
            sender
                .try_send(ConfigMapWatchEvent::bookmark(
                    state.current_resource_version(),
                ))
                .map_err(|_| ApiError::Internal)?;
        }

        let watcher_id = state.allocate_watcher_id()?;
        state.watchers.insert(
            watcher_id,
            WatchRegistration {
                namespace: request.namespace,
                label_selector: request.label_selector,
                field_selector: request.field_selector,
                sender,
            },
        );

        Ok(ConfigMapWatchSubscription {
            receiver,
            watcher_id,
            cleanup_sender: self.closed_watchers_sender.clone(),
        })
    }
}

fn parse_resource_version(raw: Option<&str>) -> Result<u64, ApiError> {
    let Some(raw) = raw.filter(|value| !value.is_empty()) else {
        return Ok(0);
    };
    raw.parse::<u64>().map_err(|_| ApiError::BadRequest {
        message: format!(
            "resourceVersion {raw:?} is not a valid opaque version for this storage backend"
        ),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use rusternetes_api_types::{FieldSelector, ObjectMeta, TypeMeta, WatchEventType};

    use super::*;

    fn config_map(name: &str) -> ConfigMap {
        ConfigMap {
            type_meta: TypeMeta::config_map(),
            metadata: ObjectMeta {
                name: Some(name.to_owned()),
                namespace: Some("default".to_owned()),
                labels: BTreeMap::from([("tier".to_owned(), "api".to_owned())]),
                ..ObjectMeta::default()
            },
            data: BTreeMap::from([("mode".to_owned(), "safe".to_owned())]),
            ..ConfigMap::default()
        }
    }

    #[tokio::test]
    async fn stale_explicit_version_conflicts_but_unconditional_update_succeeds() {
        let store = InMemoryConfigMapStore::new();
        let created = store
            .create(config_map("settings"))
            .await
            .expect("create succeeds");

        let mut first_update = created.clone();
        first_update
            .data
            .insert("mode".to_owned(), "first".to_owned());
        let first_update = store
            .update(first_update)
            .await
            .expect("conditional update succeeds");

        let mut stale_update = created;
        stale_update
            .data
            .insert("mode".to_owned(), "stale".to_owned());
        assert!(matches!(
            store.update(stale_update).await,
            Err(ApiError::Conflict { .. })
        ));

        let mut unconditional = first_update;
        unconditional.metadata.resource_version = None;
        unconditional
            .data
            .insert("mode".to_owned(), "unconditional".to_owned());
        let updated = store
            .update(unconditional)
            .await
            .expect("unconditional ConfigMap update succeeds");
        assert_eq!(updated.data.get("mode"), Some(&"unconditional".to_owned()));
    }

    #[tokio::test]
    async fn delete_preconditions_are_checked_inside_the_mutation() {
        let store = InMemoryConfigMapStore::new();
        let created = store
            .create(config_map("settings"))
            .await
            .expect("create succeeds");
        let options = DeleteOptions {
            preconditions: Some(rusternetes_api_types::Preconditions {
                uid: Some("wrong".to_owned()),
                resource_version: created.metadata.resource_version.clone(),
            }),
            ..DeleteOptions::default()
        };

        assert!(matches!(
            store.delete("default", "settings", options).await,
            Err(ApiError::Conflict { .. })
        ));
        assert!(store.get("default", "settings").await.is_ok());
    }

    #[tokio::test]
    async fn list_filters_resources_by_label_selector() {
        let store = InMemoryConfigMapStore::new();
        store
            .create(config_map("first"))
            .await
            .expect("create first");
        let mut second = config_map("second");
        second
            .metadata
            .labels
            .insert("tier".to_owned(), "worker".to_owned());
        store.create(second).await.expect("create second");

        let selector = LabelSelector::parse(Some("tier=api")).expect("selector is valid");
        let fields =
            FieldSelector::parse(Some("metadata.name=first")).expect("field selector is valid");
        let list = store.list(Some("default"), &selector, &fields).await;
        assert_eq!(list.items.len(), 1);
        assert_eq!(list.items[0].metadata.name.as_deref(), Some("first"));
    }

    #[tokio::test]
    async fn watch_replays_then_delivers_live_events_in_revision_order() {
        let store = InMemoryConfigMapStore::new();
        let created = store
            .create(config_map("settings"))
            .await
            .expect("create succeeds");
        let mut watch = store
            .watch(ConfigMapWatchRequest {
                namespace: Some("default".to_owned()),
                resource_version: Some("0".to_owned()),
                ..ConfigMapWatchRequest::default()
            })
            .await
            .expect("watch starts from history");

        let replayed = watch.recv().await.expect("replayed event is available");
        assert_eq!(replayed.event_type, WatchEventType::Added);

        let mut update = created;
        update.data.insert("mode".to_owned(), "updated".to_owned());
        let updated = store.update(update).await.expect("update succeeds");
        let modified = watch.recv().await.expect("live modification is available");
        assert_eq!(modified.event_type, WatchEventType::Modified);

        store
            .delete("default", "settings", DeleteOptions::default())
            .await
            .expect("delete succeeds");
        let deleted = watch.recv().await.expect("live deletion is available");
        assert_eq!(deleted.event_type, WatchEventType::Deleted);
        assert_eq!(updated.metadata.resource_version.as_deref(), Some("2"));
    }

    #[tokio::test]
    async fn old_resource_version_is_expired_after_history_compaction() {
        let store = InMemoryConfigMapStore::new();
        for index in 0..=WATCH_HISTORY_CAPACITY {
            store
                .create(config_map(&format!("settings-{index}")))
                .await
                .expect("create succeeds");
        }

        assert!(matches!(
            store
                .watch(ConfigMapWatchRequest {
                    resource_version: Some("0".to_owned()),
                    ..ConfigMapWatchRequest::default()
                })
                .await,
            Err(ApiError::ResourceExpired { .. })
        ));
    }

    #[tokio::test]
    async fn slow_watcher_is_removed_without_unbounded_queue_growth() {
        let store = InMemoryConfigMapStore::new();
        let slow_watcher = store
            .watch(ConfigMapWatchRequest::default())
            .await
            .expect("watch starts at current version");

        for index in 0..=WATCHER_CHANNEL_CAPACITY {
            store
                .create(config_map(&format!("queued-{index}")))
                .await
                .expect("create succeeds");
        }
        drop(slow_watcher);
        store
            .create(config_map("cleanup-trigger"))
            .await
            .expect("create after closed watcher succeeds");

        let state = store.state.read().await;
        assert!(state.watchers.is_empty());
    }
}
