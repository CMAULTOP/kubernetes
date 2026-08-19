//! Typed in-memory Pod and Namespace repositories.
//!
//! These stores deliberately mirror ConfigMap storage's per-resource linearization, bounded
//! replay history, selector filtering, and slow-consumer cleanup. They remain separate types so
//! Kubernetes resource invariants never degrade to an unstructured generic object store.

use std::collections::{BTreeMap, VecDeque};

use rusternetes_api_types::{
    DeleteOptions, FieldSelector, LabelSelector, Namespace, NamespaceList, NamespaceWatchEvent,
    Pod, PodList, PodWatchEvent,
};
use rusternetes_common::{ApiError, ResourceReference};
use time::OffsetDateTime;
use tokio::sync::{mpsc, Mutex, RwLock};
use uuid::Uuid;

use crate::{DeleteResult, WATCH_HISTORY_CAPACITY};

const WATCHER_CHANNEL_CAPACITY: usize = WATCH_HISTORY_CAPACITY + 1;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PodKey {
    namespace: String,
    name: String,
}

impl PodKey {
    fn from_resource(resource: &Pod) -> Result<Self, ApiError> {
        Ok(Self {
            namespace: resource.namespace()?.to_owned(),
            name: resource.name()?.to_owned(),
        })
    }

    fn reference(&self) -> ResourceReference {
        ResourceReference::pod(self.namespace.clone(), self.name.clone())
    }
}

#[derive(Clone)]
struct PodHistoryEvent {
    revision: u64,
    resource: Pod,
    event: PodWatchEvent,
}

struct PodWatchRegistration {
    namespace: Option<String>,
    label_selector: LabelSelector,
    field_selector: FieldSelector,
    sender: mpsc::Sender<PodWatchEvent>,
}

impl PodWatchRegistration {
    fn matches(&self, resource: &Pod) -> bool {
        self.namespace
            .as_deref()
            .is_none_or(|namespace| resource.metadata.namespace.as_deref() == Some(namespace))
            && self.label_selector.matches(&resource.metadata.labels)
            && self.field_selector.matches_pod(resource)
    }
}

#[derive(Default)]
struct PodStoreState {
    revision: u64,
    next_watcher_id: u64,
    pods: BTreeMap<PodKey, Pod>,
    history: VecDeque<PodHistoryEvent>,
    watchers: BTreeMap<u64, PodWatchRegistration>,
}

impl PodStoreState {
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

    fn publish(&mut self, history_event: PodHistoryEvent) {
        let event = history_event.event.clone();
        let resource = history_event.resource.clone();
        self.history.push_back(history_event);
        if self.history.len() > WATCH_HISTORY_CAPACITY {
            self.history.pop_front();
        }
        let stale = self
            .watchers
            .iter()
            .filter_map(|(id, watcher)| {
                if !watcher.matches(&resource) {
                    return None;
                }
                match watcher.sender.try_send(event.clone()) {
                    Ok(()) => None,
                    Err(mpsc::error::TrySendError::Full(_))
                    | Err(mpsc::error::TrySendError::Closed(_)) => Some(*id),
                }
            })
            .collect::<Vec<_>>();
        for id in stale {
            self.watchers.remove(&id);
        }
    }
}

/// Selector and resume options for Pod watch subscriptions.
#[derive(Clone, Debug, Default)]
pub struct PodWatchRequest {
    pub namespace: Option<String>,
    pub label_selector: LabelSelector,
    pub field_selector: FieldSelector,
    pub resource_version: Option<String>,
    pub allow_bookmarks: bool,
}

/// Bounded Pod watch subscription. Dropping schedules watcher registry cleanup.
pub struct PodWatchSubscription {
    receiver: mpsc::Receiver<PodWatchEvent>,
    watcher_id: u64,
    cleanup_sender: mpsc::UnboundedSender<u64>,
}

impl PodWatchSubscription {
    pub async fn recv(&mut self) -> Option<PodWatchEvent> {
        self.receiver.recv().await
    }
}

impl Drop for PodWatchSubscription {
    fn drop(&mut self) {
        let _ = self.cleanup_sender.send(self.watcher_id);
    }
}

/// In-memory typed Pod persistence with bounded watch history and atomic mutations.
pub struct InMemoryPodStore {
    state: RwLock<PodStoreState>,
    closed_watchers_sender: mpsc::UnboundedSender<u64>,
    closed_watchers_receiver: Mutex<mpsc::UnboundedReceiver<u64>>,
}

impl Default for InMemoryPodStore {
    fn default() -> Self {
        let (closed_watchers_sender, closed_watchers_receiver) = mpsc::unbounded_channel();
        Self {
            state: RwLock::new(PodStoreState::default()),
            closed_watchers_sender,
            closed_watchers_receiver: Mutex::new(closed_watchers_receiver),
        }
    }
}

impl InMemoryPodStore {
    pub fn new() -> Self {
        Self::default()
    }

    async fn drain_closed_watchers(&self) {
        let mut receiver = self.closed_watchers_receiver.lock().await;
        let mut ids = Vec::new();
        while let Ok(id) = receiver.try_recv() {
            ids.push(id);
        }
        drop(receiver);
        if ids.is_empty() {
            return;
        }
        let mut state = self.state.write().await;
        for id in ids {
            state.watchers.remove(&id);
        }
    }

    pub async fn create(&self, mut resource: Pod) -> Result<Pod, ApiError> {
        self.drain_closed_watchers().await;
        resource.enforce_type_meta()?;
        resource.validate_create()?;
        let key = PodKey::from_resource(&resource)?;
        let mut state = self.state.write().await;
        if state.pods.contains_key(&key) {
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
        let event = PodWatchEvent::added(resource.clone());
        state.pods.insert(key, resource.clone());
        let revision = state.revision;
        state.publish(PodHistoryEvent {
            revision,
            resource: resource.clone(),
            event,
        });
        Ok(resource)
    }

    pub async fn get(&self, namespace: &str, name: &str) -> Result<Pod, ApiError> {
        let key = PodKey {
            namespace: namespace.to_owned(),
            name: name.to_owned(),
        };
        self.state
            .read()
            .await
            .pods
            .get(&key)
            .cloned()
            .ok_or_else(|| ApiError::NotFound {
                resource: key.reference(),
            })
    }

    pub async fn update(&self, mut resource: Pod) -> Result<Pod, ApiError> {
        self.drain_closed_watchers().await;
        resource.enforce_type_meta()?;
        let key = PodKey::from_resource(&resource)?;
        let requested_version = resource.metadata.resource_version.clone();
        let mut state = self.state.write().await;
        let previous = state
            .pods
            .get(&key)
            .cloned()
            .ok_or_else(|| ApiError::NotFound {
                resource: key.reference(),
            })?;
        verify_update_version(
            requested_version.as_deref(),
            previous.metadata.resource_version.as_deref(),
            &key.reference(),
        )?;
        resource.validate_update(&previous)?;
        let resource_version = state.next_resource_version();
        if previous.metadata.deletion_timestamp.is_some() {
            resource.preserve_deletion_pending_update_from(&previous, resource_version);
        } else {
            resource.preserve_server_metadata_from(&previous, resource_version);
        }
        let deletion_completed = previous.metadata.deletion_timestamp.is_some()
            && resource.metadata.finalizers.is_empty();
        let event = if deletion_completed {
            state.pods.remove(&key);
            PodWatchEvent::deleted(resource.clone())
        } else {
            state.pods.insert(key, resource.clone());
            PodWatchEvent::modified(resource.clone())
        };
        let revision = state.revision;
        state.publish(PodHistoryEvent {
            revision,
            resource: resource.clone(),
            event,
        });
        Ok(resource)
    }

    /// Updates only the status projection of an existing Pod under its shared resourceVersion.
    pub async fn update_status(&self, mut resource: Pod) -> Result<Pod, ApiError> {
        self.drain_closed_watchers().await;
        resource.enforce_type_meta()?;
        let key = PodKey::from_resource(&resource)?;
        let mut state = self.state.write().await;
        let previous = state
            .pods
            .get(&key)
            .cloned()
            .ok_or_else(|| ApiError::NotFound {
                resource: key.reference(),
            })?;
        verify_update_version(
            resource.metadata.resource_version.as_deref(),
            previous.metadata.resource_version.as_deref(),
            &key.reference(),
        )?;
        resource.validate_status_update(&previous)?;
        let resource_version = state.next_resource_version();
        resource.preserve_status_update_from(&previous, resource_version);
        let event = PodWatchEvent::modified(resource.clone());
        state.pods.insert(key, resource.clone());
        let revision = state.revision;
        state.publish(PodHistoryEvent {
            revision,
            resource: resource.clone(),
            event,
        });
        Ok(resource)
    }

    /// Atomically assigns an unscheduled Pod to a node after checking the scheduler snapshot's
    /// resourceVersion. Competing schedulers receive Conflict and no assignment is overwritten.
    pub async fn bind(
        &self,
        namespace: &str,
        name: &str,
        resource_version: &str,
        node_name: &str,
    ) -> Result<Pod, ApiError> {
        self.drain_closed_watchers().await;
        let key = PodKey {
            namespace: namespace.to_owned(),
            name: name.to_owned(),
        };
        let mut state = self.state.write().await;
        let previous = state
            .pods
            .get(&key)
            .cloned()
            .ok_or_else(|| ApiError::NotFound {
                resource: key.reference(),
            })?;
        verify_update_version(
            Some(resource_version),
            previous.metadata.resource_version.as_deref(),
            &key.reference(),
        )?;
        let mut bound = previous.clone();
        bound.bind_to_node(&previous, node_name)?;
        let next_resource_version = state.next_resource_version();
        bound.preserve_server_metadata_from(&previous, next_resource_version);
        let event = PodWatchEvent::modified(bound.clone());
        state.pods.insert(key, bound.clone());
        let revision = state.revision;
        state.publish(PodHistoryEvent {
            revision,
            resource: bound.clone(),
            event,
        });
        Ok(bound)
    }

    pub async fn delete(
        &self,
        namespace: &str,
        name: &str,
        options: DeleteOptions,
    ) -> Result<DeleteResult, ApiError> {
        self.drain_closed_watchers().await;
        let key = PodKey {
            namespace: namespace.to_owned(),
            name: name.to_owned(),
        };
        let mut state = self.state.write().await;
        let mut current = state
            .pods
            .get(&key)
            .cloned()
            .ok_or_else(|| ApiError::NotFound {
                resource: key.reference(),
            })?;
        validate_delete(&current.metadata, &options, &key.reference())?;
        if current.metadata.deletion_timestamp.is_none() && !current.metadata.finalizers.is_empty()
        {
            let resource_version = state.next_resource_version();
            current.mark_deletion_requested(OffsetDateTime::now_utc(), resource_version.clone());
            let event = PodWatchEvent::modified(current.clone());
            state.pods.insert(key, current.clone());
            let revision = state.revision;
            state.publish(PodHistoryEvent {
                revision,
                resource: current,
                event,
            });
            return Ok(DeleteResult { resource_version });
        }
        if current.metadata.deletion_timestamp.is_some() {
            return Ok(DeleteResult {
                resource_version: current.metadata.resource_version.unwrap_or_default(),
            });
        }
        state.pods.remove(&key);
        let resource_version = state.next_resource_version();
        current.metadata.resource_version = Some(resource_version.clone());
        let event = PodWatchEvent::deleted(current.clone());
        let revision = state.revision;
        state.publish(PodHistoryEvent {
            revision,
            resource: current,
            event,
        });
        Ok(DeleteResult { resource_version })
    }

    pub async fn list(
        &self,
        namespace: Option<&str>,
        label_selector: &LabelSelector,
        field_selector: &FieldSelector,
    ) -> PodList {
        let state = self.state.read().await;
        let items = state
            .pods
            .iter()
            .filter(|(key, resource)| {
                namespace.is_none_or(|requested| key.namespace == requested)
                    && label_selector.matches(&resource.metadata.labels)
                    && field_selector.matches_pod(resource)
            })
            .map(|(_, resource)| resource.clone())
            .collect();
        PodList::new(state.current_resource_version(), items)
    }

    pub async fn watch(&self, request: PodWatchRequest) -> Result<PodWatchSubscription, ApiError> {
        self.drain_closed_watchers().await;
        let requested = parse_resource_version(request.resource_version.as_deref())?;
        let mut state = self.state.write().await;
        if let Some(oldest) = state.history.front() {
            if requested < oldest.revision.saturating_sub(1) {
                return Err(ApiError::ResourceExpired {
                    message: format!(
                        "too old resource version: {requested}; oldest available replay point is {}",
                        oldest.revision.saturating_sub(1)
                    ),
                });
            }
        }
        let (sender, receiver) = mpsc::channel(WATCHER_CHANNEL_CAPACITY);
        for history in state
            .history
            .iter()
            .filter(|history| history.revision > requested)
            .filter(|history| pod_watch_matches(&request, &history.resource))
        {
            sender
                .try_send(history.event.clone())
                .map_err(|_| ApiError::Internal)?;
        }
        if request.allow_bookmarks {
            sender
                .try_send(PodWatchEvent::bookmark(state.current_resource_version()))
                .map_err(|_| ApiError::Internal)?;
        }
        let watcher_id = state.allocate_watcher_id()?;
        state.watchers.insert(
            watcher_id,
            PodWatchRegistration {
                namespace: request.namespace,
                label_selector: request.label_selector,
                field_selector: request.field_selector,
                sender,
            },
        );
        Ok(PodWatchSubscription {
            receiver,
            watcher_id,
            cleanup_sender: self.closed_watchers_sender.clone(),
        })
    }
}

#[derive(Clone)]
struct NamespaceHistoryEvent {
    revision: u64,
    resource: Namespace,
    event: NamespaceWatchEvent,
}

struct NamespaceWatchRegistration {
    label_selector: LabelSelector,
    field_selector: FieldSelector,
    sender: mpsc::Sender<NamespaceWatchEvent>,
}

impl NamespaceWatchRegistration {
    fn matches(&self, resource: &Namespace) -> bool {
        self.label_selector.matches(&resource.metadata.labels)
            && self.field_selector.matches_namespace(resource)
    }
}

#[derive(Default)]
struct NamespaceStoreState {
    revision: u64,
    next_watcher_id: u64,
    namespaces: BTreeMap<String, Namespace>,
    history: VecDeque<NamespaceHistoryEvent>,
    watchers: BTreeMap<u64, NamespaceWatchRegistration>,
}

impl NamespaceStoreState {
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

    fn publish(&mut self, history_event: NamespaceHistoryEvent) {
        let event = history_event.event.clone();
        let resource = history_event.resource.clone();
        self.history.push_back(history_event);
        if self.history.len() > WATCH_HISTORY_CAPACITY {
            self.history.pop_front();
        }
        let stale = self
            .watchers
            .iter()
            .filter_map(|(id, watcher)| match watcher.matches(&resource) {
                false => None,
                true => match watcher.sender.try_send(event.clone()) {
                    Ok(()) => None,
                    Err(mpsc::error::TrySendError::Full(_))
                    | Err(mpsc::error::TrySendError::Closed(_)) => Some(*id),
                },
            })
            .collect::<Vec<_>>();
        for id in stale {
            self.watchers.remove(&id);
        }
    }
}

/// Selector and resume options for cluster-scoped Namespace watch subscriptions.
#[derive(Clone, Debug, Default)]
pub struct NamespaceWatchRequest {
    pub label_selector: LabelSelector,
    pub field_selector: FieldSelector,
    pub resource_version: Option<String>,
    pub allow_bookmarks: bool,
}

/// Bounded Namespace watch subscription. Dropping schedules watcher registry cleanup.
pub struct NamespaceWatchSubscription {
    receiver: mpsc::Receiver<NamespaceWatchEvent>,
    watcher_id: u64,
    cleanup_sender: mpsc::UnboundedSender<u64>,
}

impl NamespaceWatchSubscription {
    pub async fn recv(&mut self) -> Option<NamespaceWatchEvent> {
        self.receiver.recv().await
    }
}

impl Drop for NamespaceWatchSubscription {
    fn drop(&mut self) {
        let _ = self.cleanup_sender.send(self.watcher_id);
    }
}

/// In-memory typed cluster-scoped Namespace persistence with bounded watch history.
pub struct InMemoryNamespaceStore {
    state: RwLock<NamespaceStoreState>,
    closed_watchers_sender: mpsc::UnboundedSender<u64>,
    closed_watchers_receiver: Mutex<mpsc::UnboundedReceiver<u64>>,
}

impl Default for InMemoryNamespaceStore {
    fn default() -> Self {
        let (closed_watchers_sender, closed_watchers_receiver) = mpsc::unbounded_channel();
        Self {
            state: RwLock::new(NamespaceStoreState::default()),
            closed_watchers_sender,
            closed_watchers_receiver: Mutex::new(closed_watchers_receiver),
        }
    }
}

impl InMemoryNamespaceStore {
    pub fn new() -> Self {
        Self::default()
    }

    async fn drain_closed_watchers(&self) {
        let mut receiver = self.closed_watchers_receiver.lock().await;
        let mut ids = Vec::new();
        while let Ok(id) = receiver.try_recv() {
            ids.push(id);
        }
        drop(receiver);
        if ids.is_empty() {
            return;
        }
        let mut state = self.state.write().await;
        for id in ids {
            state.watchers.remove(&id);
        }
    }

    pub async fn create(&self, mut resource: Namespace) -> Result<Namespace, ApiError> {
        self.drain_closed_watchers().await;
        resource.enforce_type_meta()?;
        resource.validate_create()?;
        let name = resource.name()?.to_owned();
        let reference = ResourceReference::namespace(name.clone());
        let mut state = self.state.write().await;
        if state.namespaces.contains_key(&name) {
            return Err(ApiError::AlreadyExists {
                resource: reference,
            });
        }
        let resource_version = state.next_resource_version();
        resource.set_create_metadata(
            Uuid::new_v4().to_string(),
            OffsetDateTime::now_utc(),
            resource_version,
        );
        let event = NamespaceWatchEvent::added(resource.clone());
        state.namespaces.insert(name, resource.clone());
        let revision = state.revision;
        state.publish(NamespaceHistoryEvent {
            revision,
            resource: resource.clone(),
            event,
        });
        Ok(resource)
    }

    pub async fn get(&self, name: &str) -> Result<Namespace, ApiError> {
        self.state
            .read()
            .await
            .namespaces
            .get(name)
            .cloned()
            .ok_or_else(|| ApiError::NotFound {
                resource: ResourceReference::namespace(name),
            })
    }

    pub async fn update(&self, mut resource: Namespace) -> Result<Namespace, ApiError> {
        self.drain_closed_watchers().await;
        resource.enforce_type_meta()?;
        let name = resource.name()?.to_owned();
        let reference = ResourceReference::namespace(name.clone());
        let requested_version = resource.metadata.resource_version.clone();
        let mut state = self.state.write().await;
        let previous = state
            .namespaces
            .get(&name)
            .cloned()
            .ok_or_else(|| ApiError::NotFound {
                resource: reference.clone(),
            })?;
        verify_update_version(
            requested_version.as_deref(),
            previous.metadata.resource_version.as_deref(),
            &reference,
        )?;
        resource.validate_update(&previous)?;
        let resource_version = state.next_resource_version();
        resource.preserve_server_metadata_from(&previous, resource_version);
        let deletion_completed = previous.metadata.deletion_timestamp.is_some()
            && resource.metadata.finalizers.is_empty()
            && resource.spec.finalizers.is_empty();
        let event = if deletion_completed {
            state.namespaces.remove(&name);
            NamespaceWatchEvent::deleted(resource.clone())
        } else {
            state.namespaces.insert(name, resource.clone());
            NamespaceWatchEvent::modified(resource.clone())
        };
        let revision = state.revision;
        state.publish(NamespaceHistoryEvent {
            revision,
            resource: resource.clone(),
            event,
        });
        Ok(resource)
    }

    /// Updates only the status projection of an existing Namespace under its shared resourceVersion.
    pub async fn update_status(&self, mut resource: Namespace) -> Result<Namespace, ApiError> {
        self.drain_closed_watchers().await;
        resource.enforce_type_meta()?;
        let name = resource.name()?.to_owned();
        let reference = ResourceReference::namespace(name.clone());
        let requested_version = resource.metadata.resource_version.clone();
        let mut state = self.state.write().await;
        let previous = state
            .namespaces
            .get(&name)
            .cloned()
            .ok_or_else(|| ApiError::NotFound {
                resource: reference.clone(),
            })?;
        verify_update_version(
            requested_version.as_deref(),
            previous.metadata.resource_version.as_deref(),
            &reference,
        )?;
        resource.validate_status_update(&previous)?;
        let resource_version = state.next_resource_version();
        resource.preserve_status_update_from(&previous, resource_version);
        let event = NamespaceWatchEvent::modified(resource.clone());
        state.namespaces.insert(name, resource.clone());
        let revision = state.revision;
        state.publish(NamespaceHistoryEvent {
            revision,
            resource: resource.clone(),
            event,
        });
        Ok(resource)
    }

    /// Applies the dedicated Namespace `/finalize` transition under the shared resourceVersion.
    pub async fn finalize(&self, mut resource: Namespace) -> Result<Namespace, ApiError> {
        self.drain_closed_watchers().await;
        resource.enforce_type_meta()?;
        let name = resource.name()?.to_owned();
        let reference = ResourceReference::namespace(name.clone());
        let requested_version = resource.metadata.resource_version.clone();
        let mut state = self.state.write().await;
        let previous = state
            .namespaces
            .get(&name)
            .cloned()
            .ok_or_else(|| ApiError::NotFound {
                resource: reference.clone(),
            })?;
        verify_update_version(
            requested_version.as_deref(),
            previous.metadata.resource_version.as_deref(),
            &reference,
        )?;
        resource.validate_finalize_update(&previous)?;
        let resource_version = state.next_resource_version();
        resource.preserve_finalize_update_from(&previous, resource_version);
        let deletion_completed =
            resource.metadata.finalizers.is_empty() && resource.spec.finalizers.is_empty();
        let event = if deletion_completed {
            state.namespaces.remove(&name);
            NamespaceWatchEvent::deleted(resource.clone())
        } else {
            state.namespaces.insert(name, resource.clone());
            NamespaceWatchEvent::modified(resource.clone())
        };
        let revision = state.revision;
        state.publish(NamespaceHistoryEvent {
            revision,
            resource: resource.clone(),
            event,
        });
        Ok(resource)
    }

    pub async fn delete(
        &self,
        name: &str,
        options: DeleteOptions,
    ) -> Result<DeleteResult, ApiError> {
        self.drain_closed_watchers().await;
        let reference = ResourceReference::namespace(name);
        let mut state = self.state.write().await;
        let mut current =
            state
                .namespaces
                .get(name)
                .cloned()
                .ok_or_else(|| ApiError::NotFound {
                    resource: reference.clone(),
                })?;
        validate_delete_preconditions(&current.metadata, &options, &reference)?;
        if current.metadata.deletion_timestamp.is_none()
            && (!current.metadata.finalizers.is_empty() || !current.spec.finalizers.is_empty())
        {
            let resource_version = state.next_resource_version();
            current.mark_deletion_requested(OffsetDateTime::now_utc(), resource_version.clone());
            let event = NamespaceWatchEvent::modified(current.clone());
            state.namespaces.insert(name.to_owned(), current.clone());
            let revision = state.revision;
            state.publish(NamespaceHistoryEvent {
                revision,
                resource: current,
                event,
            });
            return Ok(DeleteResult { resource_version });
        }
        if current.metadata.deletion_timestamp.is_some() {
            return Ok(DeleteResult {
                resource_version: current.metadata.resource_version.unwrap_or_default(),
            });
        }
        state.namespaces.remove(name);
        let resource_version = state.next_resource_version();
        current.metadata.resource_version = Some(resource_version.clone());
        let event = NamespaceWatchEvent::deleted(current.clone());
        let revision = state.revision;
        state.publish(NamespaceHistoryEvent {
            revision,
            resource: current,
            event,
        });
        Ok(DeleteResult { resource_version })
    }

    pub async fn list(
        &self,
        label_selector: &LabelSelector,
        field_selector: &FieldSelector,
    ) -> NamespaceList {
        let state = self.state.read().await;
        let items = state
            .namespaces
            .values()
            .filter(|resource| {
                label_selector.matches(&resource.metadata.labels)
                    && field_selector.matches_namespace(resource)
            })
            .cloned()
            .collect();
        NamespaceList::new(state.current_resource_version(), items)
    }

    pub async fn watch(
        &self,
        request: NamespaceWatchRequest,
    ) -> Result<NamespaceWatchSubscription, ApiError> {
        self.drain_closed_watchers().await;
        let requested = parse_resource_version(request.resource_version.as_deref())?;
        let mut state = self.state.write().await;
        if let Some(oldest) = state.history.front() {
            if requested < oldest.revision.saturating_sub(1) {
                return Err(ApiError::ResourceExpired {
                    message: format!(
                        "too old resource version: {requested}; oldest available replay point is {}",
                        oldest.revision.saturating_sub(1)
                    ),
                });
            }
        }
        let (sender, receiver) = mpsc::channel(WATCHER_CHANNEL_CAPACITY);
        for history in state
            .history
            .iter()
            .filter(|history| history.revision > requested)
            .filter(|history| namespace_watch_matches(&request, &history.resource))
        {
            sender
                .try_send(history.event.clone())
                .map_err(|_| ApiError::Internal)?;
        }
        if request.allow_bookmarks {
            sender
                .try_send(NamespaceWatchEvent::bookmark(
                    state.current_resource_version(),
                ))
                .map_err(|_| ApiError::Internal)?;
        }
        let watcher_id = state.allocate_watcher_id()?;
        state.watchers.insert(
            watcher_id,
            NamespaceWatchRegistration {
                label_selector: request.label_selector,
                field_selector: request.field_selector,
                sender,
            },
        );
        Ok(NamespaceWatchSubscription {
            receiver,
            watcher_id,
            cleanup_sender: self.closed_watchers_sender.clone(),
        })
    }
}

fn verify_update_version(
    requested: Option<&str>,
    current: Option<&str>,
    reference: &ResourceReference,
) -> Result<(), ApiError> {
    if requested.is_some_and(|requested| Some(requested) != current) {
        return Err(ApiError::Conflict {
            resource: reference.clone(),
        });
    }
    Ok(())
}

fn validate_delete(
    metadata: &rusternetes_api_types::ObjectMeta,
    options: &DeleteOptions,
    reference: &ResourceReference,
) -> Result<(), ApiError> {
    validate_delete_preconditions(metadata, options, reference)
}

fn validate_delete_preconditions(
    metadata: &rusternetes_api_types::ObjectMeta,
    options: &DeleteOptions,
    reference: &ResourceReference,
) -> Result<(), ApiError> {
    if let Some(preconditions) = &options.preconditions {
        if preconditions.uid.is_some() && preconditions.uid != metadata.uid
            || preconditions.resource_version.is_some()
                && preconditions.resource_version != metadata.resource_version
        {
            return Err(ApiError::Conflict {
                resource: reference.clone(),
            });
        }
    }
    Ok(())
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

fn pod_watch_matches(request: &PodWatchRequest, resource: &Pod) -> bool {
    request
        .namespace
        .as_deref()
        .is_none_or(|namespace| resource.metadata.namespace.as_deref() == Some(namespace))
        && request.label_selector.matches(&resource.metadata.labels)
        && request.field_selector.matches_pod(resource)
}

fn namespace_watch_matches(request: &NamespaceWatchRequest, resource: &Namespace) -> bool {
    request.label_selector.matches(&resource.metadata.labels)
        && request.field_selector.matches_namespace(resource)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use rusternetes_api_types::{
        Container, NamespacePhase, NamespaceStatus, ObjectMeta, PodPhase, PodSpec, PodWatchObject,
        TypeMeta, WatchEventType,
    };

    use super::*;

    fn pod(name: &str) -> Pod {
        Pod {
            type_meta: TypeMeta::pod(),
            metadata: ObjectMeta {
                name: Some(name.to_owned()),
                namespace: Some("default".to_owned()),
                labels: BTreeMap::from([("tier".to_owned(), "api".to_owned())]),
                ..ObjectMeta::default()
            },
            spec: PodSpec {
                containers: vec![Container {
                    name: "app".to_owned(),
                    image: Some("example:v1".to_owned()),
                    ..Container::default()
                }],
                ..PodSpec::default()
            },
            ..Pod::default()
        }
    }

    fn namespace(name: &str) -> Namespace {
        Namespace {
            type_meta: TypeMeta::namespace(),
            metadata: ObjectMeta {
                name: Some(name.to_owned()),
                ..ObjectMeta::default()
            },
            ..Namespace::default()
        }
    }

    #[tokio::test]
    async fn pod_store_defaults_pending_and_replays_bounded_watch_history() {
        let store = InMemoryPodStore::new();
        let created = store.create(pod("web")).await.expect("Pod create succeeds");
        assert_eq!(created.status.phase, Some(PodPhase::Pending));
        let mut watch = store
            .watch(PodWatchRequest {
                resource_version: Some("0".to_owned()),
                ..PodWatchRequest::default()
            })
            .await
            .expect("watch starts");
        assert_eq!(
            watch.recv().await.expect("replay").event_type,
            WatchEventType::Added
        );
        assert!(store
            .delete("default", "web", DeleteOptions::default())
            .await
            .is_ok());
        assert_eq!(
            watch.recv().await.expect("delete").event_type,
            WatchEventType::Deleted
        );
    }

    #[tokio::test]
    async fn pod_status_update_isolated_cas_guarded_and_watched() {
        let store = InMemoryPodStore::new();
        let created = store.create(pod("web")).await.expect("Pod creates");
        let mut watch = store
            .watch(PodWatchRequest {
                namespace: Some("default".to_owned()),
                resource_version: created.metadata.resource_version.clone(),
                ..PodWatchRequest::default()
            })
            .await
            .expect("watch starts");
        let mut status_update = created.clone();
        status_update.spec.node_name = Some("attempted-spec-mutation".to_owned());
        status_update.status.phase = Some(PodPhase::Running);
        let updated = store
            .update_status(status_update)
            .await
            .expect("status update succeeds");
        assert_eq!(updated.status.phase, Some(PodPhase::Running));
        assert_eq!(updated.spec, created.spec);
        assert_ne!(
            updated.metadata.resource_version,
            created.metadata.resource_version
        );
        let event = watch.recv().await.expect("status update reaches watch");
        assert_eq!(event.event_type, WatchEventType::Modified);
        let PodWatchObject::Pod(event_pod) = event.object else {
            panic!("modified Pod watch event must carry a Pod");
        };
        assert_eq!(event_pod.status.phase, Some(PodPhase::Running));

        let mut stale = updated.clone();
        stale.metadata.resource_version = created.metadata.resource_version;
        stale.status.phase = Some(PodPhase::Failed);
        assert!(matches!(
            store.update_status(stale).await,
            Err(ApiError::Conflict { .. })
        ));
    }

    #[tokio::test]
    async fn pod_finalizer_delete_is_staged_and_regular_update_completes_it() {
        let store = InMemoryPodStore::new();
        let mut resource = pod("terminating");
        resource
            .metadata
            .finalizers
            .push("example.com/cleanup".to_owned());
        let created = store.create(resource).await.expect("Pod creates");
        let mut watch = store
            .watch(PodWatchRequest {
                namespace: Some("default".to_owned()),
                resource_version: created.metadata.resource_version.clone(),
                ..PodWatchRequest::default()
            })
            .await
            .expect("watch starts");

        store
            .delete("default", "terminating", DeleteOptions::default())
            .await
            .expect("deletion request stages the Pod");
        let pending = store
            .get("default", "terminating")
            .await
            .expect("Pod remains visible while finalizer exists");
        assert!(pending.metadata.deletion_timestamp.is_some());
        assert_eq!(
            pending.metadata.finalizers,
            vec!["example.com/cleanup".to_owned()]
        );
        assert_eq!(
            watch
                .recv()
                .await
                .expect("deletion request is watched")
                .event_type,
            WatchEventType::Modified
        );

        let mut invalid = pending.clone();
        invalid
            .metadata
            .labels
            .insert("attempted-mutation".to_owned(), "rejected".to_owned());
        assert!(matches!(
            store.update(invalid).await,
            Err(ApiError::Invalid { .. })
        ));

        let mut finalize = pending;
        finalize.metadata.finalizers.clear();
        store
            .update(finalize)
            .await
            .expect("regular update removing final finalizer completes deletion");
        assert!(matches!(
            store.get("default", "terminating").await,
            Err(ApiError::NotFound { .. })
        ));
        assert_eq!(
            watch
                .recv()
                .await
                .expect("completed deletion is watched")
                .event_type,
            WatchEventType::Deleted
        );
    }

    #[tokio::test]
    async fn namespace_store_enforces_server_status_and_delete_preconditions() {
        let store = InMemoryNamespaceStore::new();
        let created = store
            .create(namespace("development"))
            .await
            .expect("create namespace");
        assert_eq!(created.status.phase, Some(NamespacePhase::Active));
        assert_eq!(
            created.metadata.labels.get(Namespace::NAME_LABEL),
            Some(&"development".to_owned())
        );
        let mut invalid = namespace("invalid");
        invalid.status = NamespaceStatus {
            phase: Some(NamespacePhase::Terminating),
        };
        assert!(matches!(
            store.create(invalid).await,
            Err(ApiError::Invalid { .. })
        ));
        assert!(matches!(
            store
                .delete(
                    "development",
                    DeleteOptions {
                        preconditions: Some(rusternetes_api_types::Preconditions {
                            uid: Some("wrong".to_owned()),
                            resource_version: created.metadata.resource_version.clone(),
                        }),
                        ..DeleteOptions::default()
                    },
                )
                .await,
            Err(ApiError::Conflict { .. })
        ));
    }

    #[tokio::test]
    async fn namespace_finalizer_delete_is_staged_and_finalize_completes_it() {
        let store = InMemoryNamespaceStore::new();
        let mut resource = namespace("terminating");
        resource
            .spec
            .finalizers
            .push("example.com/cleanup".to_owned());
        let created = store.create(resource).await.expect("Namespace creates");
        let mut watch = store
            .watch(NamespaceWatchRequest {
                resource_version: created.metadata.resource_version.clone(),
                ..NamespaceWatchRequest::default()
            })
            .await
            .expect("watch starts");

        store
            .delete("terminating", DeleteOptions::default())
            .await
            .expect("deletion request stages the Namespace");
        let pending = store
            .get("terminating")
            .await
            .expect("Namespace remains visible");
        assert!(pending.metadata.deletion_timestamp.is_some());
        assert_eq!(pending.status.phase, Some(NamespacePhase::Terminating));
        assert_eq!(
            watch
                .recv()
                .await
                .expect("deletion request is watched")
                .event_type,
            WatchEventType::Modified
        );

        let mut finalize = pending.clone();
        finalize.spec.finalizers.clear();
        store
            .finalize(finalize)
            .await
            .expect("removing final finalizer completes deletion");
        assert!(matches!(
            store.get("terminating").await,
            Err(ApiError::NotFound { .. })
        ));
        assert_eq!(
            watch
                .recv()
                .await
                .expect("completed deletion is watched")
                .event_type,
            WatchEventType::Deleted
        );
    }

    #[tokio::test]
    async fn namespace_watch_exposes_cluster_scoped_events() {
        let store = InMemoryNamespaceStore::new();
        store
            .create(namespace("development"))
            .await
            .expect("create namespace");
        let mut watch = store
            .watch(NamespaceWatchRequest {
                resource_version: Some("0".to_owned()),
                ..NamespaceWatchRequest::default()
            })
            .await
            .expect("namespace watch starts");
        assert_eq!(
            watch.recv().await.expect("replay").event_type,
            WatchEventType::Added
        );
    }
}
