//! Typed in-memory ServiceAccount and Namespace repositories.
//!
//! These stores deliberately mirror ConfigMap storage's per-resource linearization, bounded
//! replay history, selector filtering, and slow-consumer cleanup. They remain separate types so
//! Kubernetes resource invariants never degrade to an unstructured generic object store.

use std::collections::{BTreeMap, VecDeque};

use rusternetes_api_types::{
    DeleteOptions, FieldSelector, LabelSelector, ServiceAccount, ServiceAccountList,
    ServiceAccountWatchEvent,
};
use rusternetes_common::{ApiError, ResourceReference};
use time::OffsetDateTime;
use tokio::sync::{mpsc, Mutex, RwLock};
use uuid::Uuid;

use crate::{DeleteResult, WATCH_HISTORY_CAPACITY};

const WATCHER_CHANNEL_CAPACITY: usize = WATCH_HISTORY_CAPACITY + 1;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ServiceAccountKey {
    namespace: String,
    name: String,
}

impl ServiceAccountKey {
    fn from_resource(resource: &ServiceAccount) -> Result<Self, ApiError> {
        Ok(Self {
            namespace: resource.namespace()?.to_owned(),
            name: resource.name()?.to_owned(),
        })
    }

    fn reference(&self) -> ResourceReference {
        ResourceReference::service_account(self.namespace.clone(), self.name.clone())
    }
}

#[derive(Clone)]
struct ServiceAccountHistoryEvent {
    revision: u64,
    resource: ServiceAccount,
    event: ServiceAccountWatchEvent,
}

struct ServiceAccountWatchRegistration {
    namespace: Option<String>,
    label_selector: LabelSelector,
    field_selector: FieldSelector,
    sender: mpsc::Sender<ServiceAccountWatchEvent>,
}

impl ServiceAccountWatchRegistration {
    fn matches(&self, resource: &ServiceAccount) -> bool {
        self.namespace
            .as_deref()
            .is_none_or(|namespace| resource.metadata.namespace.as_deref() == Some(namespace))
            && self.label_selector.matches(&resource.metadata.labels)
            && self.field_selector.matches_service_account(resource)
    }
}

#[derive(Default)]
struct ServiceAccountStoreState {
    revision: u64,
    next_watcher_id: u64,
    service_accounts: BTreeMap<ServiceAccountKey, ServiceAccount>,
    history: VecDeque<ServiceAccountHistoryEvent>,
    watchers: BTreeMap<u64, ServiceAccountWatchRegistration>,
}

impl ServiceAccountStoreState {
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

    fn publish(&mut self, history_event: ServiceAccountHistoryEvent) {
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

/// Selector and resume options for ServiceAccount watch subscriptions.
#[derive(Clone, Debug, Default)]
pub struct ServiceAccountWatchRequest {
    pub namespace: Option<String>,
    pub label_selector: LabelSelector,
    pub field_selector: FieldSelector,
    pub resource_version: Option<String>,
    pub allow_bookmarks: bool,
}

/// Bounded ServiceAccount watch subscription. Dropping schedules watcher registry cleanup.
pub struct ServiceAccountWatchSubscription {
    receiver: mpsc::Receiver<ServiceAccountWatchEvent>,
    watcher_id: u64,
    cleanup_sender: mpsc::UnboundedSender<u64>,
}

impl ServiceAccountWatchSubscription {
    pub async fn recv(&mut self) -> Option<ServiceAccountWatchEvent> {
        self.receiver.recv().await
    }
}

impl Drop for ServiceAccountWatchSubscription {
    fn drop(&mut self) {
        let _ = self.cleanup_sender.send(self.watcher_id);
    }
}

/// In-memory typed ServiceAccount persistence with bounded watch history and atomic mutations.
pub struct InMemoryServiceAccountStore {
    state: RwLock<ServiceAccountStoreState>,
    closed_watchers_sender: mpsc::UnboundedSender<u64>,
    closed_watchers_receiver: Mutex<mpsc::UnboundedReceiver<u64>>,
}

impl Default for InMemoryServiceAccountStore {
    fn default() -> Self {
        let (closed_watchers_sender, closed_watchers_receiver) = mpsc::unbounded_channel();
        Self {
            state: RwLock::new(ServiceAccountStoreState::default()),
            closed_watchers_sender,
            closed_watchers_receiver: Mutex::new(closed_watchers_receiver),
        }
    }
}

impl InMemoryServiceAccountStore {
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

    pub async fn create(&self, mut resource: ServiceAccount) -> Result<ServiceAccount, ApiError> {
        self.drain_closed_watchers().await;
        resource.enforce_type_meta()?;
        resource.validate_create()?;
        let key = ServiceAccountKey::from_resource(&resource)?;
        let mut state = self.state.write().await;
        if state.service_accounts.contains_key(&key) {
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
        let event = ServiceAccountWatchEvent::added(resource.clone());
        state.service_accounts.insert(key, resource.clone());
        let revision = state.revision;
        state.publish(ServiceAccountHistoryEvent {
            revision,
            resource: resource.clone(),
            event,
        });
        Ok(resource)
    }

    pub async fn get(&self, namespace: &str, name: &str) -> Result<ServiceAccount, ApiError> {
        let key = ServiceAccountKey {
            namespace: namespace.to_owned(),
            name: name.to_owned(),
        };
        self.state
            .read()
            .await
            .service_accounts
            .get(&key)
            .cloned()
            .ok_or_else(|| ApiError::NotFound {
                resource: key.reference(),
            })
    }

    pub async fn update(&self, mut resource: ServiceAccount) -> Result<ServiceAccount, ApiError> {
        self.drain_closed_watchers().await;
        resource.enforce_type_meta()?;
        let key = ServiceAccountKey::from_resource(&resource)?;
        let requested_version = resource.metadata.resource_version.clone();
        let mut state = self.state.write().await;
        let previous =
            state
                .service_accounts
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
        resource.preserve_server_metadata_from(&previous, resource_version);
        let event = ServiceAccountWatchEvent::modified(resource.clone());
        state.service_accounts.insert(key, resource.clone());
        let revision = state.revision;
        state.publish(ServiceAccountHistoryEvent {
            revision,
            resource: resource.clone(),
            event,
        });
        Ok(resource)
    }

    pub async fn delete(
        &self,
        namespace: &str,
        name: &str,
        options: DeleteOptions,
    ) -> Result<DeleteResult, ApiError> {
        self.drain_closed_watchers().await;
        let key = ServiceAccountKey {
            namespace: namespace.to_owned(),
            name: name.to_owned(),
        };
        let mut state = self.state.write().await;
        let mut current =
            state
                .service_accounts
                .get(&key)
                .cloned()
                .ok_or_else(|| ApiError::NotFound {
                    resource: key.reference(),
                })?;
        validate_delete(&current.metadata, &options, &key.reference())?;
        state.service_accounts.remove(&key);
        let resource_version = state.next_resource_version();
        current.metadata.resource_version = Some(resource_version.clone());
        let event = ServiceAccountWatchEvent::deleted(current.clone());
        let revision = state.revision;
        state.publish(ServiceAccountHistoryEvent {
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
    ) -> ServiceAccountList {
        let state = self.state.read().await;
        let items = state
            .service_accounts
            .iter()
            .filter(|(key, resource)| {
                namespace.is_none_or(|requested| key.namespace == requested)
                    && label_selector.matches(&resource.metadata.labels)
                    && field_selector.matches_service_account(resource)
            })
            .map(|(_, resource)| resource.clone())
            .collect();
        ServiceAccountList::new(state.current_resource_version(), items)
    }

    pub async fn watch(
        &self,
        request: ServiceAccountWatchRequest,
    ) -> Result<ServiceAccountWatchSubscription, ApiError> {
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
            .filter(|history| service_account_watch_matches(&request, &history.resource))
        {
            sender
                .try_send(history.event.clone())
                .map_err(|_| ApiError::Internal)?;
        }
        if request.allow_bookmarks {
            sender
                .try_send(ServiceAccountWatchEvent::bookmark(
                    state.current_resource_version(),
                ))
                .map_err(|_| ApiError::Internal)?;
        }
        let watcher_id = state.allocate_watcher_id()?;
        state.watchers.insert(
            watcher_id,
            ServiceAccountWatchRegistration {
                namespace: request.namespace,
                label_selector: request.label_selector,
                field_selector: request.field_selector,
                sender,
            },
        );
        Ok(ServiceAccountWatchSubscription {
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
    if !metadata.finalizers.is_empty() {
        return Err(ApiError::BadRequest {
            message: "deleting resources with finalizers is not implemented in phase 1".to_owned(),
        });
    }
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

fn service_account_watch_matches(
    request: &ServiceAccountWatchRequest,
    resource: &ServiceAccount,
) -> bool {
    request
        .namespace
        .as_deref()
        .is_none_or(|namespace| resource.metadata.namespace.as_deref() == Some(namespace))
        && request.label_selector.matches(&resource.metadata.labels)
        && request.field_selector.matches_service_account(resource)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use rusternetes_api_types::{ObjectMeta, ServiceAccount, TypeMeta, WatchEventType};

    use super::*;

    fn service_account() -> ServiceAccount {
        ServiceAccount {
            type_meta: TypeMeta::service_account(),
            metadata: ObjectMeta {
                name: Some("build-robot".to_owned()),
                namespace: Some("development".to_owned()),
                labels: BTreeMap::from([("team".to_owned(), "platform".to_owned())]),
                ..ObjectMeta::default()
            },
            ..ServiceAccount::default()
        }
    }

    #[tokio::test]
    async fn service_account_store_preserves_identity_uses_cas_and_watches_updates() {
        let store = InMemoryServiceAccountStore::new();
        let created = store
            .create(service_account())
            .await
            .expect("ServiceAccount creates");
        let resource_version = created
            .metadata
            .resource_version
            .clone()
            .expect("create sets resourceVersion");
        assert!(created.metadata.uid.is_some());

        let list = store
            .list(None, &LabelSelector::default(), &FieldSelector::default())
            .await;
        assert_eq!(list.items, vec![created.clone()]);

        let mut watch = store
            .watch(ServiceAccountWatchRequest {
                namespace: Some("development".to_owned()),
                resource_version: Some(resource_version),
                ..ServiceAccountWatchRequest::default()
            })
            .await
            .expect("watch opens");
        let mut updated = created.clone();
        updated
            .metadata
            .labels
            .insert("rotation".to_owned(), "current".to_owned());
        let updated = store.update(updated).await.expect("update succeeds");
        assert_ne!(
            updated.metadata.resource_version,
            created.metadata.resource_version
        );

        let event = watch.recv().await.expect("watch receives update");
        assert_eq!(event.event_type, WatchEventType::Modified);

        let mut stale = created.clone();
        stale.automount_service_account_token = Some(false);
        assert!(matches!(
            store.update(stale).await,
            Err(ApiError::Conflict { .. })
        ));
    }
}
