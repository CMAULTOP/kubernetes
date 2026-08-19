//! Typed cluster-scoped Node registration persistence.

use std::collections::{BTreeMap, VecDeque};

use rusternetes_api_types::{
    DeleteOptions, FieldSelector, LabelSelector, Node, NodeList, NodeWatchEvent, Pod,
};
use rusternetes_common::{ApiError, ResourceReference};
use time::OffsetDateTime;
use tokio::sync::{mpsc, Mutex, RwLock};
use uuid::Uuid;

use crate::{DeleteResult, WATCH_HISTORY_CAPACITY};

const WATCHER_CHANNEL_CAPACITY: usize = WATCH_HISTORY_CAPACITY + 1;

/// Selector and resume options for a cluster-scoped Node watch subscription.
#[derive(Clone, Debug, Default)]
pub struct NodeWatchRequest {
    pub label_selector: LabelSelector,
    pub field_selector: FieldSelector,
    pub resource_version: Option<String>,
    pub allow_bookmarks: bool,
}

/// One bounded Node watch receiver. Dropping it removes the watcher asynchronously.
pub struct NodeWatchSubscription {
    receiver: mpsc::Receiver<NodeWatchEvent>,
    watcher_id: u64,
    cleanup_sender: mpsc::UnboundedSender<u64>,
}

impl NodeWatchSubscription {
    pub async fn recv(&mut self) -> Option<NodeWatchEvent> {
        self.receiver.recv().await
    }
}

impl Drop for NodeWatchSubscription {
    fn drop(&mut self) {
        let _ = self.cleanup_sender.send(self.watcher_id);
    }
}

#[derive(Clone)]
struct NodeHistoryEvent {
    revision: u64,
    resource: Node,
    event: NodeWatchEvent,
}

struct NodeWatchRegistration {
    label_selector: LabelSelector,
    field_selector: FieldSelector,
    sender: mpsc::Sender<NodeWatchEvent>,
}

impl NodeWatchRegistration {
    fn matches(&self, resource: &Node) -> bool {
        self.label_selector.matches(&resource.metadata.labels)
            && self.field_selector.matches_node(resource)
    }
}

/// Typed in-memory Node registration storage. Node identity is linearized per name; heartbeats
/// require the observed resourceVersion and never permit an identity/UID replacement.
pub struct InMemoryNodeStore {
    state: RwLock<NodeStoreState>,
    closed_watchers_sender: mpsc::UnboundedSender<u64>,
    closed_watchers_receiver: Mutex<mpsc::UnboundedReceiver<u64>>,
}

impl Default for InMemoryNodeStore {
    fn default() -> Self {
        let (closed_watchers_sender, closed_watchers_receiver) = mpsc::unbounded_channel();
        Self {
            state: RwLock::new(NodeStoreState::default()),
            closed_watchers_sender,
            closed_watchers_receiver: Mutex::new(closed_watchers_receiver),
        }
    }
}

#[derive(Default)]
struct NodeStoreState {
    revision: u64,
    next_watcher_id: u64,
    nodes: BTreeMap<String, Node>,
    history: VecDeque<NodeHistoryEvent>,
    watchers: BTreeMap<u64, NodeWatchRegistration>,
}

impl NodeStoreState {
    fn next_resource_version(&mut self) -> String {
        self.revision = self.revision.checked_add(1).unwrap_or(1);
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

    fn publish(&mut self, history_event: NodeHistoryEvent) {
        let event = history_event.event.clone();
        let resource = history_event.resource.clone();
        self.history.push_back(history_event);
        if self.history.len() > WATCH_HISTORY_CAPACITY {
            self.history.pop_front();
        }
        let stale = self
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
        for watcher_id in stale {
            self.watchers.remove(&watcher_id);
        }
    }
}

impl InMemoryNodeStore {
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

    pub async fn register(&self, mut node: Node) -> Result<Node, ApiError> {
        self.drain_closed_watchers().await;
        node.enforce_type_meta()?;
        node.validate_registration()?;
        let name = node.name()?.to_owned();
        let mut state = self.state.write().await;
        if state.nodes.contains_key(&name) {
            return Err(ApiError::AlreadyExists {
                resource: ResourceReference::node(name),
            });
        }
        let resource_version = state.next_resource_version();
        node.set_registration_metadata(
            Uuid::new_v4().to_string(),
            OffsetDateTime::now_utc(),
            resource_version,
        );
        state.nodes.insert(name, node.clone());
        let revision = state.revision;
        state.publish(NodeHistoryEvent {
            revision,
            resource: node.clone(),
            event: NodeWatchEvent::added(node.clone()),
        });
        Ok(node)
    }

    pub async fn create(&self, node: Node) -> Result<Node, ApiError> {
        self.register(node).await
    }

    pub async fn get(&self, name: &str) -> Result<Node, ApiError> {
        self.state
            .read()
            .await
            .nodes
            .get(name)
            .cloned()
            .ok_or_else(|| ApiError::NotFound {
                resource: ResourceReference::node(name.to_owned()),
            })
    }

    pub async fn update(&self, mut node: Node) -> Result<Node, ApiError> {
        self.drain_closed_watchers().await;
        node.enforce_type_meta()?;
        let name = node.name()?.to_owned();
        let mut state = self.state.write().await;
        let previous = state
            .nodes
            .get(&name)
            .cloned()
            .ok_or_else(|| ApiError::NotFound {
                resource: ResourceReference::node(name.clone()),
            })?;
        if node.metadata.resource_version.as_deref()
            != previous.metadata.resource_version.as_deref()
        {
            return Err(ApiError::Conflict {
                resource: ResourceReference::node(name),
            });
        }
        node.validate_update(&previous)?;
        node.preserve_server_metadata_from(&previous, state.next_resource_version());
        state.nodes.insert(node.name()?.to_owned(), node.clone());
        let revision = state.revision;
        state.publish(NodeHistoryEvent {
            revision,
            resource: node.clone(),
            event: NodeWatchEvent::modified(node.clone()),
        });
        Ok(node)
    }

    /// Updates only the status projection of an existing Node under its shared resourceVersion.
    pub async fn update_status(&self, mut node: Node) -> Result<Node, ApiError> {
        self.drain_closed_watchers().await;
        node.enforce_type_meta()?;
        let name = node.name()?.to_owned();
        let mut state = self.state.write().await;
        let previous = state
            .nodes
            .get(&name)
            .cloned()
            .ok_or_else(|| ApiError::NotFound {
                resource: ResourceReference::node(name.clone()),
            })?;
        if node.metadata.resource_version.as_deref()
            != previous.metadata.resource_version.as_deref()
        {
            return Err(ApiError::Conflict {
                resource: ResourceReference::node(name),
            });
        }
        node.validate_status_update(&previous)?;
        node.preserve_status_update_from(&previous, state.next_resource_version());
        state.nodes.insert(node.name()?.to_owned(), node.clone());
        let revision = state.revision;
        state.publish(NodeHistoryEvent {
            revision,
            resource: node.clone(),
            event: NodeWatchEvent::modified(node.clone()),
        });
        Ok(node)
    }

    pub async fn delete(
        &self,
        name: &str,
        options: DeleteOptions,
    ) -> Result<DeleteResult, ApiError> {
        self.drain_closed_watchers().await;
        let mut state = self.state.write().await;
        let current = state
            .nodes
            .get(name)
            .cloned()
            .ok_or_else(|| ApiError::NotFound {
                resource: ResourceReference::node(name.to_owned()),
            })?;
        if let Some(preconditions) = options.preconditions {
            if preconditions.uid.is_some() && preconditions.uid != current.metadata.uid
                || preconditions.resource_version.is_some()
                    && preconditions.resource_version != current.metadata.resource_version
            {
                return Err(ApiError::Conflict {
                    resource: ResourceReference::node(name.to_owned()),
                });
            }
        }
        state.nodes.remove(name);
        let resource_version = state.next_resource_version();
        let mut deleted = current;
        deleted.metadata.resource_version = Some(resource_version.clone());
        let revision = state.revision;
        state.publish(NodeHistoryEvent {
            revision,
            resource: deleted.clone(),
            event: NodeWatchEvent::deleted(deleted),
        });
        Ok(DeleteResult { resource_version })
    }

    pub async fn watch(
        &self,
        request: NodeWatchRequest,
    ) -> Result<NodeWatchSubscription, ApiError> {
        self.drain_closed_watchers().await;
        let requested = parse_node_resource_version(request.resource_version.as_deref())?;
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
            .filter(|history| {
                request
                    .label_selector
                    .matches(&history.resource.metadata.labels)
                    && request.field_selector.matches_node(&history.resource)
            })
        {
            sender
                .try_send(history.event.clone())
                .map_err(|_| ApiError::Internal)?;
        }
        if request.allow_bookmarks {
            sender
                .try_send(NodeWatchEvent::bookmark(state.revision.to_string()))
                .map_err(|_| ApiError::Internal)?;
        }
        let watcher_id = state.allocate_watcher_id()?;
        state.watchers.insert(
            watcher_id,
            NodeWatchRegistration {
                label_selector: request.label_selector,
                field_selector: request.field_selector,
                sender,
            },
        );
        Ok(NodeWatchSubscription {
            receiver,
            watcher_id,
            cleanup_sender: self.closed_watchers_sender.clone(),
        })
    }

    pub async fn list_filtered(
        &self,
        label_selector: &LabelSelector,
        field_selector: &FieldSelector,
    ) -> NodeList {
        let state = self.state.read().await;
        let items = state
            .nodes
            .values()
            .filter(|node| {
                label_selector.matches(&node.metadata.labels) && field_selector.matches_node(node)
            })
            .cloned()
            .collect();
        NodeList::new(state.revision.to_string(), items)
    }

    /// Confirms that the reporting agent owns the node and atomically advances its liveness version.
    pub async fn heartbeat(
        &self,
        identity: &str,
        resource_version: &str,
    ) -> Result<Node, ApiError> {
        self.drain_closed_watchers().await;
        let mut state = self.state.write().await;
        let previous = state
            .nodes
            .get(identity)
            .cloned()
            .ok_or_else(|| ApiError::NotFound {
                resource: ResourceReference::node(identity.to_owned()),
            })?;
        if previous.metadata.resource_version.as_deref() != Some(resource_version) {
            return Err(ApiError::Conflict {
                resource: ResourceReference::node(identity.to_owned()),
            });
        }
        let mut current = previous;
        current.status.ready = true;
        current.metadata.resource_version = Some(state.next_resource_version());
        state.nodes.insert(identity.to_owned(), current.clone());
        let revision = state.revision;
        state.publish(NodeHistoryEvent {
            revision,
            resource: current.clone(),
            event: NodeWatchEvent::modified(current.clone()),
        });
        Ok(current)
    }

    pub async fn list(&self) -> Vec<Node> {
        self.state.read().await.nodes.values().cloned().collect()
    }

    /// Filters a supplied Pod snapshot set to only Pods bound to this registered Node identity.
    pub async fn bound_pods(
        &self,
        identity: &str,
        pods: impl IntoIterator<Item = Pod>,
    ) -> Result<Vec<Pod>, ApiError> {
        self.get(identity).await?;
        Ok(pods
            .into_iter()
            .filter(|pod| pod.spec.node_name.as_deref() == Some(identity))
            .collect())
    }
}

fn parse_node_resource_version(raw: Option<&str>) -> Result<u64, ApiError> {
    let Some(raw) = raw.filter(|value| !value.is_empty()) else {
        return Ok(0);
    };
    raw.parse::<u64>().map_err(|_| ApiError::BadRequest {
        message: format!("resourceVersion {raw:?} is not a valid in-memory revision"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_api_types::{ObjectMeta, TypeMeta};

    fn node(name: &str) -> Node {
        Node {
            type_meta: TypeMeta::node(),
            metadata: ObjectMeta {
                name: Some(name.to_owned()),
                ..ObjectMeta::default()
            },
            ..Node::default()
        }
    }

    #[tokio::test]
    async fn registration_is_identity_safe_and_heartbeat_is_version_guarded() {
        let store = InMemoryNodeStore::new();
        let created = store
            .register(node("node-a"))
            .await
            .expect("registration succeeds");
        assert!(created.status.ready);
        assert!(matches!(
            store.register(node("node-a")).await,
            Err(ApiError::AlreadyExists { .. })
        ));
        let refreshed = store
            .heartbeat(
                "node-a",
                created
                    .metadata
                    .resource_version
                    .as_deref()
                    .expect("version"),
            )
            .await
            .expect("own heartbeat succeeds");
        assert_ne!(
            refreshed.metadata.resource_version,
            created.metadata.resource_version
        );
        assert!(matches!(
            store
                .heartbeat(
                    "node-a",
                    created
                        .metadata
                        .resource_version
                        .as_deref()
                        .expect("stale version")
                )
                .await,
            Err(ApiError::Conflict { .. })
        ));
    }

    #[tokio::test]
    async fn node_watch_replays_then_delivers_live_modification() {
        let store = InMemoryNodeStore::new();
        let created = store
            .register(node("node-a"))
            .await
            .expect("registration succeeds");
        let mut watch = store
            .watch(NodeWatchRequest {
                resource_version: Some("0".to_owned()),
                ..NodeWatchRequest::default()
            })
            .await
            .expect("watch opens");
        let replay = watch.recv().await.expect("creation replays");
        assert!(matches!(
            replay.event_type,
            rusternetes_api_types::WatchEventType::Added
        ));

        let mut updated = created;
        updated.spec.unschedulable = true;
        store.update(updated).await.expect("update succeeds");
        let live = watch.recv().await.expect("modification delivers");
        assert!(matches!(
            live.event_type,
            rusternetes_api_types::WatchEventType::Modified
        ));
    }
}
