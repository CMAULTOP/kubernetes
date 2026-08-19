//! Typed cluster-scoped Node registration persistence.

use std::collections::BTreeMap;

use rusternetes_api_types::{DeleteOptions, FieldSelector, LabelSelector, Node, NodeList, Pod};
use rusternetes_common::{ApiError, ResourceReference};
use time::OffsetDateTime;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::DeleteResult;

/// Selector and resume options for a cluster-scoped Node watch subscription.
#[derive(Clone, Debug, Default)]
pub struct NodeWatchRequest {
    pub label_selector: LabelSelector,
    pub field_selector: FieldSelector,
    pub resource_version: Option<String>,
    pub allow_bookmarks: bool,
}

/// Typed in-memory Node registration storage. Node identity is linearized per name; heartbeats
/// require the observed resourceVersion and never permit an identity/UID replacement.
#[derive(Default)]
pub struct InMemoryNodeStore {
    state: RwLock<NodeStoreState>,
}

#[derive(Default)]
struct NodeStoreState {
    revision: u64,
    nodes: BTreeMap<String, Node>,
}

impl NodeStoreState {
    fn next_resource_version(&mut self) -> String {
        self.revision = self.revision.checked_add(1).unwrap_or(1);
        self.revision.to_string()
    }
}

impl InMemoryNodeStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register(&self, mut node: Node) -> Result<Node, ApiError> {
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
        Ok(node)
    }

    pub async fn delete(
        &self,
        name: &str,
        options: DeleteOptions,
    ) -> Result<DeleteResult, ApiError> {
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
        Ok(DeleteResult {
            resource_version: state.next_resource_version(),
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
}
