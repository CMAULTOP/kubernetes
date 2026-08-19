//! Kubelet-facing typed node registration and bound-Pod runtime handoff contracts.

use std::{future::Future, pin::Pin, sync::Arc};

use rusternetes_api_types::{Node, Pod};
use rusternetes_common::ApiError;
use rusternetes_storage::{InMemoryNodeStore, InMemoryPodStore};

/// The exact Pod identity and desired state delivered to a node runtime implementation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimePod {
    pub uid: String,
    pub namespace: String,
    pub name: String,
    pub node_name: String,
    pub pod: Pod,
}

/// An observed runtime result; neither outcome mutates Kubernetes Pod status in this slice.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeOutcome {
    Accepted,
    Rejected { reason: String },
}

/// Future CRI/OCI adapter boundary. Implementations must report actual observed outcomes.
pub trait RuntimeAdapter: Send + Sync {
    fn sync_pod<'a>(
        &'a self,
        pod: RuntimePod,
    ) -> Pin<Box<dyn Future<Output = Result<RuntimeOutcome, ApiError>> + Send + 'a>>;
}

/// Typed kubelet-facing agent wired to current in-memory persistence for the first vertical slice.
pub struct NodeAgent {
    identity: String,
    nodes: Arc<InMemoryNodeStore>,
    pods: Arc<InMemoryPodStore>,
    runtime: Arc<dyn RuntimeAdapter>,
}

impl NodeAgent {
    pub fn new(
        identity: impl Into<String>,
        nodes: Arc<InMemoryNodeStore>,
        pods: Arc<InMemoryPodStore>,
        runtime: Arc<dyn RuntimeAdapter>,
    ) -> Result<Self, ApiError> {
        let identity = identity.into();
        if identity.is_empty() {
            return Err(ApiError::BadRequest {
                message: "node agent identity must not be empty".to_owned(),
            });
        }
        Ok(Self {
            identity,
            nodes,
            pods,
            runtime,
        })
    }

    pub fn identity(&self) -> &str {
        &self.identity
    }

    pub async fn register(&self, mut node: Node) -> Result<Node, ApiError> {
        if node.name()? != self.identity {
            return Err(ApiError::Forbidden {
                message: "node agent may register only its own Node identity".to_owned(),
            });
        }
        node.enforce_type_meta()?;
        self.nodes.register(node).await
    }

    pub async fn heartbeat(&self, resource_version: &str) -> Result<Node, ApiError> {
        self.nodes.heartbeat(&self.identity, resource_version).await
    }

    /// Reads a stable Pod collection then hands off only Pods bound to this node identity.
    /// Status remains server-owned and unchanged until the future `/status` contract exists.
    pub async fn sync_bound_pods(&self) -> Result<Vec<(RuntimePod, RuntimeOutcome)>, ApiError> {
        self.nodes.get(&self.identity).await?;
        let pods = self
            .pods
            .list(
                None,
                &rusternetes_api_types::LabelSelector::default(),
                &rusternetes_api_types::FieldSelector::default(),
            )
            .await
            .items;
        let bound = self.nodes.bound_pods(&self.identity, pods).await?;
        let mut outcomes = Vec::with_capacity(bound.len());
        for pod in bound {
            let uid = pod.metadata.uid.clone().ok_or(ApiError::Internal)?;
            let namespace = pod.namespace()?.to_owned();
            let name = pod.name()?.to_owned();
            let runtime_pod = RuntimePod {
                uid,
                namespace,
                name,
                node_name: self.identity.clone(),
                pod,
            };
            let outcome = self.runtime.sync_pod(runtime_pod.clone()).await?;
            outcomes.push((runtime_pod, outcome));
        }
        Ok(outcomes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_api_types::{Container, ObjectMeta, PodSpec, TypeMeta};
    use tokio::sync::Mutex;

    #[derive(Default)]
    struct RecordingRuntime(Mutex<Vec<RuntimePod>>);

    impl RuntimeAdapter for RecordingRuntime {
        fn sync_pod<'a>(
            &'a self,
            pod: RuntimePod,
        ) -> Pin<Box<dyn Future<Output = Result<RuntimeOutcome, ApiError>> + Send + 'a>> {
            Box::pin(async move {
                self.0.lock().await.push(pod);
                Ok(RuntimeOutcome::Accepted)
            })
        }
    }

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

    fn pod(name: &str, node_name: Option<&str>) -> Pod {
        Pod {
            type_meta: TypeMeta::pod(),
            metadata: ObjectMeta {
                name: Some(name.to_owned()),
                namespace: Some("default".to_owned()),
                ..ObjectMeta::default()
            },
            spec: PodSpec {
                containers: vec![Container {
                    name: "app".to_owned(),
                    image: Some("example:v1".to_owned()),
                    ..Container::default()
                }],
                node_name: node_name.map(str::to_owned),
                ..PodSpec::default()
            },
            ..Pod::default()
        }
    }

    #[tokio::test]
    async fn registered_agent_syncs_only_its_bound_pods_without_status_fabrication() {
        let nodes = Arc::new(InMemoryNodeStore::new());
        let pods = Arc::new(InMemoryPodStore::new());
        let runtime = Arc::new(RecordingRuntime::default());
        let agent = NodeAgent::new("node-a", nodes, pods.clone(), runtime.clone())
            .expect("agent config valid");
        agent
            .register(node("node-a"))
            .await
            .expect("own registration succeeds");
        let mine = pods
            .create(pod("mine", Some("node-a")))
            .await
            .expect("mine persists");
        pods.create(pod("other", Some("node-b")))
            .await
            .expect("other persists");
        let outcomes = agent.sync_bound_pods().await.expect("sync succeeds");
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].0.name, "mine");
        assert_eq!(outcomes[0].1, RuntimeOutcome::Accepted);
        let persisted = pods
            .get("default", "mine")
            .await
            .expect("Pod remains persisted");
        assert_eq!(persisted.status, mine.status);
        assert_eq!(runtime.0.lock().await.len(), 1);
    }
}
