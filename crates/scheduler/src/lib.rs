//! Typed, deterministic scheduler decision and optimistic Pod binding primitives.

use std::{collections::BTreeMap, sync::Arc};

use rusternetes_api_types::Pod;
use rusternetes_common::{ApiError, ResourceReference};
use rusternetes_storage::InMemoryPodStore;
use rusternetes_storage_etcd::EtcdPodRepository;

/// Typed Node information available to the first scheduling slice.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NodeCandidate {
    pub name: String,
    pub labels: BTreeMap<String, String>,
}

/// The frozen object and candidate snapshot used by one serial scheduling cycle.
#[derive(Clone, Debug)]
pub struct SchedulingSnapshot {
    pub pod: Pod,
    pub candidates: Vec<NodeCandidate>,
}

/// Scheduler outcome which never fabricates a binding when no feasible node exists.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchedulingDecision {
    Bind { node_name: String },
    Unschedulable,
}

/// A node feasibility check. All filters must succeed for a candidate to remain feasible.
pub trait Filter: Send + Sync {
    fn allows(&self, pod: &Pod, candidate: &NodeCandidate) -> bool;
}

/// A deterministic candidate ranking extension point.
pub trait Scorer: Send + Sync {
    fn score(&self, pod: &Pod, candidate: &NodeCandidate) -> i64;
}

/// Matches the only scheduling constraint represented by the current typed Pod model: a
/// pre-assigned node, which makes the Pod ineligible for scheduler placement.
#[derive(Clone, Debug, Default)]
pub struct UnscheduledOnlyFilter;

impl Filter for UnscheduledOnlyFilter {
    fn allows(&self, pod: &Pod, _candidate: &NodeCandidate) -> bool {
        pod.spec.node_name.is_none()
    }
}

/// Default stable scorer. Lexicographic name tie-break is applied by `Scheduler`.
#[derive(Clone, Debug, Default)]
pub struct ZeroScorer;

impl Scorer for ZeroScorer {
    fn score(&self, _pod: &Pod, _candidate: &NodeCandidate) -> i64 {
        0
    }
}

/// A pluggable typed scheduling pipeline.
pub struct Scheduler {
    filters: Vec<Arc<dyn Filter>>,
    scorers: Vec<Arc<dyn Scorer>>,
}

impl Default for Scheduler {
    fn default() -> Self {
        Self {
            filters: vec![Arc::new(UnscheduledOnlyFilter)],
            scorers: vec![Arc::new(ZeroScorer)],
        }
    }
}

impl Scheduler {
    pub fn new(
        filters: Vec<Arc<dyn Filter>>,
        scorers: Vec<Arc<dyn Scorer>>,
    ) -> Result<Self, ApiError> {
        if filters.is_empty() || scorers.is_empty() {
            return Err(ApiError::BadRequest {
                message: "scheduler requires at least one filter and scorer".to_owned(),
            });
        }
        Ok(Self { filters, scorers })
    }

    pub fn decide(&self, snapshot: &SchedulingSnapshot) -> SchedulingDecision {
        let mut feasible = snapshot
            .candidates
            .iter()
            .filter(|candidate| {
                self.filters
                    .iter()
                    .all(|filter| filter.allows(&snapshot.pod, candidate))
            })
            .map(|candidate| {
                let score = self
                    .scorers
                    .iter()
                    .map(|scorer| scorer.score(&snapshot.pod, candidate))
                    .sum::<i64>();
                (score, candidate.name.as_str())
            })
            .collect::<Vec<_>>();
        feasible
            .sort_unstable_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(right.1)));
        feasible
            .first()
            .map_or(SchedulingDecision::Unschedulable, |(_, name)| {
                SchedulingDecision::Bind {
                    node_name: (*name).to_owned(),
                }
            })
    }
}

/// Typed persistence boundary for scheduler-only assignments.
#[derive(Clone)]
pub enum PodBinder {
    InMemory(Arc<InMemoryPodStore>),
    Etcd(Arc<EtcdPodRepository>),
}

impl PodBinder {
    pub async fn bind(&self, pod: &Pod, node_name: &str) -> Result<Pod, ApiError> {
        let namespace = pod.namespace()?.to_owned();
        let name = pod.name()?.to_owned();
        let resource_version =
            pod.metadata
                .resource_version
                .as_deref()
                .ok_or_else(|| ApiError::Conflict {
                    resource: ResourceReference::pod(namespace.clone(), name.clone()),
                })?;
        match self {
            Self::InMemory(store) => {
                store
                    .bind(&namespace, &name, resource_version, node_name)
                    .await
            }
            Self::Etcd(repository) => {
                repository
                    .bind(&namespace, &name, resource_version, node_name)
                    .await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_api_types::{Container, ObjectMeta, PodSpec, TypeMeta};
    use rusternetes_controller_runtime::{ReconcileKey, Reconciler};

    fn pod() -> Pod {
        Pod {
            type_meta: TypeMeta::pod(),
            metadata: ObjectMeta {
                name: Some("web".to_owned()),
                namespace: Some("default".to_owned()),
                ..ObjectMeta::default()
            },
            spec: PodSpec {
                containers: vec![Container {
                    name: "web".to_owned(),
                    image: Some("example:v1".to_owned()),
                    ..Container::default()
                }],
                ..PodSpec::default()
            },
            ..Pod::default()
        }
    }

    #[test]
    fn selects_highest_score_with_deterministic_node_name_tie_break() {
        let decision = Scheduler::default().decide(&SchedulingSnapshot {
            pod: pod(),
            candidates: vec![
                NodeCandidate {
                    name: "node-b".to_owned(),
                    ..NodeCandidate::default()
                },
                NodeCandidate {
                    name: "node-a".to_owned(),
                    ..NodeCandidate::default()
                },
            ],
        });
        assert_eq!(
            decision,
            SchedulingDecision::Bind {
                node_name: "node-a".to_owned()
            }
        );
    }

    #[tokio::test]
    async fn queued_reconcile_binds_an_unscheduled_pod_through_typed_repository() {
        let store = Arc::new(InMemoryPodStore::new());
        let created = store.create(pod()).await.expect("Pod persists");
        let reconciler = PodSchedulerReconciler::new(
            Scheduler::default(),
            PodSource::InMemory(store.clone()),
            PodBinder::InMemory(store.clone()),
            vec![NodeCandidate {
                name: "node-a".to_owned(),
                ..NodeCandidate::default()
            }],
        );
        let (_cancel, receiver) = tokio::sync::watch::channel(false);
        let outcome = reconciler
            .reconcile(ReconcileKey::core("pods", Some("default"), "web"), receiver)
            .await
            .expect("scheduler reconcile succeeds");
        assert_eq!(
            outcome,
            rusternetes_controller_runtime::ReconcileResult::Done
        );
        let persisted = store
            .get("default", "web")
            .await
            .expect("bound Pod persists");
        assert_eq!(persisted.spec.node_name.as_deref(), Some("node-a"));
        assert_ne!(
            persisted.metadata.resource_version,
            created.metadata.resource_version
        );
    }

    #[test]
    fn never_selects_for_already_bound_or_empty_candidate_snapshot() {
        let mut bound = pod();
        bound.spec.node_name = Some("node-a".to_owned());
        assert_eq!(
            Scheduler::default().decide(&SchedulingSnapshot {
                pod: bound,
                candidates: vec![NodeCandidate {
                    name: "node-a".to_owned(),
                    ..NodeCandidate::default()
                }]
            }),
            SchedulingDecision::Unschedulable
        );
        assert_eq!(
            Scheduler::default().decide(&SchedulingSnapshot {
                pod: pod(),
                candidates: Vec::new()
            }),
            SchedulingDecision::Unschedulable
        );
    }
}

/// Typed Pod read boundary used by the scheduler reconciliation adapter.
#[derive(Clone)]
pub enum PodSource {
    InMemory(Arc<InMemoryPodStore>),
    Etcd(Arc<EtcdPodRepository>),
}

impl PodSource {
    async fn get(&self, namespace: &str, name: &str) -> Result<Pod, ApiError> {
        match self {
            Self::InMemory(store) => store.get(namespace, name).await,
            Self::Etcd(repository) => repository.get(namespace, name).await,
        }
    }
}

/// Controller-runtime adapter that turns a queued Pod key into a single scheduling context.
pub struct PodSchedulerReconciler {
    scheduler: Scheduler,
    source: PodSource,
    binder: PodBinder,
    candidates: Vec<NodeCandidate>,
}

impl PodSchedulerReconciler {
    pub fn new(
        scheduler: Scheduler,
        source: PodSource,
        binder: PodBinder,
        candidates: Vec<NodeCandidate>,
    ) -> Self {
        Self {
            scheduler,
            source,
            binder,
            candidates,
        }
    }
}

impl rusternetes_controller_runtime::Reconciler for PodSchedulerReconciler {
    fn reconcile<'a>(
        &'a self,
        key: rusternetes_controller_runtime::ReconcileKey,
        cancelled: tokio::sync::watch::Receiver<bool>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<rusternetes_controller_runtime::ReconcileResult, ApiError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            if *cancelled.borrow()
                || !key.group.is_empty()
                || key.version != "v1"
                || key.resource != "pods"
            {
                return Ok(rusternetes_controller_runtime::ReconcileResult::Done);
            }
            let Some(namespace) = key.namespace else {
                return Ok(rusternetes_controller_runtime::ReconcileResult::Done);
            };
            let pod = match self.source.get(&namespace, &key.name).await {
                Ok(pod) => pod,
                Err(ApiError::NotFound { .. }) => {
                    return Ok(rusternetes_controller_runtime::ReconcileResult::Done)
                }
                Err(error) => return Err(error),
            };
            match self.scheduler.decide(&SchedulingSnapshot {
                pod: pod.clone(),
                candidates: self.candidates.clone(),
            }) {
                SchedulingDecision::Bind { node_name } => {
                    self.binder.bind(&pod, &node_name).await?;
                    Ok(rusternetes_controller_runtime::ReconcileResult::Done)
                }
                SchedulingDecision::Unschedulable => Ok(
                    rusternetes_controller_runtime::ReconcileResult::RequeueAfter(
                        std::time::Duration::from_secs(1),
                    ),
                ),
            }
        })
    }
}
