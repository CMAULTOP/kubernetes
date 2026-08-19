# Rusternetes: scheduler framework и optimistic Pod binding

**Статус:** architecture contract до реализации
**Ветка:** `rusternetes/phase-1-api-storage`
**Срез:** Phase 12 — scheduling framework и typed Pod binding

## Цель

Этот срез добавляет Rust-native decision pipeline для назначения already persisted, unscheduled Pods на typed Nodes. Он не симулирует контейнерное выполнение: успешный binding меняет единственный server-owned scheduling assignment `spec.nodeName` через compare-and-swap update, после чего будущий kubelet slice становится владельцем actual runtime lifecycle.

| Stage | Contract | First implementation |
|---|---|---|
| Candidate | Scheduler receives a typed Pod snapshot with an empty `spec.nodeName`; an already-bound Pod is rejected as conflict/no-op. | `SchedulingSnapshot` carries Pod plus a frozen `Vec<NodeCandidate>` to prevent selection against shifting source data. |
| Filter | Every configured filter must accept a Node. | Explicit `NodeName` and node label selector filters. Resource capacity, affinities, taints, topology and volumes are deferred pending typed Node/resources APIs. |
| Score | Feasible nodes are scored; best candidate wins. | Typed scorer trait; default deterministic zero score with lexicographic node-name tie-break. |
| Bind | Selection becomes a single optimistic persistent mutation. | `PodBinder::bind(pod, node)` validates identity and resourceVersion, sets `nodeName` only if absent and returns `Conflict` if a competing scheduler wins. |
| Retry | Empty feasible set or CAS conflict must not fabricate assignment. | `Unschedulable` / `Conflict` result is requeued by controller runtime with bounded backoff. |

## API and persistence boundary

The scheduler does not perform a local-only bind and does not create generic objects. `PodBinding` is a typed command containing `namespace`, `name`, `node_name` and the resource version observed during filtering/scoring. In-memory and etcd Pod repositories expose `bind` methods; the etcd implementation performs a transactional compare against the stored mod-revision and writes a fresh opaque resourceVersion.

The REST `POST /api/v1/namespaces/{namespace}/pods/{name}/binding` compatibility endpoint is deferred until typed Binding API resource and dedicated RBAC subresource actions are added. The scheduler uses the same typed repository call directly, preserving the mutation and status invariants without an internal HTTP loop.

## Reliability and concurrency invariants

The scheduling cycle serializes selection per scheduler instance. The controller-runtime leader election provides exclusive active scheduler operation under HA, while Pod repository CAS protects against client or split-brain racing mutations. Source snapshots are re-read after any failed bind. Binding does not change Pod status: it remains `Pending` until node runtime reports a later transition.

## Explicit deferrals

Nodes REST/storage, resource requests and accounting, Reserve/Unreserve state, preemption, Permit, extenders, scheduler profiles, multiple schedulers, scheduling gates, taints/tolerations, affinity, topology spread, volumes and binding HTTP subresource are each independent compatibility slices. The first slice implements no fake feasible nodes; tests provide typed candidates through a source trait and prove no node assignment is persisted when every filter rejects.

## References

[1]: https://kubernetes.io/docs/concepts/scheduling-eviction/kube-scheduler/ "Kubernetes Scheduler"
[2]: https://kubernetes.io/docs/concepts/scheduling-eviction/scheduling-framework/ "Scheduling Framework"
[3]: https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/ "Pod v1 API"
