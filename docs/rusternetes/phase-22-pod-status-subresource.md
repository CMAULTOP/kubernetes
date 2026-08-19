# Phase 22: Pod `/status` Subresource

This slice adds the Kubernetes core/v1 Pod status subresource at `GET` and `PUT /api/v1/namespaces/:namespace/pods/:name/status`. It separates actual-state reporting from desired-state mutation while retaining the Pod’s single persistence revision and ordinary Pod watch stream. Kubernetes identifies subresources as URI paths below a resource and gives each subresource its own supported verb set; the discovery registry therefore exposes `pods/status` with `get` and `update`. [1]

> A successful status mutation changes only the typed `PodStatus` projection. The stored Pod type metadata, all server-owned object metadata, and the full `PodSpec` are restored from the current persisted object before the replacement is committed.

| Concern | Implemented contract |
|---|---|
| HTTP API | The namespaced `GET` and `PUT` status routes return the full typed `Pod`, matching the existing Node status route pattern. The request object must identify the same namespace/name as the URI. |
| Discovery and RBAC attributes | `ApiRegistry::core_v1()` publishes `pods/status`; request resolution reports resource `pods`, subresource `status`, and the `update` verb for authorization. |
| Status projection | The present typed `PodStatus` carries the Kubernetes phase summary. The lifecycle model treats phase as a high-level summary rather than a complete state machine. [2] |
| Isolation | A `/status` request cannot alter containers, node assignment, labels, UID, creation timestamp, or any other desired/server-owned field. Only the inbound status survives the write. |
| Optimistic concurrency | The same `metadata.resourceVersion` protects both main-resource and status mutations. The in-memory store requires an exact version; the etcd repository accepts empty version only after reading the current object but always commits using an etcd `mod_revision` compare. A stale explicit version returns Kubernetes `409 Conflict`. [1] |
| Watch propagation | Successful status changes append a standard `MODIFIED` event to bounded in-memory history and flow through the durable etcd watch bridge, so ordinary Pod watchers observe the updated status and revision. |
| Durability | `EtcdPodRepository::update_status` uses a read/validate/compare-and-swap transaction. A failed compare writes nothing and is surfaced as `Conflict`; a committed value receives the etcd response revision as its opaque `resourceVersion`. |
| Admission boundary | The current admission chain remains on desired-state Pod create/update/delete paths. Status is deliberately committed via the dedicated status operation, avoiding mutation of the submitted desired state. |

## Compatibility boundary

The API preserves the current Rusternetes typed Pod model: `PodStatus.phase` is accepted and returned, and the normal create path initializes it to `Pending`. The upstream API has a richer status schema, including conditions and container state; those fields will be added only together with typed validation and kubelet/runtime ownership rules. This slice does not claim to interpret a phase as a complete container lifecycle state, consistent with Kubernetes documentation. [2]

## Verification coverage

| Test layer | Proof |
|---|---|
| In-memory storage | A status write preserves `PodSpec`, advances `resourceVersion`, emits `MODIFIED` to a watch subscriber, and rejects a stale version. |
| API-server black box | Real HTTP create plus `PUT /status` preserves the original spec, returns `Running`, advances the revision, and returns `409` for the original stale object. |
| Registry | Core discovery contains the namespaced `pods/status` entry and path resolution yields the expected name, namespace, and subresource. |
| Real etcd integration | A full API-server request writes a status update through `core_backend_from_etcd_config`; a direct `EtcdPodRepository` read returns exactly the persisted response and confirms spec isolation. |

No dependencies were added for this slice. The implementation uses the existing typed storage, Axum routing, etcd client, and watch infrastructure rather than duplicating protocols or introducing an alternate persistence path.

## References

[1]: https://kubernetes.io/docs/reference/using-api/api-concepts/ "Kubernetes API Concepts"
[2]: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/ "Kubernetes Pod Lifecycle"
[3]: https://github.com/kubernetes/kubernetes/blob/b3bc2ac58fa173967f27ade80f28cc5015b8c1c3/pkg/registry/core/pod/strategy.go "Kubernetes upstream Pod strategy"
