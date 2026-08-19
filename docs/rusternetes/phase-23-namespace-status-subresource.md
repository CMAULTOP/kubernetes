# Phase 23: Namespace `/status` Subresource

This slice implements `GET` and `PUT /api/v1/namespaces/:name/status` as the cluster-scoped core/v1 Namespace status subresource. It reports and updates actual namespace lifecycle state independently from Namespace finalizers and server-owned metadata. The Kubernetes Namespace API defines `phase` as lifecycle state with the `Active` and `Terminating` values, while conditions are a separate richer observation field. [1]

> A `/status` replacement preserves the persisted Namespace `spec`, including finalizers; preserves all persisted metadata, including UID, labels, creation timestamp, generation and name label; and accepts only the inbound `NamespaceStatus` projection.

| Concern | Implemented contract |
|---|---|
| API and discovery | `namespaces/status` is advertised as a cluster-scoped `Namespace` subresource supporting `get` and `update`. Its URI is `GET`/`PUT /api/v1/namespaces/:name/status`. [2] |
| Typed status model | The current Rusternetes `NamespaceStatus` retains the Kubernetes lifecycle `phase`, initially `Active` when a Namespace is created. The `Terminating` transition is accepted through `/status`. [1] |
| Isolation | A status body cannot mutate Namespace `spec.finalizers`, labels, identity, UID, timestamps, generation, or resource version other than the server-assigned next version. |
| CAS semantics | In-memory storage requires an exact shared `resourceVersion`. The etcd backend reads the live record and commits with an etcd `mod_revision` comparison, returning `409 Conflict` for stale explicit versions or racing transactions. [2] |
| Watch visibility | A successful status mutation produces the normal `MODIFIED` Namespace watch event. This remains visible through bounded in-memory history and the durable etcd watch bridge. |
| Persistence | `EtcdNamespaceRepository::update_status` writes exactly the isolated result in a transaction and returns the response revision as the opaque Kubernetes resource version. |
| Existing lifecycle admission | Namespace create, desired-state update and deletion retain their established admission paths. Status uses the dedicated actual-state write path and cannot alter submitted desired state. |

The slice does not add dependencies. It reuses the established typed objects, atomic stores, Axum API routing, registry resolution, etcd transactions and watch infrastructure.

## Verification coverage

| Layer | Verification |
|---|---|
| HTTP black box | Create a Namespace, submit a `/status` body attempting a finalizer mutation, assert `Terminating` is stored, `spec` remains unchanged, version advances, and the original object produces `409 Conflict`. |
| Real etcd | Exercise the API server with `core_backend_from_etcd_config`, read back through `EtcdNamespaceRepository`, and assert the direct durable value equals the API response. |
| Registry | Core discovery resource count and typed registry validation cover the new cluster-scoped subresource declaration. |
| Compilation | Focused type, in-memory, etcd repository and API server compilation succeeds before the full workspace quality gate. |

## References

[1]: https://kubernetes.io/docs/reference/kubernetes-api/core/namespace-v1/ "Kubernetes Namespace v1 API Reference"
[2]: https://kubernetes.io/docs/reference/using-api/api-concepts/ "Kubernetes API Concepts"
[3]: https://github.com/kubernetes/kubernetes/blob/b3bc2ac58fa173967f27ade80f28cc5015b8c1c3/pkg/registry/core/namespace/strategy.go "Kubernetes upstream Namespace strategy"
