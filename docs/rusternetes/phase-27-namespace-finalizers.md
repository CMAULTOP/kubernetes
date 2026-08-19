# Phase 27: Namespace Deletion, `deletionTimestamp`, and Finalizers

This slice implements staged deletion for core/v1 Namespaces. It replaces the earlier fail-closed rejection of finalizer-bearing Namespace deletes with a real lifecycle that is executed in the typed API server, in-memory store, and durable etcd repository.

Kubernetes finalizers delay physical deletion. A DELETE request for an object with finalizers sets `metadata.deletionTimestamp`, returns HTTP `202 Accepted`, and retains the resource while controllers complete cleanup. The resource is finally removed after relevant finalizer lists have been emptied. [1]

| Operation | Preconditions and outcome |
|---|---|
| `DELETE /api/v1/namespaces/:name` with no finalizers | The Namespace is removed atomically and the API returns `200 OK`. |
| `DELETE /api/v1/namespaces/:name` with `metadata.finalizers` or `spec.finalizers` | The backend atomically sets a server-generated `metadata.deletionTimestamp`, changes `status.phase` to `Terminating`, advances `resourceVersion`, emits `MODIFIED`, and the API returns `202 Accepted`. |
| Repeated DELETE while terminating | The resource remains deletion-pending; the operation returns the current resource version without changing the timestamp. |
| `PUT /api/v1/namespaces/:name/finalize` | Requires a deletion-pending Namespace and an optimistic-concurrency match. The Namespace `spec.finalizers` list may only lose existing entries; no additions, status changes, identity changes, or deletionTimestamp changes are accepted. |
| Final finalizer removal | When both `metadata.finalizers` and `spec.finalizers` are empty, the storage operation atomically deletes the Namespace and emits `DELETED`. Subsequent GET returns `404 Not Found`. |

> Once deletion is requested, `metadata.deletionTimestamp` cannot be changed. A deletion-pending Namespace is also protected from adding metadata finalizers; the ordinary update route can only remove existing metadata finalizers in that state. This follows Kubernetes’s one-way finalization model. [1]

## Storage and concurrency contract

| Backend | Transition mechanism | Conflict behavior | Watch behavior |
|---|---|---|---|
| In-memory | One write lock serializes lookup, resource-version verification, mutation and event publication. | A supplied stale `resourceVersion` returns `409 Conflict`. | Deletion request produces `MODIFIED`; completion produces `DELETED`; history remains bounded and slow consumers are removed. |
| etcd | Each mutation uses a transaction comparing the key’s `mod_revision`, then performs a put for pending deletion or a delete for final completion. | A failed compare returns `409 Conflict`; no blind write or delete path exists. | etcd watch translates the transactional put/delete into `MODIFIED` and `DELETED` resource events. |

The core/v1 discovery registry now publishes the cluster-scoped `namespaces/finalize` subresource with the `update` verb. The resolver was extended carefully because the path shape `/api/v1/namespaces/:name/finalize` overlaps syntactically with namespaced resource collection paths. It first recognizes a registered Namespace subresource and otherwise preserves namespaced collection resolution.

No new dependencies are introduced. The implementation uses the existing audited components: `time = 0.3.36` for UTC lifecycle timestamps, Tokio synchronization for in-memory linearization, and `etcd-client = 0.16.0` for durable compare-and-swap transactions. All are workspace-pinned, mature, compatible with `serde 1.0.217`, and available under permissive licenses documented in prior dependency audits.

## Verification

The verification matrix covers the lifecycle vertically.

| Test layer | Coverage |
|---|---|
| Typed/in-memory storage | A finalizer-bearing Namespace DELETE creates a durable Terminating snapshot, emits `MODIFIED`, and finalization of the last finalizer removes it with `DELETED`. |
| API-server black-box | Validates `202 Accepted`, observable `deletionTimestamp`, Terminating phase, `PUT /finalize`, and `404` after final deletion. |
| Registry | Validates discovery data and resolution for `/api/v1/namespaces/:name/finalize` without regressing namespaced Pod collection resolution. |
| Real etcd HTTP integration | Validates durable deletionTimestamp/Terminating persistence after DELETE and physical key removal after `/finalize`. |

## References

[1]: https://kubernetes.io/docs/concepts/overview/working-with-objects/finalizers/ "Kubernetes Finalizers"
[2]: https://kubernetes.io/docs/reference/using-api/api-concepts/ "Kubernetes API Concepts"
[3]: https://github.com/kubernetes/kubernetes/blob/b3bc2ac58fa173967f27ade80f28cc5015b8c1c3/pkg/registry/core/namespace/strategy.go "Kubernetes upstream Namespace strategy"
[4]: https://github.com/kubernetes/kubernetes/blob/b3bc2ac58fa173967f27ade80f28cc5015b8c1c3/pkg/registry/core/namespace/storage/storage.go "Kubernetes upstream Namespace storage"
