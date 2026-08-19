# Phase 31 Pod Finalizer Deletion Research

## Official Kubernetes behavior

For any object, including a Pod, a DELETE request against an object that has `metadata.finalizers` does not physically remove it immediately. The API server sets `metadata.deletionTimestamp`, leaves the object in a terminating state, and returns HTTP `202 Accepted`. Controllers then remove their own finalizers; when the finalizer list becomes empty, Kubernetes physically deletes the object. [1]

After deletion is requested, Kubernetes restricts mutation: existing finalizers may be removed, but new finalizers may not be added, and `deletionTimestamp` cannot be altered. The object cannot be resurrected; an equivalent replacement must be created only after physical deletion. [1]

The public finalizer workflow demonstrates normal `PATCH` removal of `metadata.finalizers` on a terminating object. The special `/finalize` subresource is a Namespace-specific escape hatch, not the compatibility path for Pods. Therefore this slice deliberately completes a terminating Pod through its standard `PUT` / `PATCH /api/v1/namespaces/{namespace}/pods/{name}` route rather than exposing a nonstandard Pod `/finalize` route. [2]

The Pod API declares status system-populated/read-only on the main resource. Normal Pod finalizer completion must preserve live status; only the existing `/status` route may modify status. [3]

## Required Rusternetes contract

| Transition | HTTP/storage behavior | Watch behavior |
|---|---|---|
| DELETE Pod without finalizers | Physical deletion; current success response behavior | `DELETED` |
| DELETE Pod with finalizers | Set immutable RFC3339 `metadata.deletionTimestamp`, preserve resource identity/status, return `202 Accepted` | `MODIFIED` |
| PUT/PATCH while deletion pending | Permit only finalizer removal; reject added finalizers and changes to desired state, identity, status or deletion timestamp | No event on rejection |
| Last finalizer removed | Delete atomically, return the resulting Pod representation, then absent from GET/LIST | `DELETED` |
| Finalizer remains | Persist modified finalizer list under CAS | `MODIFIED` |

## Local implementation design

The existing Namespace lifecycle is the verified local pattern. Pod deletion will use the same existing `DeleteResult` shape: the HTTP handler can determine whether a first delete is staged from the pre-delete Pod finalizer state and choose `202` versus `200`, so no shared response-type expansion is required.

The typed Pod guard will validate a deletion-pending update before server metadata is restored. It will require an identical type meta, identity, UID, generation, creation timestamp, deletion timestamp, labels, annotations, owner references, spec and status; only `metadata.finalizers` may change, and it must be a subset of the previous list. The corresponding preservation method will restore all stored metadata then retain this request's reduced finalizer list.

The in-memory update path will emit `MODIFIED` while finalizers remain and remove the key plus emit `DELETED` on the final removal. The etcd path will select an atomic mod-revision CAS `put` or `delete` transaction under the same condition. A delete against a finalizer-bearing Pod will CAS-persist `deletionTimestamp`, and a repeated DELETE while it remains pending will be idempotent.

## Sources

[1]: https://kubernetes.io/docs/concepts/overview/working-with-objects/finalizers/ "Kubernetes Finalizers"
[2]: https://kubernetes.io/blog/2021/05/14/using-finalizers-to-control-deletion/ "Using Finalizers to Control Deletion"
[3]: https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/ "Kubernetes Pod v1 API Reference"
