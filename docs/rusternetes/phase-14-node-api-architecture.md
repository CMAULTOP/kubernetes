# Rusternetes: durable Node API, registry и runtime registration integration

**Статус:** architecture contract до реализации
**Ветка:** `rusternetes/phase-1-api-storage`
**Срез:** Phase 14 — Node persistence/API parity

## Цель

This slice promotes the typed Node registration model from an in-process contract to a durable cluster-scoped resource. It adds API registry discovery, in-memory and etcd repository parity, core/v1 HTTP routes and explicit Node registration/heartbeat integration. It does not claim the complete upstream Node API or node authorizer.

| Surface | Contract |
|---|---|
| Discovery | `nodes` is core/v1, cluster scoped and advertises GET/LIST/POST/PUT/DELETE/WATCH as each implementation becomes executable. |
| Storage | In-memory and etcd stores use create-if-absent, resourceVersion CAS updates, bounded watch replay and typed errors. No etcd configuration may fall back to memory. |
| Registration | The Node agent creates only a Node with its own identity. The public API initially uses existing RBAC `nodes` resource authorization; Node authorizer/NodeRestriction are separately deferred. |
| Runtime | The in-memory adapter used by node runtime is replaced by a `NodeBackend` dispatch with same behavior on durable etcd. Bound Pod discovery is backend-specific and never returns Pods assigned to another Node. |
| Status | Generic registration endpoint accepts desired Node object only; kubelet heartbeat/status uses internal typed contract. `/status` remains deferred until dedicated subresource authorization and field ownership exist. |

## Mutation and concurrency invariants

A Node creation is a CAS create. A replacement/heartbeat begins from an observed opaque resourceVersion; mismatched revision returns `409 Conflict`. A Node name identifies the same node identity, so registration cannot silently overwrite a different UID. The initial registration allows server population of readiness while API client creation cannot set it. The etcd prefix is distinct from Pods/Namespaces and encoded key components reject slash, NUL and traversal ambiguity.

## Explicit boundaries

The exact upstream `v1.Node` capacity/allocatable/conditions/addresses schema, Node status subresource, Node authorization, NodeRestriction admission, node leases, cloud provider population, taints and full watch plumbing are all excluded unless implemented and tested on both storage paths. This slice connects the current Node runtime without converting it into a wrapper around an in-memory fake.

## References

[1]: https://kubernetes.io/docs/concepts/architecture/nodes/ "Kubernetes Nodes"
[2]: https://kubernetes.io/docs/reference/command-line-tools-reference/kubelet/ "kubelet reference"
