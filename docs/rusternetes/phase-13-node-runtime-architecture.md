# Rusternetes: Node registration и kubelet-facing runtime contracts

**Статус:** architecture contract до реализации
**Ветка:** `rusternetes/phase-1-api-storage`
**Срез:** Phase 13 — typed Node identity, registration и runtime handoff

## Цель

Этот срез добавляет typed control-plane contract между node agent и API/storage layers. Он создаёт и сохраняет Node identity/capacity/readiness, фильтрует Pods bound именно to that Node и передаёт их в explicit runtime adapter. Он не запускает контейнеры и не маркирует Pods `Running` без подтверждённого runtime outcome.

| Component | Contract | Initial implementation |
|---|---|---|
| `Node` | Cluster-scoped typed core/v1 resource with DNS name, labels, capacity and server-owned readiness status. | CRUD/repository contract first; full API discovery and HTTP surface added only with full Node slice. |
| `NodeRegistration` | Self-registration validates agent identity/name correspondence and never overwrites a different physical node's UID. | Typed command and repository CAS create/re-register semantics. |
| `NodeHeartbeat` | Node agent updates its own liveness observation; status and Lease remain explicitly distinct. | Typed heartbeat contract with clock source; external coordination Lease API deferred. |
| `BoundPodSource` | Lists only persisted Pods with `spec.nodeName == node_name`. | In-memory and etcd typed Pod store selectors; no generic resource scanning. |
| `RuntimeAdapter` | `sync_pod(RuntimePod)` returns typed observed outcome. | Trait boundary with an in-memory test adapter; CRI gRPC + OCI/containerd is a later vertical slice. |

## Safety rules

A runtime agent takes work only after Node registration has succeeded. A Pod bound to a different Node, or still unbound, is not presented to that agent. Node agents must use their own Node identity; identity mismatch fails closed. Runtime success is not inferred from assignment: a bound Pod remains `Pending` until a future status-subresource slice stores a validated report. Registration and heartbeat operations use optimistic resource version checks.

## Deferred boundaries

Node HTTP REST/discovery, Node RBAC subresources, Node authorizer / NodeRestriction, `coordination.k8s.io/v1` node Leases, status subresources, CRI v1 gRPC, containerd/CRI-O transport, OCI image lifecycle, cgroups, networking, volumes, probes, restart policy execution, logs/exec/port-forward and Node controller eviction are deliberately deferred. Each has independent authentication, persistence and operational safety requirements.

## References

[1]: https://kubernetes.io/docs/concepts/architecture/nodes/ "Kubernetes Nodes"
[2]: https://kubernetes.io/docs/reference/command-line-tools-reference/kubelet/ "kubelet reference"
[3]: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/ "Pod Lifecycle"
