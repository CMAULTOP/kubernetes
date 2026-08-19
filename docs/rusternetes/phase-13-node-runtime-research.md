# Kubelet-facing node runtime и registration: official compatibility research

**Ветка:** `rusternetes/phase-1-api-storage`
**Дата сверки:** 2026-08-19

Kubernetes Nodes may be manually created or self-registered by kubelets. The Node name is a DNS subdomain and represents one physical/logical instance; a changed replacement node must be removed and re-registered rather than silently mutating an assumed identical object [1]. A kubelet is the primary node agent: it consumes PodSpecs, normally from the API server, and ensures Kubernetes-created containers are running and healthy [2].

| Contract | Initial Rusternetes boundary |
|---|---|
| Node registration | Typed node registration request has DNS-valid name, immutable registration labels and capacity. The authenticated node identity may create/update only its own node in the future Node authorizer slice. |
| Node health | Node status and `kube-node-lease` heartbeat are separate signals [1]. First slice defines typed heartbeat and status contracts but does not yet expose the complete coordination Lease API. |
| Bound Pod discovery | Node runtime receives only Pods where `spec.nodeName` matches its registered node name. An empty `nodeName` stays scheduler-owned. |
| Runtime desired/actual convergence | A runtime adapter converts accepted bound PodSpec into a typed `RuntimePod` request. The CRI transport and OCI execution are deferred; no fake successful container run is emitted. |
| Pod lifecycle report | Pod status is system owned. `Pending` includes waiting for schedule or image setup; `Running` requires binding and created containers [3]. First slice supports typed status report validation, but persistence `/status` is a later subresource vertical slice. |

The kubelet self-registration pattern uses credentials, a node identity, node labels/capacity and periodic status updates [1]. Kubernetes Node authorization normally constrains kubelets to their own Node resource [1], and will inform later authz integration. Pods are bound only once in their lifetime; a Pod UID is not rescheduled to a different Node [3].

## References

[1]: https://kubernetes.io/docs/concepts/architecture/nodes/ "Kubernetes Nodes"
[2]: https://kubernetes.io/docs/reference/command-line-tools-reference/kubelet/ "kubelet reference"
[3]: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/ "Pod Lifecycle"
