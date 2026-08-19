# Pod и Namespace REST vertical slice: official compatibility research

**Ветка:** `rusternetes/phase-1-api-storage`
**Дата сверки:** 2026-08-19

Kubernetes core/v1 exposes Pods as namespace-scoped resources and Namespaces as cluster-scoped resources. A namespaced collection is addressable both through `/api/v1/pods` (across namespaces) and `/api/v1/namespaces/{namespace}/pods`; a Namespace collection uses `/api/v1/namespaces` [1]. Object identity is API group + resource + namespace + name for namespaced resources; cluster-scoped resources omit namespace [1].

| Resource | Scope | Initial REST surface | First typed data boundary |
|---|---|---|---|
| `Pod` | Namespace | POST / GET / LIST / PUT / DELETE and WATCH, including all-namespaces list/watch. | `PodSpec { containers, restart_policy, node_name }`, `PodStatus { phase }`, typed `Container` with required name/image. |
| `Namespace` | Cluster | POST / GET / LIST / PUT / DELETE and WATCH. | `NamespaceSpec`, `NamespaceStatus { phase }`, initialized as `Active`; a later finalization slice owns transition to `Terminating`. |

A Pod contains desired `spec` and system-populated `status`; its status is read-only in the Kubernetes contract [2]. Therefore the first slice accepts the resource's full typed JSON representation but rejects client-supplied status changes and only initializes persisted `PodStatus.phase` to `Pending`. It does **not** claim container execution, scheduling, kubelet reporting, `/status`, `/binding`, or lifecycle transition support. Pod phases are a high-level summary rather than a comprehensive state machine; `Pending` represents an accepted pod that is not fully started, including before scheduling [3].

Namespace creation adds Kubernetes' immutable `kubernetes.io/metadata.name` label whose value is its name [4]. The first slice must create the `default` Namespace at backend initialization and use the Namespace repository as the concrete `NamespaceStateReader` supplied to `NamespaceLifecyclePlugin`. This replaces the phase-9 static source for production router construction, so ConfigMap and Pod creates in a missing namespace get typed `404 NotFound`, while `Terminating` generates `403 Forbidden` [5].

| Explicitly deferred | Reason |
|---|---|
| PATCH and field-management | Existing project boundary; requires semantic merge/apply behavior. |
| `/status`, `/binding`, eviction, logs and exec subresources | Each has distinct authorization and persistence contracts. |
| Pod scheduling, binding, kubelet/CRI execution and status mutation | Requires later scheduler and node-runtime vertical slices. |
| Namespace finalization and cascading deletion | Requires durable finalizer workflow and collection-delete semantics. |
| Pagination / continue tokens, consistency options, additional encodings | Existing API slice intentionally supports JSON and current WATCH semantics only. |

## References

[1]: https://kubernetes.io/docs/reference/using-api/api-concepts/ "Kubernetes API Concepts"
[2]: https://kubernetes.io/docs/reference/kubernetes-api/core/pod-v1/ "Kubernetes core/v1 Pod API"
[3]: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/ "Pod Lifecycle"
[4]: https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/ "Namespaces"
[5]: https://github.com/kubernetes/kubernetes/blob/b3bc2ac58fa173967f27ade80f28cc5015b8c1c3/staging/src/k8s.io/apiserver/pkg/admission/plugin/namespace/lifecycle/admission.go "Kubernetes NamespaceLifecycle upstream source"
