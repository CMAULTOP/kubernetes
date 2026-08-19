# Rusternetes: typed core/v1 Pod и Namespace API

**Статус:** architecture contract до реализации
**Ветка:** `rusternetes/phase-1-api-storage`
**Срез:** Phase 10 — Pod и Namespace CRUD/LIST/WATCH

## Цель и compatibility scope

Этот срез добавляет реальные typed core/v1 ресурсы `Pod` и `Namespace` во все уже исполняемые слои: `api-types`, in-memory storage, etcd storage, registry/discovery, RBAC path normalization, admission и HTTP API. Pod является namespace-scoped объектом; Namespace — cluster-scoped. Поэтому список Pod доступен как по all-namespaces `/api/v1/pods`, так и в namespace `/api/v1/namespaces/{namespace}/pods`, а Namespace — по `/api/v1/namespaces` и `/api/v1/namespaces/{name}` [1].

| Resource | Typed stored fields | Server defaulting | Initial HTTP surface |
|---|---|---|---|
| `Pod` | `metadata`, `PodSpec { containers, restartPolicy, nodeName, schedulerName }`, `PodStatus { phase }`; container includes `name`, `image`, `command`, `args`, `workingDir`. | `apiVersion=v1`, `kind=Pod`, UID, timestamps, generation, resourceVersion; `restartPolicy=Always`; `status.phase=Pending`. | POST, GET, all-namespace and namespace LIST/WATCH, PUT, DELETE. |
| `Namespace` | `metadata`, `NamespaceSpec { finalizers }`, `NamespaceStatus { phase }`. | `apiVersion=v1`, `kind=Namespace`, UID, timestamps, generation, resourceVersion; active phase; immutable `kubernetes.io/metadata.name` label. | POST, GET, LIST/WATCH, PUT, DELETE. |

`Pod.status` is system-populated and read-only in the Kubernetes resource contract [2]. The initial handler refuses a client-provided non-empty Pod status and persistence preserves status on ordinary PUT. The first admission-protected create starts each Pod in `Pending`, the lifecycle value defined for an accepted Pod before it has been fully started or scheduled [3]. This slice does not claim runtime execution or Pod status reporting.

## Storage and concurrency contract

The storage interface remains strongly typed; there is no generic object store. `InMemoryPodStore` and `InMemoryNamespaceStore` mirror the current ConfigMap store's one-lock linearization, bounded history, selector filtering, replay, bookmark and slow-consumer cleanup. `EtcdPodRepository` and `EtcdNamespaceRepository` mirror its transactional compare-and-swap operations and etcd revision resourceVersions. Namespace keys are cluster-scoped; Pod keys preserve namespace/name key layout. A configured etcd server constructs all three etcd repositories or fails startup: there is no per-resource in-memory fallback.

| Operation | Required behavior |
|---|---|
| Create | Validates type and resource fields, returns `409` on duplicate, assigns server metadata, writes `ADDED`. Namespace injects its immutable name label. |
| Update | Reads and CAS-checks an explicit resourceVersion, returns `404` / `409` as appropriate, preserves server-owned metadata and Pod status, and writes `MODIFIED`. Pod identity/spec container set is immutable in this pre-scheduler slice. |
| Delete | Applies typed preconditions atomically and writes `DELETED`. Namespace finalization and cascading deletion are deliberately not implied. |
| List / Watch | Reuses existing labels, indexed metadata field selectors, opaque resourceVersion, bounded replay and `410 Expired` semantics. |

## Router and admission order

API routing remains `authentication → RBAC → typed handler admission → persistence`. The registry path resolver must distinguish `/api/v1/namespaces/{name}` (cluster-scoped Namespace) from `/api/v1/namespaces/{namespace}/pods` (namespace-scoped Pod). Discovery lists both resources with their correct `namespaced` flags.

The production API state creates `NamespaceLifecyclePlugin` with its durable typed Namespace repository, replacing the phase-9 static test reader. Consequently, authenticated and RBAC-authorized ConfigMap or Pod create requests see the real namespace phase before storage: missing namespace returns `404`, terminating namespace returns `403`, and active namespace permits persistence [4]. Namespace creation itself is not namespace-scoped and bypasses this lookup.

## Explicit exclusions

The following are intentionally absent rather than superficially accepted: PATCH, `/status`, `/binding`, eviction, logs/exec/attach/port-forward, Pod scheduling, Node/kubelet runtime execution, finalizer-driven Namespace deletion, pagination/continue tokens and non-JSON encodings. Each requires its own authorization, state transition and/or transport contract.

## Proof plan

Unit tests cover typed validation/defaulting, path resolver scope ambiguity, storage conflict/precondition/list-watch contracts, and NamespaceLifecycle against live repository state. HTTP integration covers Namespace creation, immutable metadata label, Pod creation in active namespace, missing/terminating rejection before both memory and etcd storage, namespace/all-namespace Pod list, durable watch replay, optimistic update and typed error status.

## References

[1]: https://kubernetes.io/docs/reference/using-api/api-concepts/ "Kubernetes API Concepts"
[2]: https://kubernetes.io/docs/reference/kubernetes-api/core/pod-v1/ "Kubernetes core/v1 Pod API"
[3]: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/ "Pod Lifecycle"
[4]: https://github.com/kubernetes/kubernetes/blob/b3bc2ac58fa173967f27ade80f28cc5015b8c1c3/staging/src/k8s.io/apiserver/pkg/admission/plugin/namespace/lifecycle/admission.go "Kubernetes NamespaceLifecycle admission implementation"
