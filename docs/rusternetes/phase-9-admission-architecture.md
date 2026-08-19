# Rusternetes: admission pipeline и validating webhook boundary

**Статус:** architecture contract до реализации
**Ветка:** `rusternetes/phase-1-api-storage`
**Срез:** Phase 9 — ConfigMap validating admission

## Scope

Admission executes after authentication and authorization, before persistence. It applies to create, update and delete operations, but never to get, list or watch [1]. Kubernetes runs mutating admission first and validating admission second; a rejection in either phase immediately rejects the request [1]. This slice delivers a typed, in-process validating chain for ConfigMap create/update/delete and an explicit, isolated interface for future `AdmissionReview` webhook transport.

| Layer | Phase 9 behavior | Deferred behavior |
|---|---|---|
| Authentication | Typed `RequestIdentity` already established. | X.509, OIDC and service-account authenticators. |
| Authorization | RBAC permits or returns `403` before admission. | RBAC policy object persistence and escalation checks. |
| Mutating admission | No mutation is implemented; the chain reserves a dedicated preceding phase. | JSONPatch and MutatingAdmissionWebhook invocation. |
| Validation | Ordered in-process plugins receive typed UID, operation, resource, identity, new/old ConfigMap and dry-run attributes. The first rejection returns its typed Kubernetes error and no backend write occurs. | Validation webhooks' remote `AdmissionReview` transport. |
| Persistence | Runs only after validation succeeds. | Transactional policy/configuration storage. |

## Webhook boundary and dependency decision

Kubernetes validating webhooks receive JSON `admission.k8s.io/v1` `AdmissionReview` over HTTP POST and must return a response bearing the same request UID and an allow / deny decision [2]. They need short timeouts and their failures are subject to configured failure policy [2] [3]. Rusternetes does **not** make an outbound HTTP or TLS webhook call in this slice. Instead it introduces `AdmissionPlugin`, a typed async boundary whose future `AdmissionReview v1` transport can use mature existing `reqwest` / `rustls` crates rather than an invented client or TLS stack.

The first built-in validator is an executable, typed `NamespaceLifecyclePlugin`. It receives an `AdmissionRequest` with a generated UID, operation, core/v1 ConfigMap resource attributes, namespace/name, new/old typed ConfigMap values, `dry_run=false`, and authenticated `RequestIdentity`; the complementary `AdmissionResponse` contract retains the UID, allowed decision, typed status, and warnings. For `CREATE`, it rejects a missing namespace with Kubernetes `404 NotFound` and a terminating namespace with `403 Forbidden`, matching upstream namespace lifecycle checks. Updates and deletes pass lifecycle validation, as in Kubernetes [4]. Namespace state comes from a `NamespaceStateReader` boundary that future typed Namespace storage implements; the executable in-memory reader is for bootstrap and integration environments. The production semantics of arbitrary operational policy must be captured in a dedicated ValidatingAdmissionPolicy / webhook slice rather than embedded in handlers.

## Proof

Unit tests cover ordered chain rejection, typed response status propagation, and active/missing/terminating namespace lifecycle semantics. Real TCP/HTTP with the etcd backend proves an RBAC-authorized bearer request can be rejected by admission before persistence and that the denied ConfigMap never appears in etcd. A distinct request to an active namespace persists normally.

## References

[1]: https://kubernetes.io/docs/reference/access-authn-authz/admission-controllers/ "Admission Controllers Reference"
[2]: https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/ "Dynamic Admission Control"
[3]: https://kubernetes.io/docs/concepts/cluster-administration/admission-webhooks-good-practices/ "Admission Webhook Good Practices"
[4]: https://github.com/kubernetes/kubernetes/blob/b3bc2ac58fa173967f27ade80f28cc5015b8c1c3/staging/src/k8s.io/apiserver/pkg/admission/plugin/namespace/lifecycle/admission.go "Kubernetes NamespaceLifecycle admission implementation"
