# Rusternetes: RBAC authorization boundary

**Статус:** architecture contract до реализации  
**Ветка:** `rusternetes/phase-1-api-storage`  
**Срез:** Phase 7 — typed RBAC authorizer

## Scope

Authorization следует за authentication и получает только typed `RequestIdentity` плюс normalised API request attributes. Kubernetes authorizes `user`, `groups`, `extra`, API group, resource, subresource, namespace, name, path and Kubernetes verb; any request that no authorizer allows is denied with HTTP `403` [1]. RBAC rules are additive only: Role / ClusterRole supply rules, RoleBinding scopes grants to one namespace, and ClusterRoleBinding grants ClusterRole permissions cluster-wide [2].

The first executable slice adds static, in-memory policy objects for a full ConfigMap REST surface. It includes `Role`, `ClusterRole`, `RoleBinding`, `ClusterRoleBinding`, user / group / ServiceAccount subjects, core-group resource rules, wildcards and `resourceNames`. RBAC policy CRUD persistence, aggregation, escalation / bind checks, and SubjectAccessReview endpoints are separate follow-up slices; this code never claims they are implemented.

## Request classification and decision contract

| HTTP API request | Kubernetes attributes | Initial rule matching |
|---|---|---|
| `POST .../namespaces/{ns}/configmaps` | core group, `configmaps`, `create`, namespace `{ns}` | RoleBinding in `{ns}` and all matching ClusterRoleBindings. |
| `GET .../configmaps` | `list`, or `watch=true` becomes `watch` | Collection rule. A `resourceNames` rule does not grant an unnamed collection request in this slice. |
| `GET/PUT/DELETE .../configmaps/{name}` | `get` / `update` / `delete`, resource name | Allows a matching named rule only if `resourceNames` contains `{name}` or is empty. |
| discovery and version URLs | non-resource path, lowercased HTTP verb | ClusterRole non-resource URL rule; `/*` remains suffix-only glob. |

Rules must match group, resource with an optional `resource/subresource`, verb, namespace applicability and resource name. A RoleBinding referring to a ClusterRole continues to restrict that role's namespaced-resource rules to binding namespace, as documented by Kubernetes [2]. The RBAC authorizer has no implicit `system:masters` bypass: identities must be explicitly bound, so test and bootstrap policy remain auditable.

## Runtime modes

`AuthorizationMode::AlwaysAllow` preserves current development compatibility only. `AuthorizationMode::Rbac(policy)` is fail-closed: an unbound authenticated or anonymous identity receives a Kubernetes `403 Forbidden`; it never falls back to anonymous, memory storage or AlwaysAllow. The API Server middleware order is immutable: authentication → authorization → API handler → admission (future).

## Rust ecosystem decision

There is no selected mature Rust crate implementing Kubernetes' complete RBAC policy semantics as an embeddable control-plane authorizer. This slice is domain logic rather than a reinvention of TLS, HTTP, JWT, gRPC or storage protocols; it uses the existing Axum middleware, typed request identity and typed API status layers. A future Open Policy Agent or webhook authorizer is an additional authorizer implementation, not a replacement for the native RBAC evaluator.

## Proof

Tests exercise RoleBinding namespace boundaries, Group and ServiceAccount subject matching, wildcard rules, resource-name restrictions and non-resource patterns. A real TCP/HTTP test verifies that a bearer-authenticated identity can create ConfigMaps only in its bound namespace and receives typed `403 Forbidden` elsewhere.

## References

[1]: https://kubernetes.io/docs/reference/access-authn-authz/authorization/ "Kubernetes authorization"
[2]: https://kubernetes.io/docs/reference/access-authn-authz/rbac/ "Using RBAC Authorization"
