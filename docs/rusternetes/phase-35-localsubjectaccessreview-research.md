# Phase 35 — LocalSubjectAccessReview contract and implementation design

**Author:** Manus AI
**Status:** implementation contract
**Scope:** `authorization.k8s.io/v1` `LocalSubjectAccessReview` only

## Upstream contract

`LocalSubjectAccessReview` is a **namespaced, create-only, non-persistent** review object. It checks whether an explicit user or group may perform a resource action in the namespace addressed by the request URL. Its namespace-scoped API surface permits namespace-scoped policy to grant the ability to ask the question.[1] [2]

The resource shares `SubjectAccessReviewSpec`: at least one of `user` or `groups` is required, while exactly one review target is normally required. The local variant narrows that shared contract: a non-resource target is forbidden; the object metadata may contain only its namespace; and `resourceAttributes.namespace`, when supplied, must match the request namespace. Kubernetes documents that an empty resource namespace is defaulted to the request namespace.[1] [3]

> The upstream REST storage has no backing store. It validates the object, obtains the namespace from the request context, evaluates authorization attributes, populates the server-owned status, and returns the object.[2]

## Rusternetes Phase 35 invariants

| Concern | Decision |
|---|---|
| Endpoint | `POST /apis/authorization.k8s.io/v1/namespaces/:namespace/localsubjectaccessreviews` |
| Discovery | `localsubjectaccessreviews`, `Namespaced`, `create` only |
| Caller authorization | The normal middleware evaluates the authenticated caller against `create authorization.k8s.io/localsubjectaccessreviews` **in the URL namespace**. |
| Evaluated subject | The handler builds the hypothetical identity solely from `spec.user`, `spec.uid`, `spec.groups`, and `spec.extra`; it never substitutes the caller. |
| Namespace binding | The path namespace becomes `metadata.namespace`. A conflicting body metadata namespace is rejected. An empty `resourceAttributes.namespace` is defaulted to the path namespace; a conflicting non-empty value is rejected. |
| Target restriction | Only `resourceAttributes` is valid. `nonResourceAttributes` is rejected because a local review cannot evaluate a non-resource URL. |
| Metadata and status | Metadata is otherwise empty and status is server-owned. The review is never stored and emits no watch event. |
| Decision model | `AlwaysAllow` returns `allowed: true`. The additive RBAC evaluator returns `allowed: true` on a matching binding and `allowed: false, denied: false` for no opinion. |
| Selector boundary | Current review attributes do not expose field/label selector constraints. These fields are intentionally not accepted until the authorizer request model can preserve and evaluate them safely. |

## Test requirements

The slice must prove all of the following: correct typed metadata canonicalization; path namespace defaulting; mismatch rejection; non-resource rejection; namespace-scoped endpoint authorization; delegated user evaluation; groups-only identity; server-owned status; typed discovery; `AlwaysAllow`; and a complete real-etcd core-router request. No dependency is needed: the existing typed serde model, Axum routing, `RequestIdentity`, and `RbacAuthorizer` provide the required mechanisms.

## References

[1]: https://kubernetes.io/docs/reference/kubernetes-api/definitions/local-subject-access-review-v1-authorization/ "Kubernetes LocalSubjectAccessReview v1 reference"
[2]: https://raw.githubusercontent.com/kubernetes/kubernetes/master/pkg/registry/authorization/localsubjectaccessreview/rest.go "Kubernetes LocalSubjectAccessReview REST implementation"
[3]: https://raw.githubusercontent.com/kubernetes/kubernetes/master/pkg/apis/authorization/validation/validation.go "Kubernetes authorization validation"
[4]: https://raw.githubusercontent.com/kubernetes/api/master/authorization/v1/types.go "Kubernetes authorization v1 API types"
