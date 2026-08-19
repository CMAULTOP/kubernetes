# Phase 33 SelfSubjectAccessReview Research

## Official contract

`SelfSubjectAccessReview` is a cluster-scoped, create-only `authorization.k8s.io/v1` resource that determines whether the currently authenticated caller may perform one requested action. Omitting `spec.resourceAttributes.namespace` means all namespaces. Kubernetes treats the self-review endpoint specially: users should be able to ask whether they may perform an action. [1] [2]

The request must specify **exactly one** of `spec.resourceAttributes` and `spec.nonResourceAttributes`. `status` is filled by the server. `status.allowed` is required; `status.denied` must not be true when allowed is true, while both false represents no authorizer opinion. [1] [3]

| Request variant | Required information | Rusternetes evaluation target |
|---|---|---|
| Resource review | `verb`, optional `group`, `resource`, optional `subresource`, `namespace`, `name` | Existing `AuthorizationRequest::resource` and `RbacAuthorizer` |
| Non-resource review | URL `path` and lower-case HTTP `verb` | Existing `AuthorizationRequest::non_resource` and `RbacAuthorizer` |

The upstream access-review types are non-persistent response objects. The API must return its decision in the created object’s `status`, rather than mutating stored state. [3]

## Compatibility and safety boundaries

The endpoint derives the reviewed subject only from the already authenticated request extension; it must reject any user/group/UID injection fields because this is a *self* review. Under `AuthorizationMode::Rbac`, evaluation calls the existing Rust RBAC authorizer using the caller identity and requested attributes. Under `AuthorizationMode::AlwaysAllow`, it reports `allowed: true`; no implicit fallback from RBAC denial to allow is permitted.

The user-facing POST itself still traverses the configured authorization middleware. It is registered as `authorization.k8s.io/selfsubjectaccessreviews`, verb `create`, enabling explicit RBAC control while retaining Kubernetes’s expected self-review API surface for callers that hold this permission.

## Sources

[1]: https://kubernetes.io/docs/reference/kubernetes-api/definitions/self-subject-access-review-v1-authorization/ "Kubernetes SelfSubjectAccessReview v1 API Reference"
[2]: https://kubernetes.io/docs/reference/access-authn-authz/authorization/ "Kubernetes Authorization"
[3]: https://raw.githubusercontent.com/kubernetes/api/master/authorization/v1/types.go "Upstream authorization/v1 Type Definitions"

## Local implementation design

The typed API will support resource attributes (`verb`, `group`, `version`, `resource`, optional `subresource`, `namespace`, `name`) and non-resource attributes (`path`, HTTP `verb`). Exactly one branch is required. The current slice does not claim selector-based access-review semantics: unimplemented selector attributes are rejected rather than silently ignored and possibly broadening a decision.

The handler obtains the reviewed identity solely from the authenticated request extension. It maps resource attributes to `AuthorizationRequest::resource` and non-resource attributes to `AuthorizationRequest::non_resource`. Existing `RbacAuthorizer::authorize` is the exclusive RBAC decision engine; it already implements additive role/binding matching with default deny. `AlwaysAllow` produces an allowed response only for review evaluation, preserving the configured behavior of direct requests.

The access-review endpoint itself remains a create request to `authorization.k8s.io/selfsubjectaccessreviews` and is passed through standard authorization middleware. In RBAC mode, a caller must be granted this create permission before it can perform a self-review. The review result then evaluates the *requested* target independently.

The registry will gain a named `authorization.k8s.io/v1` group with one cluster-scoped, create-only `selfsubjectaccessreviews` resource. Existing generic named-group discovery and resolver machinery added by the TokenReview slice will produce discovery and request attributes without a second routing framework.
