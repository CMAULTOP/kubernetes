# Phase 34 — Authorization API Direction Audit

**Status:** design and compatibility audit in progress

## Upstream contract findings

Kubernetes authorizes an authenticated request by evaluating normalized user, group, extra, resource/non-resource target, verb, namespace, API group, resource name, and subresource attributes. Authorization occurs before admission. When every configured authorizer has no opinion, the request is denied at the API boundary.[1]

`SelfSubjectAccessReview` is the correct first review API because its subject is always the authenticated caller. Its specification requires exactly one of `resourceAttributes` and `nonResourceAttributes`; its status is server-owned. The API is intended to let a user evaluate their own effective access.[2]

The next directly adjacent contract is cluster-scoped `SubjectAccessReview`. It uses the same target model and status model, but carries an explicit user, optional UID, groups, and extra attributes for the subject being evaluated. Upstream validation requires **at least one of user or groups**, rather than an always-present user.[3] `LocalSubjectAccessReview` adds namespace-path enforcement and defaulting, while `SelfSubjectRulesReview` requires permission enumeration rather than a single authorization decision.[4]

Upstream `SubjectAccessReview` is non-persistent: its REST implementation validates the object, converts its explicit spec into authorization attributes, obtains an allow / deny / no-opinion decision, then populates status in the returned object. The existing Rusternetes design already has the corresponding non-persistent, typed-handler shape. Upstream also permits resource field and label selector constraints; Rusternetes does not yet expose those attributes in its access-review types, so this must remain an explicit compatibility boundary until the authorization request model can preserve and safely evaluate them.[5] [6]

## Direction assessment

The existing Rusternetes sequence is structurally sound for a Rust-native Kubernetes-compatible control plane:

| Already established | Why it is foundational |
|---|---|
| Typed API objects and registry-driven discovery | Keeps wire schemas and discovery coupled to the server implementation. |
| Normalized `AuthorizationRequest` for resource and non-resource targets | Matches Kubernetes authorization attribute categories and avoids endpoint-specific policy evaluation. |
| Authentication middleware that injects `RequestIdentity` | Makes caller identity authoritative and prevents body/header identity spoofing in self-review. |
| RBAC evaluator with explicit no-opinion outcome | Preserves the distinction between `allowed: false, denied: false` and an explicit deny. |
| SelfSubjectAccessReview | Validates the complete review path without delegated-subject semantics. |

The recommended next vertical slice is **cluster-scoped `SubjectAccessReview`**. It maximizes reuse of the verified target/status evaluation path while introducing the Kubernetes-compatible delegated identity contract. `LocalSubjectAccessReview` should follow it because it reuses `SubjectAccessReviewSpec` and adds only namespace-path restrictions; `SelfSubjectRulesReview` should come later because complete rule enumeration cannot be modeled as a single `authorize()` call.

## Required Phase 34 invariants

1. `SubjectAccessReview` remains create-only and non-persistent.
2. The **caller** is authorized by normal middleware against `create` on `subjectaccessreviews`; the subject in the body is only the identity whose permissions are evaluated.
3. The endpoint requires at least one of `spec.user` or `spec.groups`, accepts explicit groups/UID/extra, and requires exactly one target kind.
4. The response type/status remains server-owned and does not echo a client-provided decision.
5. Every user-provided identity component is propagated exactly to the RBAC evaluator, with no fallback to the caller identity.
6. Tests cover allow, no-opinion, arbitrary-subject delegation protection through endpoint RBAC, non-resource targets, validation, discovery, and a real-etcd router path.

## References

[1]: https://kubernetes.io/docs/reference/access-authn-authz/authorization/ "Kubernetes authorization"
[2]: https://kubernetes.io/docs/reference/kubernetes-api/definitions/self-subject-access-review-v1-authorization/ "SelfSubjectAccessReview v1 API reference"
[3]: https://kubernetes.io/docs/reference/kubernetes-api/definitions/subject-access-review-v1-authorization/ "SubjectAccessReview v1 API reference"
[4]: https://kubernetes.io/docs/reference/kubernetes-api/definitions/local-subject-access-review-v1-authorization/ "LocalSubjectAccessReview v1 API reference"
[5]: https://raw.githubusercontent.com/kubernetes/kubernetes/master/pkg/registry/authorization/subjectaccessreview/rest.go "Upstream SubjectAccessReview REST implementation"
[6]: https://raw.githubusercontent.com/kubernetes/kubernetes/master/pkg/apis/authorization/validation/validation.go "Upstream authorization API validation"

## Selected vertical slice — SubjectAccessReview

`SubjectAccessReview` is selected as Phase 34. It is the smallest next step that exposes the Kubernetes delegated-authorization API without changing persistence, controller, or runtime behavior. It avoids an incorrect shortcut of treating a body subject as the authenticated HTTP caller, while deliberately postponing `LocalSubjectAccessReview` namespace binding and `SelfSubjectRulesReview` rule enumeration.

| Concern | Phase 34 design |
|---|---|
| Wire objects | Add `SubjectAccessReview` and `SubjectAccessReviewSpec`; retain existing self-review target types unchanged. |
| Subject model | Accept user, UID, groups, and extra; validate that user or groups is non-empty. A groups-only subject is valid. |
| Evaluated identity | Build a **hypothetical** `RequestIdentity` directly from the request body. Do not use `RequestIdentity::authenticated()`, because that constructor injects `system:authenticated`, which would alter the requested subject. |
| Target model | Reuse the existing resource/non-resource attributes and a single normalization helper. Exactly one target remains mandatory. |
| Caller protection | Existing middleware must authorize the authenticated caller to `create` cluster-scoped `authorization.k8s.io/subjectaccessreviews` before the handler evaluates the body subject. |
| Decision | Evaluate the explicit body subject using the existing `RbacAuthorizer`; map an RBAC miss to `allowed: false, denied: false`, matching the current no-opinion capability. |
| Persistence | No storage write, resource version, or watch event; return the request object with server-populated status. |
| Unsupported selector attributes | Do not accept fields that the current `AuthorizationRequest` cannot preserve safely. They remain a separately tracked compatibility expansion, rather than being ignored. |

The implementation will introduce no dependency: the established `serde`, `BTreeMap`, `RequestIdentity`, and `RbacAuthorizer` components cover schema decoding, identity representation, and policy evaluation.

### Test matrix

1. A caller with only `create subjectaccessreviews` can review a **different** permitted user, proving body subject and caller are distinct.
2. A caller with no `create subjectaccessreviews` is rejected before the delegated subject can be evaluated.
3. A groups-only subject is accepted and matches a group-bound RBAC rule without synthetic `system:authenticated` membership.
4. Resource and non-resource allow/no-opinion outcomes, `AlwaysAllow`, status ownership, target union, and subject-presence validation are covered through HTTP.
5. Discovery and normalized endpoint RBAC attributes are covered.
6. A real-etcd core server serves the non-persistent endpoint through the complete authn/authz/router path.

## Sources reviewed

The audit checked official Kubernetes authorization documentation and generated v1 API definitions, then the upstream `SubjectAccessReview` REST and handwritten validation sources. The latter establishes the non-persistent create-and-evaluate behavior and confirms that at least one user or group is required. See references [1]–[6].
