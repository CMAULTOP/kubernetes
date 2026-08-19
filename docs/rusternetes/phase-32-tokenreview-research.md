# Phase 32 TokenReview Research

## Official API contract

`authentication.k8s.io/v1` TokenReview attempts to authenticate an opaque bearer token. The request has required `spec.token` and optional `spec.audiences`; the server fills `status`, rather than treating an unauthenticated token as an HTTP authentication error. [1]

An audience-aware authenticator accepts only a token intended for at least one requested audience. On success, `status.audiences` must contain compatible audience values. When no request audience is specified, an empty successful `status.audiences` means the token is valid for the Kubernetes API server audience. [1] [2]

`status.authenticated` indicates whether the token maps to a known user. On successful authentication `status.user` contains username, UID, groups and optional extra attributes; `status.error` is reserved for a token that could not be checked. [1] [3]

The upstream type is non-namespaced and create-only, served as `POST /apis/authentication.k8s.io/v1/tokenreviews`. [3]

## ServiceAccount compatibility requirements

The existing Rusternetes ServiceAccount JWT verifier already checks signature, issuer, expiry, requested audience and live ServiceAccount / Pod / Node bindings. TokenReview must call this same verifier path, fail closed for disabled/unconfigured JWT validation, and report failed validation with `authenticated: false` rather than leaking JWT parser or signature detail. Valid ServiceAccount identities use `system:serviceaccount:{namespace}:{name}`, with the standard `system:serviceaccounts` and `system:serviceaccounts:{namespace}` groups. [2]

## Sources

[1]: https://kubernetes.io/docs/reference/kubernetes-api/definitions/token-review-v1-authentication/ "Kubernetes TokenReview v1 API Reference"
[2]: https://kubernetes.io/docs/reference/access-authn-authz/authentication/ "Kubernetes Authentication"
[3]: https://raw.githubusercontent.com/kubernetes/api/master/authentication/v1/types.go "Upstream authentication/v1 TokenReview Type Definitions"

## Local implementation design

The slice will add typed `TokenReview`, `TokenReviewSpec`, `TokenReviewStatus` and `UserInfo` types with fixed `authentication.k8s.io/v1` / `TokenReview` type metadata. Request validation will reject an empty `spec.token` and client-supplied non-empty `status`; `status` remains server-owned.

A TokenReview must be callable by an already authenticated and authorized caller, but its `spec.token` is **not** used as the HTTP request credential. The existing middleware therefore continues authenticating the caller from `Authorization`; the handler separately verifies `spec.token` using a new audience-parameterized ServiceAccount JWT verifier method and the existing live ServiceAccount / Pod / Node binding function.

Invalid JWTs, unknown ServiceAccounts, UID mismatches, expired credentials, unsupported algorithms, unavailable verifier configuration and audience mismatch will all return HTTP `200 OK` with a typed TokenReview response whose `status.authenticated` is false. The handler will expose neither detailed signature/parser failure information nor any forged identity data. An authenticator-infrastructure failure remains distinct and can populate the permitted `status.error` field only when the token could not be checked.

For `spec.audiences`, the verifier must cryptographically require a non-empty intersection with JWT `aud`; on success, TokenReview returns exactly that intersection. With no requested audiences, it uses the configured Kubernetes API server audience verification and returns an empty status audience list, matching the upstream convention.

The registry will serve the cluster-scoped, create-only `tokenreviews` resource in `authentication.k8s.io/v1`; generic API-group discovery and API-group path resolution will replace the current core-only assumption so RBAC receives resource attributes `group=authentication.k8s.io`, `resource=tokenreviews`, `verb=create`.

No dependencies are required: the established `jsonwebtoken` 10.2.0 verifier already performs RS256, issuer, time and audience checks, while typed HTTP serialization uses workspace `serde` / `serde_json`.

## Executable contract

| Concern | Implemented behavior |
|---|---|
| Served resource | Cluster-scoped `POST /apis/authentication.k8s.io/v1/tokenreviews`; `GET /apis`, `GET /apis/authentication.k8s.io` and `GET /apis/authentication.k8s.io/v1` expose typed discovery. |
| Caller boundary | Middleware authenticates the caller from its HTTP `Authorization` header. The body token is never substituted as the caller credential. RBAC receives `create` for API group `authentication.k8s.io`, resource `tokenreviews`. |
| Token validation | The existing ServiceAccount verifier performs RS256, key ID, issuer, expiry/nbf and audience validation. The existing live binding reader then requires the referenced ServiceAccount and optional bound Pod or Node to exist with the signed UID. |
| Successful review | Returns `200 OK`, `status.authenticated: true`, Kubernetes ServiceAccount username/UID/groups and the exact requested-audience intersection. When the caller did not request audiences, standard configured API-server audience validation applies and returned `status.audiences` is empty. |
| Rejected token | Returns `200 OK` and the explicit `status.authenticated: false` without a user identity or verifier-detail disclosure, including malformed, expired, signature-invalid, audience-mismatched, revoked or verifier-unconfigured ServiceAccount tokens. |
| Invalid request | Empty `spec.token`, empty audience elements, wrong type metadata or a client-provided non-empty `status` receive typed request validation errors; status remains server-owned. |
| Persistence | TokenReview itself is create-only and non-persistent. Live ServiceAccount lookup goes through the selected backend, including real etcd, so object deletion or UID replacement revokes a previously signed token. |

## Verification evidence

The slice has focused API-server coverage for discovery, standard authentication output, requested audience intersection, malformed token failure, unconfigured verifier failure, request status ownership and RBAC request mapping. It also has a real-etcd test that creates and independently reads a durable ServiceAccount, issues a JWT through `TokenRequest`, then validates it through `TokenReview`.
