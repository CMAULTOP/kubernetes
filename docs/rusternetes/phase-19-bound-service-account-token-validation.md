# Phase 19: Pod- and Node-bound ServiceAccount token validation

This slice extends the ServiceAccount JWT authentication boundary with live validation of optional Kubernetes private claims. After cryptographic verification, issuer/audience/time checks, ServiceAccount subject validation, and current ServiceAccount UID validation, the API server validates any bound object through the same selected in-memory or etcd backend that serves the request.

| Token claim shape | Live validation behavior |
|---|---|
| `kubernetes.io.pod` | The API server loads the Pod by its signed namespace and name, then requires its current UID to equal the signed UID. Missing, deleted, or recreated Pods reject the token. |
| `kubernetes.io.node` without a Pod claim | The API server loads the Node by its signed name and requires its current UID to equal the signed UID. Missing, deleted, or recreated Nodes reject the token. |
| Pod claim plus Node claim | The Pod is validated, but the embedded Node claim is not resolved. This matches Kubernetes API server semantics for Pod-projected tokens: Node metadata is included for external consumers and is not a token-authentication revocation dependency. |
| Empty name or UID in either bound claim | Cryptographic claim validation fails before any identity can be created. |

The implementation preserves a fail-closed boundary. Neither an unavailable bound object nor a UID mismatch falls back to static bearer authentication or anonymous access. Tests prove direct Node deletion revokes a Node-bound token, Pod deletion revokes a Pod-bound token, and a nonexistent embedded Node does not reject an otherwise valid Pod-bound token.

The design follows Kubernetes’ [ServiceAccount administration guide](https://kubernetes.io/docs/reference/access-authn-authz/service-accounts-admin/), which specifies that an object-bound token is rejected when the referenced object is absent or its UID differs, and distinguishes the unverified Node metadata embedded in Pod-bound tokens. The exact local upstream reference remains `pkg/serviceaccount/claims.go`.
