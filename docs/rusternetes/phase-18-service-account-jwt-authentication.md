# Phase 18: ServiceAccount JWT authentication boundary

This slice introduces a **fail-closed RS256 ServiceAccount JWT verifier** at the existing authentication boundary. It verifies a configured issuer, a non-empty configured audience set, `exp` and `nbf` through the JWT validation library, RS256 algorithm selection, an optional `kid`-selected public verification key, and the signed Kubernetes ServiceAccount claim structure. A token is not converted into a request identity merely because its signature verifies.

The API server performs a second, asynchronous check against the selected live `ServiceAccountBackend`. It loads the `namespace` and `serviceaccount.name` from the verified `kubernetes.io` claim and requires its current UID to equal the signed `serviceaccount.uid`. A missing ServiceAccount or UID mismatch produces `Unauthorized`; the result is therefore revoked immediately on deletion or recreation. On success the identity is `system:serviceaccount:<namespace>:<name>` and is assigned the Kubernetes groups `system:serviceaccounts`, `system:serviceaccounts:<namespace>`, and `system:authenticated`.

| Contract boundary | Behaviour |
|---|---|
| Signature and algorithm | Only RS256 tokens validated against configured PEM public keys are accepted. |
| Issuer and audience | Both must match configured values; missing configuration is rejected at construction. |
| Identity claims | `sub` must equal `system:serviceaccount:<namespace>:<name>` and the signed UID must be non-empty. |
| Revocation | The selected in-memory or etcd backend is queried on every JWT-authenticated request; no static identity cache is used. |
| Failure behaviour | Invalid header, signature, issuer, audience, temporal claim, malformed Kubernetes claim, missing object and UID mismatch are rejected without anonymous fallback. |

Pod- and Node-bound ServiceAccount token claim verification remains a separate follow-on extension because it requires parsing and live validation of the optional bound-object claim branches as an equally complete end-to-end slice. This phase does not expose a partial success path for those claims.

## Dependency audit

| Field | Decision |
|---|---|
| crate | `jsonwebtoken` |
| version | `10.2.0`, pinned in the workspace |
| license | MIT |
| purpose | Standards-based JWS parsing and JWT signature, issuer, audience and temporal claim verification. |
| selected features | `rust_crypto` and `use_pem`; pure-Rust cryptographic backends and PEM public-key parsing. |
| alternative | `jwt-simple` and handwritten JWT/JWS validation. |
| reason for choosing | `jsonwebtoken` provides the mature typed JWT/JWK and PEM validation primitives required here. Handwritten parsing or signature verification would materially increase security risk. Version 11 requires a newer `serde` than the workspace pin, while 10.2.0 is compatible with Rust 1.91 and `serde` 1.0.217. |

## Sources

The compatibility contract follows the Kubernetes [ServiceAccount token projection and bound-token documentation](https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/) and the upstream checked-out `pkg/serviceaccount/claims.go` implementation. The verifier dependency is documented at [jsonwebtoken 10.2.0](https://docs.rs/jsonwebtoken/10.2.0/jsonwebtoken/).
