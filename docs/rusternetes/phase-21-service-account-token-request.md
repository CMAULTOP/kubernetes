# Phase 21: ServiceAccount TokenRequest

This slice implements the create-only Kubernetes subresource `POST /api/v1/namespaces/:namespace/serviceaccounts/:name/token`. It accepts an `authentication.k8s.io/v1` `TokenRequest`, validates it against the currently stored ServiceAccount and optional bound object, and returns a non-persistent `TokenRequest` status containing a newly signed short-lived RS256 JWT. This follows Kubernetes’ TokenRequest model for bounded credentials rather than creating token Secrets. [1] [2]

> The request never enters either in-memory history or etcd. It is an operation over live identities, and each successful response is independently signed with a new `jti`.

| Concern | Implemented contract |
|---|---|
| Typed API | `TokenRequest`, `TokenRequestSpec`, `TokenRequestStatus`, and `BoundObjectReference` serialize as `authentication.k8s.io/v1` wire objects. The status includes `token` and RFC 3339 `expirationTimestamp`. |
| Route and discovery | The core registry advertises `serviceaccounts/token` as a namespaced `TokenRequest` resource with only the `create` verb. Namespaced subresource resolution feeds `create` plus subresource `token` to RBAC. |
| Signer configuration | A `ServiceAccountTokenIssuer` signs only RS256 and requires issuer, non-empty default audiences, a PKCS#8 RSA private key, key ID, default TTL, and capped maximum TTL. A single token can never exceed one year. |
| Verification invariant | `AuthenticationChain` accepts an issuer only when its derived RSA public modulus/exponent and `kid` match a configured `ServiceAccountJwtVerifier` key, and issuer/default audiences match. The control plane cannot publish a signing path whose output it cannot verify. |
| Claims | Each JWT carries `iss`, `sub`, `aud`, `iat`, `nbf`, `exp`, `jti`, and the existing Kubernetes private claim `kubernetes.io` with ServiceAccount namespace/name/UID. Pod- and Node-bound claims reuse the schema already live-validated by authentication. [1] |
| Requested audiences and TTL | Omitted audiences use the configured API defaults. Empty audience values and non-positive durations are rejected. Requested positive TTLs are capped to the configured maximum. |
| ServiceAccount consistency | A named ServiceAccount must currently exist and have a UID. If request `metadata.uid` is supplied, it must equal that live UID before signing. |
| Bound objects | `core/v1` Pod and Node references must contain kind, API version, name, UID, and must resolve to a live UID-matching object. A Pod-bound token contains the live Pod claim and optional currently assigned Node claim; runtime authentication validates the Pod binding. A standalone Node-bound token validates the Node binding. [1] |
| Unimplemented dependencies | `Secret` binding is explicitly rejected because no Secret typed store/live validator exists yet. Issuing such a token would violate fail-closed revocation semantics. |
| Disabled signing | Without complete signing/verifier configuration, the TokenRequest endpoint returns a typed `400 BadRequest` and never emits a token. Partial process configuration aborts API-server startup. |

## Process configuration

Token issuance is disabled unless all of the following file-backed settings are configured together. Supplying only a subset causes startup failure; secret key material is not accepted as an environment-variable value.

| Variable | Required | Meaning |
|---|---:|---|
| `RUSTERNETES_SERVICE_ACCOUNT_ISSUER` | Yes | JWT issuer string. |
| `RUSTERNETES_SERVICE_ACCOUNT_SIGNING_KEY_FILE` | Yes | PKCS#8 PEM RSA private signing key path. |
| `RUSTERNETES_SERVICE_ACCOUNT_VERIFICATION_KEY_FILE` | Yes | PEM RSA public verification key path corresponding to the signing key. |
| `RUSTERNETES_SERVICE_ACCOUNT_KEY_ID` | No | JWT/JWK key ID; defaults to `service-account-0`. |
| `RUSTERNETES_SERVICE_ACCOUNT_AUDIENCES` | No | Comma-separated default audiences; defaults to the issuer. |
| `RUSTERNETES_SERVICE_ACCOUNT_TOKEN_DEFAULT_EXPIRATION_SECONDS` | No | Default requested lifetime; defaults to 3600 seconds. |
| `RUSTERNETES_SERVICE_ACCOUNT_TOKEN_MAX_EXPIRATION_SECONDS` | No | Cap for a requested lifetime; defaults to 86400 seconds and has a hard one-year upper limit. |

## Verification coverage

The slice includes typed signer/verifier coverage for RS256 claims, key ID, default audiences, and expiration capping. API-server black-box tests create a live ServiceAccount through the in-memory store, request a token through the route, verify the returned JWT cryptographically, and exercise disabled signing, missing Pod binding, and stale ServiceAccount UID rejection. Registry tests assert both discovery and namespaced subresource resolution.

The only private PEM file committed with this slice is an isolated test fixture under `crates/api-server/testdata/`; it is used exclusively by unit tests and is not runtime configuration material.

## Dependency audit

| crate | purpose | version | license | alternative | reason for choosing |
|---|---|---:|---|---|---|
| `jsonwebtoken` | RS256 JWS signing and verification | `10.2.0` | MIT | Handwritten JWS implementation | Already workspace-pinned for verification; provides maintained signing primitives and avoids custom cryptographic protocol code. |
| `rsa` | Parse signing/public PEM and compare public key modulus/exponent | `0.9.10` | MIT OR Apache-2.0 | Handwritten ASN.1 parsing or OpenSSL FFI | Mature pure-Rust crate already used for JWKS construction; comparison enforces signer/verifier coherence. |
| `time` | Bounded expiration arithmetic and RFC 3339 status serialization | `0.3.36` | MIT OR Apache-2.0 | Manual timestamp arithmetic | Existing workspace dependency with typed overflow-aware duration handling and serde support. |
| `uuid` | Per-token standard JWT `jti` | `1.11.0` | MIT OR Apache-2.0 | Random ID implementation | Existing mature workspace dependency with v4 support, avoiding ad hoc uniqueness code. |

## References

[1]: https://kubernetes.io/docs/reference/access-authn-authz/service-accounts-admin/ "Kubernetes: Service Accounts Administration"
[2]: https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/ "Kubernetes: Configure Service Accounts for Pods"
[3]: https://github.com/kubernetes/kubernetes/blob/b3bc2ac58fa173967f27ade80f28cc5015b8c1c3/pkg/registry/core/serviceaccount/storage/token.go "Kubernetes upstream TokenRequest REST storage"
[4]: https://github.com/kubernetes/kubernetes/blob/b3bc2ac58fa173967f27ade80f28cc5015b8c1c3/pkg/serviceaccount/claims.go "Kubernetes upstream ServiceAccount JWT claims"
