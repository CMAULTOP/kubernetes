# Phase 20: ServiceAccount OIDC discovery and JWKS

This slice publishes OIDC-compatible metadata only when it is derived from the same configured `ServiceAccountJwtVerifier` that verifies ServiceAccount JWTs. The API server exposes `/.well-known/openid-configuration` and `/openid/v1/jwks`; when OIDC publication is not configured, both routes return `404 NotFound` rather than advertising synthetic or stale key material.

| Endpoint | Configured response |
|---|---|
| `/.well-known/openid-configuration` | `issuer`, configured HTTPS `jwks_uri`, `id_token` response type, `public` subject type, and RS256 signing-algorithm support. |
| `/openid/v1/jwks` | RSA JWK set created from the configured PEM verification keys, including `kty=RSA`, `use=sig`, `alg=RS256`, optional `kid`, and base64url-unpadded modulus/exponent values. |

The discovery builder refuses an empty key set, non-HTTPS issuer/JWKS URIs, malformed PEM public keys, or an attempt to enable discovery without a configured JWT verifier. Thus published JWKs cannot diverge from the ServiceAccount verification trust set through the public API. Discovery routes intentionally bypass request authentication and RBAC because they expose only public key material; all typed Kubernetes resource routes retain their existing middleware chain.

## Dependency audit

| Field | Decision |
|---|---|
| crate | `rsa` |
| version | `0.9.10`, workspace-pinned |
| license | MIT OR Apache-2.0 |
| purpose | Parse configured PEM RSA public keys and retrieve public modulus/exponent for JWK serialization. |
| alternative | Handwritten ASN.1 / PEM parsing or system OpenSSL calls. |
| reason for choosing | The pure-Rust, mature crate is already transitively resolved by the selected JWT verifier. An explicit dependency provides a typed stable public-key API and avoids custom cryptographic parsing. |
| crate | `base64` |
| version | `0.22.1`, already workspace-pinned |
| license | MIT OR Apache-2.0 |
| purpose | RFC 7515-compatible unpadded base64url JWK `n` and `e` encoding. |
| alternative | Custom base64url encoder. |
| reason for choosing | Mature, existing direct workspace dependency; custom encoding would create avoidable interoperability risk. |

The behavior follows Kubernetes ServiceAccount issuer discovery documentation, which describes OIDC discovery at `/.well-known/openid-configuration` and JWKS publication at `/openid/v1/jwks`: <https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/>.
