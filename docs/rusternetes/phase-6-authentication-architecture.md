# Rusternetes: authentication и request identity boundary

**Статус:** architecture contract до реализации  
**Ветка:** `rusternetes/phase-1-api-storage`  
**Срез:** Phase 6 — request authentication boundary

## Scope

Authentication предшествует будущему RBAC authorizer. Каждый ConfigMap HTTP request будет нести typed `RequestIdentity`: username, optional UID, groups и extra attributes. Kubernetes определяет именно эти четыре attributes как результат authentication; successful identities получают группу `system:authenticated`, а anonymous identity использует `system:anonymous` и `system:unauthenticated` [1].

Первый executable slice предоставляет в памяти явно настроенный bearer-token authenticator, предназначенный для development, integration tests и bootstrap control-plane wiring. Он защищает contract middleware и error semantics, но не пытается выдавать статический token mapping за production service account / OIDC implementation. Invalid supplied bearer token is `401 Unauthorized`; missing credential either becomes the configured anonymous identity or returns `401`, never silently impersonating an authenticated user.

## Design

| Element | Contract |
|---|---|
| `rusternetes-authn` | Typed user identity, authenticator chain, static bearer mapping и anonymous policy. Это самостоятельный Rust crate; HTTP code не парсит credentials. |
| API Server middleware | Runs before API handlers, authenticates `Authorization: Bearer <token>`, and stores `RequestIdentity` in Axum request extensions. |
| Anonymous policy | Explicit `AnonymousPolicy::Allow` or `Deny`, defaulting to allow to retain Kubernetes-style configured anonymous semantics. A malformed or invalid header is never anonymous. |
| API error | Typed Kubernetes `Status` with HTTP `401` and reason `Unauthorized`; no token values are echoed in error messages or logs. |
| Request extension | Handlers and future RBAC use one immutable typed identity; identity cannot be submitted in JSON request bodies or query parameters. |

## Dependency audit

| crate | purpose | version / license | decision | alternative and reason |
|---|---|---|---|---|
| `subtle` | Constant-time bearer-secret comparison for static bootstrap map | `2.x`, BSD-3-Clause | Adopt in this slice; small, mature, focused primitive. | `==` is simpler but inappropriate for raw bearer credential comparison. |
| `jsonwebtoken` | Signed JWT / OIDC and ServiceAccount token validation | `11.0.0`, MIT | Deferred to the next credential-specific slice after issuer/JWKS, audience and key-rotation configuration is added. | Hand-rolled JWS/JWT verification is explicitly rejected. |
| `x509-parser` | Parsing client certificate subjects and extensions | current, MIT/Apache-2.0 | Deferred until mutual-TLS listener and cluster CA trust configuration exist. | Manual DER / ASN.1 parsing is explicitly rejected. |
| `rustls` / `webpki` | TLS peer certificate transport validation | current mature Rust TLS stack | Deferred with mTLS listener wiring. | Custom TLS or certificate verifier is rejected. |

## Non-goals

This phase does not add a normal-user database, password authentication, JWT signature verification, OIDC discovery, service-account token issuance, client-certificate TLS verification, an authenticating proxy trust configuration, or RBAC. Each requires distinct durable configuration and operational contracts. The next phase consumes `RequestIdentity` for RBAC decisions.

## Proof

Integration tests issue a real HTTP request against the Axum server. They verify: authenticated bearer identity reaches a protected ConfigMap handler; invalid bearer gets Kubernetes `401 Unauthorized`; missing credentials follow the explicit anonymous policy; and caller-controlled headers cannot forge the request extension.

## References

[1]: https://kubernetes.io/docs/reference/access-authn-authz/authentication/ "Kubernetes Authentication"
[2]: https://docs.rs/jsonwebtoken/11.0.0/jsonwebtoken/ "jsonwebtoken 11 documentation"
[3]: https://docs.rs/x509-parser/latest/x509_parser/ "x509-parser documentation"
