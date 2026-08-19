# Phase 28 Pagination Research Notes

## Official API contract

Kubernetes collection LIST requests accept `limit` and `continue`. When a server chunks a list, `metadata.continue` encodes both the list snapshot `resourceVersion` and the last observed position. A client must resume using the returned token without changing selectors or request scope. The list sequence is intended to be a consistent snapshot; a server may return `410 Gone` once continuation state is no longer valid, and clients must restart the list. [1]

The implementation boundary for this slice is core/v1 ConfigMaps, Pods, ServiceAccounts, Nodes and Namespaces. Each continuation token therefore needs to be opaque and bind at least: resource kind, namespace scope, canonicalized label and field selectors, snapshot resourceVersion, last ordering key, issuance time, and token format version. A token supplied to another resource, namespace, or selector is invalid rather than silently reused.

## Candidate design constraints

| Requirement | Rusternetes design consequence |
|---|---|
| `limit` is a maximum page size | Parse bounded positive integer; zero means no pagination; reject invalid or excessively large values. |
| `continue` is opaque | Use URL-safe base64 encoded, versioned JSON with strict decoding and no client-controlled deserialization into untyped storage logic. |
| Multi-page results preserve a snapshot | Pin the current list resourceVersion in the token and reject incompatible/missing history as `ResourceExpired`. |
| Selector/scope must not change mid-list | Put a canonical request fingerprint in the token and recompute it before every continuation request. |
| A response exposes continuation metadata | Extend typed list metadata with `continue` and `remainingItemCount` while preserving the existing resourceVersion field. |

## Upstream storage implementation observations

The upstream `k8s.io/apiserver/pkg/storage/continue.go` implements a compact, versioned JSON token encoded with unpadded URL-safe base64. It validates the serialized version and mandatory resourceVersion/start-key fields before constructing the continuation range. Its `DecodeContinue` path explicitly rejects path traversal or non-canonical keys before applying a storage prefix; `PrepareContinueToken` starts the next range strictly after the previous final key and only publishes `remainingItemCount` when no selector makes the value unreliable. [3]

Rusternetes will preserve these safety properties while using resource identity, request-scope fingerprinting, and an internal snapshot entry rather than exposing etcd path keys. This avoids cross-resource continuation and guards the in-memory backend against request replay with altered selectors.

## Finalized Rusternetes contract

The API server will own a bounded, mutex-protected snapshot cache. A first paginated LIST delegates to the selected typed backend exactly once, retaining the resulting typed list items and its storage resourceVersion as a snapshot. Continuation calls look up that immutable snapshot and return subsequent contiguous item windows. Consequently, a write after page one cannot make later pages omit, duplicate, or mutate snapshot objects. The same public behavior applies to both in-memory and etcd backends; etcd's prefix GET already supplies one storage revision for its first-list snapshot.

Each opaque token is an unpadded URL-safe Base64 encoding of a versioned JSON envelope. The signed-by-server lookup key is a random UUID not derived from object names or etcd paths. The envelope additionally binds a resource identifier, exact namespace scope (or all-namespaces), raw selectors, requested page size, issued-at timestamp and cursor offset. Decoding uses strict JSON/type validation; the cache repeats the binding checks rather than trusting the client envelope. Unknown, malformed, expired, scope-mismatched, selector-mismatched and resource-mismatched tokens are rejected. Expired or evicted snapshots return existing `ApiError::ResourceExpired` / HTTP 410, while malformed or mismatched request data returns existing `ApiError::BadRequest` / HTTP 400.

The cache retains at most 1,024 active snapshots and expires an unused continuation after five minutes, aligning with the documented default Kubernetes continuation lifetime. Creation and consumption prune expired entries while holding a Tokio mutex, so concurrent requests cannot observe partial cache mutation. A final page removes its snapshot immediately. `limit=0` retains current unpaginated behavior, and `limit` / `continue` are rejected on watch requests. `remainingItemCount` is only emitted for unfiltered requests, as upstream does not promise a selector-aware count. [1] [3]

## Dependency audit

| Crate | Purpose | Version | License | Why and alternative | Decision |
|---|---|---:|---|---|---|
| `base64` | URL-safe opaque token transport encoding | `0.22.1` | MIT OR Apache-2.0 | Existing workspace dependency, compatible with serde 1.0.217, and directly implements the RFC 4648 URL-safe unpadded encoding used by upstream. The alternative is a custom encoder, which is materially less safe. | Reuse existing mature crate in `api-server`. |
| `uuid` | Cryptographically random opaque snapshot identifier | `1.11.0` with `v4` | MIT OR Apache-2.0 | Existing workspace dependency, compatible with serde 1.0.217, and avoids predictable server-side snapshot keys. The alternative is hand-rolled randomness/state, which is unnecessary and riskier. | Reuse existing mature crate in `api-server`. |
| `serde_json` | Versioned token envelope serialization | `1.0.134` | MIT OR Apache-2.0 | Existing workspace dependency, compatible with serde 1.0.217, and already used for Kubernetes JSON wire handling. The alternative is manual JSON parsing. | Reuse existing mature crate. |

## Sources

[1]: https://kubernetes.io/docs/reference/using-api/api-concepts/ "Kubernetes API Concepts"
[2]: https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#list-options-v1-meta "Kubernetes ListOptions"
[3]: https://github.com/kubernetes/kubernetes/tree/b3bc2ac58fa173967f27ade80f28cc5015b8c1c3/staging/src/k8s.io/apiserver/pkg/storage "Kubernetes upstream storage package"
