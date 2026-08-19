# Phase 24: ConfigMap PATCH

This slice adds the Kubernetes namespaced ConfigMap PATCH endpoint:

```text
PATCH /api/v1/namespaces/:namespace/configmaps/:name
```

The implementation deliberately supports the two general-purpose JSON patch representations already used by Rusternetes Node PATCH: RFC 6902 JSON Patch and RFC 7396 JSON Merge Patch. Kubernetes documents `PATCH` as distinct from `PUT` and exposes patching as an in-place update operation. [1] The official patch guide distinguishes JSON Merge Patch from strategic merge patch and notes that JSON Merge Patch replaces a supplied list as a whole. [2]

| Request `Content-Type` | Processing | Result |
|---|---|---|
| `application/json-patch+json` | Parses `json_patch::Patch` and applies operations atomically to the current serialized ConfigMap. | RFC 6902 operation errors are returned as a typed invalid request; no storage update occurs. |
| `application/merge-patch+json` | Parses an object and applies `json_patch::merge` to the current serialized ConfigMap. | RFC 7396-style partial object update; malformed or non-object patches fail before storage update. |
| Any other or missing value | Does not interpret the request body. | `415 Unsupported Media Type`. |

> The URI namespace and name remain authoritative. The server first reads the live ConfigMap, applies the patch to its JSON representation, deserializes the result to a typed `ConfigMap`, then validates the URI identity and applies normal admission and storage update paths.

| Integrity property | Enforcement |
|---|---|
| Namespace and name isolation | A patch that changes `metadata.namespace` or `metadata.name` away from the URI fails with a typed invalid response. |
| Typed schema enforcement | Patched JSON must deserialize into the typed `ConfigMap`; malformed resulting documents are rejected. |
| Admission compatibility | The final typed update is passed through the established `AdmissionRequest::update` validation path. |
| Optimistic concurrency | `metadata.resourceVersion` inherited from the live object is used by the normal backend update. A merge patch that supplies a stale version reaches the storage CAS boundary and returns `409 Conflict`. |
| Durability and watches | Existing in-memory and etcd ConfigMap update implementations are reused; therefore a successful patch retains their atomic persistence and `MODIFIED` watch behavior. |

No dependency was added. The existing workspace-pinned dependency is retained:

| Crate | Version | License | Purpose | Alternative | Reason selected |
|---|---:|---|---|---|---|
| `json-patch` | `4.2.0` | MIT OR Apache-2.0 | Parses and applies RFC 6902 patches and merge patches. | New handwritten JSON-pointer/patch engine. | Already mature, audited in the Node PATCH slice, API-compatible with workspace `serde_json`, and avoids implementing a security-sensitive patch engine. |

## Verification

The API-server black-box test creates a ConfigMap and verifies a JSON Merge Patch, an RFC 6902 JSON Patch, metadata-name isolation rejection, unsupported media-type rejection, and a stale-resourceVersion `409 Conflict`. The full workspace quality gate additionally runs all crate and real-etcd integration tests, format checking, Clippy with warnings denied, and `git diff --check`.

## References

[1]: https://kubernetes.io/docs/reference/using-api/api-concepts/ "Kubernetes API Concepts"
[2]: https://kubernetes.io/docs/tasks/manage-kubernetes-objects/update-api-object-kubectl-patch/ "Update API Objects in Place Using kubectl patch"
[3]: https://datatracker.ietf.org/doc/html/rfc6902 "RFC 6902: JavaScript Object Notation (JSON) Patch"
[4]: https://datatracker.ietf.org/doc/html/rfc7386 "RFC 7386: JSON Merge Patch"
