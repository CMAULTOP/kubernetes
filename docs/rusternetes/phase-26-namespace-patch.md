# Phase 26: Namespace PATCH

This slice adds the cluster-scoped core/v1 Namespace PATCH endpoint:

```text
PATCH /api/v1/namespaces/:name
```

The endpoint accepts RFC 6902 JSON Patch (`application/json-patch+json`) and RFC 7396 JSON Merge Patch (`application/merge-patch+json`) using the same mature Rust patch implementation already deployed by Node, ConfigMap and Pod PATCH routes. Kubernetes identifies PATCH as an in-place API operation distinct from PUT. [1] Kubernetes’s patch guide distinguishes JSON Merge Patch from strategic merge patch and notes that JSON Merge Patch replaces lists supplied in the patch. [2]

> Namespace PATCH permits metadata changes accepted by the typed update validator. Namespace `spec.finalizers` remains reserved for the future `/finalize` lifecycle path, and observed lifecycle status remains exclusively owned by `/status`.

| Concern | Enforced behavior |
|---|---|
| Supported formats | Only `application/json-patch+json` and `application/merge-patch+json` are interpreted. Missing or unsupported `Content-Type` produces `415 Unsupported Media Type`. |
| Patch transformation | The API server reads the live Namespace, applies the patch to its JSON representation, and requires the output to deserialize as a typed Namespace. |
| Identity protection | The patched `metadata.name` must equal the URI name. A mismatch is rejected before admission or persistence. |
| Finalizer safety | Existing `Namespace::validate_update` rejects any `spec` difference. PATCH cannot create, remove or alter `spec.finalizers`; the dedicated finalize workflow remains unimplemented rather than exposed through an unsafe generic update path. |
| Status safety | Existing typed validation rejects a status difference on the primary resource. Observed phase transitions use the published `/api/v1/namespaces/:name/status` route. |
| Concurrency | The live resourceVersion is included in the source document. A merge patch that supplies an older value reaches the existing CAS backend update and receives `409 Conflict`. |
| Admission and durable behavior | After typed identity validation, the normal Namespace admission request and backend update execute. In-memory and etcd storage therefore retain their atomic update and normal `MODIFIED` watch semantics. |

No dependency is added. The workspace already pins `json-patch = 4.2.0` (MIT OR Apache-2.0) for JSON Patch and JSON Merge Patch. It is compatible with the existing `serde_json` representation and avoids adding a handwritten JSON-pointer engine to the API-server security boundary.

## Verification

The API-server black-box test creates a Namespace, applies merge and JSON Patch updates to labels, and proves that patches attempting finalizer, status or name mutation are rejected. It also proves stale resourceVersion conflict behavior. The release gate runs format checking, full workspace compilation and tests (including real-etcd integrations), Clippy with warnings denied, and `git diff --check`.

## References

[1]: https://kubernetes.io/docs/reference/using-api/api-concepts/ "Kubernetes API Concepts"
[2]: https://kubernetes.io/docs/tasks/manage-kubernetes-objects/update-api-object-kubectl-patch/ "Update API Objects in Place Using kubectl patch"
[3]: https://github.com/kubernetes/kubernetes/blob/b3bc2ac58fa173967f27ade80f28cc5015b8c1c3/pkg/registry/core/namespace/strategy.go "Kubernetes upstream Namespace strategy"
[4]: https://datatracker.ietf.org/doc/html/rfc6902 "RFC 6902: JSON Patch"
[5]: https://datatracker.ietf.org/doc/html/rfc7386 "RFC 7386: JSON Merge Patch"
