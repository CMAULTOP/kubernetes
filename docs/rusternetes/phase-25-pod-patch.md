# Phase 25: Pod PATCH

This slice adds the namespaced core/v1 Pod PATCH endpoint:

```text
PATCH /api/v1/namespaces/:namespace/pods/:name
```

Rusternetes accepts `application/json-patch+json` (RFC 6902) and `application/merge-patch+json` (RFC 7396), matching the established Node and ConfigMap PATCH boundary. Kubernetes treats PATCH as an in-place API operation distinct from PUT. [1] The standard Kubernetes patch guide distinguishes JSON Merge Patch from strategic merge patch and documents that a JSON Merge Patch replaces a supplied list as a whole. [2]

> The initial Rusternetes Pod lifecycle intentionally permits only metadata-level PATCH updates. Its typed update validation rejects desired-state `spec` changes until the scheduler/runtime slices support the corresponding Kubernetes mutation semantics, and it rejects status changes outside `/status`.

| Concern | Enforced behavior |
|---|---|
| Supported media types | `application/json-patch+json` and `application/merge-patch+json`. Missing or unsupported content types yield `415 Unsupported Media Type`. |
| Patch execution | The server reads the live typed Pod, serializes it, applies the selected patch using the existing mature `json-patch` crate, and deserializes the result back to a typed Pod. |
| URI identity | `metadata.namespace` and `metadata.name` must match the request path after patching. Mismatches are invalid and never reach storage. |
| Desired state | Existing `Pod::validate_update` rejects all `spec` differences. This excludes changes to containers, scheduling and bindings from general PATCH in the present lifecycle slice. |
| Status | Existing typed validation rejects any status difference on the primary endpoint; callers use `/status` for observed state mutations. |
| Concurrency | The live resourceVersion is inherited by default. A supplied stale resourceVersion follows the normal backend CAS path and returns `409 Conflict`. |
| Admission, storage and watch | The final typed Pod passes through the existing pod update admission request and shared in-memory/etcd update paths, preserving atomic persistence and `MODIFIED` watch publication. |

No new dependency is introduced. `json-patch = 4.2.0` is already workspace-pinned and was previously selected for Node PATCH and ConfigMap PATCH. It is a mature, `serde_json`-compatible implementation of JSON Patch and JSON Merge Patch with MIT OR Apache-2.0 licensing; a handwritten JSON-pointer and patch engine is intentionally not introduced.

## Verification

The API-server test creates a Pod, applies a JSON Merge Patch and RFC 6902 JSON Patch to labels, then proves that PATCH attempts to modify containers, status, or metadata name are rejected. It also proves that a stale `metadata.resourceVersion` yields a conflict. The complete API-server test suite passes after the change, followed by the workspace quality gate: formatting, workspace check, all tests including real etcd integration coverage, Clippy with warnings denied, and whitespace-diff validation.

## References

[1]: https://kubernetes.io/docs/reference/using-api/api-concepts/ "Kubernetes API Concepts"
[2]: https://kubernetes.io/docs/tasks/manage-kubernetes-objects/update-api-object-kubectl-patch/ "Update API Objects in Place Using kubectl patch"
[3]: https://github.com/kubernetes/kubernetes/blob/b3bc2ac58fa173967f27ade80f28cc5015b8c1c3/pkg/registry/core/pod/strategy.go "Kubernetes upstream Pod strategy"
[4]: https://datatracker.ietf.org/doc/html/rfc6902 "RFC 6902: JSON Patch"
[5]: https://datatracker.ietf.org/doc/html/rfc7386 "RFC 7386: JSON Merge Patch"
