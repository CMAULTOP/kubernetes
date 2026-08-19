# Rusternetes: Node PATCH compatibility contract

**Ветка:** `rusternetes/phase-1-api-storage`
**Статус:** implemented and verified

Rusternetes now executes `PATCH /api/v1/nodes/{name}` for the two standards-based JSON forms that Kubernetes dispatches through its JSON patch handler: RFC 6902 JSON Patch and RFC 7396 JSON Merge Patch.[1] Node is cluster-scoped, and the request is authorized as RBAC verb `patch` on resource `nodes`.

| Content-Type | Status | Semantics |
|---|---|---|
| `application/json-patch+json` | Supported | The server parses and atomically applies RFC 6902 operations to a JSON serialization of the current typed Node. |
| `application/merge-patch+json` | Supported | The server applies RFC 7396 object merge semantics to the current typed Node. Arrays retain RFC 7396 replacement semantics. |
| `application/strategic-merge-patch+json` | Explicitly rejected with `415 UnsupportedMediaType` | Strategic merge relies on Kubernetes schema-specific patch strategy and merge-key metadata. It is not incorrectly treated as ordinary merge.[2] |
| `application/apply-patch+yaml` | Not part of this slice | Server-side apply requires managed-fields ownership and field-manager semantics, which must be added as a separate end-to-end capability. |

The handler reads the current typed Node, serializes it to JSON, applies the selected standard patch, deserializes the result back into `Node`, binds the URI identity, and invokes the existing persistence update operation. Consequently, both in-memory and etcd storage retain their existing optimistic-concurrency and revision machinery. A patch can specify `metadata.resourceVersion` for conflict detection; without it, the durable backend still uses an observed etcd mod-revision compare so a race cannot become a blind overwrite.

| Invariant | Enforcement |
|---|---|
| Status isolation | The primary Node update path restores persisted status after validation. A PATCH containing `status` therefore cannot modify readiness; `PUT /status` remains the status mutation endpoint. |
| Object identity | `metadata.name` must match the URI, and Node validation continues to reject namespace assignment or identity changes. |
| Versioning and events | Each committed PATCH obtains the next shared resource version and emits exactly one `MODIFIED` event through the existing watch implementation. |
| Failure safety | Empty or malformed documents receive typed client errors; unsupported media types receive typed `415`; failed RFC 6902 application is reported as Kubernetes `422 Invalid`. No PATCH path returns success without a storage mutation. |
| Durable storage | The real-etcd integration test reads back the persisted Node from a direct `EtcdNodeRepository`, proving the HTTP path does not fall back to memory. |

> Upstream Kubernetes distinguishes JSON Patch and JSON Merge Patch from Strategic Merge Patch. Strategic behavior depends on per-field `patchStrategy` and `patchMergeKey` metadata, while JSON Merge Patch replaces a supplied list as a whole.[2]

| Dependency | Purpose | Version | License | Alternative | Reason for choosing |
|---|---|---:|---|---|---|
| `json-patch` | RFC 6902 operation parsing/application and RFC 7396 merge application | `4.2.0` | MIT OR Apache-2.0 | A bespoke JSON Pointer/patch engine; `kube` client patch enums | The crate directly implements both open RFC formats, is small, and is used only at the HTTP-document boundary. `kube` is a client library and cannot supply API-server storage, validation, or authorization semantics. |

The slice has in-memory HTTP coverage for both supported formats, status isolation, watch delivery and unsupported-media failures. A separate real-etcd test validates durable merge PATCH persistence. `cargo check --workspace` succeeded after adding the pinned dependency; the full workspace quality gate follows before publication.

## References

[1]: https://kubernetes.io/docs/reference/using-api/api-concepts/ "Kubernetes API Concepts"
[2]: https://kubernetes.io/docs/tasks/manage-kubernetes-objects/update-api-object-kubectl-patch/ "Update API Objects in Place Using kubectl patch"
[3]: https://docs.rs/json-patch/4.2.0/json_patch/ "json-patch 4.2.0 documentation"
[4]: https://github.com/kubernetes/kubernetes/blob/b3bc2ac58fa173967f27ade80f28cc5015b8c1c3/staging/src/k8s.io/apiserver/pkg/endpoints/handlers/patch.go "Kubernetes API server PATCH handler"
