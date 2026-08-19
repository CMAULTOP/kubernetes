# Phase 30 Node Metadata PATCH Research Notes

## Official API contract

The core/v1 API exposes `PATCH /api/v1/nodes/{name}` and separately exposes the Node status subresource. The Node API describes `status` as system-populated and read-only on the main resource, so the main-resource PATCH path must preserve stored Node status. Status mutations belong to `/api/v1/nodes/{name}/status`. [1]

The API reference lists `metadata` and `spec` as the regular Node resource fields. For non-apply patch requests, `fieldManager` is optional. This Rusternetes slice remains scoped to RFC 6902 JSON Patch and RFC 7396 JSON Merge Patch using the established `json-patch` crate; it does not claim strategic-merge or server-side apply support. [1] [2]

## Required local contract

| Area | Required behavior |
|---|---|
| Main PATCH | Apply patch over the stored Node; route name remains authoritative. |
| Mutable payload | Permit metadata and current supported NodeSpec changes. |
| Status ownership | Restore stored status before regular backend update; expose status writes only on existing `/status` route. |
| Identity and server metadata | Reject name changes; preserve UID, creation timestamp, generation and server-assigned resource version through the backend update. |
| Concurrency | Carry patched `metadata.resourceVersion` into the existing in-memory CAS and etcd mod-revision update paths. |
| Media types | Accept `application/json-patch+json` and `application/merge-patch+json`; reject unsupported or malformed content. |

## Confirmed local audit

The existing main Node update path already calls `preserve_server_metadata_from`, which restores the stored Node status after validating metadata, name, UID and labels. The Node status route separately calls `preserve_status_update_from`, which restores type meta, metadata and spec while retaining only the submitted status. Consequently no new persistence primitive is necessary. The missing vertical coverage is to prove regular JSON Patch and Merge Patch metadata changes persist while an attempted main-resource status mutation is discarded, in both in-memory and real-etcd paths.

## Sources

[1]: https://kubernetes.io/docs/reference/kubernetes-api/core/node-v1/ "Kubernetes Node v1 API Reference"
[2]: https://kubernetes.io/docs/tasks/manage-kubernetes-objects/update-api-object-kubectl-patch/ "Update API Objects in Place Using kubectl patch"
