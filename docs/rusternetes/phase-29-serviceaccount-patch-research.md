# Phase 29 ServiceAccount PATCH Research Notes

## Official API contract

The core/v1 API explicitly exposes `PATCH /api/v1/namespaces/{namespace}/serviceaccounts/{name}`. Non-apply patch requests do not require `fieldManager`; the currently supported Rusternetes scope will be RFC 6902 JSON Patch (`application/json-patch+json`) and RFC 7396 JSON Merge Patch (`application/merge-patch+json`), not strategic merge or server-side apply. [1]

ServiceAccount is namespace-scoped. Its documented mutable user-facing fields include `automountServiceAccountToken`, which controls default service-account token mounting and may be overridden by the Pod-level setting, plus metadata and the `imagePullSecrets` / `secrets` lists. Kubernetes documents `secrets` as a keyed strategic-merge list but this slice uses JSON Patch and JSON Merge Patch only; a merge-patch list replacement is therefore expected RFC 7396 behavior. [1] [2]

## Compatibility and safety boundaries

| Area | Required Rusternetes behavior |
|---|---|
| Endpoint identity | Bind namespace and name from the route after patch application; reject a patched object that attempts to change them. |
| Server ownership | Preserve type meta, UID, creation timestamp, generation/resource version and other server-controlled identity fields according to existing ServiceAccount replace rules. |
| Concurrency | Respect a supplied `metadata.resourceVersion` through the existing durable/in-memory update paths; a stale patch returns Conflict. |
| Media types | Accept only JSON Patch and JSON Merge Patch, using the established `json-patch` crate; reject unsupported media types. |
| Authorization and admission | Retain the same route authorization verb and update-time admission ordering as the existing resource update pipeline. |
| Integration | Validate both in-memory and configured real-etcd paths; no fallback from durable storage. |

## Confirmed local implementation boundaries

The typed ServiceAccount implementation validates DNS-safe namespace/name, labels, and all secret references. Its update validator already rejects namespace or name changes and storage update paths preserve server metadata while enforcing optional optimistic resourceVersion checks. Therefore PATCH must start from the persisted object, apply the selected RFC patch, bind the request URI identity, then use the unchanged backend `update` method. This preserves in-memory watch publication, etcd mod-revision CAS, and server metadata behavior without new storage code.

The existing ConfigMap, Pod, Namespace and Node handlers already share a `NodePatchFormat` media-type decoder and `json-patch` transformations. The ServiceAccount implementation will use that same decoder, error taxonomy and request-body validation rather than adding duplicate generic PATCH parsing.

## Reused dependency audit

| Crate | Purpose | Version | License | Reason for use |
|---|---|---:|---|---|
| `json-patch` | RFC 6902 and RFC 7396 transformations | `4.2.0` | MIT | Existing mature workspace dependency already used by ConfigMap, Pod and Namespace PATCH paths; reuse avoids separate parsers. |
| `serde_json` | JSON serialization/deserialization around patch application | `1.0.134` | MIT OR Apache-2.0 | Existing workspace dependency, compatible with workspace serde 1.0.217. |

## Sources

[1]: https://kubernetes.io/docs/reference/kubernetes-api/core/service-account-v1/ "Kubernetes ServiceAccount v1 API Reference"
[2]: https://kubernetes.io/docs/tasks/manage-kubernetes-objects/update-api-object-kubectl-patch/ "Update API Objects in Place Using kubectl patch"
[3]: https://kubernetes.io/docs/concepts/security/service-accounts/ "Kubernetes Service Accounts"
