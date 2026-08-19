# Rusternetes: Node `/status` compatibility contract

**Ветка:** `rusternetes/phase-1-api-storage`
**Статус:** implemented and verified

Node status is an executable cluster-scoped core/v1 subresource at `GET` and `PUT /api/v1/nodes/{name}/status`. Kubernetes treats subresources as resource paths beneath an individual object, and exposes the verbs of each subresource independently.[1] The implementation therefore registers `nodes/status` in discovery and resolves it to a distinct RBAC subresource attribute, rather than treating the path as an unstructured or non-resource request.

| Concern | Implemented contract |
|---|---|
| Request shape | The endpoint accepts a full typed `Node` object and requires that `metadata.name` match the URI. |
| Writable projection | Only `status` from the submitted object is committed. `spec`, labels, identity, UID, creation timestamp, generation, and type metadata remain from the persisted Node. |
| Main resource route | `PUT` and `PATCH /api/v1/nodes/{name}` ignore submitted status changes; callers use `/status` when they intend to mutate status. |
| Concurrency | Status and main resource share one `metadata.resourceVersion`; stale status writes receive Kubernetes `409 Conflict`. |
| Persistence | In-memory storage performs the update under its write lock. etcd storage reads the current object, validates the supplied version, and commits a compare-on-mod-revision transaction. |
| WATCH | A successful status update advances the shared revision and produces one typed `MODIFIED` Node event in both in-memory and etcd watch paths. |
| Authorization | `PUT /status` is converted to RBAC verb `update`, resource `nodes`, subresource `status`. Rules must explicitly allow `nodes/status`; access to `nodes` alone does not match it. |

> Kubernetes status-subresource semantics require that changes under `.status` are ignored by the main resource endpoint, while `/status` receives a full object but considers only the `.status` projection. `spec` and status also share storage and `resourceVersion`.[2]

The checked-out Kubernetes Node strategy confirms the built-in-resource specialization: the main Node strategy restores old status during ordinary updates, while the dedicated status strategy restores old spec before validation and storage.[3] Rusternetes mirrors that separation through typed `Node::preserve_server_metadata_from` and `Node::preserve_status_update_from` helpers. No generic unstructured object framework is introduced.

| Dependency | Purpose | Version | License | Decision |
|---|---|---:|---|---|
| `axum` | Existing typed HTTP routing and response handling | `0.7.9` | MIT | Retained; its route and extractor model already serves the primary Node API. |
| `etcd-client` | Existing gRPC etcd transaction and watch transport | `0.16.0` | Apache-2.0 | Retained; it provides mature durable CAS and watch transport while Rusternetes owns Kubernetes resource semantics. |
| New dependency | Node status projection | — | — | **None.** This is Kubernetes domain behavior over existing typed models and durable-store primitives; no mature Rust crate can supply this API-server-specific contract without becoming an external control-plane wrapper. |

The slice is tested at the registry, authorization mapping, in-memory HTTP, and real-etcd HTTP layers. The durable test verifies that a `PUT /status` mutation is persisted through the configured `EtcdNodeRepository`, preserves submitted-spec isolation, emits `MODIFIED` to a Node watch, and rejects a stale version.

## References

[1]: https://kubernetes.io/docs/reference/using-api/api-concepts/ "Kubernetes API Concepts"
[2]: https://github.com/kubernetes/design-proposals-archive/blob/main/api-machinery/customresources-subresources.md "Kubernetes custom-resource subresources proposal"
[3]: https://github.com/kubernetes/kubernetes/blob/b3bc2ac58fa173967f27ade80f28cc5015b8c1c3/pkg/registry/core/node/strategy.go "Kubernetes Node strategy"
