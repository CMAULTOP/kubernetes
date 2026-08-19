# Rusternetes: versioned API registry и resource strategy contracts

**Статус:** architecture contract до реализации  
**Ветка:** `rusternetes/phase-1-api-storage`  
**Срез:** Phase 8 — typed API registry

## Scope

Kubernetes publishes served API groups, versions, resources, scope and supported verbs through Discovery API. Core resources are served below `/api/<version>`, named API groups below `/apis/<group>/<version>`, while the resource identity is group, resource, namespace and name [1] [2]. Rusternetes will replace hand-authored discovery fragments and path assumptions with a registry that owns this metadata and dispatches a typed resource strategy.

| Registry responsibility | Contract in this slice |
|---|---|
| Group/version identity | Core `v1` registration is explicit and stable; duplicate registrations and unknown group-version lookups fail deterministically. |
| Discovery | `/api`, `/api/v1` and the future `/apis` root derive their advertised resources and verbs from registry entries, not duplicate route-local values. |
| Resource strategy | One strategy declaration supplies resource plural name, kind, scope and Kubernetes verbs. ConfigMap uses core group, `v1`, namespaced scope, and create/get/list/watch/update/delete. |
| Compatibility | A registered served version is immutable within the running registry. Future conversion layers must retain round-trip data, consistent with Kubernetes versioning policy [3]. |
| Routing boundary | Registry resolves known versioned paths into resource attributes for RBAC and later admission. It does not create a generic dynamic handler or pretend all Kubernetes resources are implemented. |

## Design constraints

Kubernetes Discovery describes APIs and available operations, whereas OpenAPI contains full schemas [1]. This phase provides discovery and typed route metadata only; it does not generate OpenAPI, introduce generic JSON objects, or add a CRD system. The only served resource remains the existing Rust ConfigMap strategy, so every discovery claim remains executable.

The core API path stays `/api/v1`. The initial registry is deliberately configured in code for one stable group/version rather than inventing an unfinished dynamic API registration protocol. Later core and named group resources extend the same registry through typed entries; they must not reintroduce hard-coded discovery lists into the HTTP server.

## Proof

Unit tests demonstrate that registry discovery, scope and verb metadata have a single source of truth, rejects duplicate `(group, version, resource)` registrations, and maps a ConfigMap HTTP URL to its resource strategy. HTTP integration tests confirm the advertised `APIResourceList` remains consistent with a usable ConfigMap endpoint.

## References

[1]: https://kubernetes.io/docs/concepts/overview/kubernetes-api/ "The Kubernetes API — Discovery, groups, and versions"
[2]: https://kubernetes.io/docs/reference/using-api/api-concepts/ "Kubernetes API Concepts — resource paths and verbs"
[3]: https://kubernetes.io/docs/reference/using-api/deprecation-policy/ "Kubernetes API Deprecation Policy"
