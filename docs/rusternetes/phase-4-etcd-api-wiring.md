# Rusternetes: подключение etcd ConfigMap storage к API Server

**Статус:** architecture contract до реализации  
**Ветка:** `rusternetes/phase-1-api-storage`  
**Цель:** сделать созданный `storage-etcd` реальным выбираемым backend HTTP CRUD API, не меняя публичную ConfigMap семантику и не заявляя неподдержанную durable WATCH функциональность.

## Backend boundary

API Server получает `ConfigMapBackend` — небольшой closed enum с двумя реализациями:

| Variant | CRUD/List | WATCH | Назначение |
|---|---|---|---|
| `InMemory` | existing single-process semantics | Phase 2 bounded in-memory replay and live fan-out | default для development и текущих HTTP WATCH tests |
| `Etcd` | `storage-etcd` JSON/key mapping и transactional operations | **явно не поддерживается в этой фазе** | durable ConfigMap CRUD через реальный etcd v3 |

HTTP handlers остаются единственным местом маршрутизации и namespace binding. Backend enum dispatches typed create/get/list/update/delete calls; validation, `Status` serialization и REST paths не дублируются.

## Runtime configuration

`rusternetes-api-server` использует in-memory backend по умолчанию. Чтобы включить durability, процесс получает оба параметра:

```text
RUSTERNETES_ETCD_ENDPOINTS=http://127.0.0.1:2379[,http://host:2379]
RUSTERNETES_ETCD_PREFIX=/registry/configmaps
```

`RUSTERNETES_ETCD_ENDPOINTS` пустой или отсутствующий выбирает in-memory backend. Если variable задана, startup connect failure останавливает server с non-zero error: процесс не silently falls back to volatile storage. Prefix defaults to `/registry/configmaps` only after endpoints are explicitly chosen.

## WATCH boundary

The Phase 2 in-memory `WATCH` contract requires replay, selector filtering, bookmarks, history compaction and bounded slow-consumer behavior. The current etcd repository has only CRUD/list transaction semantics. Therefore `watch=true` against `Etcd` returns a typed Kubernetes `400 BadRequest` explaining that durable WATCH wiring is pending. It never routes a persistent request to a separate in-memory store and never pretends that an etcd-backed stream is available.

Durable HTTP WATCH is a later end-to-end slice: it must translate etcd watch reconnect/compaction behavior and preserve the existing public selectors and response format.

## Proof

The integration suite starts one ephemeral etcd process, creates an `Etcd` API Server router and makes actual TCP/HTTP requests. It proves POST→GET persistence, namespaced and all-namespaces LIST, PUT revision change, stale conditional update `409`, DELETE, and explicit rejection of `watch=true`. A separate startup-config unit test proves endpoint parsing and fail-closed backend selection.
