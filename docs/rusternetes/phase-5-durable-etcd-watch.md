# Rusternetes: durable ConfigMap WATCH через etcd

**Статус:** architecture contract до реализации  
**Ветка:** `rusternetes/phase-1-api-storage`  
**Срез:** Phase 5 — durable ConfigMap WATCH

## Цель и публичный контракт

Этот срез заменяет намеренное `400 BadRequest` для `watch=true` при `Etcd` backend на настоящий TCP/HTTP stream, источником которого является etcd v3 Watch API. HTTP API сохраняет уже реализованные Kubernetes пути, `resourceVersion`, label selector, field selector и JSON event envelope.

Kubernetes определяет watch как поток операций, произошедших после указанного `resourceVersion`; клиент восстанавливает state по `410 Gone`, выполняя LIST и новый WATCH [1]. etcd предоставляет кластерную монотонную revision как логические часы, а его Watch API читает события с заданной historical revision [2]. Rusternetes сопоставляет одно положительное etcd revision со строковым `metadata.resourceVersion` без иной интерпретации.

## Согласованная семантика

| Concern | Contract |
|---|---|
| Scope | Watch subscribes only below this repository's dedicated etcd prefix; namespace path narrows this key prefix. |
| Resume | An HTTP `resourceVersion=N` starts etcd watch at `N + 1`, matching Kubernetes' «changes after N» semantics. Empty or omitted version starts from the current etcd revision. |
| Event translation | `PUT` with etcd key version `1` becomes `ADDED`; subsequent `PUT` becomes `MODIFIED`; `DELETE` becomes `DELETED` using etcd `prev_kv`. |
| Object version | Every event object's `metadata.resourceVersion` is derived from etcd event `mod_revision`. |
| Selectors | Label and field selectors filter translated resources before they enter the bounded HTTP subscriber channel. |
| Replay | etcd historical watch replay supplies changes from the requested version before live events on the same stream. |
| Compaction | A preflight historical range and canceled etcd watch detect compacted requested revisions. Preflight failure returns Kubernetes `410 Gone` / `Expired` before HTTP body start. A compaction race after stream creation terminates that stream rather than falsely claiming continuity. |
| Backpressure | A bounded Tokio channel decouples etcd gRPC from each HTTP body. A slow HTTP consumer causes the bridge task to stop and drop its etcd stream; no unbounded per-client buffer is retained. |
| Bookmarks | This first durable stream does not synthesize bookmarks. `allowWatchBookmarks=true` is accepted, but clients must not depend on receipt because Kubernetes does not guarantee bookmark delivery. |

## Rust ecosystem boundary

`etcd-client 0.16` remains responsible for gRPC transport, protocol encoding and watch streaming. Rusternetes only provides Kubernetes-specific key mapping, event translation, selector filtering, `resourceVersion` conversion and HTTP serialization. Tokio provides the bounded channel and cancellation-by-drop mechanics. No Go Kubernetes component is invoked.

## Proof

The integration test starts real ephemeral etcd and a real Axum TCP server. It establishes HTTP `watch=true` with a prior resourceVersion, confirms replay plus a later mutation, verifies event ordering/object versions and verifies namespace/selector filtering. A compacted historical version is checked through the typed `410 Expired` preflight path.

## References

[1]: https://kubernetes.io/docs/reference/using-api/api-concepts/ "Kubernetes API Concepts — Efficient detection of changes"
[2]: https://etcd.io/docs/v3.7/learning/api/ "etcd v3 API — revisions and Watch API"
[3]: https://docs.rs/etcd-client/0.16.0/etcd_client/ "etcd-client 0.16 API documentation"
