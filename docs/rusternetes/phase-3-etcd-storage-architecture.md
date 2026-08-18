# Rusternetes: durable ConfigMap storage на etcd

**Статус:** architecture contract до реализации  
**Ветка:** `rusternetes/phase-1-api-storage`  
**Цель фазы:** добавить Rust-native etcd v3 repository без реализации собственного KV, transaction или gRPC протокола.

## Решение о зависимости

`etcd-client` выбран как async Rust client для etcd v3. Он использует Tokio и Tonic, покрывает KV, transactions и Watch APIs, тестируется с etcd 3.5 и распространяется под Apache-2.0 OR MIT. [1] [2] Это именно транспортная зависимость: mapping Kubernetes resource → etcd key/value, server validation, deletion preconditions и публичные Kubernetes errors остаются кодом Rusternetes.

Текущий upstream `etcd-client` требует Rust 1.80+, поэтому workspace поднимает минимальную версию Rust до **1.91**. Это снимает предыдущее временное ограничение Phase 1 и делает использование зрелого supported crate предпочтительнее самодельного etcd/gRPC клиента. [2]

| Crate | Purpose | Version | License | Alternative | Decision |
|---|---|---:|---|---|---|
| `etcd-client` | etcd v3 KV/Txn transport | `0.16` | Apache-2.0 OR MIT | handwritten gRPC / etcd protocol | Использовать: готовый async client с typed transactions. |
| `tokio` | async runtime | `1.42.0` | MIT | custom executor | Использовать: already selected runtime. |
| `serde_json` | canonical persisted ConfigMap payload | `1.0.134` | MIT OR Apache-2.0 | custom JSON | Использовать: object encoding/decoding не реализуется вручную. |

## Key layout и ownership

ConfigMap `namespace/name` хранится как один etcd key:

```text
/registry/configmaps/{namespace}/{name}
```

Имя и namespace проходят текущую validation до вызова backend; key encoding дополнительно отклоняет разделитель `/`, чтобы пользовательский вход не мог изменить etcd prefix. Value — JSON typed `ConfigMap`. Хранимый object всегда имеет `metadata.resourceVersion`, равный etcd `mod_revision` после commit.

> etcd revision — cluster-wide monotonic logical clock; transaction groups operations atomically and mutation increases revision только один раз. Поэтому etcd `mod_revision` является естественным opaque Kubernetes `resourceVersion` для этого backend. [3]

## CRUD/transaction semantics

| Operation | etcd primitive | Success | Failure mapping |
|---|---|---|---|
| Create | `Txn`: compare `Version(key)==0`, then `Put` | serialise response at committed revision; `resourceVersion=header.revision` | failed compare → `AlreadyExists` |
| Get | linearizable `Get(key)` | decode JSON; overwrite `resourceVersion` from `mod_revision` | no key → `NotFound`; invalid payload → `Internal` |
| List namespace | `Get(prefix)` | decode sorted key set; list `resourceVersion=header.revision` | decode/transport → `Internal` |
| Update with explicit version | `Txn`: compare `ModRevision(key)==expected`, then `Put` | committed revision becomes response RV | no key → `NotFound`; failed compare → `Conflict` |
| Update with omitted version | Get then Txn compare latest `ModRevision` | retries bounded only for server metadata preparation | conflict after read → `Conflict`, never blind overwrite |
| Delete | `Txn`: compare requested UID/RV where supplied and `Delete` | `DeleteResult.resourceVersion=header.revision` | no key / failed compare → `NotFound` or `Conflict` |

The update and delete paths deliberately use conditional transactions. etcd documents transactions as atomic If/Then/Else comparisons and shows `mod_revision` as the primitive for compare-and-swap. [3]

## Scope boundary

The repository provides durable CRUD/list and preserves etcd revisions. Phase 3 **does not yet expose etcd Watch through the HTTP API**: reconnection, compaction translation, selector filtering, bookmarks and HTTP slow-consumer policy must be integrated as one end-to-end replacement of the Phase 2 in-memory watch path. This phase uses etcd server integration tests to prove actual gRPC persistence and transactional conflicts, but does not claim durable HTTP WATCH yet.

The existing `InMemoryConfigMapStore` remains the default API Server backend while persistence interface extraction is completed in a later API wiring slice. No fallback to Go Kubernetes components exists.

## Test proof

Integration tests run an ephemeral `etcd` server process against the real `etcd-client` gRPC transport and prove create/reopen/get, namespaced list, conditional conflict and delete/revision behavior. Unit tests cover key encoding and `resourceVersion` conversion separately.

## References

[1]: https://docs.rs/etcd-client/latest/etcd_client/ "etcd-client API documentation"
[2]: https://github.com/etcdv3/etcd-client "etcd-client repository and compatibility policy"
[3]: https://etcd.io/docs/v3.6/learning/api/ "etcd v3 API: revisions, transactions and watch"
[4]: https://etcd.io/docs/v3.6/learning/why/ "etcd metadata-store design"
