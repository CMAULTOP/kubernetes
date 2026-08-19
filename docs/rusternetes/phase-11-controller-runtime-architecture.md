# Rusternetes: controller reconciliation runtime и etcd leader election

**Статус:** architecture contract до реализации
**Ветка:** `rusternetes/phase-1-api-storage`
**Срез:** Phase 11 — typed controller runtime и durable leader election

## Цель

Этот срез добавляет исполняемый framework для независимых control loops, но не притворяется реализацией всех built-in Kubernetes controllers. Каждый loop получает typed identity объекта, выполняет идемпотентный reconcile и возвращает явный результат requeue. Workers запускаются исключительно под подтверждённым leadership lease и отменяются до повторной обработки очереди при потере права лидера.

| Component | Contract | Chosen foundation |
|---|---|---|
| `ReconcileKey` | Closed typed resource identity: group, version, resource, namespace and name. | Собственный small value type; это project API contract, не HTTP client. |
| `Reconciler` | `async reconcile(key, context) -> ReconcileResult`; result is `Done`, immediate requeue or bounded delayed retry. | Native Rust trait and Tokio cancellation primitives. |
| `WorkQueue` | Bounded deduplicated keys; shutdown-aware blocking pop; exponential retry per key and explicit rate-limit reset after success. | Tokio `Mutex`, `Notify`, `time`; avoids external queue that does not model Kubernetes key de-duplication. |
| `ElectionLeaseStore` | Atomic create-if-absent, read, compare-and-swap renew/takeover and optional release. | Existing `etcd-client 0.16`; it supplies maintained gRPC transport and etcd Txn primitives. |
| `LeaderElector` | Acquire → start workers → renew through deadline → cancel workers on loss; release only if current holder. | Tokio tasks, cancellation token and injected clock abstraction. |

## Election record and storage

A record at reserved etcd key `/{prefix}/leader-election/{name}` contains `holder_identity`, `lease_duration_seconds`, `acquire_time`, `renew_time`, `lease_transitions` and its etcd `mod_revision`. Create uses `Compare::version(key, Equal, 0)`. Renew and expired takeover use `Compare::mod_revision(key, Equal, observed_mod_revision)`. The record's resource version is never guessed; each successful transaction uses the etcd response revision.

| State observed by candidate | Candidate action | Safety result |
|---|---|---|
| Lock absent | Transactional create for own identity. | At most one successful creator. |
| Own unexpired lock | CAS update `renew_time`, preserve `acquire_time` and transitions. | Leader continues only after server confirmation. |
| Foreign unexpired lock | Do not mutate; wait retry interval. | Standby cannot run reconciliation. |
| Foreign expired lock | CAS takeover, set fresh acquire/renew and increment transitions. | One candidate wins transition; losers re-read. |
| Own renewal cannot be confirmed until deadline | Cancel all leader workers and surface lost leadership. | No work continues on an unconfirmed holder. |

Wall-clock skew cannot be fully eliminated by Lease-based election; this follows Kubernetes' documented Lease model. The runtime validates duration/deadline/retry ordering and uses an injected `Clock` in deterministic tests. It does not use process-local `Mutex` as a substitute for durable authority.

## Queue and reconcile lifecycle

Source adapters enqueue only `ReconcileKey`, so repeated watch events coalesce. A worker removes an in-flight key, invokes the reconciler under the leader cancellation token, then resets its retry state on `Done`, schedules a requested requeue, or applies capped exponential backoff after a retriable failure. Panics are contained at the task boundary and reported as terminal runtime failure rather than silently dropped. A controller must re-read desired/observed state in reconcile; queue events are hints, not durable commands.

## Initial integration and exclusions

The first runtime exposes an in-process `ControllerManager` library plus an executable test harness. It does not start inside `rusternetes-api-server`, and no production controller is enabled until a concrete resource controller owns its semantics. The implementation uses existing `tokio` and `etcd-client`; no immature Kubernetes wrapper is added. The dependency audit will record the direct reuse of `etcd-client` and Tokio for this slice.

Informer caches, `coordination.k8s.io/v1` Lease REST endpoints, webhook-triggered controllers, metrics, leader handoff preference, sharding, and controller-specific business logic remain deliberately absent. Their API or reliability surface requires separate vertical slices.

## Proof plan

Unit tests cover queue de-duplication, retry scheduling, cancellation and stale holder rejection. Real-etcd integration starts two identities on one lease, asserts exactly one active worker, verifies CAS renewal, terminates or stalls the leader and observes bounded takeover by the standby. A controller test proves repeated queue events lead to an idempotent converged result rather than duplicated mutation.

## References

[1]: https://kubernetes.io/docs/concepts/architecture/controller/ "Kubernetes Controllers"
[2]: https://kubernetes.io/docs/concepts/architecture/leases/ "Kubernetes Leases"
[3]: https://github.com/kubernetes/kubernetes/blob/b3bc2ac58fa173967f27ade80f28cc5015b8c1c3/staging/src/k8s.io/client-go/tools/leaderelection/leaderelection.go#L208-L312 "client-go Run and renew"
[4]: https://github.com/kubernetes/kubernetes/blob/b3bc2ac58fa173967f27ade80f28cc5015b8c1c3/staging/src/k8s.io/client-go/tools/leaderelection/leaderelection.go#L441-L515 "client-go acquisition and renewal"
