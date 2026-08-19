# Controller runtime и Lease leader election: official compatibility research

**Ветка:** `rusternetes/phase-1-api-storage`
**Дата сверки:** 2026-08-19

Kubernetes определяет controller как незавершающийся control loop: он наблюдает как минимум один ресурс с желаемым состоянием (`spec`), предпринимает действия, приближающие наблюдаемое состояние к желаемому, и сообщает результат через API [1]. Контроллеры изолированы по ответственности, могут аварийно завершаться и должны быть способны продолжить работу после отказа; control plane запускает несколько копий для высокой доступности [1] [2].

| Contract | Verified upstream behavior | Rusternetes first implementation boundary |
|---|---|---|
| Reconciliation | Work derives from observed resource changes; each retry must be idempotent because cluster state can change at any point. | Typed `Reconciler` receives `ReconcileKey`, returns `RequeueAfter` / terminal outcome, and re-reads state through an explicit source interface. |
| Queueing | Multiple source events may collapse onto the same object; failed transient work is retried with bounded exponential backoff. | Bounded, key-deduplicating work queue with injected clock and cancellation-aware worker loops; no unbounded channels. |
| Leader exclusivity | HA control-plane components use a `coordination.k8s.io/v1 Lease` so one instance actively reconciles and peers are standby [2]. | etcd CAS Lease record with holder identity, renew deadline, duration and transition count; only a confirmed holder starts workers. |
| Renewal loss | Upstream starts work after acquiring the lease, retries renewal through a deadline, cancels leading work when renewal fails, then calls the stopped-leading callback [3]. | Cancellation token is triggered before workers may execute further queue work when renewal expires/conflicts; the process does not continue as an unconfirmed leader. |
| Acquisition | Standard Kubernetes flow attempts optimistic renewal, creates an absent lock, refuses an unexpired foreign holder, and uses a compare-and-swap update for takeover/renew [4]. | Atomic create-if-absent and mod-revision CAS; leadership observation is separate from authority to reconcile. |

The Kubernetes Lease object provides `holderIdentity`, `leaseDurationSeconds`, `acquireTime`, `renewTime` and `leaseTransitions`; expiration is based on the last observed renewal time [2] [4]. The first Rusternetes controller slice will use a narrow internal durable lease record under a reserved etcd prefix rather than prematurely expose the full `coordination.k8s.io/v1` REST resource. A subsequent API slice will make typed Lease objects externally visible without changing the election invariants.

The local upstream `client-go` reference establishes a critical safety ordering. It acquires the lock, starts `OnStartedLeading` only after acquisition, attempts renewal until `RenewDeadline`, and cancels the leader context immediately when renewal cannot be confirmed [3]. Its standard path creates only a missing record, rejects an unexpired foreign holder, preserves the acquisition time and increments transitions on holder change, then updates atomically [4]. Rusternetes will preserve those properties while delegating etcd transport and transaction mechanics to `etcd-client`.

## Explicit initial exclusions

The first runtime does not implement informer cache synchronization, shared-index informers, Kubernetes `Lease` HTTP API, coordinated leader election feature-gate behavior, shard assignment, metrics endpoints, controller hot reload, garbage collection, or scheduler binding. Those features require independently testable API/discovery, observability, or ownership contracts. No controller will be started from the API server binary until its lifecycle, identity and shutdown contract are wired in a dedicated executable surface.

## References

[1]: https://kubernetes.io/docs/concepts/architecture/controller/ "Kubernetes Controllers"
[2]: https://kubernetes.io/docs/concepts/architecture/leases/ "Leases"
[3]: https://github.com/kubernetes/kubernetes/blob/b3bc2ac58fa173967f27ade80f28cc5015b8c1c3/staging/src/k8s.io/client-go/tools/leaderelection/leaderelection.go#L208-L312 "client-go LeaderElector Run and renew"
[4]: https://github.com/kubernetes/kubernetes/blob/b3bc2ac58fa173967f27ade80f28cc5015b8c1c3/staging/src/k8s.io/client-go/tools/leaderelection/leaderelection.go#L441-L515 "client-go standard acquire or renew"
