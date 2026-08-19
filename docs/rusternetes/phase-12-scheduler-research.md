# Scheduler и Pod binding: official compatibility research

**Ветка:** `rusternetes/phase-1-api-storage`
**Дата сверки:** 2026-08-19

Kubernetes scheduler наблюдает Pods без назначенного `spec.nodeName`, находит feasible Nodes, оценивает их и сообщает API server решение через binding [1]. Scheduling Framework разделяет каждый scheduling context на serial scheduling cycle, который выбирает Node, и потенциально concurrent binding cycle, который применяет решение; unschedulable и internal-error outcomes возвращают Pod в очередь для retry [2].

| Upstream contract | Initial Rusternetes scheduler boundary |
|---|---|
| Candidate selection | Только typed Pods с empty `spec.nodeName`; scheduling gates, preemption и profiles остаются deferred. |
| Filter | Pluggable typed `Filter` plugins reject infeasible Node candidates. Первый built-in проверяет explicit node selector only after Node API exists. |
| Score | Scores rank feasible Nodes; deterministic lexicographic tie-break in the first slice, avoiding accidental non-determinism. |
| Reserve / bind | Reservation protects scheduler runtime state before bind; failure triggers idempotent unreserve. First slice uses CAS bind and does not reserve resources until Node allocatable contracts exist. |
| Binding | Binding applies selected `nodeName`; a Pod can become kubelet responsibility once it is set [3]. API write must be resourceVersion guarded and not be emulated by local scheduler state. |
| Retry | No feasible Node leaves Pod Pending and returns typed unschedulable result/requeue; no fabricated successful binding. |

Kubernetes documents Node selection as filtering followed by scoring, choosing the highest ranked feasible Node, then binding [1]. The framework documents serial scheduling cycles, concurrent-capable binding cycles, and the requirement to return rejected Pods to scheduling queue [2]. `nodeName` is not the user's placement preference: an empty value makes a Pod a scheduling candidate and a set value assigns kubelet responsibility [3].

## References

[1]: https://kubernetes.io/docs/concepts/scheduling-eviction/kube-scheduler/ "Kubernetes Scheduler"
[2]: https://kubernetes.io/docs/concepts/scheduling-eviction/scheduling-framework/ "Scheduling Framework"
[3]: https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/ "Pod v1 API"
