# Rusternetes: Node WATCH parity contract

**Ветка:** `rusternetes/phase-1-api-storage`
**Статус:** implemented and verified

Node WATCH will be exposed only when both backend modes provide the same visible contract. The Node resource is cluster scoped, so its request contains label selector, field selector, opaque `resourceVersion`, and optional bookmarks; it never accepts a namespace selector.

| Concern | Required behavior |
|---|---|
| Linearization | Create, update, heartbeat and delete append a typed Node event at the same revision as their storage mutation. |
| Replay | A requested retained resourceVersion replays strictly newer matching events before live registration. |
| Compaction | In-memory bounded history and etcd compaction report typed `410 Expired`; no stream implies continuity after replay has been lost. |
| Backpressure | Watcher queues are bounded. A slow or disconnected consumer is removed rather than growing memory. |
| Filtering | Label and field selectors apply to both replay and live events. |
| HTTP | JSON watch envelopes are newline-delimited, with the same serialization path for both backends. |

The registry advertises `watch` for Nodes because this contract is implemented and integration tested. Node status subresource remains separate: a heartbeat may cause a `MODIFIED` event but does not make a general status update route available.
