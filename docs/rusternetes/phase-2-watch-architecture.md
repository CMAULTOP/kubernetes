# Rusternetes: архитектура WATCH-среза фазы 2

**Статус:** согласованный implementation contract до изменения кода  
**Рабочая ветка:** `rusternetes/phase-1-api-storage`  
**Эталон:** Kubernetes commit `b3bc2ac58fa173967f27ade80f28cc5015b8c1c3`

## Цель

Этот срез превращает уже работающие ConfigMap CRUD и storage revisions в работающий путь `LIST → WATCH`: клиент получает список с `metadata.resourceVersion`, начинает `GET .../configmaps?watch=true&resourceVersion=<rv>`, получает упорядоченные JSON watch events и может переподключиться с последней полученной версией. Каждая запись меняет revision ровно один раз и публикует один соответствующий event.

Первый WATCH-срез не симулирует бесконечный stream: он использует bounded in-memory history и bounded очередь каждого consumer. Для первой версии это корректный single-process implementation, но не durable backend. Рестарт сервера теряет state и history; это намеренно зафиксированное ограничение до etcd storage slice.

## Наблюдаемый контракт

| Сценарий | Ответ / событие | Причина |
|---|---|---|
| Создание ConfigMap | `ADDED` с серверным `resourceVersion` | Соответствует Kubernetes WatchEvent для нового объекта. [1] |
| Успешный PUT | `MODIFIED` с новой версией | Событие несёт новое состояние объекта. [1] |
| Успешный DELETE | `DELETED` с удаляемым объектом и revision удаления | Событие несёт состояние непосредственно перед удалением. [1] |
| `resourceVersion` в history window | Replay всех подходящих событий со строго большей версией, затем live events | `LIST` + watch не должны терять изменения. [2] |
| Версия старее начала history | HTTP `410 Gone`, Kubernetes `Status.reason: Expired` | Kubernetes трактует compacted/stale watch resourceVersion как ResourceExpired. [3] [4] |
| `allowWatchBookmarks=true` | Единственный `BOOKMARK` после replay, с текущей версией | Документация допускает bookmark, но запрещает обещать фиксированный интервал. [2] |
| Label/field selector | Filter применяется одинаково к replay и live events | LIST/WATCH selectors ограничивают возвращаемое множество объектов. [2] [5] |
| Медленный consumer | Его bounded queue не расширяется; subscription закрывается на первой переполненной отправке | Не допускает неограниченного роста памяти. Клиент переподключается с последней полученной версией. |
| Отмена HTTP client | Receiver уничтожается; следующий publish очищает closed sender | Состояние subscription не переживает отмену потока. |

## Wire model

Watch body использует Kubernetes JSON event envelope:

```json
{"type":"ADDED","object":{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"settings","resourceVersion":"42"}}}
```

Каждый JSON object отделён символом новой строки. `object` является либо типизированным `ConfigMap`, либо Kubernetes-подобным `Status` для event type `ERROR`. В этом срезе expired version является HTTP `410` до открытия stream, поэтому не выдаётся фиктивный `ERROR` event после успешного HTTP `200`.

## State ownership и concurrency model

| State | Owner | Concurrent operations | Primitive и причина |
|---|---|---|---|
| ConfigMap map и monotonically increasing revision | `InMemoryConfigMapStore` | CRUD, LIST, subscription setup | Единственный `tokio::sync::RwLock` на storage boundary. HTTP handlers не держат lock. |
| Bounded history (`VecDeque`) | Тот же storage state | Append на mutation, replay на subscription | Под тем же write lock, поэтому revision, history и registration имеют линейную точку. |
| Watcher registry | Тот же storage state | Register, fan-out, cleanup | Bounded `tokio::mpsc::Sender`; send выполняется только `try_send` в короткой write section и никогда не await-ится под lock. |
| Per-watcher event buffer | `tokio::mpsc` | Storage пишет, HTTP body читает | Capacity равна history capacity; это гарантирует bounded memory и сохраняет полный replay. |

Во время `watch(resourceVersion=r)` storage берёт write lock, проверяет compaction boundary, кладёт replay событий `revision > r` в новый bounded channel и **только затем** регистрирует sender в registry. Mutation не может попасть между replay и регистрацией, поэтому событие не теряется. После регистрации mutation добавляет event в history и вызывает `try_send` для подходящих watchers в increasing revision order.

## History и compaction

History — циклическое ограниченное окно из **256** событий. После переполнения самое старое событие удаляется. Если requested `resourceVersion < oldest_event.resourceVersion - 1`, storage возвращает typed `ResourceExpired`, которое API Server отображает в HTTP `410 Gone`. Это повторяет основной контракт Kubernetes watch-cache: history interval содержит только версии строго больше заданной, а запрос ниже recoverable boundary считается expired. [3] [4]

`resourceVersion` остаётся opaque string на HTTP wire, но первый backend кодирует монотонный `u64`. Парсинг происходит только storage boundary; invalid/non-numeric version возвращает `400 BadRequest`, а не panic.

## Backpressure и failure semantics

Использование unbounded broadcast запрещено. Replay никогда не превышает history capacity, а queue каждого watcher имеет ту же фиксированную capacity. Для live publish `try_send` различает `Closed` и `Full`; оба случая удаляют watcher из registry. В случае `Full` поток закрывается: Kubernetes-compatible client обязан reconnect с последнего обработанного `resourceVersion`. Этот policy сознательно предпочитает loss-free replay после reconnect неограниченному удержанию памяти.

В срез не включены periodic bookmarks, `sendInitialEvents`, `timeoutSeconds`, pagination, durable compaction или cross-process fan-out. `allowWatchBookmarks=true` означает только immediately available bookmark после replay; интервал не обещается.

## Integration proof

Интеграционный тест будет запускать Rust API Server на реальном TCP listener и проверять следующее: list возвращает revision; watch c этой версией получает `ADDED`/`MODIFIED`/`DELETED` в порядке; selector исключает не подходящие objects; bookmark выдаётся только при explicit opt-in; replay работает после reconnection; compacted revision получает HTTP `410` с `Expired`; медленный consumer не раздувает queue.

## References

[1]: https://github.com/kubernetes/kubernetes/blob/b3bc2ac58fa173967f27ade80f28cc5015b8c1c3/staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/watch.go "Kubernetes WatchEvent wire type"
[2]: https://kubernetes.io/docs/reference/using-api/api-concepts/ "Kubernetes API Concepts: watches, bookmarks and reconnect"
[3]: https://github.com/kubernetes/kubernetes/blob/b3bc2ac58fa173967f27ade80f28cc5015b8c1c3/staging/src/k8s.io/apiserver/pkg/storage/cacher/watch_cache_history.go "Kubernetes watch-cache history interval"
[4]: https://github.com/kubernetes/kubernetes/blob/b3bc2ac58fa173967f27ade80f28cc5015b8c1c3/staging/src/k8s.io/apimachinery/pkg/api/errors/errors.go "Kubernetes ResourceExpired API error"
[5]: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/ "Kubernetes Labels and Selectors"
