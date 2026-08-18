# Сверка фазы 1 с официальной документацией Kubernetes

**Дата сверки:** 18 августа 2026 года  
**Область:** `core/v1` `ConfigMap`, API discovery, CRUD, metadata, selectors и границы следующего WATCH-среза  
**Эталон реализации:** commit `b3bc2ac58fa173967f27ade80f28cc5015b8c1c3`

## Вывод

Первый Rust-срез должен быть ориентирован не только на внутренние Go interfaces Kubernetes, но и на наблюдаемое HTTP-поведение. Официальная документация подтверждает типизированную модель `ConfigMap`, namespace-scoped identity, JSON по умолчанию, discovery, `resourceVersion` для list/watch-перехода, server-generated UID и весь набор equality/set-based label selectors. [1] [2] [3] [4]

Сверка выявила три изменения, которые обязательны **до** заявления готовности фазы 1: API Server обязан поддержать cluster-wide list `GET /api/v1/configmaps`, storage/API обязаны поддержать field selector как минимум для `metadata.name` и `metadata.namespace`, а namespace из URL должен заполнять отсутствующий `metadata.namespace` либо отвергать несовпадение. Это устраняет неоправданное расхождение с контрактом namespace-scoped REST paths. [1] [2]

| Контракт | Подтверждение в документации | Требование к Rust-срезу |
|---|---|---|
| Идентичность namespaced object | Идентичность задаётся group, resource, namespace и name; `ConfigMap` находится на `/api/v1/namespaces/{namespace}/configmaps`. [1] [2] | Хранить ключ `(namespace, name)`, отклонять конфликт namespace из пути и тела, выдавать `409` при повторном create. |
| Core API path | Core ресурсы используют `/api`, а не `/apis`; discovery рекламирует scope и supported verbs. [1] [3] | Реализовать `/api`, `/api/v1` и discovery `configmaps`; не рекламировать `PATCH`/`WATCH`, пока они не существуют. |
| ConfigMap fields | `data` и `binaryData` необязательны; binary values передаются как base64; ключи не пересекаются. [2] [4] | Хранить две раздельные typed map, base64-валидировать `binaryData`, отклонять пересечения и неверные ключи. |
| Immutable ConfigMap | После `immutable: true` нельзя вернуть флаг назад или изменить `data`/`binaryData`; metadata допускается менять. [2] [4] | Сохранять server metadata, валидировать update против предыдущего объекта. |
| Labels | Допустимы `=`, `==`, `!=`, `in`, `notin`, existence и does-not-exist; требования разделены запятыми и объединены AND. [5] | Парсер selector должен поддерживать все эти операторы, включая пробелы и списки в скобках. |
| Field selectors | ConfigMap REST API принимает `fieldSelector` для list. [2] | В фазе 1 реализовать `metadata.name` и `metadata.namespace` для equality/inequality; явно отклонять неподдерживаемые поля, а не игнорировать фильтр. |
| Resource versions | Каждый объект и list response содержат `resourceVersion`; list-then-watch не должен терять изменения. [1] | Каждая успешная mutation увеличивает revision; list возвращает snapshot revision. WATCH откладывается, но версия должна быть пригодна как его курсор. |
| Update | Kubernetes internally различает create/update для `PUT`; `ConfigMap` strategy разрешает unconditional update при пустой версии. [1] [7] | При переданной версии делать CAS и `409` при stale version; при пустой версии сохранять documented ConfigMap unconditional update. |
| Delete options | `DeleteOptions` задаёт preconditions; обычное удаление возвращает `Status`. [2] | UID/resourceVersion preconditions сравнивать внутри одной mutation с delete. |
| Finalizers | Удаление объекта с finalizer задаёт `deletionTimestamp`, оставляет объект и возвращает `202 Accepted`. [6] | Эта семантика **не реализована в фазе 1**. Временный API обязан отвечать явной ошибкой при delete объекта с finalizers, а не удалять его или симулировать success. Полная двухфазная deletion-поддержка — отдельный следующий vertical slice. |
| WATCH | Watch — streaming change tracking по resourceVersion; backlog ограничен историей, при истечении требуется `410 Gone`. [1] | Не создавать псевдо-stream. Следующий срез обязан включать ordered change log, compaction и bounded per-watcher queues. |

## Зафиксированные ограничения первой фазы

Первый срез намеренно не включает `PATCH`, `dryRun`, `fieldManager`, status subresources, server-side apply, pagination/continue tokens, authentication/authorization/admission, durable storage, `WATCH`, finalizer workflow и OpenAPI. Эти возможности не могут возвращать фиктивный `200`: несуществующий handler выдаёт Kubernetes-подобный error response. Discovery включает только действительно реализованные verbs.

`generateName` также не входит в первый срез. Документация разрешает API Server генерировать имя, но не требует его для object create. Поэтому `metadata.name` пока обязателен, а запрос без него получает typed validation error. [8]

## Следствия для следующих фаз

Документация подтверждает, что WATCH нельзя реализовать как неограниченный `broadcast`: он привязан к list resource version, может переподключаться с последнего revision, требует обработки истёкшей истории через `410 Gone`, а bookmark events не должны обещаться по фиксированному интервалу. [1] Поэтому Phase 2 получает отдельный durable/change-log slice до запуска controllers.

Финализаторы, owner references и garbage collection образуют связанную семантику: `deletionTimestamp`, запрет resurrect, ограничение мутаций finalizers и работа controller/GC. [6] [9] Их реализация должна быть отдельным сквозным срезом, а не условной ветвью внутри обычного delete handler.

## References

[1]: https://kubernetes.io/docs/reference/using-api/api-concepts/ "Kubernetes API Concepts"
[2]: https://kubernetes.io/docs/reference/kubernetes-api/core/config-map-v1/ "ConfigMap v1 API reference"
[3]: https://kubernetes.io/docs/concepts/overview/kubernetes-api/ "The Kubernetes API"
[4]: https://kubernetes.io/docs/concepts/configuration/configmap/ "ConfigMaps"
[5]: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/ "Labels and Selectors"
[6]: https://kubernetes.io/docs/concepts/overview/working-with-objects/finalizers/ "Finalizers"
[7]: https://github.com/kubernetes/kubernetes/blob/b3bc2ac58fa173967f27ade80f28cc5015b8c1c3/pkg/registry/core/configmap/strategy.go "ConfigMap strategy"
[8]: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/ "Object Names and IDs"
[9]: https://kubernetes.io/docs/concepts/overview/working-with-objects/owners-dependents/ "Owners and Dependents"
