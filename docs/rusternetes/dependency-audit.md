# Dependency audit: Rust ecosystem для Rusternetes

**Дата:** 18 августа 2026 года  
**Область:** фактически подключённые зависимости первого вертикального среза и заранее оценённые foundations для следующих slices  
**Принцип:** зрелый crate применяется для протокола, криптографии, HTTP, serialization, generated schemas, клиентского доступа и controller mechanics. Rusternetes сохраняет собственную реализацию только для server-side Kubernetes semantics: admission ordering, authorization decisions, API registry, persistence mapping, resource-version protocol, watch history, reconciliation policy и lifecycle control plane.

> Использование `kube`, `k8s-openapi`, `etcd-client`, `tonic`, `rustls` или OCI crates не означает запуск Go Kubernetes. Это библиотеки внутри Rust-процессов; оригинальные Go-компоненты не являются ни runtime dependency, ни fallback path.

## Метод оценки

Для каждого кандидата проверялись назначение, исходный проект, API-граница, версия/API compatibility, лицензия, operational maturity и соответствие архитектурной роли. Для control-plane server-side функций критически различается **библиотека-клиент** и **server implementation**: клиентский crate полезен для compatibility tests и controllers, но не может заменить API Server, storage или watch semantics.

## Прямые зависимости фазы 1

| Crate | Purpose | Закреплённая версия | License | Why | Alternative | Reason for choosing |
|---|---|---:|---|---|---|---|
| `axum` | HTTP router, extraction и response composition API Server | `0.7.9` | MIT | Нужны типизированные routes и предсказуемые response/error boundaries без собственного HTTP stack. | Direct Hyper; Actix Web | Тонкий слой над Hyper, построен на Tower middleware, не навязывает data model. Текущий выбор зафиксирован на `0.7.9` из-за Rust `1.75` в sandbox; текущий upstream axum требует Rust `1.80`, поэтому production toolchain перед релизом должен быть обновлён. [1] |
| `tokio` | async runtime, TCP listener, `RwLock`, task execution | `1.42.0` | MIT | API Server и storage используют async I/O и bounded async synchronization. | async-std; bespoke executor | Стандартная зрелая основа экосистемы Axum/Hyper/Tonic; не пишется самостоятельно. |
| `tower` | `Service` utilities в in-process HTTP tests; будущие middleware | `0.5.2` | MIT | Нужен лишь для test adapter сейчас, а впоследствии для timeout/trace/auth layers. | handwritten middleware | Соответствует native middleware model Axum; не включается в storage/model crates. [1] |
| `serde` / `serde_json` | JSON wire format и typed decoding/encoding | `1.0.217` / `1.0.134` | MIT OR Apache-2.0 | Kubernetes JSON API не следует сериализовать вручную. | manual encoder; simd-json | De-facto Rust serialization foundation, нужная для Kubernetes wire compatibility. |
| `thiserror` | structured domain errors | `1.0.69` | MIT OR Apache-2.0 | Сохраняет типизированные ошибки без самодельных макросов. | `anyhow`; handwritten `Display` | `anyhow` не подходит для public API error taxonomy; `thiserror` сохраняет concrete variants. |
| `time` | RFC 3339 `creationTimestamp` | `0.3.36` | MIT OR Apache-2.0 | Kubernetes metadata содержит timestamp; формирование строк вручную рискованно. | `chrono` | Минимально достаточный typed datetime API; версия совместима с текущей сборочной средой. |
| `uuid` | server-generated Kubernetes UID | `1.11.0` | Apache-2.0 OR MIT | UID обязан создаваться сервером и иметь уникальные historical occurrences. | custom random IDs | Реализует UUID правильно; собственная генерация запрещена. [2] |
| `base64` | wire validation `ConfigMap.binaryData` | `0.22.1` | MIT OR Apache-2.0 | `binaryData` в Kubernetes JSON передаётся base64. | manual base64 | Минимальная проверенная реализация encoding. [3] [4] |
| `reqwest` | black-box HTTP integration client | `0.12.12` | MIT OR Apache-2.0 | Проверяет реальный socket/API boundary, а не только Rust functions. | Hyper client; kube client | Независимый HTTP клиент годится для CRUD semantics. Это **test-only**, не часть production control plane. |

Все перечисленные версии и лицензии фактических зависимостей получены из локально разрешённых Cargo manifests. `http` и `idna_adapter` не считаются product dependencies: `http` будет удалён как неиспользуемый direct dependency, а `idna_adapter` останется только транзитивной записью lockfile, необходимой для временного Rust `1.75` toolchain.

## Kubernetes-specific candidates

| Crate | Purpose | Evaluated version | License | API / compatibility assessment | Decision |
|---|---|---:|---|---|---|
| `k8s-openapi` | Generated Rust bindings для Kubernetes API objects и OpenAPI concepts | `0.28.0`, feature `v1_36` | Apache-2.0 | Поддерживает ровно один Kubernetes feature-version в dependency graph и предоставляет generated core/API machinery types. Нельзя включать разные version features в разных crates. [5] | **Adopt at compatibility edges.** Добавить в отдельный compatibility-test crate после обновления build toolchain; использовать generated types для decode/round-trip API fixtures и будущих typed built-ins. Не использовать как единственную server model: crate не реализует server validation, storage, conversion, admission или resource versions. |
| `kube` / `kube-runtime` / `kube-derive` | Kubernetes client, generic API, watcher/reflector/controller runtime и CRD derive | `4.2.0` | Apache-2.0 | Facade объединяет client, runtime, derive и core abstractions. Runtime покрывает watcher recovery, reflectors, work scheduling, reconciliation/error policy и finalizer helper. [6] [7] | **Adopt for controllers and compatibility consumers**, когда server поддержит WATCH. Не использовать для реализации API Server/storage: это client-side framework, рассчитанный на существующий Kubernetes-compatible HTTP API. Использование контроллеров `kube-runtime` поверх API Rusternetes допустимо и предпочтительно собственному controller runtime. |
| `k8s-openapi-ext` | Builder/fluent extensions для generated Kubernetes objects | `0.27.5` | Apache-2.0 OR MIT — проверяется при точном pin | Уменьшает только boilerplate построения client objects. | **Defer.** Не нужен Phase 1; рассмотреть в controller tests после pinning `k8s-openapi`. |
| `schemars` | JSON Schema для CRD | `1.x` | MIT | Используется `kube-derive` для structural CRD schema. [6] | **Adopt with `kube-derive`** в CRD slice, не раньше. |

## Protocol, persistence, security и node candidates

| Crate | Purpose | Evaluated version | License | Why / alternative | Decision |
|---|---|---:|---|---|---|
| `etcd-client` | Async etcd v3 KV, transaction, watch, lease, lock, election и TLS client | `0.16` documented; exact release re-evaluated at implementation time | Apache-2.0 OR MIT | Покрывает необходимые etcd primitives и отделяет transport from Rusternetes storage semantics. Документация подчёркивает, что WatchClient — низкоуровневый stub: recovery/ordering/compaction policy остаются ответственностью Rusternetes. [8] [9] | **Adopt for durable storage slice** после toolchain upgrade (upstream current MSRV `1.80`). Реализовать собственный `ResourceStore` mapping и resource-version/watch invariants поверх etcd transactions; не писать etcd protocol client. |
| `tonic` + `prost` | gRPC/Protobuf transport и codegen | `0.14.x` | MIT | CRI определён Kubernetes как Protobuf + gRPC API; писать gRPC или protobuf codec вручную недопустимо. [10] [11] | **Adopt for CRI slice**, но до этого обновить Rust: текущий Tonic upstream указывает MSRV `1.88`. Generate bindings from proto, закреплённого к выбранному Kubernetes baseline; не копировать generated types вручную. |
| `k8s-cri` | Pre-generated CRI Rust types/client/server over Tonic | latest registry version re-evaluated at adoption | License and source provenance must pass exact-version audit | Найден новый crate с generated CRI surface, однако pin, provenance и version alignment с baseline пока не подтверждены. | **Do not adopt yet.** Предпочтительный fallback — `tonic-build` с official pinned `api.proto`; это code generation, а не собственная gRPC implementation. |
| `oci-spec` | OCI image, distribution и runtime spec types | `0.10.0` | Apache-2.0 | Полностью typed spec objects и builders; поддерживает Rust >=1.54. [12] [13] | **Adopt for runtime/OCI slice.** Не писать собственные OCI manifest/runtime types. |
| `oci-client` | OCI Distribution client для pull/push manifests and layers | version pinned at adoption | License revalidated in exact-version audit | Реализует registry protocol and registry auth surface. [14] | **Adopt conditionally** after security and performance review of exact release. Image cache, pull policy, garbage collection и kubelet lifecycle остаются native components. |
| `rustls` + `tokio-rustls` | TLS 1.2/1.3, certificate handling, crypto provider integration | latest compatible release pinned at security slice | Apache-2.0 OR MIT OR ISC | Rustls is production-used, implements TLS 1.2/1.3 and offers deliberate crypto-provider selection; custom cryptography is prohibited. [15] | **Adopt** for API Server and etcd mTLS. Default production policy will select an explicit provider; certificate rotation remains Rusternetes logic. |
| `jsonwebtoken` / `x509-parser` | JWT and X.509 parsing/verification primitives | exact versions audited at auth slice | License revalidated at pin | Avoid custom crypto and parser code. | **Candidate only.** Kubernetes authentication chain, service-account issuer rules, audience and RBAC remain Rusternetes server behavior. |
| `rtnetlink` | Async Linux netlink for CNI/node network plumbing | exact version audited at networking slice | Apache-2.0 OR MIT (verify at pin) | Replacing netlink protocol by ad-hoc sockets is unjustified. | **Candidate only.** CNI contract, IP allocation and network policy are project components, not delegated to a generic crate. |

## Observability choices

| Crate | Purpose | Evaluated version | License | Decision |
|---|---|---:|---|---|
| `tracing` + `tracing-subscriber` | Structured logs and async spans | `0.1` / `0.3` | MIT | **Adopt in observability slice.** Tokio-maintained structured instrumentation API; libraries emit spans but do not install global subscriber. [16] |
| `opentelemetry`, `opentelemetry-sdk`, `opentelemetry-otlp` | Traces, metrics, log export via OTLP | exact compatible release pinned at observability slice | Apache-2.0 | **Adopt with maturity guard.** Official project recommends `tracing` for structured logs; current docs mark different signal components at different maturity levels. Exporter configuration stays outside core business logic. [17] [18] |
| `tracing-opentelemetry` | Bridge `tracing` spans to OTEL trace/metric pipeline | exact compatible release pinned at observability slice | Apache-2.0 | **Adopt when OTEL enabled.** Beware documented bridge stability and keep request/reconcile/storage metrics additionally explicit, not inferred from logs. [19] |

## Explicit non-duplication boundaries

| Capability | Reuse | Rusternetes owns |
|---|---|---|
| HTTP/TLS | Axum/Hyper/Tower and Rustls | Kubernetes route registry, REST resource semantics, content negotiation policy, typed `Status` mapping. |
| Built-in resource shapes | `k8s-openapi` at compatibility edges and generated fixture tests | Defaulting, validation, conversion, metadata mutation, persistence representation and resource versioning. |
| External Kubernetes client / controller | `kube`, `kube-runtime`, `kube-derive` | API Server, server-side WATCH, storage and cluster controller policy. |
| Persistence transport | `etcd-client` | etcd key schema, transactions, revisions, compaction policy, list/watch consistency and failure translation. |
| CRI / gRPC | `tonic`, `prost`, generated official proto | Kubelet state machine, pod lifecycle, retry and runtime selection. |
| OCI | `oci-spec`, evaluated `oci-client` | Image pull policy, cache accounting, GC and integration with the node agent. |
| Observability mechanics | `tracing`, OpenTelemetry SDK/exporters | Metric names, cardinality policy, request IDs, business spans and operational SLO semantics. |
| RBAC/admission | generated `k8s-openapi` API types and Rust crypto/TLS primitives | Kubernetes RBAC policy evaluation, admission pipeline ordering and audit decision semantics. |

## Consequences for Phase 1

The existing Phase 1 code is kept intentionally small: it does not attempt to replace HTTP, JSON, time, UUID, base64 or client networking libraries. Its custom pieces are limited to the selected ConfigMap server contract, validation, storage revision ownership and public API errors. Before the controller slice, `kube-runtime` will be integrated instead of creating a parallel generic controller framework. Before the CRD slice, `kube-derive`/`schemars` will be used. Before the durable WATCH slice, `etcd-client` will be evaluated against the selected toolchain and pinned baseline.

The sandbox currently uses Rust `1.75`; this is lower than the current MSRV stated by current axum, etcd-client and tonic releases. The checked-in Phase 1 dependency pins preserve reproducible builds in the current environment, but a production release gate must update to a current stable Rust toolchain and repeat this audit before adopting later-phase crates. This is an explicit build-environment constraint, not a reason to reimplement those libraries.

## References

[1]: https://github.com/tokio-rs/axum "Axum repository"
[2]: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/ "Kubernetes object names and UIDs"
[3]: https://kubernetes.io/docs/reference/kubernetes-api/core/config-map-v1/ "ConfigMap v1 API reference"
[4]: https://kubernetes.io/docs/concepts/configuration/configmap/ "ConfigMaps"
[5]: https://docs.rs/k8s-openapi/latest/k8s_openapi/ "k8s-openapi documentation"
[6]: https://kube.rs/architecture/ "kube-rs architecture"
[7]: https://github.com/kube-rs/kube "kube-rs repository"
[8]: https://docs.rs/etcd-client/latest/etcd_client/ "etcd-client documentation"
[9]: https://github.com/etcdv3/etcd-client "etcd-client repository"
[10]: https://github.com/kubernetes/cri-api "Kubernetes CRI API"
[11]: https://github.com/hyperium/tonic "Tonic repository"
[12]: https://docs.rs/oci-spec/latest/oci_spec/ "oci-spec documentation"
[13]: https://github.com/containers/oci-spec-rs "oci-spec-rs repository"
[14]: https://docs.rs/oci-client/latest/oci_client/ "oci-client documentation"
[15]: https://github.com/rustls/rustls "Rustls repository"
[16]: https://github.com/tokio-rs/tracing "Tracing repository"
[17]: https://opentelemetry.io/docs/languages/rust/ "OpenTelemetry Rust documentation"
[18]: https://github.com/open-telemetry/opentelemetry-rust "OpenTelemetry Rust repository"
[19]: https://docs.rs/tracing-opentelemetry/latest/tracing_opentelemetry/ "tracing-opentelemetry documentation"
