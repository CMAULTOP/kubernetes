//! Axum HTTP API server for the first Rusternetes vertical slice.

use std::{io, sync::Arc};

use async_stream::stream;
use axum::{
    body::{Body, Bytes},
    extract::{Extension, Path, Query, Request, State},
    http::{header, HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use rusternetes_admission::{
    AdmissionChain, AdmissionRequest, NamespaceLifecyclePlugin,
    NamespacePhase as AdmissionNamespacePhase, NamespaceStateReader,
};
use rusternetes_api_registry::ApiRegistry;
use rusternetes_api_types::{
    ApiStatus, ConfigMap, DeleteOptions, FieldSelector, LabelSelector, Namespace, NamespacePhase,
    Node, Pod, ServiceAccount,
};
use rusternetes_authn::{AuthenticationChain, RequestIdentity, VerifiedServiceAccountJwt};
use rusternetes_authz_rbac::{AuthorizationRequest, RbacAuthorizer};
use rusternetes_common::{ApiError, ResourceReference};
use rusternetes_storage::{
    ConfigMapWatchRequest, ConfigMapWatchSubscription as InMemoryConfigMapWatchSubscription,
    DeleteResult, InMemoryConfigMapStore, InMemoryNamespaceStore, InMemoryNodeStore,
    InMemoryPodStore, InMemoryServiceAccountStore, NamespaceWatchRequest,
    NamespaceWatchSubscription as InMemoryNamespaceWatchSubscription, NodeWatchRequest,
    NodeWatchSubscription as InMemoryNodeWatchSubscription, PodWatchRequest,
    PodWatchSubscription as InMemoryPodWatchSubscription, ServiceAccountWatchRequest,
    ServiceAccountWatchSubscription as InMemoryServiceAccountWatchSubscription,
};
use rusternetes_storage_etcd::{
    EtcdConfigMapRepository, EtcdConfigMapWatchSubscription, EtcdNamespaceRepository,
    EtcdNamespaceWatchSubscription, EtcdNodeRepository, EtcdNodeWatchSubscription,
    EtcdPodRepository, EtcdPodWatchSubscription, EtcdServiceAccountRepository,
    EtcdServiceAccountWatchSubscription,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The ConfigMap persistence implementation selected when building an API Server router.
///
/// The enum is intentionally closed for this phase: all HTTP handlers retain one public contract
/// while backend-specific semantics remain explicit. The etcd variant never falls back to memory.
#[derive(Clone)]
pub enum ConfigMapBackend {
    InMemory(Arc<InMemoryConfigMapStore>),
    Etcd(Arc<EtcdConfigMapRepository>),
}

/// A uniform stream receiver for the HTTP layer. Backends retain ownership of their transport and
/// cancellation semantics; the API Server only serializes the typed Kubernetes event envelope.
pub enum ConfigMapWatchSubscription {
    InMemory(InMemoryConfigMapWatchSubscription),
    Etcd(EtcdConfigMapWatchSubscription),
}

impl ConfigMapWatchSubscription {
    async fn recv(&mut self) -> Option<rusternetes_api_types::ConfigMapWatchEvent> {
        match self {
            Self::InMemory(subscription) => subscription.recv().await,
            Self::Etcd(subscription) => subscription.recv().await,
        }
    }
}

impl ConfigMapBackend {
    async fn create(&self, resource: ConfigMap) -> Result<ConfigMap, ApiError> {
        match self {
            Self::InMemory(store) => store.create(resource).await,
            Self::Etcd(repository) => repository.create(resource).await,
        }
    }

    async fn get(&self, namespace: &str, name: &str) -> Result<ConfigMap, ApiError> {
        match self {
            Self::InMemory(store) => store.get(namespace, name).await,
            Self::Etcd(repository) => repository.get(namespace, name).await,
        }
    }

    async fn list(
        &self,
        namespace: Option<&str>,
        label_selector: &LabelSelector,
        field_selector: &FieldSelector,
    ) -> Result<rusternetes_api_types::ConfigMapList, ApiError> {
        match (self, namespace) {
            (Self::InMemory(store), namespace) => {
                Ok(store.list(namespace, label_selector, field_selector).await)
            }
            (Self::Etcd(repository), Some(namespace)) => {
                repository
                    .list(namespace, label_selector, field_selector)
                    .await
            }
            (Self::Etcd(repository), None) => {
                repository.list_all(label_selector, field_selector).await
            }
        }
    }

    async fn update(&self, resource: ConfigMap) -> Result<ConfigMap, ApiError> {
        match self {
            Self::InMemory(store) => store.update(resource).await,
            Self::Etcd(repository) => repository.update(resource).await,
        }
    }

    async fn delete(
        &self,
        namespace: &str,
        name: &str,
        options: DeleteOptions,
    ) -> Result<DeleteResult, ApiError> {
        match self {
            Self::InMemory(store) => store.delete(namespace, name, options).await,
            Self::Etcd(repository) => repository.delete(namespace, name, options).await,
        }
    }

    async fn watch(
        &self,
        request: ConfigMapWatchRequest,
    ) -> Result<ConfigMapWatchSubscription, ApiError> {
        match self {
            Self::InMemory(store) => store
                .watch(request)
                .await
                .map(ConfigMapWatchSubscription::InMemory),
            Self::Etcd(repository) => repository
                .watch(request)
                .await
                .map(ConfigMapWatchSubscription::Etcd),
        }
    }
}

/// The Pod persistence implementation selected alongside the ConfigMap backend.
#[derive(Clone)]
pub enum PodBackend {
    InMemory(Arc<InMemoryPodStore>),
    Etcd(Arc<EtcdPodRepository>),
}

pub enum PodWatchSubscription {
    InMemory(InMemoryPodWatchSubscription),
    Etcd(EtcdPodWatchSubscription),
}

impl PodWatchSubscription {
    async fn recv(&mut self) -> Option<rusternetes_api_types::PodWatchEvent> {
        match self {
            Self::InMemory(subscription) => subscription.recv().await,
            Self::Etcd(subscription) => subscription.recv().await,
        }
    }
}

impl PodBackend {
    async fn create(&self, resource: Pod) -> Result<Pod, ApiError> {
        match self {
            Self::InMemory(store) => store.create(resource).await,
            Self::Etcd(repository) => repository.create(resource).await,
        }
    }

    async fn get(&self, namespace: &str, name: &str) -> Result<Pod, ApiError> {
        match self {
            Self::InMemory(store) => store.get(namespace, name).await,
            Self::Etcd(repository) => repository.get(namespace, name).await,
        }
    }

    async fn list(
        &self,
        namespace: Option<&str>,
        label_selector: &LabelSelector,
        field_selector: &FieldSelector,
    ) -> Result<rusternetes_api_types::PodList, ApiError> {
        match (self, namespace) {
            (Self::InMemory(store), namespace) => {
                Ok(store.list(namespace, label_selector, field_selector).await)
            }
            (Self::Etcd(repository), Some(namespace)) => {
                repository
                    .list(namespace, label_selector, field_selector)
                    .await
            }
            (Self::Etcd(repository), None) => {
                repository.list_all(label_selector, field_selector).await
            }
        }
    }

    async fn update(&self, resource: Pod) -> Result<Pod, ApiError> {
        match self {
            Self::InMemory(store) => store.update(resource).await,
            Self::Etcd(repository) => repository.update(resource).await,
        }
    }

    async fn delete(
        &self,
        namespace: &str,
        name: &str,
        options: DeleteOptions,
    ) -> Result<DeleteResult, ApiError> {
        match self {
            Self::InMemory(store) => store.delete(namespace, name, options).await,
            Self::Etcd(repository) => repository.delete(namespace, name, options).await,
        }
    }

    async fn watch(&self, request: PodWatchRequest) -> Result<PodWatchSubscription, ApiError> {
        match self {
            Self::InMemory(store) => store
                .watch(request)
                .await
                .map(PodWatchSubscription::InMemory),
            Self::Etcd(repository) => repository
                .watch(request)
                .await
                .map(PodWatchSubscription::Etcd),
        }
    }
}

/// The ServiceAccount persistence implementation selected alongside other core/v1 backends.
#[derive(Clone)]
pub enum ServiceAccountBackend {
    InMemory(Arc<InMemoryServiceAccountStore>),
    Etcd(Arc<EtcdServiceAccountRepository>),
}

pub enum ServiceAccountWatchSubscription {
    InMemory(InMemoryServiceAccountWatchSubscription),
    Etcd(EtcdServiceAccountWatchSubscription),
}

impl ServiceAccountWatchSubscription {
    async fn recv(&mut self) -> Option<rusternetes_api_types::ServiceAccountWatchEvent> {
        match self {
            Self::InMemory(subscription) => subscription.recv().await,
            Self::Etcd(subscription) => subscription.recv().await,
        }
    }
}

impl ServiceAccountBackend {
    async fn create(&self, resource: ServiceAccount) -> Result<ServiceAccount, ApiError> {
        match self {
            Self::InMemory(store) => store.create(resource).await,
            Self::Etcd(repository) => repository.create(resource).await,
        }
    }

    async fn get(&self, namespace: &str, name: &str) -> Result<ServiceAccount, ApiError> {
        match self {
            Self::InMemory(store) => store.get(namespace, name).await,
            Self::Etcd(repository) => repository.get(namespace, name).await,
        }
    }

    async fn list(
        &self,
        namespace: Option<&str>,
        label_selector: &LabelSelector,
        field_selector: &FieldSelector,
    ) -> Result<rusternetes_api_types::ServiceAccountList, ApiError> {
        match (self, namespace) {
            (Self::InMemory(store), namespace) => {
                Ok(store.list(namespace, label_selector, field_selector).await)
            }
            (Self::Etcd(repository), Some(namespace)) => {
                repository
                    .list(namespace, label_selector, field_selector)
                    .await
            }
            (Self::Etcd(repository), None) => {
                repository.list_all(label_selector, field_selector).await
            }
        }
    }

    async fn update(&self, resource: ServiceAccount) -> Result<ServiceAccount, ApiError> {
        match self {
            Self::InMemory(store) => store.update(resource).await,
            Self::Etcd(repository) => repository.update(resource).await,
        }
    }

    async fn delete(
        &self,
        namespace: &str,
        name: &str,
        options: DeleteOptions,
    ) -> Result<DeleteResult, ApiError> {
        match self {
            Self::InMemory(store) => store.delete(namespace, name, options).await,
            Self::Etcd(repository) => repository.delete(namespace, name, options).await,
        }
    }

    async fn watch(
        &self,
        request: ServiceAccountWatchRequest,
    ) -> Result<ServiceAccountWatchSubscription, ApiError> {
        match self {
            Self::InMemory(store) => store
                .watch(request)
                .await
                .map(ServiceAccountWatchSubscription::InMemory),
            Self::Etcd(repository) => repository
                .watch(request)
                .await
                .map(ServiceAccountWatchSubscription::Etcd),
        }
    }
}

/// The cluster-scoped Namespace persistence implementation selected with other core/v1 stores.
#[derive(Clone)]
pub enum NamespaceBackend {
    InMemory(Arc<InMemoryNamespaceStore>),
    Etcd(Arc<EtcdNamespaceRepository>),
}

pub enum NamespaceWatchSubscription {
    InMemory(InMemoryNamespaceWatchSubscription),
    Etcd(EtcdNamespaceWatchSubscription),
}

impl NamespaceWatchSubscription {
    async fn recv(&mut self) -> Option<rusternetes_api_types::NamespaceWatchEvent> {
        match self {
            Self::InMemory(subscription) => subscription.recv().await,
            Self::Etcd(subscription) => subscription.recv().await,
        }
    }
}

impl NamespaceBackend {
    async fn create(&self, resource: Namespace) -> Result<Namespace, ApiError> {
        match self {
            Self::InMemory(store) => store.create(resource).await,
            Self::Etcd(repository) => repository.create(resource).await,
        }
    }

    async fn get(&self, name: &str) -> Result<Namespace, ApiError> {
        match self {
            Self::InMemory(store) => store.get(name).await,
            Self::Etcd(repository) => repository.get(name).await,
        }
    }

    async fn list(
        &self,
        label_selector: &LabelSelector,
        field_selector: &FieldSelector,
    ) -> Result<rusternetes_api_types::NamespaceList, ApiError> {
        match self {
            Self::InMemory(store) => Ok(store.list(label_selector, field_selector).await),
            Self::Etcd(repository) => repository.list(label_selector, field_selector).await,
        }
    }

    async fn update(&self, resource: Namespace) -> Result<Namespace, ApiError> {
        match self {
            Self::InMemory(store) => store.update(resource).await,
            Self::Etcd(repository) => repository.update(resource).await,
        }
    }

    async fn delete(&self, name: &str, options: DeleteOptions) -> Result<DeleteResult, ApiError> {
        match self {
            Self::InMemory(store) => store.delete(name, options).await,
            Self::Etcd(repository) => repository.delete(name, options).await,
        }
    }

    async fn watch(
        &self,
        request: NamespaceWatchRequest,
    ) -> Result<NamespaceWatchSubscription, ApiError> {
        match self {
            Self::InMemory(store) => store
                .watch(request)
                .await
                .map(NamespaceWatchSubscription::InMemory),
            Self::Etcd(repository) => repository
                .watch(request)
                .await
                .map(NamespaceWatchSubscription::Etcd),
        }
    }
}

/// The cluster-scoped Node persistence implementation selected with the core/v1 backend set.
#[derive(Clone)]
pub enum NodeBackend {
    InMemory(Arc<InMemoryNodeStore>),
    Etcd(Arc<EtcdNodeRepository>),
}

pub enum NodeWatchSubscription {
    InMemory(InMemoryNodeWatchSubscription),
    Etcd(EtcdNodeWatchSubscription),
}

impl NodeWatchSubscription {
    async fn recv(&mut self) -> Option<rusternetes_api_types::NodeWatchEvent> {
        match self {
            Self::InMemory(subscription) => subscription.recv().await,
            Self::Etcd(subscription) => subscription.recv().await,
        }
    }
}

impl NodeBackend {
    async fn create(&self, resource: Node) -> Result<Node, ApiError> {
        match self {
            Self::InMemory(store) => store.create(resource).await,
            Self::Etcd(repository) => repository.create(resource).await,
        }
    }

    async fn get(&self, name: &str) -> Result<Node, ApiError> {
        match self {
            Self::InMemory(store) => store.get(name).await,
            Self::Etcd(repository) => repository.get(name).await,
        }
    }

    async fn list(
        &self,
        label_selector: &LabelSelector,
        field_selector: &FieldSelector,
    ) -> Result<rusternetes_api_types::NodeList, ApiError> {
        match self {
            Self::InMemory(store) => Ok(store.list_filtered(label_selector, field_selector).await),
            Self::Etcd(repository) => repository.list(label_selector, field_selector).await,
        }
    }

    async fn update(&self, resource: Node) -> Result<Node, ApiError> {
        match self {
            Self::InMemory(store) => store.update(resource).await,
            Self::Etcd(repository) => repository.update(resource).await,
        }
    }

    async fn update_status(&self, resource: Node) -> Result<Node, ApiError> {
        match self {
            Self::InMemory(store) => store.update_status(resource).await,
            Self::Etcd(repository) => repository.update_status(resource).await,
        }
    }

    async fn delete(&self, name: &str, options: DeleteOptions) -> Result<DeleteResult, ApiError> {
        match self {
            Self::InMemory(store) => store.delete(name, options).await,
            Self::Etcd(repository) => repository.delete(name, options).await,
        }
    }

    async fn watch(&self, request: NodeWatchRequest) -> Result<NodeWatchSubscription, ApiError> {
        match self {
            Self::InMemory(store) => store
                .watch(request)
                .await
                .map(NodeWatchSubscription::InMemory),
            Self::Etcd(repository) => repository
                .watch(request)
                .await
                .map(NodeWatchSubscription::Etcd),
        }
    }
}

/// All typed core/v1 persistence backends supplied to an executable API Server.
#[derive(Clone)]
pub struct CoreApiBackend {
    pub config_maps: ConfigMapBackend,
    pub pods: PodBackend,
    pub service_accounts: ServiceAccountBackend,
    pub namespaces: NamespaceBackend,
    pub nodes: NodeBackend,
}

/// Adapts the selected typed Namespace repository to the admission lifecycle contract.
#[derive(Clone)]
pub struct BackendNamespaceStateReader {
    backend: NamespaceBackend,
}

impl BackendNamespaceStateReader {
    pub fn new(backend: NamespaceBackend) -> Self {
        Self { backend }
    }
}

impl NamespaceStateReader for BackendNamespaceStateReader {
    fn phase<'a>(
        &'a self,
        namespace: &'a str,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<AdmissionNamespacePhase, ApiError>> + Send + 'a,
        >,
    > {
        Box::pin(async move {
            match self.backend.get(namespace).await {
                Ok(resource) => match resource.status.phase {
                    Some(NamespacePhase::Terminating) => Ok(AdmissionNamespacePhase::Terminating),
                    Some(NamespacePhase::Active) | None => Ok(AdmissionNamespacePhase::Active),
                },
                Err(ApiError::NotFound { .. }) => Ok(AdmissionNamespacePhase::Missing),
                Err(error) => Err(error),
            }
        })
    }
}

/// Builds the fail-closed NamespaceLifecycle admission stage backed by the selected namespace store.
pub fn namespace_lifecycle_admission(backend: NamespaceBackend) -> AdmissionChain {
    AdmissionChain::new(vec![Arc::new(NamespaceLifecyclePlugin::new(Arc::new(
        BackendNamespaceStateReader::new(backend),
    )))])
}

impl CoreApiBackend {
    pub fn in_memory(config_maps: Arc<InMemoryConfigMapStore>) -> Self {
        Self::legacy(ConfigMapBackend::InMemory(config_maps))
    }

    fn legacy(config_maps: ConfigMapBackend) -> Self {
        Self {
            config_maps,
            pods: PodBackend::InMemory(Arc::new(InMemoryPodStore::new())),
            service_accounts: ServiceAccountBackend::InMemory(Arc::new(
                InMemoryServiceAccountStore::new(),
            )),
            namespaces: NamespaceBackend::InMemory(Arc::new(InMemoryNamespaceStore::new())),
            nodes: NodeBackend::InMemory(Arc::new(InMemoryNodeStore::new())),
        }
    }
}

/// Shared immutable application state. Every typed resource backend remains the sole owner of data.
#[derive(Clone)]
pub struct AppState {
    backend: CoreApiBackend,
    registry: ApiRegistry,
    authorization: AuthorizationMode,
    admission: AdmissionChain,
}

impl AppState {
    pub fn new(backend: ConfigMapBackend) -> Self {
        Self::with_registry_authorization_and_admission(
            CoreApiBackend::legacy(backend),
            ApiRegistry::core_v1(),
            AuthorizationMode::default(),
            AdmissionChain::default(),
        )
    }

    pub fn with_registry_authorization_and_admission(
        backend: CoreApiBackend,
        registry: ApiRegistry,
        authorization: AuthorizationMode,
        admission: AdmissionChain,
    ) -> Self {
        Self {
            backend,
            registry,
            authorization,
            admission,
        }
    }
}

#[derive(Clone)]
struct AuthenticationState {
    chain: AuthenticationChain,
    service_accounts: ServiceAccountBackend,
    pods: PodBackend,
    nodes: NodeBackend,
}

/// Builds an API Server router using the current in-memory development backend.
pub fn router(store: Arc<InMemoryConfigMapStore>) -> Router {
    router_with_backend(ConfigMapBackend::InMemory(store))
}

/// Builds the API Server router with an explicitly selected ConfigMap persistence backend.
///
/// The default authentication chain permits the documented anonymous Kubernetes identity. Production
/// configuration can instead construct [`router_with_backend_and_auth`] with credentials and a
/// restrictive anonymous policy.
pub fn router_with_backend(backend: ConfigMapBackend) -> Router {
    router_with_backend_and_auth(backend, AuthenticationChain::default())
}

/// Authorization configuration used by the API Server after authentication succeeds.
#[derive(Clone, Debug, Default)]
pub enum AuthorizationMode {
    /// Development compatibility mode. Production callers should select [`Self::Rbac`].
    #[default]
    AlwaysAllow,
    /// Fail-closed Kubernetes RBAC policy evaluation.
    Rbac(RbacAuthorizer),
}

/// Builds the API Server router with explicit storage and authentication implementations.
pub fn router_with_backend_and_auth(
    backend: ConfigMapBackend,
    authentication: AuthenticationChain,
) -> Router {
    router_with_backend_auth_and_authorization(
        backend,
        authentication,
        AuthorizationMode::default(),
    )
}

/// Builds the API Server router with explicit storage, authentication, and authorization layers.
///
/// Middleware order is fixed: credentials establish `RequestIdentity`, RBAC evaluates that identity
/// and normalized request attributes, then the handler executes typed validating admission before
/// it invokes any persistence mutation.
pub fn router_with_backend_auth_and_authorization(
    backend: ConfigMapBackend,
    authentication: AuthenticationChain,
    authorization: AuthorizationMode,
) -> Router {
    router_with_backend_auth_authorization_and_admission(
        backend,
        authentication,
        authorization,
        AdmissionChain::default(),
    )
}

/// Builds the API Server router with explicit storage, authentication, authorization, and
/// pre-persistence validating admission implementations.
pub fn router_with_backend_auth_authorization_and_admission(
    backend: ConfigMapBackend,
    authentication: AuthenticationChain,
    authorization: AuthorizationMode,
    admission: AdmissionChain,
) -> Router {
    router_with_core_backend_auth_authorization_and_admission(
        CoreApiBackend::legacy(backend),
        authentication,
        authorization,
        admission,
    )
}

/// Builds the API Server router with explicit typed core/v1 storage implementations.
pub fn router_with_core_backend_auth_authorization_and_admission(
    backend: CoreApiBackend,
    authentication: AuthenticationChain,
    authorization: AuthorizationMode,
    admission: AdmissionChain,
) -> Router {
    let state = AppState::with_registry_authorization_and_admission(
        backend,
        ApiRegistry::core_v1(),
        authorization,
        admission,
    );
    let authentication_state = AuthenticationState {
        chain: authentication,
        service_accounts: state.backend.service_accounts.clone(),
        pods: state.backend.pods.clone(),
        nodes: state.backend.nodes.clone(),
    };
    Router::new()
        .route("/version", get(version))
        .route("/api", get(api_versions))
        .route("/api/v1", get(core_v1_api_resources))
        .route("/api/v1/configmaps", get(list_all_config_maps))
        .route("/api/v1/pods", get(list_all_pods))
        .route("/api/v1/nodes", get(list_nodes).post(create_node))
        .route(
            "/api/v1/nodes/:name",
            get(get_node)
                .put(replace_node)
                .patch(patch_node)
                .delete(delete_node),
        )
        .route(
            "/api/v1/nodes/:name/status",
            get(get_node_status).put(replace_node_status),
        )
        .route(
            "/api/v1/namespaces",
            get(list_namespaces).post(create_namespace),
        )
        .route(
            "/api/v1/namespaces/:name",
            get(get_namespace)
                .put(replace_namespace)
                .delete(delete_namespace),
        )
        .route(
            "/api/v1/namespaces/:namespace/configmaps",
            get(list_config_maps).post(create_config_map),
        )
        .route(
            "/api/v1/namespaces/:namespace/configmaps/:name",
            get(get_config_map)
                .put(replace_config_map)
                .delete(delete_config_map),
        )
        .route(
            "/api/v1/namespaces/:namespace/pods",
            get(list_pods).post(create_pod),
        )
        .route(
            "/api/v1/namespaces/:namespace/pods/:name",
            get(get_pod).put(replace_pod).delete(delete_pod),
        )
        .route(
            "/api/v1/namespaces/:namespace/serviceaccounts",
            get(list_service_accounts).post(create_service_account),
        )
        .route(
            "/api/v1/namespaces/:namespace/serviceaccounts/:name",
            get(get_service_account)
                .put(replace_service_account)
                .delete(delete_service_account),
        )
        .fallback(not_found)
        // Axum applies the last layer first. Authentication must populate extensions before RBAC
        // evaluates them, so authorization is added before authentication here.
        .layer(middleware::from_fn_with_state(
            state.clone(),
            authorize_request,
        ))
        .layer(middleware::from_fn_with_state(
            authentication_state,
            authenticate_request,
        ))
        .with_state(state)
}

/// Builds a router over all typed core/v1 backends with a lifecycle admission reader bound to the
/// same Namespace repository.
pub fn router_with_core_backend(backend: CoreApiBackend) -> Router {
    let admission = namespace_lifecycle_admission(backend.namespaces.clone());
    router_with_core_backend_auth_authorization_and_admission(
        backend,
        AuthenticationChain::default(),
        AuthorizationMode::default(),
        admission,
    )
}

/// Selects an API Server backend from optional process configuration values.
///
/// Absence (or whitespace-only content) selects the development in-memory store. A non-empty etcd
/// setting is fail-closed: connection errors are returned to the caller instead of silently
/// falling back to volatile state.
pub async fn backend_from_etcd_config(
    endpoints: Option<&str>,
    key_prefix: Option<&str>,
) -> Result<ConfigMapBackend, ApiError> {
    let Some(endpoints) = endpoints.filter(|value| !value.trim().is_empty()) else {
        return Ok(ConfigMapBackend::InMemory(Arc::new(
            InMemoryConfigMapStore::new(),
        )));
    };
    let endpoints = endpoints
        .split(',')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if endpoints.is_empty() {
        return Err(ApiError::BadRequest {
            message: "RUSTERNETES_ETCD_ENDPOINTS must contain at least one non-empty endpoint"
                .to_owned(),
        });
    }
    let key_prefix = key_prefix.filter(|value| !value.trim().is_empty());
    let repository = EtcdConfigMapRepository::connect(endpoints, key_prefix).await?;
    Ok(ConfigMapBackend::Etcd(Arc::new(repository)))
}

/// Selects all executable core/v1 repositories atomically from process configuration.
///
/// A configured etcd endpoint must initialize ConfigMap, Pod, and Namespace repositories together;
/// any connection or prefix failure aborts startup rather than producing a mixed durable/volatile
/// control plane. The prefix is the core-v1 registry root, with per-resource paths below it.
pub async fn core_backend_from_etcd_config(
    endpoints: Option<&str>,
    key_prefix: Option<&str>,
) -> Result<CoreApiBackend, ApiError> {
    let Some(raw_endpoints) = endpoints.filter(|value| !value.trim().is_empty()) else {
        let backend = CoreApiBackend::in_memory(Arc::new(InMemoryConfigMapStore::new()));
        ensure_default_namespace(&backend.namespaces).await?;
        return Ok(backend);
    };
    let endpoints = raw_endpoints
        .split(',')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if endpoints.is_empty() {
        return Err(ApiError::BadRequest {
            message: "RUSTERNETES_ETCD_ENDPOINTS must contain at least one non-empty endpoint"
                .to_owned(),
        });
    }
    let root = key_prefix
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("/registry");
    let config_maps_prefix = core_resource_prefix(root, "configmaps")?;
    let pods_prefix = core_resource_prefix(root, "pods")?;
    let service_accounts_prefix = core_resource_prefix(root, "serviceaccounts")?;
    let namespaces_prefix = core_resource_prefix(root, "namespaces")?;
    let nodes_prefix = core_resource_prefix(root, "nodes")?;
    let config_maps =
        EtcdConfigMapRepository::connect(endpoints.clone(), Some(&config_maps_prefix)).await?;
    let pods = EtcdPodRepository::connect(endpoints.clone(), Some(&pods_prefix)).await?;
    let service_accounts =
        EtcdServiceAccountRepository::connect(endpoints.clone(), Some(&service_accounts_prefix))
            .await?;
    let namespaces =
        EtcdNamespaceRepository::connect(endpoints.clone(), Some(&namespaces_prefix)).await?;
    let nodes = EtcdNodeRepository::connect(endpoints, Some(&nodes_prefix)).await?;
    let backend = CoreApiBackend {
        config_maps: ConfigMapBackend::Etcd(Arc::new(config_maps)),
        pods: PodBackend::Etcd(Arc::new(pods)),
        service_accounts: ServiceAccountBackend::Etcd(Arc::new(service_accounts)),
        namespaces: NamespaceBackend::Etcd(Arc::new(namespaces)),
        nodes: NodeBackend::Etcd(Arc::new(nodes)),
    };
    ensure_default_namespace(&backend.namespaces).await?;
    Ok(backend)
}

fn core_resource_prefix(root: &str, resource: &str) -> Result<String, ApiError> {
    let root = root.trim();
    if !root.starts_with('/') || root.ends_with('/') || root.contains('\0') {
        return Err(ApiError::BadRequest {
            message: "RUSTERNETES_ETCD_PREFIX must start with '/', must not end with '/', and must not contain NUL"
                .to_owned(),
        });
    }
    Ok(format!("{root}/{resource}"))
}

async fn ensure_default_namespace(backend: &NamespaceBackend) -> Result<(), ApiError> {
    let namespace = Namespace {
        type_meta: rusternetes_api_types::TypeMeta::namespace(),
        metadata: rusternetes_api_types::ObjectMeta {
            name: Some("default".to_owned()),
            ..rusternetes_api_types::ObjectMeta::default()
        },
        ..Namespace::default()
    };
    match backend.create(namespace).await {
        Ok(_) | Err(ApiError::AlreadyExists { .. }) => Ok(()),
        Err(error) => Err(error),
    }
}

async fn authenticate_request(
    State(authentication): State<AuthenticationState>,
    mut request: Request,
    next: Next,
) -> Response {
    let authorization = match request.headers().get(header::AUTHORIZATION) {
        Some(value) => match value.to_str() {
            Ok(value) => Some(value),
            Err(_) => {
                return ApiRejection(ApiError::Unauthorized {
                    message: "Authorization header is not valid HTTP text".to_owned(),
                })
                .into_response()
            }
        },
        None => None,
    };
    let identity = match authentication.chain.authenticate(authorization) {
        Ok(identity) => Ok(identity),
        Err(static_error) => match authentication
            .chain
            .verify_service_account_jwt(authorization)
        {
            Ok(Some(claims)) => {
                authenticate_live_service_account(
                    &authentication.service_accounts,
                    &authentication.pods,
                    &authentication.nodes,
                    claims,
                )
                .await
            }
            Ok(None) | Err(_) => Err(static_error),
        },
    };
    match identity {
        Ok(identity) => {
            request.extensions_mut().insert(identity);
            next.run(request).await
        }
        Err(error) => ApiRejection(error).into_response(),
    }
}

async fn authenticate_live_service_account(
    backend: &ServiceAccountBackend,
    pods: &PodBackend,
    nodes: &NodeBackend,
    claims: VerifiedServiceAccountJwt,
) -> Result<RequestIdentity, ApiError> {
    let namespace = claims.kubernetes.namespace;
    let pod_claim = claims.kubernetes.pod;
    let node_claim = claims.kubernetes.node;
    let name = claims.kubernetes.service_account.name;
    let expected_uid = claims.kubernetes.service_account.uid;
    let resource = backend
        .get(&namespace, &name)
        .await
        .map_err(|error| match error {
            ApiError::NotFound { .. } => ApiError::Unauthorized {
                message: "ServiceAccount JWT refers to a missing ServiceAccount".to_owned(),
            },
            other => other,
        })?;
    if resource.metadata.uid.as_deref() != Some(expected_uid.as_str()) {
        return Err(ApiError::Unauthorized {
            message: "ServiceAccount JWT UID does not match the current ServiceAccount".to_owned(),
        });
    }
    if let Some(pod) = pod_claim {
        let resource = pods
            .get(&namespace, &pod.name)
            .await
            .map_err(|error| match error {
                ApiError::NotFound { .. } => ApiError::Unauthorized {
                    message: "ServiceAccount JWT refers to a missing bound Pod".to_owned(),
                },
                other => other,
            })?;
        if resource.metadata.uid.as_deref() != Some(pod.uid.as_str()) {
            return Err(ApiError::Unauthorized {
                message: "ServiceAccount JWT UID does not match the current bound Pod".to_owned(),
            });
        }
    } else if let Some(node) = node_claim {
        let resource = nodes.get(&node.name).await.map_err(|error| match error {
            ApiError::NotFound { .. } => ApiError::Unauthorized {
                message: "ServiceAccount JWT refers to a missing bound Node".to_owned(),
            },
            other => other,
        })?;
        if resource.metadata.uid.as_deref() != Some(node.uid.as_str()) {
            return Err(ApiError::Unauthorized {
                message: "ServiceAccount JWT UID does not match the current bound Node".to_owned(),
            });
        }
    }
    RequestIdentity::authenticated(
        format!("system:serviceaccount:{namespace}:{name}"),
        Some(expected_uid),
        [
            "system:serviceaccounts".to_owned(),
            format!("system:serviceaccounts:{namespace}"),
        ],
        Default::default(),
    )
}

async fn authorize_request(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    match &state.authorization {
        AuthorizationMode::AlwaysAllow => next.run(request).await,
        AuthorizationMode::Rbac(authorizer) => {
            let Some(identity) = request.extensions().get::<RequestIdentity>() else {
                return ApiRejection(ApiError::Forbidden {
                    message: "request reached authorization without an authenticated identity"
                        .to_owned(),
                })
                .into_response();
            };
            let attributes =
                authorization_request_from_http(&state.registry, request.method(), request.uri());
            if authorizer.authorize(identity, &attributes) {
                next.run(request).await
            } else {
                ApiRejection(ApiError::Forbidden {
                    message: "RBAC policy does not allow this request".to_owned(),
                })
                .into_response()
            }
        }
    }
}

fn authorization_request_from_http(
    registry: &ApiRegistry,
    method: &axum::http::Method,
    uri: &axum::http::Uri,
) -> AuthorizationRequest {
    let Some(resolved) = registry.resolve_core_v1_path(uri.path()) else {
        return AuthorizationRequest::non_resource(
            method.as_str().to_ascii_lowercase(),
            uri.path().to_owned(),
        );
    };
    let collection = resolved.name.is_none();
    let watch = uri.query().is_some_and(|query| {
        query
            .split('&')
            .any(|item| matches!(item, "watch=true" | "watch=1"))
    });
    let verb = match method.as_str() {
        "POST" => "create",
        "PUT" => "update",
        "PATCH" => "patch",
        "DELETE" if collection => "deletecollection",
        "DELETE" => "delete",
        "GET" | "HEAD" if watch => "watch",
        "GET" | "HEAD" if collection => "list",
        "GET" | "HEAD" => "get",
        other => other,
    };
    AuthorizationRequest::resource(
        verb,
        resolved.group,
        resolved.resource.plural,
        resolved.subresource.map(str::to_owned),
        resolved.namespace,
        resolved.name,
    )
}

/// A local response wrapper makes the API error representation available to Axum without
/// violating Rust's orphan rules.
struct ApiRejection(ApiError);

type ApiResult<T> = Result<T, ApiRejection>;

impl From<ApiError> for ApiRejection {
    fn from(error: ApiError) -> Self {
        Self(error)
    }
}

impl IntoResponse for ApiRejection {
    fn into_response(self) -> Response {
        let status =
            StatusCode::from_u16(self.0.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        (status, Json(ApiStatus::from(&self.0))).into_response()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListQuery {
    #[serde(default)]
    label_selector: Option<String>,
    #[serde(default)]
    field_selector: Option<String>,
    #[serde(default)]
    resource_version: Option<String>,
    #[serde(default)]
    watch: Option<String>,
    #[serde(default)]
    allow_watch_bookmarks: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct VersionInfo {
    major: &'static str,
    minor: &'static str,
    git_version: &'static str,
    git_commit: &'static str,
    git_tree_state: &'static str,
    build_date: &'static str,
    go_version: &'static str,
    compiler: &'static str,
    platform: &'static str,
}

async fn version() -> Json<VersionInfo> {
    Json(VersionInfo {
        major: "0",
        minor: "1",
        git_version: "v0.1.0-rusternetes",
        git_commit: "b3bc2ac58fa173967f27ade80f28cc5015b8c1c3",
        git_tree_state: "clean",
        build_date: "2026-08-18T00:00:00Z",
        go_version: "not-applicable",
        compiler: "rustc",
        platform: "linux/amd64",
    })
}

async fn api_versions(State(state): State<AppState>) -> Json<rusternetes_api_types::ApiVersions> {
    Json(state.registry.core_api_versions())
}

async fn core_v1_api_resources(
    State(state): State<AppState>,
) -> Json<rusternetes_api_types::ApiResourceList> {
    // The route itself exists only because core/v1 is registered by the built-in strategy.
    Json(
        state
            .registry
            .discovery("", "v1")
            .expect("core/v1 API route requires a registered core/v1 strategy"),
    )
}

async fn list_all_config_maps(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> ApiResult<Response> {
    if is_watch_request(&query)? {
        return open_config_map_watch(&state, &query, None).await;
    }
    reject_list_resource_version(&query)?;
    let label_selector = LabelSelector::parse(query.label_selector.as_deref())?;
    let field_selector = FieldSelector::parse(query.field_selector.as_deref())?;
    Ok(Json(
        state
            .backend
            .config_maps
            .list(None, &label_selector, &field_selector)
            .await?,
    )
    .into_response())
}

async fn list_config_maps(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    Query(query): Query<ListQuery>,
) -> ApiResult<Response> {
    if is_watch_request(&query)? {
        return open_config_map_watch(&state, &query, Some(namespace)).await;
    }
    reject_list_resource_version(&query)?;
    let label_selector = LabelSelector::parse(query.label_selector.as_deref())?;
    let field_selector = FieldSelector::parse(query.field_selector.as_deref())?;
    Ok(Json(
        state
            .backend
            .config_maps
            .list(Some(&namespace), &label_selector, &field_selector)
            .await?,
    )
    .into_response())
}

async fn create_config_map(
    State(state): State<AppState>,
    Extension(identity): Extension<RequestIdentity>,
    Path(namespace): Path<String>,
    body: Bytes,
) -> ApiResult<(StatusCode, Json<ConfigMap>)> {
    let resource = bind_namespace(decode_config_map(body)?, &namespace)?;
    let admission_request = AdmissionRequest::create(identity, resource.clone())?;
    state.admission.validate(&admission_request).await?;
    let created = state.backend.config_maps.create(resource).await?;
    Ok((StatusCode::CREATED, Json(created)))
}

async fn get_config_map(
    State(state): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
) -> ApiResult<Json<ConfigMap>> {
    Ok(Json(
        state.backend.config_maps.get(&namespace, &name).await?,
    ))
}

async fn replace_config_map(
    State(state): State<AppState>,
    Extension(identity): Extension<RequestIdentity>,
    Path((namespace, name)): Path<(String, String)>,
    body: Bytes,
) -> ApiResult<Json<ConfigMap>> {
    let resource = bind_update_identity(decode_config_map(body)?, &namespace, &name)?;
    let old_object = state.backend.config_maps.get(&namespace, &name).await?;
    let admission_request = AdmissionRequest::update(identity, resource.clone(), old_object)?;
    state.admission.validate(&admission_request).await?;
    Ok(Json(state.backend.config_maps.update(resource).await?))
}

async fn delete_config_map(
    State(state): State<AppState>,
    Extension(identity): Extension<RequestIdentity>,
    Path((namespace, name)): Path<(String, String)>,
    body: Bytes,
) -> ApiResult<Json<ApiStatus>> {
    let options = decode_delete_options(body)?;
    let old_object = state.backend.config_maps.get(&namespace, &name).await?;
    let admission_request = AdmissionRequest::delete(identity, old_object)?;
    state.admission.validate(&admission_request).await?;
    let result = state
        .backend
        .config_maps
        .delete(&namespace, &name, options)
        .await?;
    Ok(Json(ApiStatus::success(format!(
        "configmaps {name:?} deleted at resourceVersion {}",
        result.resource_version
    ))))
}

async fn list_service_accounts(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    Query(query): Query<ListQuery>,
) -> ApiResult<Response> {
    if is_watch_request(&query)? {
        return open_service_account_watch(&state, &query, namespace).await;
    }
    reject_list_resource_version(&query)?;
    let label_selector = LabelSelector::parse(query.label_selector.as_deref())?;
    let field_selector = FieldSelector::parse(query.field_selector.as_deref())?;
    Ok(Json(
        state
            .backend
            .service_accounts
            .list(Some(&namespace), &label_selector, &field_selector)
            .await?,
    )
    .into_response())
}

async fn create_service_account(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    body: Bytes,
) -> ApiResult<(StatusCode, Json<ServiceAccount>)> {
    let resource = bind_service_account_namespace(decode_service_account(body)?, &namespace)?;
    let created = state.backend.service_accounts.create(resource).await?;
    Ok((StatusCode::CREATED, Json(created)))
}

async fn get_service_account(
    State(state): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
) -> ApiResult<Json<ServiceAccount>> {
    Ok(Json(
        state
            .backend
            .service_accounts
            .get(&namespace, &name)
            .await?,
    ))
}

async fn replace_service_account(
    State(state): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
    body: Bytes,
) -> ApiResult<Json<ServiceAccount>> {
    let resource =
        bind_service_account_update_identity(decode_service_account(body)?, &namespace, &name)?;
    Ok(Json(state.backend.service_accounts.update(resource).await?))
}

async fn delete_service_account(
    State(state): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
    body: Bytes,
) -> ApiResult<Json<ApiStatus>> {
    let options = decode_delete_options(body)?;
    let result = state
        .backend
        .service_accounts
        .delete(&namespace, &name, options)
        .await?;
    Ok(Json(ApiStatus::success(format!(
        "serviceaccounts {name:?} deleted at resourceVersion {}",
        result.resource_version
    ))))
}

async fn list_all_pods(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> ApiResult<Response> {
    if is_watch_request(&query)? {
        return open_pod_watch(&state, &query, None).await;
    }
    reject_list_resource_version(&query)?;
    let label_selector = LabelSelector::parse(query.label_selector.as_deref())?;
    let field_selector = FieldSelector::parse(query.field_selector.as_deref())?;
    Ok(Json(
        state
            .backend
            .pods
            .list(None, &label_selector, &field_selector)
            .await?,
    )
    .into_response())
}

async fn list_pods(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    Query(query): Query<ListQuery>,
) -> ApiResult<Response> {
    if is_watch_request(&query)? {
        return open_pod_watch(&state, &query, Some(namespace)).await;
    }
    reject_list_resource_version(&query)?;
    let label_selector = LabelSelector::parse(query.label_selector.as_deref())?;
    let field_selector = FieldSelector::parse(query.field_selector.as_deref())?;
    Ok(Json(
        state
            .backend
            .pods
            .list(Some(&namespace), &label_selector, &field_selector)
            .await?,
    )
    .into_response())
}

async fn create_pod(
    State(state): State<AppState>,
    Extension(identity): Extension<RequestIdentity>,
    Path(namespace): Path<String>,
    body: Bytes,
) -> ApiResult<(StatusCode, Json<Pod>)> {
    let resource = bind_pod_namespace(decode_pod(body)?, &namespace)?;
    let admission_request = AdmissionRequest::pod_create(identity, resource.clone())?;
    state.admission.validate(&admission_request).await?;
    let created = state.backend.pods.create(resource).await?;
    Ok((StatusCode::CREATED, Json(created)))
}

async fn get_pod(
    State(state): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
) -> ApiResult<Json<Pod>> {
    Ok(Json(state.backend.pods.get(&namespace, &name).await?))
}

async fn replace_pod(
    State(state): State<AppState>,
    Extension(identity): Extension<RequestIdentity>,
    Path((namespace, name)): Path<(String, String)>,
    body: Bytes,
) -> ApiResult<Json<Pod>> {
    let resource = bind_pod_update_identity(decode_pod(body)?, &namespace, &name)?;
    let old_object = state.backend.pods.get(&namespace, &name).await?;
    let admission_request = AdmissionRequest::pod_update(identity, resource.clone(), old_object)?;
    state.admission.validate(&admission_request).await?;
    Ok(Json(state.backend.pods.update(resource).await?))
}

async fn delete_pod(
    State(state): State<AppState>,
    Extension(identity): Extension<RequestIdentity>,
    Path((namespace, name)): Path<(String, String)>,
    body: Bytes,
) -> ApiResult<Json<ApiStatus>> {
    let options = decode_delete_options(body)?;
    let old_object = state.backend.pods.get(&namespace, &name).await?;
    let admission_request = AdmissionRequest::pod_delete(identity, old_object)?;
    state.admission.validate(&admission_request).await?;
    let result = state
        .backend
        .pods
        .delete(&namespace, &name, options)
        .await?;
    Ok(Json(ApiStatus::success(format!(
        "pods {name:?} deleted at resourceVersion {}",
        result.resource_version
    ))))
}

async fn list_namespaces(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> ApiResult<Response> {
    if is_watch_request(&query)? {
        return open_namespace_watch(&state, &query).await;
    }
    reject_list_resource_version(&query)?;
    let label_selector = LabelSelector::parse(query.label_selector.as_deref())?;
    let field_selector = FieldSelector::parse(query.field_selector.as_deref())?;
    Ok(Json(
        state
            .backend
            .namespaces
            .list(&label_selector, &field_selector)
            .await?,
    )
    .into_response())
}

async fn create_namespace(
    State(state): State<AppState>,
    Extension(identity): Extension<RequestIdentity>,
    body: Bytes,
) -> ApiResult<(StatusCode, Json<Namespace>)> {
    let resource = decode_namespace(body)?;
    let admission_request = AdmissionRequest::namespace_create(identity, resource.clone())?;
    state.admission.validate(&admission_request).await?;
    let created = state.backend.namespaces.create(resource).await?;
    Ok((StatusCode::CREATED, Json(created)))
}

async fn get_namespace(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<Json<Namespace>> {
    Ok(Json(state.backend.namespaces.get(&name).await?))
}

async fn replace_namespace(
    State(state): State<AppState>,
    Extension(identity): Extension<RequestIdentity>,
    Path(name): Path<String>,
    body: Bytes,
) -> ApiResult<Json<Namespace>> {
    let resource = bind_namespace_update_identity(decode_namespace(body)?, &name)?;
    let old_object = state.backend.namespaces.get(&name).await?;
    let admission_request =
        AdmissionRequest::namespace_update(identity, resource.clone(), old_object)?;
    state.admission.validate(&admission_request).await?;
    Ok(Json(state.backend.namespaces.update(resource).await?))
}

async fn delete_namespace(
    State(state): State<AppState>,
    Extension(identity): Extension<RequestIdentity>,
    Path(name): Path<String>,
    body: Bytes,
) -> ApiResult<Json<ApiStatus>> {
    let options = decode_delete_options(body)?;
    let old_object = state.backend.namespaces.get(&name).await?;
    let admission_request = AdmissionRequest::namespace_delete(identity, old_object)?;
    state.admission.validate(&admission_request).await?;
    let result = state.backend.namespaces.delete(&name, options).await?;
    Ok(Json(ApiStatus::success(format!(
        "namespaces {name:?} deleted at resourceVersion {}",
        result.resource_version
    ))))
}

async fn list_nodes(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> ApiResult<Response> {
    if is_watch_request(&query)? {
        return open_node_watch(&state, &query).await;
    }
    reject_list_resource_version(&query)?;
    let label_selector = LabelSelector::parse(query.label_selector.as_deref())?;
    let field_selector = FieldSelector::parse(query.field_selector.as_deref())?;
    Ok(Json(
        state
            .backend
            .nodes
            .list(&label_selector, &field_selector)
            .await?,
    )
    .into_response())
}

async fn create_node(
    State(state): State<AppState>,
    body: Bytes,
) -> ApiResult<(StatusCode, Json<Node>)> {
    let created = state.backend.nodes.create(decode_node(body)?).await?;
    Ok((StatusCode::CREATED, Json(created)))
}

async fn get_node(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<Json<Node>> {
    Ok(Json(state.backend.nodes.get(&name).await?))
}

async fn replace_node(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> ApiResult<Json<Node>> {
    let resource = bind_node_update_identity(decode_node(body)?, &name)?;
    Ok(Json(state.backend.nodes.update(resource).await?))
}

async fn patch_node(
    State(state): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<Node>> {
    let current = state.backend.nodes.get(&name).await?;
    let patched = bind_node_update_identity(apply_node_patch(current, &headers, body)?, &name)?;
    Ok(Json(state.backend.nodes.update(patched).await?))
}

async fn get_node_status(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<Json<Node>> {
    Ok(Json(state.backend.nodes.get(&name).await?))
}

async fn replace_node_status(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> ApiResult<Json<Node>> {
    let resource = bind_node_status_update_identity(decode_node(body)?, &name)?;
    Ok(Json(state.backend.nodes.update_status(resource).await?))
}

async fn delete_node(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> ApiResult<Json<ApiStatus>> {
    let result = state
        .backend
        .nodes
        .delete(&name, decode_delete_options(body)?)
        .await?;
    Ok(Json(ApiStatus::success(format!(
        "nodes {name:?} deleted at resourceVersion {}",
        result.resource_version
    ))))
}

async fn not_found() -> ApiRejection {
    ApiError::NotFound {
        resource: ResourceReference {
            group: String::new(),
            resource: "the requested endpoint".to_owned(),
            namespace: None,
            name: None,
        },
    }
    .into()
}

async fn open_config_map_watch(
    state: &AppState,
    query: &ListQuery,
    namespace: Option<String>,
) -> ApiResult<Response> {
    let label_selector = LabelSelector::parse(query.label_selector.as_deref())?;
    let field_selector = FieldSelector::parse(query.field_selector.as_deref())?;
    let subscription = state
        .backend
        .config_maps
        .watch(ConfigMapWatchRequest {
            namespace,
            label_selector,
            field_selector,
            resource_version: query.resource_version.clone(),
            allow_bookmarks: parse_boolean(
                query.allow_watch_bookmarks.as_deref(),
                "allowWatchBookmarks",
            )?,
        })
        .await?;
    Ok(watch_response(subscription))
}

async fn open_service_account_watch(
    state: &AppState,
    query: &ListQuery,
    namespace: String,
) -> ApiResult<Response> {
    let label_selector = LabelSelector::parse(query.label_selector.as_deref())?;
    let field_selector = FieldSelector::parse(query.field_selector.as_deref())?;
    let subscription = state
        .backend
        .service_accounts
        .watch(ServiceAccountWatchRequest {
            namespace: Some(namespace),
            label_selector,
            field_selector,
            resource_version: query.resource_version.clone(),
            allow_bookmarks: parse_boolean(
                query.allow_watch_bookmarks.as_deref(),
                "allowWatchBookmarks",
            )?,
        })
        .await?;
    Ok(service_account_watch_response(subscription))
}

async fn open_pod_watch(
    state: &AppState,
    query: &ListQuery,
    namespace: Option<String>,
) -> ApiResult<Response> {
    let label_selector = LabelSelector::parse(query.label_selector.as_deref())?;
    let field_selector = FieldSelector::parse(query.field_selector.as_deref())?;
    let subscription = state
        .backend
        .pods
        .watch(PodWatchRequest {
            namespace,
            label_selector,
            field_selector,
            resource_version: query.resource_version.clone(),
            allow_bookmarks: parse_boolean(
                query.allow_watch_bookmarks.as_deref(),
                "allowWatchBookmarks",
            )?,
        })
        .await?;
    Ok(pod_watch_response(subscription))
}

async fn open_node_watch(state: &AppState, query: &ListQuery) -> ApiResult<Response> {
    let label_selector = LabelSelector::parse(query.label_selector.as_deref())?;
    let field_selector = FieldSelector::parse(query.field_selector.as_deref())?;
    let subscription = state
        .backend
        .nodes
        .watch(NodeWatchRequest {
            label_selector,
            field_selector,
            resource_version: query.resource_version.clone(),
            allow_bookmarks: parse_boolean(
                query.allow_watch_bookmarks.as_deref(),
                "allowWatchBookmarks",
            )?,
        })
        .await?;
    Ok(node_watch_response(subscription))
}

async fn open_namespace_watch(state: &AppState, query: &ListQuery) -> ApiResult<Response> {
    let label_selector = LabelSelector::parse(query.label_selector.as_deref())?;
    let field_selector = FieldSelector::parse(query.field_selector.as_deref())?;
    let subscription = state
        .backend
        .namespaces
        .watch(NamespaceWatchRequest {
            label_selector,
            field_selector,
            resource_version: query.resource_version.clone(),
            allow_bookmarks: parse_boolean(
                query.allow_watch_bookmarks.as_deref(),
                "allowWatchBookmarks",
            )?,
        })
        .await?;
    Ok(namespace_watch_response(subscription))
}

fn is_watch_request(query: &ListQuery) -> ApiResult<bool> {
    match query.watch.as_deref() {
        None | Some("false") | Some("0") => Ok(false),
        Some("true") | Some("1") => Ok(true),
        Some(value) => Err(ApiError::BadRequest {
            message: format!("watch {value:?} must be a boolean"),
        }
        .into()),
    }
}

fn parse_boolean(raw: Option<&str>, field: &str) -> ApiResult<bool> {
    match raw {
        None | Some("false") | Some("0") => Ok(false),
        Some("true") | Some("1") => Ok(true),
        Some(value) => Err(ApiError::BadRequest {
            message: format!("{field} {value:?} must be a boolean"),
        }
        .into()),
    }
}

fn reject_list_resource_version(query: &ListQuery) -> ApiResult<()> {
    if query
        .resource_version
        .as_deref()
        .is_some_and(|value| !value.is_empty())
    {
        return Err(ApiError::BadRequest {
            message: "resourceVersion on LIST is not implemented by the in-memory backend; use it only with watch=true"
                .to_owned(),
        }
        .into());
    }
    Ok(())
}

fn node_watch_response(subscription: NodeWatchSubscription) -> Response {
    let event_stream = stream! {
        let mut subscription = subscription;
        while let Some(event) = subscription.recv().await {
            let mut encoded = match serde_json::to_vec(&event) {
                Ok(encoded) => encoded,
                Err(error) => {
                    yield Err::<Bytes, io::Error>(io::Error::other(error));
                    break;
                }
            };
            encoded.push(b'\n');
            yield Ok::<Bytes, io::Error>(Bytes::from(encoded));
        }
    };
    let mut response = Response::new(Body::from_stream(event_stream));
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/json;stream=watch"),
    );
    response
}

fn watch_response(subscription: ConfigMapWatchSubscription) -> Response {
    let event_stream = stream! {
        let mut subscription = subscription;
        while let Some(event) = subscription.recv().await {
            let mut encoded = match serde_json::to_vec(&event) {
                Ok(encoded) => encoded,
                Err(error) => {
                    yield Err::<Bytes, io::Error>(io::Error::other(error));
                    break;
                }
            };
            encoded.push(b'\n');
            yield Ok::<Bytes, io::Error>(Bytes::from(encoded));
        }
    };
    let mut response = Response::new(Body::from_stream(event_stream));
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/json"),
    );
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-cache"),
    );
    response
}

fn service_account_watch_response(subscription: ServiceAccountWatchSubscription) -> Response {
    let event_stream = stream! {
        let mut subscription = subscription;
        while let Some(event) = subscription.recv().await {
            let mut encoded = match serde_json::to_vec(&event) {
                Ok(encoded) => encoded,
                Err(error) => {
                    yield Err::<Bytes, io::Error>(io::Error::other(error));
                    break;
                }
            };
            encoded.push(b'\n');
            yield Ok::<Bytes, io::Error>(Bytes::from(encoded));
        }
    };
    let mut response = Response::new(Body::from_stream(event_stream));
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/json"),
    );
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-cache"),
    );
    response
}

fn pod_watch_response(subscription: PodWatchSubscription) -> Response {
    let event_stream = stream! {
        let mut subscription = subscription;
        while let Some(event) = subscription.recv().await {
            let mut encoded = match serde_json::to_vec(&event) {
                Ok(encoded) => encoded,
                Err(error) => {
                    yield Err::<Bytes, io::Error>(io::Error::other(error));
                    break;
                }
            };
            encoded.push(b'\n');
            yield Ok::<Bytes, io::Error>(Bytes::from(encoded));
        }
    };
    let mut response = Response::new(Body::from_stream(event_stream));
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/json"),
    );
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-cache"),
    );
    response
}

fn namespace_watch_response(subscription: NamespaceWatchSubscription) -> Response {
    let event_stream = stream! {
        let mut subscription = subscription;
        while let Some(event) = subscription.recv().await {
            let mut encoded = match serde_json::to_vec(&event) {
                Ok(encoded) => encoded,
                Err(error) => {
                    yield Err::<Bytes, io::Error>(io::Error::other(error));
                    break;
                }
            };
            encoded.push(b'\n');
            yield Ok::<Bytes, io::Error>(Bytes::from(encoded));
        }
    };
    let mut response = Response::new(Body::from_stream(event_stream));
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/json"),
    );
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-cache"),
    );
    response
}

fn decode_config_map(body: Bytes) -> ApiResult<ConfigMap> {
    if body.is_empty() {
        return Err(ApiError::BadRequest {
            message: "a ConfigMap JSON request body is required".to_owned(),
        }
        .into());
    }
    serde_json::from_slice(&body).map_err(|error| {
        ApiError::BadRequest {
            message: format!("invalid ConfigMap JSON: {error}"),
        }
        .into()
    })
}

fn decode_pod(body: Bytes) -> ApiResult<Pod> {
    decode_typed_resource(body, "Pod")
}

fn decode_service_account(body: Bytes) -> ApiResult<ServiceAccount> {
    decode_typed_resource(body, "ServiceAccount")
}

fn decode_namespace(body: Bytes) -> ApiResult<Namespace> {
    decode_typed_resource(body, "Namespace")
}

fn decode_node(body: Bytes) -> ApiResult<Node> {
    decode_typed_resource(body, "Node")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NodePatchFormat {
    JsonPatch,
    MergePatch,
}

fn apply_node_patch(current: Node, headers: &HeaderMap, body: Bytes) -> ApiResult<Node> {
    if body.is_empty() {
        return Err(ApiError::BadRequest {
            message: "a Node PATCH request body is required".to_owned(),
        }
        .into());
    }
    let mut document = serde_json::to_value(current).map_err(|_| ApiError::Internal)?;
    match node_patch_format(headers)? {
        NodePatchFormat::JsonPatch => {
            let patch: json_patch::Patch =
                serde_json::from_slice(&body).map_err(|error| ApiError::BadRequest {
                    message: format!("invalid JSON Patch document: {error}"),
                })?;
            json_patch::patch(&mut document, &patch).map_err(|error| ApiError::Invalid {
                message: format!("JSON Patch could not be applied: {error}"),
            })?;
        }
        NodePatchFormat::MergePatch => {
            let patch: Value =
                serde_json::from_slice(&body).map_err(|error| ApiError::BadRequest {
                    message: format!("invalid JSON Merge Patch document: {error}"),
                })?;
            if !patch.is_object() {
                return Err(ApiError::BadRequest {
                    message: "JSON Merge Patch document must be an object".to_owned(),
                }
                .into());
            }
            json_patch::merge(&mut document, &patch);
        }
    }
    serde_json::from_value(document).map_err(|error| {
        ApiError::Invalid {
            message: format!("PATCH result is not a valid Node: {error}"),
        }
        .into()
    })
}

fn node_patch_format(headers: &HeaderMap) -> ApiResult<NodePatchFormat> {
    let Some(raw) = headers.get(header::CONTENT_TYPE) else {
        return Err(ApiError::UnsupportedMediaType {
            media_type: "<missing>".to_owned(),
        }
        .into());
    };
    let raw = raw.to_str().map_err(|_| ApiError::UnsupportedMediaType {
        media_type: "<invalid HTTP header>".to_owned(),
    })?;
    let media_type = raw
        .split(';')
        .next()
        .map(str::trim)
        .unwrap_or_default()
        .to_ascii_lowercase();
    match media_type.as_str() {
        "application/json-patch+json" => Ok(NodePatchFormat::JsonPatch),
        "application/merge-patch+json" => Ok(NodePatchFormat::MergePatch),
        _ => Err(ApiError::UnsupportedMediaType { media_type }.into()),
    }
}

fn decode_typed_resource<T: serde::de::DeserializeOwned>(body: Bytes, kind: &str) -> ApiResult<T> {
    if body.is_empty() {
        return Err(ApiError::BadRequest {
            message: format!("a {kind} JSON request body is required"),
        }
        .into());
    }
    serde_json::from_slice(&body).map_err(|error| {
        ApiError::BadRequest {
            message: format!("invalid {kind} JSON: {error}"),
        }
        .into()
    })
}

fn decode_delete_options(body: Bytes) -> ApiResult<DeleteOptions> {
    if body.is_empty() {
        return Ok(DeleteOptions::default());
    }
    serde_json::from_slice(&body).map_err(|error| {
        ApiError::BadRequest {
            message: format!("invalid DeleteOptions JSON: {error}"),
        }
        .into()
    })
}

/// Makes the URI namespace authoritative while retaining an explicit mismatch failure.
fn bind_namespace(mut resource: ConfigMap, namespace: &str) -> ApiResult<ConfigMap> {
    match resource.metadata.namespace.as_deref() {
        Some(body_namespace) if body_namespace != namespace => Err(ApiError::Invalid {
            message: format!(
                "metadata.namespace {body_namespace:?} does not match request namespace {namespace:?}"
            ),
        }
        .into()),
        Some(_) => Ok(resource),
        None => {
            resource.metadata.namespace = Some(namespace.to_owned());
            Ok(resource)
        }
    }
}

fn bind_service_account_namespace(
    mut resource: ServiceAccount,
    namespace: &str,
) -> ApiResult<ServiceAccount> {
    match resource.metadata.namespace.as_deref() {
        Some(body_namespace) if body_namespace != namespace => Err(ApiError::Invalid {
            message: format!(
                "metadata.namespace {body_namespace:?} does not match request namespace {namespace:?}"
            ),
        }
        .into()),
        _ => {
            resource.metadata.namespace = Some(namespace.to_owned());
            Ok(resource)
        }
    }
}

fn bind_service_account_update_identity(
    resource: ServiceAccount,
    namespace: &str,
    name: &str,
) -> ApiResult<ServiceAccount> {
    let resource = bind_service_account_namespace(resource, namespace)?;
    match resource.metadata.name.as_deref() {
        Some(body_name) if body_name == name => Ok(resource),
        Some(body_name) => Err(ApiError::Invalid {
            message: format!("metadata.name {body_name:?} does not match request name {name:?}"),
        }
        .into()),
        None => Err(ApiError::Invalid {
            message: "metadata.name is required".to_owned(),
        }
        .into()),
    }
}

fn bind_pod_namespace(mut resource: Pod, namespace: &str) -> ApiResult<Pod> {
    match resource.metadata.namespace.as_deref() {
        Some(body_namespace) if body_namespace != namespace => Err(ApiError::Invalid {
            message: format!(
                "metadata.namespace {body_namespace:?} does not match request namespace {namespace:?}"
            ),
        }
        .into()),
        Some(_) => Ok(resource),
        None => {
            resource.metadata.namespace = Some(namespace.to_owned());
            Ok(resource)
        }
    }
}

fn bind_pod_update_identity(resource: Pod, namespace: &str, name: &str) -> ApiResult<Pod> {
    let resource = bind_pod_namespace(resource, namespace)?;
    match resource.metadata.name.as_deref() {
        Some(body_name) if body_name == name => Ok(resource),
        Some(body_name) => Err(ApiError::Invalid {
            message: format!("metadata.name {body_name:?} does not match request name {name:?}"),
        }
        .into()),
        None => Err(ApiError::Invalid {
            message: "metadata.name is required for a replace request".to_owned(),
        }
        .into()),
    }
}

fn bind_node_update_identity(resource: Node, name: &str) -> ApiResult<Node> {
    match resource.metadata.name.as_deref() {
        Some(body_name) if body_name == name => Ok(resource),
        Some(body_name) => Err(ApiError::Invalid {
            message: format!("metadata.name {body_name:?} does not match request name {name:?}"),
        }
        .into()),
        None => Err(ApiError::Invalid {
            message: "metadata.name is required for a replace request".to_owned(),
        }
        .into()),
    }
}

/// Ensures a status replacement cannot target a different Node than its URI declares.
fn bind_node_status_update_identity(resource: Node, name: &str) -> ApiResult<Node> {
    bind_node_update_identity(resource, name).map_err(|error| match error.0 {
        ApiError::Invalid { message }
            if message == "metadata.name is required for a replace request" =>
        {
            ApiError::Invalid {
                message: "metadata.name is required for a status replace request".to_owned(),
            }
            .into()
        }
        error => ApiRejection(error),
    })
}

fn bind_namespace_update_identity(resource: Namespace, name: &str) -> ApiResult<Namespace> {
    match resource.metadata.name.as_deref() {
        Some(body_name) if body_name == name => Ok(resource),
        Some(body_name) => Err(ApiError::Invalid {
            message: format!("metadata.name {body_name:?} does not match request name {name:?}"),
        }
        .into()),
        None => Err(ApiError::Invalid {
            message: "metadata.name is required for a replace request".to_owned(),
        }
        .into()),
    }
}

/// Ensures a replace request cannot silently target a different object than the URI declares.
fn bind_update_identity(resource: ConfigMap, namespace: &str, name: &str) -> ApiResult<ConfigMap> {
    let resource = bind_namespace(resource, namespace)?;
    match resource.metadata.name.as_deref() {
        Some(body_name) if body_name == name => Ok(resource),
        Some(body_name) => Err(ApiError::Invalid {
            message: format!("metadata.name {body_name:?} does not match request name {name:?}"),
        }
        .into()),
        None => Err(ApiError::Invalid {
            message: "metadata.name is required for a replace request".to_owned(),
        }
        .into()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{
        body::{to_bytes, Body},
        extract::Extension,
        http::Request,
        routing::get,
    };
    use rusternetes_api_types::{Container, Node, ObjectMeta, Pod, PodSpec, TypeMeta};
    use rusternetes_authn::{
        AnonymousPolicy, AuthenticationChain, RequestIdentity, StaticBearerToken,
    };
    use rusternetes_common::StatusReason;
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn live_service_account_identity_requires_current_matching_uid() {
        let store = Arc::new(InMemoryServiceAccountStore::new());
        let backend = ServiceAccountBackend::InMemory(store.clone());
        let pod_store = Arc::new(InMemoryPodStore::new());
        let pods = PodBackend::InMemory(pod_store.clone());
        let node_store = Arc::new(InMemoryNodeStore::new());
        let nodes = NodeBackend::InMemory(node_store.clone());
        let created = store
            .create(ServiceAccount {
                type_meta: TypeMeta::service_account(),
                metadata: ObjectMeta {
                    name: Some("build-robot".to_owned()),
                    namespace: Some("default".to_owned()),
                    ..ObjectMeta::default()
                },
                ..ServiceAccount::default()
            })
            .await
            .expect("ServiceAccount creates");
        let uid = created.metadata.uid.expect("server assigns UID");
        let claims = || VerifiedServiceAccountJwt {
            iss: "https://issuer.example".to_owned(),
            sub: "system:serviceaccount:default:build-robot".to_owned(),
            aud: vec!["api".to_owned()],
            kubernetes: rusternetes_authn::KubernetesServiceAccountClaims {
                namespace: "default".to_owned(),
                service_account: rusternetes_authn::KubernetesServiceAccountIdentityClaims {
                    name: "build-robot".to_owned(),
                    uid: uid.clone(),
                },
                pod: None,
                node: None,
            },
        };
        let identity = authenticate_live_service_account(&backend, &pods, &nodes, claims())
            .await
            .expect("current ServiceAccount UID is accepted");
        assert_eq!(
            identity.username,
            "system:serviceaccount:default:build-robot"
        );
        assert!(identity.groups.contains("system:serviceaccounts"));
        assert!(identity.groups.contains("system:serviceaccounts:default"));

        let bound_pod = pods
            .create(Pod {
                type_meta: TypeMeta::pod(),
                metadata: ObjectMeta {
                    name: Some("worker-pod".to_owned()),
                    namespace: Some("default".to_owned()),
                    ..ObjectMeta::default()
                },
                spec: PodSpec {
                    containers: vec![Container {
                        name: "main".to_owned(),
                        ..Container::default()
                    }],
                    ..PodSpec::default()
                },
                ..Pod::default()
            })
            .await
            .expect("Pod creates");
        let mut pod_bound = claims();
        pod_bound.kubernetes.pod = Some(rusternetes_authn::KubernetesBoundObjectClaims {
            name: "worker-pod".to_owned(),
            uid: bound_pod.metadata.uid.clone().expect("Pod gets UID"),
        });
        // Kubernetes includes Node metadata with Pod-bound tokens but does not validate it at
        // token authentication time; the Pod UID remains the live revocation object.
        pod_bound.kubernetes.node = Some(rusternetes_authn::KubernetesBoundObjectClaims {
            name: "unrelated-node".to_owned(),
            uid: "nonexistent-node-uid".to_owned(),
        });
        authenticate_live_service_account(&backend, &pods, &nodes, pod_bound.clone())
            .await
            .expect("current Pod-bound token is accepted without Node lookup");
        pod_store
            .delete("default", "worker-pod", DeleteOptions::default())
            .await
            .expect("Pod deletes");
        assert!(matches!(
            authenticate_live_service_account(&backend, &pods, &nodes, pod_bound).await,
            Err(ApiError::Unauthorized { .. })
        ));

        let bound_node = nodes
            .create(Node {
                type_meta: TypeMeta::node(),
                metadata: ObjectMeta {
                    name: Some("worker-a".to_owned()),
                    ..ObjectMeta::default()
                },
                ..Node::default()
            })
            .await
            .expect("Node creates");
        let mut node_bound = claims();
        node_bound.kubernetes.node = Some(rusternetes_authn::KubernetesBoundObjectClaims {
            name: "worker-a".to_owned(),
            uid: bound_node.metadata.uid.clone().expect("Node gets UID"),
        });
        authenticate_live_service_account(&backend, &pods, &nodes, node_bound.clone())
            .await
            .expect("current direct Node-bound token is accepted");
        node_store
            .delete("worker-a", DeleteOptions::default())
            .await
            .expect("Node deletes");
        assert!(matches!(
            authenticate_live_service_account(&backend, &pods, &nodes, node_bound).await,
            Err(ApiError::Unauthorized { .. })
        ));

        let mut stale = claims();
        stale.kubernetes.service_account.uid = "recreated-uid".to_owned();
        assert!(matches!(
            authenticate_live_service_account(&backend, &pods, &nodes, stale).await,
            Err(ApiError::Unauthorized { .. })
        ));
        store
            .delete("default", "build-robot", DeleteOptions::default())
            .await
            .expect("ServiceAccount deletes");
        assert!(matches!(
            authenticate_live_service_account(&backend, &pods, &nodes, claims()).await,
            Err(ApiError::Unauthorized { .. })
        ));
    }

    #[test]
    fn node_status_path_maps_to_a_distinct_rbac_subresource() {
        let request = authorization_request_from_http(
            &ApiRegistry::core_v1(),
            &axum::http::Method::PUT,
            &"/api/v1/nodes/node-a/status"
                .parse()
                .expect("valid Node status URI"),
        );
        assert_eq!(request.verb, "update");
        assert!(matches!(
            request.target,
            rusternetes_authz_rbac::RequestTarget::Resource {
                api_group,
                resource,
                subresource,
                namespace,
                name,
            } if api_group.is_empty()
                && resource == "nodes"
                && subresource.as_deref() == Some("status")
                && namespace.is_none()
                && name.as_deref() == Some("node-a")
        ));
    }

    #[tokio::test]
    async fn core_v1_discovery_is_derived_from_the_registered_strategy() {
        let app = router(Arc::new(InMemoryConfigMapStore::new()));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1")
                    .body(Body::empty())
                    .expect("valid discovery request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("discovery response body is readable");
        let discovery: serde_json::Value =
            serde_json::from_slice(&bytes).expect("response is APIResourceList JSON");
        let config_maps = discovery["resources"]
            .as_array()
            .expect("discovery resources are an array")
            .iter()
            .find(|resource| resource["name"] == "configmaps")
            .expect("ConfigMap strategy is discoverable");
        assert_eq!(config_maps["namespaced"], true);
        assert!(config_maps["verbs"]
            .as_array()
            .expect("ConfigMap verbs are an array")
            .iter()
            .any(|verb| verb == "watch"));
    }

    #[tokio::test]
    async fn missing_namespace_is_bound_from_the_resource_path() {
        let app = router(Arc::new(InMemoryConfigMapStore::new()));
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/namespaces/default/configmaps")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"settings"},"data":{"mode":"safe"}}"#,
            ))
            .unwrap_or_else(|error| panic!("valid test request: {error}"));

        let response = app
            .oneshot(request)
            .await
            .unwrap_or_else(|error| panic!("router is infallible: {error}"));
        assert_eq!(response.status(), StatusCode::CREATED);
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap_or_else(|error| panic!("response body is readable: {error}"));
        let created: ConfigMap = serde_json::from_slice(&bytes)
            .unwrap_or_else(|error| panic!("response is a ConfigMap: {error}"));
        assert_eq!(created.metadata.namespace.as_deref(), Some("default"));
    }

    fn strict_authentication() -> AuthenticationChain {
        let identity = RequestIdentity::authenticated(
            "cluster-bootstrap",
            Some("bootstrap-uid".to_owned()),
            ["system:bootstrappers".to_owned()],
            Default::default(),
        )
        .expect("test identity is valid");
        let token = StaticBearerToken::new("bootstrap-secret", identity)
            .expect("test bearer credential is valid");
        AuthenticationChain::new(AnonymousPolicy::Deny, vec![token])
    }

    async fn identity_handler(Extension(identity): Extension<RequestIdentity>) -> String {
        identity.username
    }

    fn identity_test_router(authentication: AuthenticationChain) -> Router {
        Router::new()
            .route("/identity", get(identity_handler))
            .layer(middleware::from_fn_with_state(
                AuthenticationState {
                    chain: authentication,
                    service_accounts: ServiceAccountBackend::InMemory(Arc::new(
                        InMemoryServiceAccountStore::new(),
                    )),
                    pods: PodBackend::InMemory(Arc::new(InMemoryPodStore::new())),
                    nodes: NodeBackend::InMemory(Arc::new(InMemoryNodeStore::new())),
                },
                authenticate_request,
            ))
    }

    #[tokio::test]
    async fn bearer_authentication_injects_identity_and_rejects_forged_headers() {
        let authenticated = identity_test_router(strict_authentication())
            .oneshot(
                Request::builder()
                    .uri("/identity")
                    .header("authorization", "Bearer bootstrap-secret")
                    .header("x-rusternetes-user", "system:masters")
                    .body(Body::empty())
                    .expect("valid authenticated request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(authenticated.status(), StatusCode::OK);
        let bytes = to_bytes(authenticated.into_body(), usize::MAX)
            .await
            .expect("identity response body is readable");
        assert_eq!(bytes.as_ref(), b"cluster-bootstrap");

        let forged = identity_test_router(strict_authentication())
            .oneshot(
                Request::builder()
                    .uri("/identity")
                    .header("x-rusternetes-user", "system:masters")
                    .body(Body::empty())
                    .expect("valid unauthenticated request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(forged.status(), StatusCode::UNAUTHORIZED);
        let bytes = to_bytes(forged.into_body(), usize::MAX)
            .await
            .expect("unauthorized response body is readable");
        let status: ApiStatus =
            serde_json::from_slice(&bytes).expect("unauthorized response is Kubernetes Status");
        assert_eq!(status.reason, StatusReason::Unauthorized);
    }

    #[tokio::test]
    async fn invalid_bearer_is_unauthorized_and_default_policy_is_explicitly_anonymous() {
        let rejected = identity_test_router(strict_authentication())
            .oneshot(
                Request::builder()
                    .uri("/identity")
                    .header("authorization", "Bearer incorrect")
                    .body(Body::empty())
                    .expect("valid invalid-token request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(rejected.status(), StatusCode::UNAUTHORIZED);

        let anonymous = identity_test_router(AuthenticationChain::default())
            .oneshot(
                Request::builder()
                    .uri("/identity")
                    .body(Body::empty())
                    .expect("valid anonymous request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(anonymous.status(), StatusCode::OK);
        let bytes = to_bytes(anonymous.into_body(), usize::MAX)
            .await
            .expect("anonymous response body is readable");
        assert_eq!(bytes.as_ref(), b"system:anonymous");
    }

    #[tokio::test]
    async fn absent_etcd_endpoints_select_in_memory_backend() {
        let backend = backend_from_etcd_config(None, None)
            .await
            .expect("default backend is available");
        assert!(matches!(backend, ConfigMapBackend::InMemory(_)));
    }

    #[tokio::test]
    async fn invalid_etcd_prefix_fails_closed_before_server_startup() {
        assert!(matches!(
            backend_from_etcd_config(Some("http://127.0.0.1:2379"), Some("invalid-prefix")).await,
            Err(ApiError::BadRequest { .. })
        ));
    }
}
