//! Axum HTTP API server for the first Rusternetes vertical slice.

mod pagination;

use std::{io, sync::Arc};

use async_stream::stream;
use axum::{
    body::{Body, Bytes},
    extract::{Extension, Path, Query, Request, State},
    http::{header, HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, put},
    Json, Router,
};
use pagination::{ListPaginationRequest, ListResource, PaginationCache, SnapshotItems};
use rusternetes_admission::{
    AdmissionChain, AdmissionRequest, NamespaceLifecyclePlugin,
    NamespacePhase as AdmissionNamespacePhase, NamespaceStateReader,
};
use rusternetes_api_registry::ApiRegistry;
use rusternetes_api_types::{
    ApiGroup, ApiGroupList, ApiResourceList, ApiStatus, ConfigMap, ConfigMapList, DeleteOptions,
    FieldSelector, LabelSelector, Namespace, NamespaceList, NamespacePhase, Node, NodeList, Pod,
    PodList, SelfSubjectAccessReview, ServiceAccount, ServiceAccountList,
    SubjectAccessReviewStatus, TokenRequest, TokenReview, TokenReviewStatus, TokenReviewUserInfo,
    TypeMeta,
};
use rusternetes_authn::{
    AuthenticationChain, KubernetesBoundObjectClaims, RequestIdentity, ServiceAccountJwksDocument,
    ServiceAccountOidcDiscovery, ServiceAccountOidcDiscoveryDocument, ServiceAccountTokenIssuer,
    ServiceAccountTokenSubject, VerifiedServiceAccountJwt,
};
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
use time::OffsetDateTime;

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

    async fn update_status(&self, resource: Pod) -> Result<Pod, ApiError> {
        match self {
            Self::InMemory(store) => store.update_status(resource).await,
            Self::Etcd(repository) => repository.update_status(resource).await,
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

    async fn update_status(&self, resource: Namespace) -> Result<Namespace, ApiError> {
        match self {
            Self::InMemory(store) => store.update_status(resource).await,
            Self::Etcd(repository) => repository.update_status(resource).await,
        }
    }

    async fn finalize(&self, resource: Namespace) -> Result<Namespace, ApiError> {
        match self {
            Self::InMemory(store) => store.finalize(resource).await,
            Self::Etcd(repository) => repository.finalize(resource).await,
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
    service_account_token_issuer: Option<ServiceAccountTokenIssuer>,
    service_account_oidc_discovery: Option<ServiceAccountOidcDiscovery>,
    token_review_authentication: Option<AuthenticationState>,
    pagination: Arc<PaginationCache>,
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
            service_account_token_issuer: None,
            service_account_oidc_discovery: None,
            token_review_authentication: None,
            pagination: Arc::new(PaginationCache::default()),
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
    let mut state = AppState::with_registry_authorization_and_admission(
        backend,
        ApiRegistry::core_v1(),
        authorization,
        admission,
    );
    state.service_account_token_issuer = authentication.service_account_token_issuer().cloned();
    state.service_account_oidc_discovery = authentication.service_account_oidc_discovery().cloned();
    let authentication_state = AuthenticationState {
        chain: authentication,
        service_accounts: state.backend.service_accounts.clone(),
        pods: state.backend.pods.clone(),
        nodes: state.backend.nodes.clone(),
    };
    state.token_review_authentication = Some(authentication_state.clone());
    Router::new()
        .route(
            "/.well-known/openid-configuration",
            get(service_account_oidc_discovery),
        )
        .route("/openid/v1/jwks", get(service_account_jwks))
        .route("/version", get(version))
        .route("/api", get(api_versions))
        .route("/apis", get(named_api_groups))
        .route("/apis/:group", get(named_api_group))
        .route("/apis/:group/:version", get(named_api_resources))
        .route(
            "/apis/authentication.k8s.io/v1/tokenreviews",
            axum::routing::post(create_token_review),
        )
        .route(
            "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews",
            axum::routing::post(create_self_subject_access_review),
        )
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
                .patch(patch_namespace)
                .delete(delete_namespace),
        )
        .route(
            "/api/v1/namespaces/:name/status",
            get(get_namespace_status).put(replace_namespace_status),
        )
        .route("/api/v1/namespaces/:name/finalize", put(finalize_namespace))
        .route(
            "/api/v1/namespaces/:namespace/configmaps",
            get(list_config_maps).post(create_config_map),
        )
        .route(
            "/api/v1/namespaces/:namespace/configmaps/:name",
            get(get_config_map)
                .put(replace_config_map)
                .patch(patch_config_map)
                .delete(delete_config_map),
        )
        .route(
            "/api/v1/namespaces/:namespace/pods",
            get(list_pods).post(create_pod),
        )
        .route(
            "/api/v1/namespaces/:namespace/pods/:name",
            get(get_pod)
                .put(replace_pod)
                .patch(patch_pod)
                .delete(delete_pod),
        )
        .route(
            "/api/v1/namespaces/:namespace/pods/:name/status",
            get(get_pod_status).put(replace_pod_status),
        )
        .route(
            "/api/v1/namespaces/:namespace/serviceaccounts",
            get(list_service_accounts).post(create_service_account),
        )
        .route(
            "/api/v1/namespaces/:namespace/serviceaccounts/:name",
            get(get_service_account)
                .put(replace_service_account)
                .patch(patch_service_account)
                .delete(delete_service_account),
        )
        .route(
            "/api/v1/namespaces/:namespace/serviceaccounts/:name/token",
            axum::routing::post(create_service_account_token),
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
    router_with_core_backend_and_auth(backend, AuthenticationChain::default())
}

/// Builds a router over all typed core/v1 backends with lifecycle admission and explicit
/// authentication configuration, including a validated optional ServiceAccount TokenRequest issuer.
pub fn router_with_core_backend_and_auth(
    backend: CoreApiBackend,
    authentication: AuthenticationChain,
) -> Router {
    router_with_core_backend_auth_and_authorization(
        backend,
        authentication,
        AuthorizationMode::default(),
    )
}

/// Builds a router over all typed core/v1 backends with lifecycle admission plus explicit
/// authentication and authorization configuration.
pub fn router_with_core_backend_auth_and_authorization(
    backend: CoreApiBackend,
    authentication: AuthenticationChain,
    authorization: AuthorizationMode,
) -> Router {
    let admission = namespace_lifecycle_admission(backend.namespaces.clone());
    router_with_core_backend_auth_authorization_and_admission(
        backend,
        authentication,
        authorization,
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

fn is_service_account_oidc_path(path: &str) -> bool {
    matches!(
        path,
        "/.well-known/openid-configuration" | "/openid/v1/jwks"
    )
}

async fn authenticate_request(
    State(authentication): State<AuthenticationState>,
    mut request: Request,
    next: Next,
) -> Response {
    if is_service_account_oidc_path(request.uri().path()) {
        return next.run(request).await;
    }
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
    if is_service_account_oidc_path(request.uri().path()) {
        return next.run(request).await;
    }
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
    let Some(resolved) = registry.resolve_path(uri.path()) else {
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
    #[serde(default)]
    limit: Option<i64>,
    #[serde(rename = "continue", default)]
    continue_token: Option<String>,
}

const MAX_LIST_PAGE_LIMIT: usize = 10_000;

fn pagination_request(
    query: &ListQuery,
    resource: ListResource,
    namespace: Option<&str>,
) -> Result<ListPaginationRequest, ApiError> {
    let limit = match query.limit {
        None => None,
        Some(limit) if limit < 0 => {
            return Err(ApiError::BadRequest {
                message: "limit must be a non-negative integer".to_owned(),
            });
        }
        Some(limit) => {
            let limit = usize::try_from(limit).map_err(|_| ApiError::BadRequest {
                message: "limit is too large for this server".to_owned(),
            })?;
            if limit > MAX_LIST_PAGE_LIMIT {
                return Err(ApiError::BadRequest {
                    message: format!("limit must not exceed {MAX_LIST_PAGE_LIMIT}"),
                });
            }
            Some(limit)
        }
    };
    if query.continue_token.is_some() && limit == Some(0) {
        return Err(ApiError::BadRequest {
            message: "continue may not be combined with limit=0".to_owned(),
        });
    }
    Ok(ListPaginationRequest {
        resource,
        namespace: namespace.map(str::to_owned),
        label_selector: query
            .label_selector
            .as_deref()
            .filter(|value| !value.is_empty())
            .map(str::to_owned),
        field_selector: query
            .field_selector
            .as_deref()
            .filter(|value| !value.is_empty())
            .map(str::to_owned),
        limit,
    })
}

fn reject_watch_pagination(query: &ListQuery) -> ApiResult<()> {
    if query.limit.is_some() || query.continue_token.is_some() {
        return Err(ApiError::BadRequest {
            message: "limit and continue are not supported with watch=true".to_owned(),
        }
        .into());
    }
    Ok(())
}

async fn paginated_config_map_list(
    state: &AppState,
    query: &ListQuery,
    namespace: Option<&str>,
    label_selector: &LabelSelector,
    field_selector: &FieldSelector,
) -> Result<ConfigMapList, ApiError> {
    let request = pagination_request(query, ListResource::ConfigMaps, namespace)?;
    let page = match query.continue_token.as_deref() {
        Some(token) => state.pagination.continue_page(request, token).await?,
        None => {
            let list = state
                .backend
                .config_maps
                .list(namespace, label_selector, field_selector)
                .await?;
            state
                .pagination
                .first_page(
                    request,
                    list.metadata.resource_version,
                    SnapshotItems::ConfigMaps(list.items),
                )
                .await?
        }
    };
    Ok(ConfigMapList {
        type_meta: TypeMeta::config_map_list(),
        metadata: page.metadata,
        items: page.items.into_config_maps().ok_or(ApiError::Internal)?,
    })
}

async fn paginated_pod_list(
    state: &AppState,
    query: &ListQuery,
    namespace: Option<&str>,
    label_selector: &LabelSelector,
    field_selector: &FieldSelector,
) -> Result<PodList, ApiError> {
    let request = pagination_request(query, ListResource::Pods, namespace)?;
    let page = match query.continue_token.as_deref() {
        Some(token) => state.pagination.continue_page(request, token).await?,
        None => {
            let list = state
                .backend
                .pods
                .list(namespace, label_selector, field_selector)
                .await?;
            state
                .pagination
                .first_page(
                    request,
                    list.metadata.resource_version,
                    SnapshotItems::Pods(list.items),
                )
                .await?
        }
    };
    Ok(PodList {
        type_meta: TypeMeta::pod_list(),
        metadata: page.metadata,
        items: page.items.into_pods().ok_or(ApiError::Internal)?,
    })
}

async fn paginated_service_account_list(
    state: &AppState,
    query: &ListQuery,
    namespace: &str,
    label_selector: &LabelSelector,
    field_selector: &FieldSelector,
) -> Result<ServiceAccountList, ApiError> {
    let request = pagination_request(query, ListResource::ServiceAccounts, Some(namespace))?;
    let page = match query.continue_token.as_deref() {
        Some(token) => state.pagination.continue_page(request, token).await?,
        None => {
            let list = state
                .backend
                .service_accounts
                .list(Some(namespace), label_selector, field_selector)
                .await?;
            state
                .pagination
                .first_page(
                    request,
                    list.metadata.resource_version,
                    SnapshotItems::ServiceAccounts(list.items),
                )
                .await?
        }
    };
    Ok(ServiceAccountList {
        type_meta: TypeMeta::service_account_list(),
        metadata: page.metadata,
        items: page
            .items
            .into_service_accounts()
            .ok_or(ApiError::Internal)?,
    })
}

async fn paginated_namespace_list(
    state: &AppState,
    query: &ListQuery,
    label_selector: &LabelSelector,
    field_selector: &FieldSelector,
) -> Result<NamespaceList, ApiError> {
    let request = pagination_request(query, ListResource::Namespaces, None)?;
    let page = match query.continue_token.as_deref() {
        Some(token) => state.pagination.continue_page(request, token).await?,
        None => {
            let list = state
                .backend
                .namespaces
                .list(label_selector, field_selector)
                .await?;
            state
                .pagination
                .first_page(
                    request,
                    list.metadata.resource_version,
                    SnapshotItems::Namespaces(list.items),
                )
                .await?
        }
    };
    Ok(NamespaceList {
        type_meta: TypeMeta::namespace_list(),
        metadata: page.metadata,
        items: page.items.into_namespaces().ok_or(ApiError::Internal)?,
    })
}

async fn paginated_node_list(
    state: &AppState,
    query: &ListQuery,
    label_selector: &LabelSelector,
    field_selector: &FieldSelector,
) -> Result<NodeList, ApiError> {
    let request = pagination_request(query, ListResource::Nodes, None)?;
    let page = match query.continue_token.as_deref() {
        Some(token) => state.pagination.continue_page(request, token).await?,
        None => {
            let list = state
                .backend
                .nodes
                .list(label_selector, field_selector)
                .await?;
            state
                .pagination
                .first_page(
                    request,
                    list.metadata.resource_version,
                    SnapshotItems::Nodes(list.items),
                )
                .await?
        }
    };
    Ok(NodeList {
        type_meta: TypeMeta::node_list(),
        metadata: page.metadata,
        items: page.items.into_nodes().ok_or(ApiError::Internal)?,
    })
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

async fn service_account_oidc_discovery(
    State(state): State<AppState>,
) -> Result<Json<ServiceAccountOidcDiscoveryDocument>, ApiRejection> {
    let discovery = state
        .service_account_oidc_discovery
        .as_ref()
        .ok_or_else(|| ApiError::NotFound {
            resource: ResourceReference {
                group: "authentication.k8s.io".to_owned(),
                resource: "openid-configuration".to_owned(),
                namespace: None,
                name: None,
            },
        })?;
    Ok(Json(discovery.document().clone()))
}

async fn service_account_jwks(
    State(state): State<AppState>,
) -> Result<Json<ServiceAccountJwksDocument>, ApiRejection> {
    let discovery = state
        .service_account_oidc_discovery
        .as_ref()
        .ok_or_else(|| ApiError::NotFound {
            resource: ResourceReference {
                group: "authentication.k8s.io".to_owned(),
                resource: "openid/v1/jwks".to_owned(),
                namespace: None,
                name: None,
            },
        })?;
    Ok(Json(discovery.jwks().clone()))
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

async fn core_v1_api_resources(State(state): State<AppState>) -> Json<ApiResourceList> {
    // The route itself exists only because core/v1 is registered by the built-in strategy.
    Json(
        state
            .registry
            .discovery("", "v1")
            .expect("core/v1 API route requires a registered core/v1 strategy"),
    )
}

async fn named_api_groups(State(state): State<AppState>) -> Json<ApiGroupList> {
    Json(state.registry.named_api_groups())
}

async fn named_api_group(
    State(state): State<AppState>,
    Path(group): Path<String>,
) -> ApiResult<Json<ApiGroup>> {
    state
        .registry
        .named_api_group(&group)
        .map(Json)
        .ok_or_else(|| {
            ApiError::NotFound {
                resource: ResourceReference {
                    group,
                    resource: "apigroups".to_owned(),
                    namespace: None,
                    name: None,
                },
            }
            .into()
        })
}

async fn named_api_resources(
    State(state): State<AppState>,
    Path((group, version)): Path<(String, String)>,
) -> ApiResult<Json<ApiResourceList>> {
    state
        .registry
        .discovery(&group, &version)
        .map(Json)
        .ok_or_else(|| {
            ApiError::NotFound {
                resource: ResourceReference {
                    group,
                    resource: "apiversions".to_owned(),
                    namespace: None,
                    name: Some(version),
                },
            }
            .into()
        })
}

async fn list_all_config_maps(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> ApiResult<Response> {
    if is_watch_request(&query)? {
        reject_watch_pagination(&query)?;
        return open_config_map_watch(&state, &query, None).await;
    }
    reject_list_resource_version(&query)?;
    let label_selector = LabelSelector::parse(query.label_selector.as_deref())?;
    let field_selector = FieldSelector::parse(query.field_selector.as_deref())?;
    Ok(Json(
        paginated_config_map_list(&state, &query, None, &label_selector, &field_selector).await?,
    )
    .into_response())
}

async fn list_config_maps(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    Query(query): Query<ListQuery>,
) -> ApiResult<Response> {
    if is_watch_request(&query)? {
        reject_watch_pagination(&query)?;
        return open_config_map_watch(&state, &query, Some(namespace)).await;
    }
    reject_list_resource_version(&query)?;
    let label_selector = LabelSelector::parse(query.label_selector.as_deref())?;
    let field_selector = FieldSelector::parse(query.field_selector.as_deref())?;
    Ok(Json(
        paginated_config_map_list(
            &state,
            &query,
            Some(&namespace),
            &label_selector,
            &field_selector,
        )
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

async fn patch_config_map(
    State(state): State<AppState>,
    Extension(identity): Extension<RequestIdentity>,
    Path((namespace, name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<ConfigMap>> {
    let current = state.backend.config_maps.get(&namespace, &name).await?;
    let patched = bind_update_identity(
        apply_config_map_patch(current.clone(), &headers, body)?,
        &namespace,
        &name,
    )?;
    let admission_request = AdmissionRequest::update(identity, patched.clone(), current)?;
    state.admission.validate(&admission_request).await?;
    Ok(Json(state.backend.config_maps.update(patched).await?))
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
        reject_watch_pagination(&query)?;
        return open_service_account_watch(&state, &query, namespace).await;
    }
    reject_list_resource_version(&query)?;
    let label_selector = LabelSelector::parse(query.label_selector.as_deref())?;
    let field_selector = FieldSelector::parse(query.field_selector.as_deref())?;
    Ok(Json(
        paginated_service_account_list(
            &state,
            &query,
            &namespace,
            &label_selector,
            &field_selector,
        )
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

async fn patch_service_account(
    State(state): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<ServiceAccount>> {
    let current = state
        .backend
        .service_accounts
        .get(&namespace, &name)
        .await?;
    let patched = bind_service_account_update_identity(
        apply_service_account_patch(current, &headers, body)?,
        &namespace,
        &name,
    )?;
    Ok(Json(state.backend.service_accounts.update(patched).await?))
}

async fn create_service_account_token(
    State(state): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
    body: Bytes,
) -> ApiResult<Json<TokenRequest>> {
    let mut request = decode_token_request(body)?;
    request.enforce_type_meta()?;
    request.validate_spec()?;
    let issuer =
        state
            .service_account_token_issuer
            .as_ref()
            .ok_or_else(|| ApiError::BadRequest {
                message: "ServiceAccount TokenRequest signing is not configured".to_owned(),
            })?;
    let service_account = state
        .backend
        .service_accounts
        .get(&namespace, &name)
        .await?;
    let service_account_uid = service_account
        .metadata
        .uid
        .clone()
        .ok_or(ApiError::Internal)?;
    if request
        .metadata
        .uid
        .as_deref()
        .is_some_and(|uid| uid != service_account_uid)
    {
        return Err(ApiError::Invalid {
            message: "metadata.uid does not match the live ServiceAccount UID".to_owned(),
        }
        .into());
    }
    let (pod, node) = token_request_bound_claims(&state, &namespace, &request).await?;
    let issued = issuer.issue(
        ServiceAccountTokenSubject {
            namespace,
            service_account_name: name,
            service_account_uid,
            pod,
            node,
        },
        &request.spec.audiences,
        request.spec.expiration_seconds,
        OffsetDateTime::now_utc(),
    )?;
    request.status.token = issued.token;
    request.status.expiration_timestamp = Some(issued.expiration_timestamp);
    Ok(Json(request))
}

/// Authenticates a presented ServiceAccount JWT without using it as the caller credential.
///
/// TokenReview itself is authorized by middleware from the caller's `Authorization` header. The
/// opaque token in the request body is evaluated independently and failures are represented by a
/// successful TokenReview response with `status.authenticated == false`.
async fn create_token_review(
    State(state): State<AppState>,
    body: Bytes,
) -> ApiResult<Json<TokenReview>> {
    let mut review = decode_token_review(body)?;
    review.enforce_type_meta()?;
    review.validate_request()?;
    let Some(authentication) = state.token_review_authentication.as_ref() else {
        return Ok(Json(review));
    };
    let Some(verifier) = authentication.chain.service_account_jwt_verifier() else {
        return Ok(Json(review));
    };
    let claims = if review.spec.audiences.is_empty() {
        verifier.verify(&review.spec.token)
    } else {
        verifier.verify_for_audiences(&review.spec.token, &review.spec.audiences)
    };
    let Ok(claims) = claims else {
        return Ok(Json(review));
    };
    let audiences = review
        .spec
        .audiences
        .iter()
        .filter(|audience| claims.aud.contains(*audience))
        .cloned()
        .collect::<Vec<_>>();
    let identity = authenticate_live_service_account(
        &authentication.service_accounts,
        &authentication.pods,
        &authentication.nodes,
        claims,
    )
    .await;
    let Ok(identity) = identity else {
        return Ok(Json(review));
    };
    review.status = TokenReviewStatus {
        authenticated: true,
        user: TokenReviewUserInfo {
            username: identity.username,
            uid: identity.uid.unwrap_or_default(),
            groups: identity.groups.into_iter().collect(),
            extra: identity.extra,
        },
        audiences,
        error: String::new(),
    };
    Ok(Json(review))
}

/// Evaluates the authenticated caller's own requested authorization attributes.
///
/// The body cannot select another subject. Its target is normalized into the same Rust RBAC
/// request model used by the HTTP authorization middleware, preserving one policy evaluator.
async fn create_self_subject_access_review(
    State(state): State<AppState>,
    Extension(identity): Extension<RequestIdentity>,
    body: Bytes,
) -> ApiResult<Json<SelfSubjectAccessReview>> {
    let mut review = decode_self_subject_access_review(body)?;
    review.enforce_type_meta()?;
    review.validate_request()?;
    let request = self_subject_access_review_request(&review)?;
    let allowed = match &state.authorization {
        AuthorizationMode::AlwaysAllow => true,
        AuthorizationMode::Rbac(authorizer) => authorizer.authorize(&identity, &request),
    };
    review.status = SubjectAccessReviewStatus {
        allowed,
        // This RBAC implementation has additive allow rules only. A non-match is a no-opinion
        // result, which remains fail-closed in the API server but is not an explicit deny verdict.
        denied: false,
        reason: String::new(),
        evaluation_error: String::new(),
    };
    Ok(Json(review))
}

fn self_subject_access_review_request(
    review: &SelfSubjectAccessReview,
) -> ApiResult<AuthorizationRequest> {
    let optional = |value: &str| (!value.is_empty()).then(|| value.to_owned());
    match (
        review.spec.resource_attributes.as_ref(),
        review.spec.non_resource_attributes.as_ref(),
    ) {
        (Some(resource), None) => Ok(AuthorizationRequest::resource(
            resource.verb.clone(),
            resource.group.clone(),
            resource.resource.clone(),
            optional(&resource.subresource),
            optional(&resource.namespace),
            optional(&resource.name),
        )),
        (None, Some(non_resource)) => Ok(AuthorizationRequest::non_resource(
            non_resource.verb.clone(),
            non_resource.path.clone(),
        )),
        _ => Err(ApiError::Invalid {
            message: "exactly one access-review target must be set".to_owned(),
        }
        .into()),
    }
}

async fn token_request_bound_claims(
    state: &AppState,
    namespace: &str,
    request: &TokenRequest,
) -> ApiResult<(
    Option<KubernetesBoundObjectClaims>,
    Option<KubernetesBoundObjectClaims>,
)> {
    let Some(reference) = &request.spec.bound_object_ref else {
        return Ok((None, None));
    };
    match (reference.api_version.as_str(), reference.kind.as_str()) {
        ("v1", "Pod") => {
            let pod = state.backend.pods.get(namespace, &reference.name).await?;
            validate_token_request_bound_uid(
                "Pod",
                &reference.name,
                &reference.uid,
                pod.metadata.uid.as_deref(),
            )?;
            let pod_claim = KubernetesBoundObjectClaims {
                name: reference.name.clone(),
                uid: reference.uid.clone(),
            };
            let node_claim = match pod.spec.node_name.as_deref() {
                Some(node_name) => match state.backend.nodes.get(node_name).await {
                    Ok(node) if node.metadata.uid.is_some() => Some(KubernetesBoundObjectClaims {
                        name: node_name.to_owned(),
                        uid: node.metadata.uid.expect("checked above"),
                    }),
                    Ok(_) => return Err(ApiError::Internal.into()),
                    Err(ApiError::NotFound { .. }) => None,
                    Err(error) => return Err(error.into()),
                },
                None => None,
            };
            Ok((Some(pod_claim), node_claim))
        }
        ("v1", "Node") => {
            let node = state.backend.nodes.get(&reference.name).await?;
            validate_token_request_bound_uid(
                "Node",
                &reference.name,
                &reference.uid,
                node.metadata.uid.as_deref(),
            )?;
            Ok((
                None,
                Some(KubernetesBoundObjectClaims {
                    name: reference.name.clone(),
                    uid: reference.uid.clone(),
                }),
            ))
        }
        ("v1", "Secret") => Err(ApiError::Invalid {
            message:
                "Secret-bound TokenRequest is unavailable until Secret live validation is active"
                    .to_owned(),
        }
        .into()),
        _ => Err(ApiError::Invalid {
            message:
                "spec.boundObjectRef supports only core/v1 Pod or Node in this control-plane slice"
                    .to_owned(),
        }
        .into()),
    }
}

fn validate_token_request_bound_uid(
    kind: &str,
    name: &str,
    requested_uid: &str,
    current_uid: Option<&str>,
) -> ApiResult<()> {
    if current_uid != Some(requested_uid) {
        return Err(ApiError::Invalid {
            message: format!("spec.boundObjectRef {kind} {name:?} UID does not match live object"),
        }
        .into());
    }
    Ok(())
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
        reject_watch_pagination(&query)?;
        return open_pod_watch(&state, &query, None).await;
    }
    reject_list_resource_version(&query)?;
    let label_selector = LabelSelector::parse(query.label_selector.as_deref())?;
    let field_selector = FieldSelector::parse(query.field_selector.as_deref())?;
    Ok(
        Json(paginated_pod_list(&state, &query, None, &label_selector, &field_selector).await?)
            .into_response(),
    )
}

async fn list_pods(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    Query(query): Query<ListQuery>,
) -> ApiResult<Response> {
    if is_watch_request(&query)? {
        reject_watch_pagination(&query)?;
        return open_pod_watch(&state, &query, Some(namespace)).await;
    }
    reject_list_resource_version(&query)?;
    let label_selector = LabelSelector::parse(query.label_selector.as_deref())?;
    let field_selector = FieldSelector::parse(query.field_selector.as_deref())?;
    Ok(Json(
        paginated_pod_list(
            &state,
            &query,
            Some(&namespace),
            &label_selector,
            &field_selector,
        )
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

async fn patch_pod(
    State(state): State<AppState>,
    Extension(identity): Extension<RequestIdentity>,
    Path((namespace, name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<Pod>> {
    let current = state.backend.pods.get(&namespace, &name).await?;
    let patched = bind_pod_update_identity(
        apply_pod_patch(current.clone(), &headers, body)?,
        &namespace,
        &name,
    )?;
    let admission_request = AdmissionRequest::pod_update(identity, patched.clone(), current)?;
    state.admission.validate(&admission_request).await?;
    Ok(Json(state.backend.pods.update(patched).await?))
}

async fn get_pod_status(
    State(state): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
) -> ApiResult<Json<Pod>> {
    Ok(Json(state.backend.pods.get(&namespace, &name).await?))
}

async fn replace_pod_status(
    State(state): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
    body: Bytes,
) -> ApiResult<Json<Pod>> {
    let resource = bind_pod_status_update_identity(decode_pod(body)?, &namespace, &name)?;
    Ok(Json(state.backend.pods.update_status(resource).await?))
}

async fn delete_pod(
    State(state): State<AppState>,
    Extension(identity): Extension<RequestIdentity>,
    Path((namespace, name)): Path<(String, String)>,
    body: Bytes,
) -> ApiResult<Response> {
    let options = decode_delete_options(body)?;
    let old_object = state.backend.pods.get(&namespace, &name).await?;
    let deletion_is_pending = old_object.metadata.deletion_timestamp.is_none()
        && !old_object.metadata.finalizers.is_empty();
    let admission_request = AdmissionRequest::pod_delete(identity, old_object)?;
    state.admission.validate(&admission_request).await?;
    let result = state
        .backend
        .pods
        .delete(&namespace, &name, options)
        .await?;
    let status = if deletion_is_pending {
        StatusCode::ACCEPTED
    } else {
        StatusCode::OK
    };
    Ok((
        status,
        Json(ApiStatus::success(format!(
            "pods {name:?} deletion accepted at resourceVersion {}",
            result.resource_version
        ))),
    )
        .into_response())
}

async fn list_namespaces(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> ApiResult<Response> {
    if is_watch_request(&query)? {
        reject_watch_pagination(&query)?;
        return open_namespace_watch(&state, &query).await;
    }
    reject_list_resource_version(&query)?;
    let label_selector = LabelSelector::parse(query.label_selector.as_deref())?;
    let field_selector = FieldSelector::parse(query.field_selector.as_deref())?;
    Ok(
        Json(paginated_namespace_list(&state, &query, &label_selector, &field_selector).await?)
            .into_response(),
    )
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

async fn patch_namespace(
    State(state): State<AppState>,
    Extension(identity): Extension<RequestIdentity>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<Namespace>> {
    let current = state.backend.namespaces.get(&name).await?;
    let patched = bind_namespace_update_identity(
        apply_namespace_patch(current.clone(), &headers, body)?,
        &name,
    )?;
    let admission_request = AdmissionRequest::namespace_update(identity, patched.clone(), current)?;
    state.admission.validate(&admission_request).await?;
    Ok(Json(state.backend.namespaces.update(patched).await?))
}

async fn finalize_namespace(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> ApiResult<Json<Namespace>> {
    let resource = bind_namespace_update_identity(decode_namespace(body)?, &name)?;
    Ok(Json(state.backend.namespaces.finalize(resource).await?))
}

async fn get_namespace_status(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<Json<Namespace>> {
    Ok(Json(state.backend.namespaces.get(&name).await?))
}

async fn replace_namespace_status(
    State(state): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> ApiResult<Json<Namespace>> {
    let resource = bind_namespace_status_update_identity(decode_namespace(body)?, &name)?;
    Ok(Json(
        state.backend.namespaces.update_status(resource).await?,
    ))
}

async fn delete_namespace(
    State(state): State<AppState>,
    Extension(identity): Extension<RequestIdentity>,
    Path(name): Path<String>,
    body: Bytes,
) -> ApiResult<Response> {
    let options = decode_delete_options(body)?;
    let old_object = state.backend.namespaces.get(&name).await?;
    let deletion_is_pending = old_object.metadata.deletion_timestamp.is_none()
        && (!old_object.metadata.finalizers.is_empty() || !old_object.spec.finalizers.is_empty());
    let admission_request = AdmissionRequest::namespace_delete(identity, old_object)?;
    state.admission.validate(&admission_request).await?;
    let result = state.backend.namespaces.delete(&name, options).await?;
    let status = if deletion_is_pending {
        StatusCode::ACCEPTED
    } else {
        StatusCode::OK
    };
    Ok((
        status,
        Json(ApiStatus::success(format!(
            "namespaces {name:?} deletion accepted at resourceVersion {}",
            result.resource_version
        ))),
    )
        .into_response())
}

async fn list_nodes(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> ApiResult<Response> {
    if is_watch_request(&query)? {
        reject_watch_pagination(&query)?;
        return open_node_watch(&state, &query).await;
    }
    reject_list_resource_version(&query)?;
    let label_selector = LabelSelector::parse(query.label_selector.as_deref())?;
    let field_selector = FieldSelector::parse(query.field_selector.as_deref())?;
    Ok(
        Json(paginated_node_list(&state, &query, &label_selector, &field_selector).await?)
            .into_response(),
    )
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

fn decode_token_request(body: Bytes) -> ApiResult<TokenRequest> {
    serde_json::from_slice(&body).map_err(|error| {
        ApiError::BadRequest {
            message: format!("invalid TokenRequest JSON: {error}"),
        }
        .into()
    })
}

fn decode_token_review(body: Bytes) -> ApiResult<TokenReview> {
    if body.is_empty() {
        return Err(ApiError::BadRequest {
            message: "a TokenReview JSON request body is required".to_owned(),
        }
        .into());
    }
    serde_json::from_slice(&body).map_err(|error| {
        ApiError::BadRequest {
            message: format!("invalid TokenReview JSON: {error}"),
        }
        .into()
    })
}

fn decode_self_subject_access_review(body: Bytes) -> ApiResult<SelfSubjectAccessReview> {
    if body.is_empty() {
        return Err(ApiError::BadRequest {
            message: "a SelfSubjectAccessReview JSON request body is required".to_owned(),
        }
        .into());
    }
    serde_json::from_slice(&body).map_err(|error| {
        ApiError::BadRequest {
            message: format!("invalid SelfSubjectAccessReview JSON: {error}"),
        }
        .into()
    })
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

fn apply_namespace_patch(
    current: Namespace,
    headers: &HeaderMap,
    body: Bytes,
) -> ApiResult<Namespace> {
    if body.is_empty() {
        return Err(ApiError::BadRequest {
            message: "a Namespace PATCH request body is required".to_owned(),
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
            message: format!("PATCH result is not a valid Namespace: {error}"),
        }
        .into()
    })
}

fn apply_pod_patch(current: Pod, headers: &HeaderMap, body: Bytes) -> ApiResult<Pod> {
    if body.is_empty() {
        return Err(ApiError::BadRequest {
            message: "a Pod PATCH request body is required".to_owned(),
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
            message: format!("PATCH result is not a valid Pod: {error}"),
        }
        .into()
    })
}

fn apply_service_account_patch(
    current: ServiceAccount,
    headers: &HeaderMap,
    body: Bytes,
) -> ApiResult<ServiceAccount> {
    if body.is_empty() {
        return Err(ApiError::BadRequest {
            message: "a ServiceAccount PATCH request body is required".to_owned(),
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
            message: format!("PATCH result is not a valid ServiceAccount: {error}"),
        }
        .into()
    })
}

fn apply_config_map_patch(
    current: ConfigMap,
    headers: &HeaderMap,
    body: Bytes,
) -> ApiResult<ConfigMap> {
    if body.is_empty() {
        return Err(ApiError::BadRequest {
            message: "a ConfigMap PATCH request body is required".to_owned(),
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
            message: format!("PATCH result is not a valid ConfigMap: {error}"),
        }
        .into()
    })
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

/// Ensures a status replacement cannot target a different Pod than its URI declares.
fn bind_pod_status_update_identity(resource: Pod, namespace: &str, name: &str) -> ApiResult<Pod> {
    bind_pod_update_identity(resource, namespace, name).map_err(|error| match error.0 {
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

/// Ensures a status replacement cannot target a different Namespace than its URI declares.
fn bind_namespace_status_update_identity(resource: Namespace, name: &str) -> ApiResult<Namespace> {
    bind_namespace_update_identity(resource, name).map_err(|error| match error.0 {
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
        AnonymousPolicy, AuthenticationChain, RequestIdentity, ServiceAccountJwtKey,
        ServiceAccountJwtVerifier, StaticBearerToken,
    };
    use rusternetes_common::StatusReason;
    use tower::ServiceExt;

    use super::*;

    #[tokio::test]
    async fn oidc_discovery_and_jwks_publish_only_configured_service_account_keys() {
        const PUBLIC_KEY: &str = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAht2MPPSji0Vrbgk/gCyZ\nDLAfFNHUx7R697SBlj2meld3M7DUf5IVa4C9BxyTf2uhb35JNPAW0TYgQ9bg4n4/\n5DD8dFz1soqSHumaMO7a969VwHtJO5cPYAkKqXiGSxwkTQiF4MSmaoCvPlMkYF0/\n21stDUJkJcHr1VLsQfK5X660tdK9suWeW6zxYidwWCt94LalQ85lOcZjw3YfKymX\nnRrCWNPUme7dLFo2lBJ/K2wNuucUZXPGg50aeEgmr4OVTPxVApRL5b85taacmbGu\nXVy/oaUvF0M3iDkRgZNKN0vZPNvP4tc+KF/+DWDO1msmFYkiiG6848zqtRnY0DJ6\nyQIDAQAB\n-----END PUBLIC KEY-----\n";
        let verifier = ServiceAccountJwtVerifier::new(
            "https://issuer.example",
            vec!["api".to_owned()],
            vec![ServiceAccountJwtKey {
                key_id: Some("active".to_owned()),
                rsa_public_key_pem: PUBLIC_KEY.to_owned(),
            }],
        )
        .expect("verification configuration is valid");
        let authentication = AuthenticationChain::new(AnonymousPolicy::Deny, Vec::new())
            .with_service_account_jwt_verifier(verifier)
            .with_service_account_oidc_discovery(
                "https://issuer.example",
                "https://issuer.example/openid/v1/jwks",
            )
            .expect("OIDC discovery is derived from configured verifier");
        let app = router_with_backend_and_auth(
            ConfigMapBackend::InMemory(Arc::new(InMemoryConfigMapStore::new())),
            authentication,
        );
        let discovery = app
            .oneshot(
                Request::builder()
                    .uri("/.well-known/openid-configuration")
                    .body(Body::empty())
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(discovery.status(), StatusCode::OK);
        let document: serde_json::Value = serde_json::from_slice(
            &to_bytes(discovery.into_body(), usize::MAX)
                .await
                .expect("discovery body is readable"),
        )
        .expect("discovery is JSON");
        assert_eq!(document["issuer"], "https://issuer.example");
        assert_eq!(
            document["jwks_uri"],
            "https://issuer.example/openid/v1/jwks"
        );

        let app = router_with_backend_and_auth(
            ConfigMapBackend::InMemory(Arc::new(InMemoryConfigMapStore::new())),
            AuthenticationChain::default(),
        );
        let disabled = app
            .oneshot(
                Request::builder()
                    .uri("/openid/v1/jwks")
                    .body(Body::empty())
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(disabled.status(), StatusCode::NOT_FOUND);
    }

    fn token_request_authentication() -> (AuthenticationChain, ServiceAccountJwtVerifier) {
        const PRIVATE_KEY: &str = include_str!("../testdata/tokenrequest-test-private.pem");
        const PUBLIC_KEY: &str = include_str!("../testdata/tokenrequest-test-public.pem");
        let verifier = ServiceAccountJwtVerifier::new(
            "https://issuer.example",
            vec!["api".to_owned()],
            vec![ServiceAccountJwtKey {
                key_id: Some("tokenrequest-test".to_owned()),
                rsa_public_key_pem: PUBLIC_KEY.to_owned(),
            }],
        )
        .expect("test verifier is valid");
        let issuer = ServiceAccountTokenIssuer::new(
            "https://issuer.example",
            vec!["api".to_owned()],
            PRIVATE_KEY,
            "tokenrequest-test",
            60,
            120,
        )
        .expect("test issuer is valid");
        let identity = RequestIdentity::authenticated(
            "bootstrap-admin",
            None,
            ["system:masters".to_owned()],
            Default::default(),
        )
        .expect("test identity is valid");
        let authentication = AuthenticationChain::new(
            AnonymousPolicy::Deny,
            vec![StaticBearerToken::new("bootstrap-secret", identity).expect("test token")],
        )
        .with_service_account_jwt_verifier(verifier.clone())
        .with_service_account_token_issuer(issuer)
        .expect("issuer matches verifier");
        (authentication, verifier)
    }

    async fn configured_token_request_app() -> (Router, ServiceAccountJwtVerifier) {
        let config_maps = Arc::new(InMemoryConfigMapStore::new());
        let service_accounts = Arc::new(InMemoryServiceAccountStore::new());
        let created = service_accounts
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
        assert!(created.metadata.uid.is_some());
        let backend = CoreApiBackend {
            config_maps: ConfigMapBackend::InMemory(config_maps),
            pods: PodBackend::InMemory(Arc::new(InMemoryPodStore::new())),
            service_accounts: ServiceAccountBackend::InMemory(service_accounts),
            namespaces: NamespaceBackend::InMemory(Arc::new(InMemoryNamespaceStore::new())),
            nodes: NodeBackend::InMemory(Arc::new(InMemoryNodeStore::new())),
        };
        let (authentication, verifier) = token_request_authentication();
        (
            router_with_core_backend_and_auth(backend, authentication),
            verifier,
        )
    }

    #[tokio::test]
    async fn token_request_issues_verifiable_service_account_jwt() {
        let (app, verifier) = configured_token_request_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/namespaces/default/serviceaccounts/build-robot/token")
                    .header(header::AUTHORIZATION, "Bearer bootstrap-secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "apiVersion": "authentication.k8s.io/v1",
                            "kind": "TokenRequest",
                            "spec": { "audiences": ["api"], "expirationSeconds": 3600 }
                        })
                        .to_string(),
                    ))
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_slice(
            &to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("response body is readable"),
        )
        .expect("response is JSON");
        assert_eq!(body["apiVersion"], "authentication.k8s.io/v1");
        assert_eq!(body["kind"], "TokenRequest");
        let token = body["status"]["token"]
            .as_str()
            .expect("status token is populated");
        assert!(body["status"]["expirationTimestamp"].is_string());
        let claims = verifier.verify(token).expect("issued token verifies");
        assert_eq!(claims.sub, "system:serviceaccount:default:build-robot");
        assert_eq!(claims.aud, ["api"]);
    }

    async fn issue_token_review_test_token(app: Router) -> String {
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/namespaces/default/serviceaccounts/build-robot/token")
                    .header(header::AUTHORIZATION, "Bearer bootstrap-secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "apiVersion": "authentication.k8s.io/v1",
                            "kind": "TokenRequest",
                            "spec": { "audiences": ["api"] }
                        })
                        .to_string(),
                    ))
                    .expect("valid TokenRequest"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(response.status(), StatusCode::OK);
        let response: serde_json::Value = serde_json::from_slice(
            &to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("TokenRequest body is readable"),
        )
        .expect("TokenRequest response is JSON");
        response["status"]["token"]
            .as_str()
            .expect("TokenRequest returns a token")
            .to_owned()
    }

    #[tokio::test]
    async fn token_review_authenticates_live_serviceaccount_jwts_and_advertises_group_discovery() {
        let (app, _) = configured_token_request_app().await;
        let token = issue_token_review_test_token(app.clone()).await;

        let groups = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/apis")
                    .header(header::AUTHORIZATION, "Bearer bootstrap-secret")
                    .body(Body::empty())
                    .expect("valid group discovery request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(groups.status(), StatusCode::OK);
        let groups: serde_json::Value = serde_json::from_slice(
            &to_bytes(groups.into_body(), usize::MAX)
                .await
                .expect("group discovery body is readable"),
        )
        .expect("group discovery is JSON");
        assert!(groups["groups"]
            .as_array()
            .is_some_and(|groups| groups.iter().any(|group| {
                group["name"] == "authentication.k8s.io"
                    && group["preferredVersion"]["groupVersion"] == "authentication.k8s.io/v1"
            })));
        let resources = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/apis/authentication.k8s.io/v1")
                    .header(header::AUTHORIZATION, "Bearer bootstrap-secret")
                    .body(Body::empty())
                    .expect("valid resource discovery request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(resources.status(), StatusCode::OK);
        let resources: serde_json::Value = serde_json::from_slice(
            &to_bytes(resources.into_body(), usize::MAX)
                .await
                .expect("resource discovery body is readable"),
        )
        .expect("resource discovery is JSON");
        assert_eq!(resources["groupVersion"], "authentication.k8s.io/v1");
        assert!(resources["resources"]
            .as_array()
            .is_some_and(|resources| resources.iter().any(|resource| resource["name"]
                == "tokenreviews"
                && resource["namespaced"] == false
                && resource["kind"] == "TokenReview"
                && resource["verbs"] == serde_json::json!(["create"]))));

        let review = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/apis/authentication.k8s.io/v1/tokenreviews")
                    .header(header::AUTHORIZATION, "Bearer bootstrap-secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "apiVersion": "authentication.k8s.io/v1",
                            "kind": "TokenReview",
                            "spec": { "token": token, "audiences": ["api"] }
                        })
                        .to_string(),
                    ))
                    .expect("valid TokenReview request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(review.status(), StatusCode::OK);
        let review: serde_json::Value = serde_json::from_slice(
            &to_bytes(review.into_body(), usize::MAX)
                .await
                .expect("TokenReview body is readable"),
        )
        .expect("TokenReview response is JSON");
        assert_eq!(review["apiVersion"], "authentication.k8s.io/v1");
        assert_eq!(review["kind"], "TokenReview");
        assert_eq!(review["status"]["authenticated"], true);
        assert_eq!(
            review["status"]["user"]["username"],
            "system:serviceaccount:default:build-robot"
        );
        assert!(review["status"]["user"]["uid"].is_string());
        assert!(review["status"]["user"]["groups"]
            .as_array()
            .is_some_and(|groups| groups.iter().any(|group| group == "system:authenticated")));
        assert_eq!(review["status"]["audiences"], serde_json::json!(["api"]));
    }

    #[tokio::test]
    async fn token_review_fails_closed_for_invalid_or_unconfigured_tokens_and_rejects_client_status(
    ) {
        let (app, _) = configured_token_request_app().await;
        let invalid = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/apis/authentication.k8s.io/v1/tokenreviews")
                    .header(header::AUTHORIZATION, "Bearer bootstrap-secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "apiVersion": "authentication.k8s.io/v1",
                            "kind": "TokenReview",
                            "spec": { "token": "not-a-jwt" }
                        })
                        .to_string(),
                    ))
                    .expect("valid invalid-token review request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(invalid.status(), StatusCode::OK);
        let invalid: serde_json::Value = serde_json::from_slice(
            &to_bytes(invalid.into_body(), usize::MAX)
                .await
                .expect("invalid review body is readable"),
        )
        .expect("invalid review response is JSON");
        assert_eq!(invalid["status"]["authenticated"], serde_json::json!(false));
        assert!(invalid["status"].get("user").is_none());

        let disabled = router_with_backend(ConfigMapBackend::InMemory(Arc::new(
            InMemoryConfigMapStore::new(),
        )));
        let unconfigured = disabled
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/apis/authentication.k8s.io/v1/tokenreviews")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "apiVersion": "authentication.k8s.io/v1",
                            "kind": "TokenReview",
                            "spec": { "token": "unconfigured-token" }
                        })
                        .to_string(),
                    ))
                    .expect("valid unconfigured review request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(unconfigured.status(), StatusCode::OK);
        let unconfigured: serde_json::Value = serde_json::from_slice(
            &to_bytes(unconfigured.into_body(), usize::MAX)
                .await
                .expect("unconfigured review body is readable"),
        )
        .expect("unconfigured review response is JSON");
        assert_eq!(
            unconfigured["status"]["authenticated"],
            serde_json::json!(false)
        );

        let client_status = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/apis/authentication.k8s.io/v1/tokenreviews")
                    .header(header::AUTHORIZATION, "Bearer bootstrap-secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "apiVersion": "authentication.k8s.io/v1",
                            "kind": "TokenReview",
                            "spec": { "token": "not-a-jwt" },
                            "status": { "authenticated": true }
                        })
                        .to_string(),
                    ))
                    .expect("valid client-status review request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(client_status.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn token_request_fails_closed_without_configured_signer() {
        let app = router_with_backend(ConfigMapBackend::InMemory(Arc::new(
            InMemoryConfigMapStore::new(),
        )));
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/namespaces/default/serviceaccounts/build-robot/token")
                    .body(Body::from("{}"))
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn token_request_rejects_missing_bound_pod() {
        let (app, _) = configured_token_request_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/namespaces/default/serviceaccounts/build-robot/token")
                    .header(header::AUTHORIZATION, "Bearer bootstrap-secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "apiVersion": "authentication.k8s.io/v1",
                            "kind": "TokenRequest",
                            "spec": {
                                "boundObjectRef": {
                                    "apiVersion": "v1",
                                    "kind": "Pod",
                                    "name": "does-not-exist",
                                    "uid": "wrong-uid"
                                }
                            }
                        })
                        .to_string(),
                    ))
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn token_request_rejects_service_account_metadata_uid_mismatch() {
        let (app, _) = configured_token_request_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/namespaces/default/serviceaccounts/build-robot/token")
                    .header(header::AUTHORIZATION, "Bearer bootstrap-secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "apiVersion": "authentication.k8s.io/v1",
                            "kind": "TokenRequest",
                            "metadata": { "uid": "stale-service-account-uid" }
                        })
                        .to_string(),
                    ))
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn config_map_patch_supports_json_and_merge_patch_with_guards() {
        let app = router(Arc::new(InMemoryConfigMapStore::new()));
        let create = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/namespaces/default/configmaps")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "apiVersion": "v1",
                            "kind": "ConfigMap",
                            "metadata": { "name": "patch-settings" },
                            "data": { "mode": "safe" }
                        })
                        .to_string(),
                    ))
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(create.status(), StatusCode::CREATED);
        let created: ConfigMap = serde_json::from_slice(
            &to_bytes(create.into_body(), usize::MAX)
                .await
                .expect("create body is readable"),
        )
        .expect("created ConfigMap is JSON");

        let merged_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/api/v1/namespaces/default/configmaps/patch-settings")
                    .header(header::CONTENT_TYPE, "application/merge-patch+json")
                    .body(Body::from(r#"{"data":{"mode":"merge","extra":"value"}}"#))
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(merged_response.status(), StatusCode::OK);
        let merged: ConfigMap = serde_json::from_slice(
            &to_bytes(merged_response.into_body(), usize::MAX)
                .await
                .expect("merge body is readable"),
        )
        .expect("merge ConfigMap is JSON");
        assert_eq!(merged.data.get("mode"), Some(&"merge".to_owned()));
        assert_eq!(merged.data.get("extra"), Some(&"value".to_owned()));

        let json_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/api/v1/namespaces/default/configmaps/patch-settings")
                    .header(header::CONTENT_TYPE, "application/json-patch+json")
                    .body(Body::from(
                        r#"[{"op":"replace","path":"/data/mode","value":"json"}]"#,
                    ))
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(json_response.status(), StatusCode::OK);
        let json_patched: ConfigMap = serde_json::from_slice(
            &to_bytes(json_response.into_body(), usize::MAX)
                .await
                .expect("JSON Patch body is readable"),
        )
        .expect("JSON Patch ConfigMap is JSON");
        assert_eq!(json_patched.data.get("mode"), Some(&"json".to_owned()));

        let identity_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/api/v1/namespaces/default/configmaps/patch-settings")
                    .header(header::CONTENT_TYPE, "application/merge-patch+json")
                    .body(Body::from(r#"{"metadata":{"name":"different"}}"#))
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(identity_response.status(), StatusCode::UNPROCESSABLE_ENTITY);

        let unsupported_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/api/v1/namespaces/default/configmaps/patch-settings")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(
            unsupported_response.status(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE
        );

        let stale_response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/api/v1/namespaces/default/configmaps/patch-settings")
                    .header(header::CONTENT_TYPE, "application/merge-patch+json")
                    .body(Body::from(
                        serde_json::json!({
                            "metadata": { "resourceVersion": created.metadata.resource_version },
                            "data": { "mode": "stale" }
                        })
                        .to_string(),
                    ))
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(stale_response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn pod_status_subresource_isolated_and_resource_version_guarded() {
        let app = router(Arc::new(InMemoryConfigMapStore::new()));
        let create = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/namespaces/default/pods")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&Pod {
                            type_meta: TypeMeta::pod(),
                            metadata: ObjectMeta {
                                name: Some("status-web".to_owned()),
                                ..ObjectMeta::default()
                            },
                            spec: PodSpec {
                                containers: vec![Container {
                                    name: "app".to_owned(),
                                    image: Some("example:v1".to_owned()),
                                    ..Container::default()
                                }],
                                ..PodSpec::default()
                            },
                            ..Pod::default()
                        })
                        .expect("Pod serializes"),
                    ))
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(create.status(), StatusCode::CREATED);
        let created: Pod = serde_json::from_slice(
            &to_bytes(create.into_body(), usize::MAX)
                .await
                .expect("create body is readable"),
        )
        .expect("created Pod is JSON");
        let mut status_update = created.clone();
        status_update.spec.node_name = Some("forbidden-spec-change".to_owned());
        status_update.status.phase = Some(rusternetes_api_types::PodPhase::Running);
        let updated_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/v1/namespaces/default/pods/status-web/status")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&status_update).expect("status Pod serializes"),
                    ))
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(updated_response.status(), StatusCode::OK);
        let updated: Pod = serde_json::from_slice(
            &to_bytes(updated_response.into_body(), usize::MAX)
                .await
                .expect("status body is readable"),
        )
        .expect("status Pod is JSON");
        assert_eq!(
            updated.status.phase,
            Some(rusternetes_api_types::PodPhase::Running)
        );
        assert_eq!(updated.spec, created.spec);
        assert_ne!(
            updated.metadata.resource_version,
            created.metadata.resource_version
        );

        let mut stale = created;
        stale.status.phase = Some(rusternetes_api_types::PodPhase::Failed);
        let stale_response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/v1/namespaces/default/pods/status-web/status")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&stale).expect("stale Pod serializes"),
                    ))
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(stale_response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn pod_patch_preserves_spec_and_status_invariants() {
        let app = router(Arc::new(InMemoryConfigMapStore::new()));
        let create = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/namespaces/default/pods")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "apiVersion": "v1",
                            "kind": "Pod",
                            "metadata": { "name": "patch-pod" },
                            "spec": { "containers": [{ "name": "app", "image": "v1" }] }
                        })
                        .to_string(),
                    ))
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(create.status(), StatusCode::CREATED);
        let created: Pod = serde_json::from_slice(
            &to_bytes(create.into_body(), usize::MAX)
                .await
                .expect("create body is readable"),
        )
        .expect("created Pod is JSON");

        let label_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/api/v1/namespaces/default/pods/patch-pod")
                    .header(header::CONTENT_TYPE, "application/merge-patch+json")
                    .body(Body::from(r#"{"metadata":{"labels":{"patched":"true"}}}"#))
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(label_response.status(), StatusCode::OK);
        let labeled: Pod = serde_json::from_slice(
            &to_bytes(label_response.into_body(), usize::MAX)
                .await
                .expect("label response is readable"),
        )
        .expect("labeled Pod is JSON");
        assert_eq!(
            labeled.metadata.labels.get("patched"),
            Some(&"true".to_owned())
        );
        assert_eq!(labeled.spec, created.spec);
        assert_eq!(labeled.status, created.status);

        let json_patch_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/api/v1/namespaces/default/pods/patch-pod")
                    .header(header::CONTENT_TYPE, "application/json-patch+json")
                    .body(Body::from(
                        r#"[{"op":"add","path":"/metadata/labels/json-patched","value":"true"}]"#,
                    ))
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(json_patch_response.status(), StatusCode::OK);
        let json_patched: Pod = serde_json::from_slice(
            &to_bytes(json_patch_response.into_body(), usize::MAX)
                .await
                .expect("JSON Patch body is readable"),
        )
        .expect("JSON patched Pod is JSON");
        assert_eq!(
            json_patched.metadata.labels.get("json-patched"),
            Some(&"true".to_owned())
        );

        for body in [
            r#"{"spec":{"containers":[{"name":"app","image":"v2"}]}}"#,
            r#"{"status":{"phase":"Running"}}"#,
            r#"{"metadata":{"name":"different"}}"#,
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("PATCH")
                        .uri("/api/v1/namespaces/default/pods/patch-pod")
                        .header(header::CONTENT_TYPE, "application/merge-patch+json")
                        .body(Body::from(body))
                        .expect("valid request"),
                )
                .await
                .expect("router is infallible");
            assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        }

        let stale_response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/api/v1/namespaces/default/pods/patch-pod")
                    .header(header::CONTENT_TYPE, "application/merge-patch+json")
                    .body(Body::from(
                        serde_json::json!({
                            "metadata": { "resourceVersion": created.metadata.resource_version,
                                          "labels": { "stale": "true" } }
                        })
                        .to_string(),
                    ))
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(stale_response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn namespace_delete_waits_for_finalize_and_then_removes_the_object() {
        let app = router(Arc::new(InMemoryConfigMapStore::new()));
        let create = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/namespaces")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "apiVersion": "v1",
                            "kind": "Namespace",
                            "metadata": { "name": "terminating-development" },
                            "spec": { "finalizers": ["example.com/cleanup"] }
                        })
                        .to_string(),
                    ))
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(create.status(), StatusCode::CREATED);

        let delete = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/namespaces/terminating-development")
                    .body(Body::empty())
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(delete.status(), StatusCode::ACCEPTED);

        let pending_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/namespaces/terminating-development")
                    .body(Body::empty())
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(pending_response.status(), StatusCode::OK);
        let mut pending: Namespace = serde_json::from_slice(
            &to_bytes(pending_response.into_body(), usize::MAX)
                .await
                .expect("pending Namespace body is readable"),
        )
        .expect("pending Namespace is JSON");
        assert!(pending.metadata.deletion_timestamp.is_some());
        assert_eq!(pending.status.phase, Some(NamespacePhase::Terminating));

        pending.spec.finalizers.clear();
        let finalize = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/v1/namespaces/terminating-development/finalize")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&pending).expect("finalize Namespace serializes"),
                    ))
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(finalize.status(), StatusCode::OK);

        let after_finalize = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/namespaces/terminating-development")
                    .body(Body::empty())
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(after_finalize.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn namespace_patch_preserves_finalizer_and_status_invariants() {
        let app = router(Arc::new(InMemoryConfigMapStore::new()));
        let create = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/namespaces")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "apiVersion": "v1",
                            "kind": "Namespace",
                            "metadata": { "name": "patch-development" }
                        })
                        .to_string(),
                    ))
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(create.status(), StatusCode::CREATED);
        let created: Namespace = serde_json::from_slice(
            &to_bytes(create.into_body(), usize::MAX)
                .await
                .expect("create body is readable"),
        )
        .expect("created Namespace is JSON");

        let merged_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/api/v1/namespaces/patch-development")
                    .header(header::CONTENT_TYPE, "application/merge-patch+json")
                    .body(Body::from(r#"{"metadata":{"labels":{"patched":"true"}}}"#))
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(merged_response.status(), StatusCode::OK);
        let merged: Namespace = serde_json::from_slice(
            &to_bytes(merged_response.into_body(), usize::MAX)
                .await
                .expect("merge response is readable"),
        )
        .expect("merged Namespace is JSON");
        assert_eq!(
            merged.metadata.labels.get("patched"),
            Some(&"true".to_owned())
        );

        let json_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/api/v1/namespaces/patch-development")
                    .header(header::CONTENT_TYPE, "application/json-patch+json")
                    .body(Body::from(
                        r#"[{"op":"add","path":"/metadata/labels/json-patched","value":"true"}]"#,
                    ))
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(json_response.status(), StatusCode::OK);

        for body in [
            r#"{"spec":{"finalizers":["forbidden"]}}"#,
            r#"{"status":{"phase":"Terminating"}}"#,
            r#"{"metadata":{"name":"different"}}"#,
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("PATCH")
                        .uri("/api/v1/namespaces/patch-development")
                        .header(header::CONTENT_TYPE, "application/merge-patch+json")
                        .body(Body::from(body))
                        .expect("valid request"),
                )
                .await
                .expect("router is infallible");
            assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        }

        let stale_response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/api/v1/namespaces/patch-development")
                    .header(header::CONTENT_TYPE, "application/merge-patch+json")
                    .body(Body::from(
                        serde_json::json!({
                            "metadata": { "resourceVersion": created.metadata.resource_version,
                                          "labels": { "stale": "true" } }
                        })
                        .to_string(),
                    ))
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(stale_response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn namespace_status_subresource_isolated_and_resource_version_guarded() {
        let app = router(Arc::new(InMemoryConfigMapStore::new()));
        let create = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/namespaces")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&Namespace {
                            type_meta: TypeMeta::namespace(),
                            metadata: ObjectMeta {
                                name: Some("status-development".to_owned()),
                                ..ObjectMeta::default()
                            },
                            ..Namespace::default()
                        })
                        .expect("Namespace serializes"),
                    ))
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(create.status(), StatusCode::CREATED);
        let created: Namespace = serde_json::from_slice(
            &to_bytes(create.into_body(), usize::MAX)
                .await
                .expect("create body is readable"),
        )
        .expect("created Namespace is JSON");
        let mut status_update = created.clone();
        status_update
            .spec
            .finalizers
            .push("forbidden-finalizer".to_owned());
        status_update.status.phase = Some(rusternetes_api_types::NamespacePhase::Terminating);
        let updated_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/v1/namespaces/status-development/status")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&status_update).expect("status Namespace serializes"),
                    ))
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(updated_response.status(), StatusCode::OK);
        let updated: Namespace = serde_json::from_slice(
            &to_bytes(updated_response.into_body(), usize::MAX)
                .await
                .expect("status body is readable"),
        )
        .expect("status Namespace is JSON");
        assert_eq!(
            updated.status.phase,
            Some(rusternetes_api_types::NamespacePhase::Terminating)
        );
        assert_eq!(updated.spec, created.spec);
        assert_ne!(
            updated.metadata.resource_version,
            created.metadata.resource_version
        );

        let mut stale = created;
        stale.status.phase = Some(rusternetes_api_types::NamespacePhase::Terminating);
        let stale_response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/v1/namespaces/status-development/status")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&stale).expect("stale Namespace serializes"),
                    ))
                    .expect("valid request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(stale_response.status(), StatusCode::CONFLICT);
    }

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

    #[test]
    fn token_review_path_maps_to_authentication_group_create_resource() {
        let request = authorization_request_from_http(
            &ApiRegistry::core_v1(),
            &axum::http::Method::POST,
            &"/apis/authentication.k8s.io/v1/tokenreviews"
                .parse()
                .expect("valid TokenReview URI"),
        );
        assert_eq!(request.verb, "create");
        assert!(matches!(
            request.target,
            rusternetes_authz_rbac::RequestTarget::Resource {
                api_group,
                resource,
                subresource,
                namespace,
                name,
            } if api_group == "authentication.k8s.io"
                && resource == "tokenreviews"
                && subresource.is_none()
                && namespace.is_none()
                && name.is_none()
        ));
    }

    fn self_subject_access_review_test_app() -> Router {
        let identity = RequestIdentity::authenticated(
            "access-reviewer",
            Some("reviewer-uid".to_owned()),
            ["reviewers".to_owned()],
            Default::default(),
        )
        .expect("access-review identity is valid");
        let authentication = AuthenticationChain::new(
            AnonymousPolicy::Deny,
            vec![StaticBearerToken::new("reviewer-secret", identity)
                .expect("access-review bearer token is valid")],
        );
        let authorizer = RbacAuthorizer::new(
            Vec::new(),
            vec![rusternetes_authz_rbac::ClusterRole {
                name: "self-reviewer".to_owned(),
                rules: vec![
                    rusternetes_authz_rbac::PolicyRule {
                        api_groups: vec!["authorization.k8s.io".to_owned()],
                        resources: vec!["selfsubjectaccessreviews".to_owned()],
                        verbs: vec!["create".to_owned()],
                        ..rusternetes_authz_rbac::PolicyRule::default()
                    },
                    rusternetes_authz_rbac::PolicyRule {
                        api_groups: vec![String::new()],
                        resources: vec!["configmaps".to_owned()],
                        verbs: vec!["get".to_owned()],
                        ..rusternetes_authz_rbac::PolicyRule::default()
                    },
                    rusternetes_authz_rbac::PolicyRule {
                        verbs: vec!["get".to_owned()],
                        non_resource_urls: vec![
                            "/healthz".to_owned(),
                            "/apis/authorization.k8s.io/v1".to_owned(),
                        ],
                        ..rusternetes_authz_rbac::PolicyRule::default()
                    },
                ],
            }],
            Vec::new(),
            vec![rusternetes_authz_rbac::ClusterRoleBinding {
                name: "self-reviewer-binding".to_owned(),
                subjects: vec![rusternetes_authz_rbac::Subject::User(
                    "access-reviewer".to_owned(),
                )],
                role_ref: "self-reviewer".to_owned(),
            }],
        );
        router_with_backend_auth_and_authorization(
            ConfigMapBackend::InMemory(Arc::new(InMemoryConfigMapStore::new())),
            authentication,
            AuthorizationMode::Rbac(authorizer),
        )
    }

    #[tokio::test]
    async fn self_subject_access_review_evaluates_current_identity_with_rbac_and_typed_discovery() {
        let app = self_subject_access_review_test_app();
        let discovery = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/apis/authorization.k8s.io/v1")
                    .header("authorization", "Bearer reviewer-secret")
                    .body(Body::empty())
                    .expect("valid authorization discovery request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(discovery.status(), StatusCode::OK);
        let discovery: serde_json::Value = serde_json::from_slice(
            &to_bytes(discovery.into_body(), usize::MAX)
                .await
                .expect("authorization discovery body is readable"),
        )
        .expect("authorization discovery is JSON");
        assert_eq!(discovery["groupVersion"], "authorization.k8s.io/v1");
        assert!(discovery["resources"]
            .as_array()
            .is_some_and(|resources| resources.iter().any(|resource| resource["name"]
                == "selfsubjectaccessreviews"
                && resource["namespaced"] == false
                && resource["kind"] == "SelfSubjectAccessReview"
                && resource["verbs"] == serde_json::json!(["create"]))));

        let allowed = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/apis/authorization.k8s.io/v1/selfsubjectaccessreviews")
                    .header("authorization", "Bearer reviewer-secret")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "apiVersion": "authorization.k8s.io/v1",
                            "kind": "SelfSubjectAccessReview",
                            "spec": {
                                "resourceAttributes": {
                                    "verb": "get",
                                    "resource": "configmaps",
                                    "namespace": "default"
                                }
                            }
                        })
                        .to_string(),
                    ))
                    .expect("valid allowed SSAR request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(allowed.status(), StatusCode::OK);
        let allowed: serde_json::Value = serde_json::from_slice(
            &to_bytes(allowed.into_body(), usize::MAX)
                .await
                .expect("allowed SSAR body is readable"),
        )
        .expect("allowed SSAR is JSON");
        assert_eq!(allowed["status"]["allowed"], true);
        assert!(allowed["status"].get("denied").is_none());

        let no_opinion = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/apis/authorization.k8s.io/v1/selfsubjectaccessreviews")
                    .header("authorization", "Bearer reviewer-secret")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "apiVersion": "authorization.k8s.io/v1",
                            "kind": "SelfSubjectAccessReview",
                            "spec": {
                                "resourceAttributes": {
                                    "verb": "create",
                                    "resource": "pods",
                                    "namespace": "default"
                                }
                            }
                        })
                        .to_string(),
                    ))
                    .expect("valid no-opinion SSAR request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(no_opinion.status(), StatusCode::OK);
        let no_opinion: serde_json::Value = serde_json::from_slice(
            &to_bytes(no_opinion.into_body(), usize::MAX)
                .await
                .expect("no-opinion SSAR body is readable"),
        )
        .expect("no-opinion SSAR is JSON");
        assert_eq!(no_opinion["status"]["allowed"], false);
        assert!(no_opinion["status"].get("denied").is_none());

        let non_resource = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/apis/authorization.k8s.io/v1/selfsubjectaccessreviews")
                    .header("authorization", "Bearer reviewer-secret")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "apiVersion": "authorization.k8s.io/v1",
                            "kind": "SelfSubjectAccessReview",
                            "spec": {
                                "nonResourceAttributes": { "verb": "get", "path": "/healthz" }
                            }
                        })
                        .to_string(),
                    ))
                    .expect("valid non-resource SSAR request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(non_resource.status(), StatusCode::OK);
        let non_resource: serde_json::Value = serde_json::from_slice(
            &to_bytes(non_resource.into_body(), usize::MAX)
                .await
                .expect("non-resource SSAR body is readable"),
        )
        .expect("non-resource SSAR is JSON");
        assert_eq!(non_resource["status"]["allowed"], true);
    }

    #[tokio::test]
    async fn self_subject_access_review_rejects_missing_ambiguous_or_server_owned_fields() {
        let app = self_subject_access_review_test_app();
        let invalid_requests = [
            (
                "missing spec",
                serde_json::json!({
                    "apiVersion": "authorization.k8s.io/v1",
                    "kind": "SelfSubjectAccessReview"
                }),
            ),
            (
                "ambiguous resource and non-resource targets",
                serde_json::json!({
                    "apiVersion": "authorization.k8s.io/v1",
                    "kind": "SelfSubjectAccessReview",
                    "spec": {
                        "resourceAttributes": { "verb": "get", "resource": "configmaps" },
                        "nonResourceAttributes": { "verb": "get", "path": "/healthz" }
                    }
                }),
            ),
            (
                "client-supplied status",
                serde_json::json!({
                    "apiVersion": "authorization.k8s.io/v1",
                    "kind": "SelfSubjectAccessReview",
                    "spec": {
                        "resourceAttributes": { "verb": "get", "resource": "configmaps" }
                    },
                    "status": { "allowed": true }
                }),
            ),
        ];
        for (description, body) in invalid_requests {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/apis/authorization.k8s.io/v1/selfsubjectaccessreviews")
                        .header("authorization", "Bearer reviewer-secret")
                        .header("content-type", "application/json")
                        .body(Body::from(body.to_string()))
                        .unwrap_or_else(|error| {
                            panic!("valid {description} SSAR request: {error}")
                        }),
                )
                .await
                .expect("router is infallible");
            assert_eq!(
                response.status(),
                StatusCode::UNPROCESSABLE_ENTITY,
                "{description} is rejected"
            );
        }
    }

    #[tokio::test]
    async fn self_subject_access_review_always_allow_returns_allowed_for_current_identity() {
        let response = router(Arc::new(InMemoryConfigMapStore::new()))
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/apis/authorization.k8s.io/v1/selfsubjectaccessreviews")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "apiVersion": "authorization.k8s.io/v1",
                            "kind": "SelfSubjectAccessReview",
                            "spec": {
                                "resourceAttributes": {
                                    "verb": "delete",
                                    "resource": "secrets",
                                    "namespace": "default"
                                }
                            }
                        })
                        .to_string(),
                    ))
                    .expect("valid AlwaysAllow SSAR request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(response.status(), StatusCode::OK);
        let review: serde_json::Value = serde_json::from_slice(
            &to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("AlwaysAllow SSAR response body is readable"),
        )
        .expect("AlwaysAllow SSAR response is JSON");
        assert_eq!(review["status"]["allowed"], true);
        assert!(review["status"].get("denied").is_none());
    }

    #[test]
    fn self_subject_access_review_path_maps_to_authorization_group_create_resource() {
        let request = authorization_request_from_http(
            &ApiRegistry::core_v1(),
            &axum::http::Method::POST,
            &"/apis/authorization.k8s.io/v1/selfsubjectaccessreviews"
                .parse()
                .expect("valid SelfSubjectAccessReview URI"),
        );
        assert_eq!(request.verb, "create");
        assert!(matches!(
            request.target,
            rusternetes_authz_rbac::RequestTarget::Resource {
                api_group,
                resource,
                subresource,
                namespace,
                name,
            } if api_group == "authorization.k8s.io"
                && resource == "selfsubjectaccessreviews"
                && subresource.is_none()
                && namespace.is_none()
                && name.is_none()
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

#[cfg(test)]
mod pagination_route_tests {
    use std::{collections::BTreeMap, sync::Arc};

    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use rusternetes_api_types::{ConfigMap, ConfigMapList, ObjectMeta, TypeMeta};
    use rusternetes_common::StatusReason;
    use rusternetes_storage::InMemoryConfigMapStore;
    use tower::ServiceExt;

    use super::*;

    fn config_map(name: &str, namespace: &str, tier: &str) -> ConfigMap {
        ConfigMap {
            type_meta: TypeMeta::config_map(),
            metadata: ObjectMeta {
                name: Some(name.to_owned()),
                namespace: Some(namespace.to_owned()),
                labels: BTreeMap::from([("tier".to_owned(), tier.to_owned())]),
                ..ObjectMeta::default()
            },
            ..ConfigMap::default()
        }
    }

    async fn decode_list(response: axum::response::Response) -> ConfigMapList {
        serde_json::from_slice(
            &to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("list body is readable"),
        )
        .expect("list response is ConfigMapList JSON")
    }

    async fn decode_status(response: axum::response::Response) -> ApiStatus {
        serde_json::from_slice(
            &to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("status body is readable"),
        )
        .expect("error response is Kubernetes Status JSON")
    }

    #[tokio::test]
    async fn config_map_pagination_is_snapshot_consistent_and_token_bound_to_scope_and_selector() {
        let store = Arc::new(InMemoryConfigMapStore::new());
        for name in ["a", "b", "c"] {
            store
                .create(config_map(name, "default", "frontend"))
                .await
                .expect("fixture ConfigMap creates");
        }
        store
            .create(config_map("backend", "default", "backend"))
            .await
            .expect("non-matching fixture ConfigMap creates");
        let app = router(store.clone());

        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/namespaces/default/configmaps?limit=2&labelSelector=tier%3Dfrontend")
                    .body(Body::empty())
                    .expect("valid first page request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(first.status(), StatusCode::OK);
        let first = decode_list(first).await;
        assert_eq!(
            first
                .items
                .iter()
                .map(|item| item.metadata.name.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("a"), Some("b")]
        );
        assert_eq!(first.metadata.remaining_item_count, None);
        let initial_resource_version = first.metadata.resource_version.clone();
        let first_token = first
            .metadata
            .continue_token
            .clone()
            .expect("first page returns opaque continue token");

        let mismatched_scope = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/v1/namespaces/other/configmaps?continue={first_token}"
                    ))
                    .body(Body::empty())
                    .expect("valid scope-mismatch request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(mismatched_scope.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            decode_status(mismatched_scope).await.reason,
            StatusReason::BadRequest
        );

        let mismatched_selector = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/v1/namespaces/default/configmaps?continue={first_token}&labelSelector=tier%3Dbackend"
                    ))
                    .body(Body::empty())
                    .expect("valid selector-mismatch request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(mismatched_selector.status(), StatusCode::BAD_REQUEST);

        store
            .create(config_map("later", "default", "frontend"))
            .await
            .expect("post-snapshot ConfigMap creates");
        let final_page = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/v1/namespaces/default/configmaps?continue={first_token}&labelSelector=tier%3Dfrontend"
                    ))
                    .body(Body::empty())
                    .expect("valid continuation request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(final_page.status(), StatusCode::OK);
        let final_page = decode_list(final_page).await;
        assert_eq!(final_page.items.len(), 1);
        assert_eq!(final_page.items[0].metadata.name.as_deref(), Some("c"));
        assert_eq!(
            final_page.metadata.resource_version,
            initial_resource_version
        );
        assert!(final_page.metadata.continue_token.is_none());

        let malformed = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/namespaces/default/configmaps?continue=not+url-safe")
                    .body(Body::empty())
                    .expect("valid malformed-token request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);

        let incompatible_watch = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/namespaces/default/configmaps?watch=true&limit=1")
                    .body(Body::empty())
                    .expect("valid watch pagination request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(incompatible_watch.status(), StatusCode::BAD_REQUEST);
    }
}

#[cfg(test)]
mod service_account_patch_tests {
    use std::{collections::BTreeMap, sync::Arc};

    use axum::{
        body::{to_bytes, Body},
        http::{header, Request, StatusCode},
    };
    use rusternetes_api_types::{ApiStatus, ObjectMeta, ServiceAccount, TypeMeta};
    use rusternetes_common::StatusReason;
    use rusternetes_storage::InMemoryConfigMapStore;
    use tower::ServiceExt;

    use super::*;

    fn service_account(name: &str) -> ServiceAccount {
        ServiceAccount {
            type_meta: TypeMeta::service_account(),
            metadata: ObjectMeta {
                name: Some(name.to_owned()),
                namespace: Some("default".to_owned()),
                labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                ..ObjectMeta::default()
            },
            automount_service_account_token: Some(true),
            ..ServiceAccount::default()
        }
    }

    async fn decode_service_account(response: axum::response::Response) -> ServiceAccount {
        serde_json::from_slice(
            &to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("ServiceAccount response body is readable"),
        )
        .expect("ServiceAccount response is JSON")
    }

    async fn decode_status(response: axum::response::Response) -> ApiStatus {
        serde_json::from_slice(
            &to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("status response body is readable"),
        )
        .expect("status response is Kubernetes Status JSON")
    }

    #[tokio::test]
    async fn service_account_patch_supports_merge_and_json_patch_with_identity_and_cas_guards() {
        let app = router(Arc::new(InMemoryConfigMapStore::new()));
        let collection = "/api/v1/namespaces/default/serviceaccounts";
        let resource = service_account("build-robot");
        let created_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(collection)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::to_vec(&resource).expect("fixture ServiceAccount serializes"),
                    ))
                    .expect("valid create request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(created_response.status(), StatusCode::CREATED);
        let created = decode_service_account(created_response).await;
        let endpoint = format!("{collection}/build-robot");

        let merged_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(&endpoint)
                    .header(header::CONTENT_TYPE, "application/merge-patch+json")
                    .body(Body::from(r#"{"metadata":{"labels":{"team":"platform"}}}"#))
                    .expect("valid merge patch request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(merged_response.status(), StatusCode::OK);
        let merged = decode_service_account(merged_response).await;
        assert_eq!(merged.metadata.labels.get("app"), Some(&"api".to_owned()));
        assert_eq!(
            merged.metadata.labels.get("team"),
            Some(&"platform".to_owned())
        );
        assert_eq!(merged.metadata.uid, created.metadata.uid);
        assert_ne!(
            merged.metadata.resource_version,
            created.metadata.resource_version
        );

        let json_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(&endpoint)
                    .header(header::CONTENT_TYPE, "application/json-patch+json")
                    .body(Body::from(
                        r#"[{"op":"replace","path":"/automountServiceAccountToken","value":false}]"#,
                    ))
                    .expect("valid JSON patch request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(json_response.status(), StatusCode::OK);
        let patched = decode_service_account(json_response).await;
        assert_eq!(patched.automount_service_account_token, Some(false));
        assert_eq!(patched.metadata.labels, merged.metadata.labels);

        let stale_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(&endpoint)
                    .header(header::CONTENT_TYPE, "application/merge-patch+json")
                    .body(Body::from(
                        serde_json::json!({
                            "metadata": {"resourceVersion": created.metadata.resource_version}
                        })
                        .to_string(),
                    ))
                    .expect("valid stale patch request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(stale_response.status(), StatusCode::CONFLICT);

        let identity_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(&endpoint)
                    .header(header::CONTENT_TYPE, "application/merge-patch+json")
                    .body(Body::from(r#"{"metadata":{"namespace":"other"}}"#))
                    .expect("valid identity mutation request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(identity_response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            decode_status(identity_response).await.reason,
            StatusReason::Invalid
        );

        let media_type_response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(&endpoint)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .expect("valid unsupported patch request"),
            )
            .await
            .expect("router is infallible");
        assert_eq!(
            media_type_response.status(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE
        );
    }
}
