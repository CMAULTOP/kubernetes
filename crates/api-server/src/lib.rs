//! Axum HTTP API server for the first Rusternetes vertical slice.

use std::{io, sync::Arc};

use async_stream::stream;
use axum::{
    body::{Body, Bytes},
    extract::{Path, Query, Request, State},
    http::{header, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use rusternetes_api_registry::ApiRegistry;
use rusternetes_api_types::{ApiStatus, ConfigMap, DeleteOptions, FieldSelector, LabelSelector};
use rusternetes_authn::{AuthenticationChain, RequestIdentity};
use rusternetes_authz_rbac::{AuthorizationRequest, RbacAuthorizer};
use rusternetes_common::{ApiError, ResourceReference};
use rusternetes_storage::{
    ConfigMapWatchRequest, ConfigMapWatchSubscription as InMemoryConfigMapWatchSubscription,
    DeleteResult, InMemoryConfigMapStore,
};
use rusternetes_storage_etcd::{EtcdConfigMapRepository, EtcdConfigMapWatchSubscription};
use serde::{Deserialize, Serialize};

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

/// Shared immutable application state. The selected backend remains the sole owner of data.
#[derive(Clone)]
pub struct AppState {
    backend: ConfigMapBackend,
    registry: ApiRegistry,
    authorization: AuthorizationMode,
}

impl AppState {
    pub fn new(backend: ConfigMapBackend) -> Self {
        Self::with_registry_and_authorization(
            backend,
            ApiRegistry::core_v1(),
            AuthorizationMode::default(),
        )
    }

    pub fn with_registry_and_authorization(
        backend: ConfigMapBackend,
        registry: ApiRegistry,
        authorization: AuthorizationMode,
    ) -> Self {
        Self {
            backend,
            registry,
            authorization,
        }
    }
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
/// and normalized request attributes, then the resource handler may execute.
pub fn router_with_backend_auth_and_authorization(
    backend: ConfigMapBackend,
    authentication: AuthenticationChain,
    authorization: AuthorizationMode,
) -> Router {
    let state =
        AppState::with_registry_and_authorization(backend, ApiRegistry::core_v1(), authorization);
    Router::new()
        .route("/version", get(version))
        .route("/api", get(api_versions))
        .route("/api/v1", get(core_v1_api_resources))
        .route("/api/v1/configmaps", get(list_all_config_maps))
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
        .fallback(not_found)
        // Axum applies the last layer first. Authentication must populate extensions before RBAC
        // evaluates them, so authorization is added before authentication here.
        .layer(middleware::from_fn_with_state(
            state.clone(),
            authorize_request,
        ))
        .layer(middleware::from_fn_with_state(
            authentication,
            authenticate_request,
        ))
        .with_state(state)
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

async fn authenticate_request(
    State(authentication): State<AuthenticationChain>,
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
    match authentication.authenticate(authorization) {
        Ok(identity) => {
            request.extensions_mut().insert(identity);
            next.run(request).await
        }
        Err(error) => ApiRejection(error).into_response(),
    }
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
        None,
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
            .list(Some(&namespace), &label_selector, &field_selector)
            .await?,
    )
    .into_response())
}

async fn create_config_map(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
    body: Bytes,
) -> ApiResult<(StatusCode, Json<ConfigMap>)> {
    let resource = bind_namespace(decode_config_map(body)?, &namespace)?;
    let created = state.backend.create(resource).await?;
    Ok((StatusCode::CREATED, Json(created)))
}

async fn get_config_map(
    State(state): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
) -> ApiResult<Json<ConfigMap>> {
    Ok(Json(state.backend.get(&namespace, &name).await?))
}

async fn replace_config_map(
    State(state): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
    body: Bytes,
) -> ApiResult<Json<ConfigMap>> {
    let resource = bind_update_identity(decode_config_map(body)?, &namespace, &name)?;
    Ok(Json(state.backend.update(resource).await?))
}

async fn delete_config_map(
    State(state): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
    body: Bytes,
) -> ApiResult<Json<ApiStatus>> {
    let options = decode_delete_options(body)?;
    let result = state.backend.delete(&namespace, &name, options).await?;
    Ok(Json(ApiStatus::success(format!(
        "configmaps {name:?} deleted at resourceVersion {}",
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
    use rusternetes_authn::{
        AnonymousPolicy, AuthenticationChain, RequestIdentity, StaticBearerToken,
    };
    use rusternetes_common::StatusReason;
    use tower::ServiceExt;

    use super::*;

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
                authentication,
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
