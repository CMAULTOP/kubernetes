//! Axum HTTP API server for the first Rusternetes vertical slice.

use std::{io, sync::Arc};

use async_stream::stream;
use axum::{
    body::{Body, Bytes},
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use rusternetes_api_types::{
    core_api_versions, core_v1_resources, ApiStatus, ConfigMap, DeleteOptions, FieldSelector,
    LabelSelector,
};
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
}

impl AppState {
    pub fn new(backend: ConfigMapBackend) -> Self {
        Self { backend }
    }
}

/// Builds an API Server router using the current in-memory development backend.
pub fn router(store: Arc<InMemoryConfigMapStore>) -> Router {
    router_with_backend(ConfigMapBackend::InMemory(store))
}

/// Builds the API Server router with an explicitly selected ConfigMap persistence backend.
pub fn router_with_backend(backend: ConfigMapBackend) -> Router {
    let state = AppState::new(backend);
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

async fn api_versions() -> Json<rusternetes_api_types::ApiVersions> {
    Json(core_api_versions())
}

async fn core_v1_api_resources() -> Json<rusternetes_api_types::ApiResourceList> {
    Json(core_v1_resources())
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
        http::Request,
    };
    use tower::ServiceExt;

    use super::*;

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
