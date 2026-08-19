//! Typed admission control primitives for Rusternetes API writes.
//!
//! Admission is intentionally independent from HTTP and storage transports. The API Server invokes
//! this chain only after authentication and authorization and before backend mutation. A later
//! webhook transport can map the same request/response contract to `AdmissionReview/v1`.

use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc};

use rusternetes_api_types::ConfigMap;
use rusternetes_authn::RequestIdentity;
use rusternetes_common::{ApiError, ResourceReference, StatusReason};
use uuid::Uuid;

/// Kubernetes write operations which participate in admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionOperation {
    Create,
    Update,
    Delete,
}

/// Typed group/version/resource attributes of the admitted object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmissionResource {
    pub group: String,
    pub version: String,
    pub resource: String,
}

impl AdmissionResource {
    pub fn config_maps() -> Self {
        Self {
            group: String::new(),
            version: "v1".to_owned(),
            resource: "configmaps".to_owned(),
        }
    }
}

/// Immutable ConfigMap-specific attributes passed to admission plugins.
///
/// This first API slice uses typed ConfigMap values instead of unstructured JSON. Additional
/// resource variants join this boundary as their API types are implemented.
#[derive(Clone, Debug)]
pub struct AdmissionRequest {
    pub uid: String,
    pub operation: AdmissionOperation,
    pub resource: AdmissionResource,
    pub namespace: String,
    pub name: String,
    pub object: Option<ConfigMap>,
    pub old_object: Option<ConfigMap>,
    pub dry_run: bool,
    pub user_info: RequestIdentity,
}

impl AdmissionRequest {
    pub fn create(user_info: RequestIdentity, object: ConfigMap) -> Result<Self, ApiError> {
        Self::write(AdmissionOperation::Create, user_info, object, None)
    }

    pub fn update(
        user_info: RequestIdentity,
        object: ConfigMap,
        old_object: ConfigMap,
    ) -> Result<Self, ApiError> {
        Self::write(
            AdmissionOperation::Update,
            user_info,
            object,
            Some(old_object),
        )
    }

    pub fn delete(user_info: RequestIdentity, old_object: ConfigMap) -> Result<Self, ApiError> {
        let namespace = old_object.namespace()?.to_owned();
        let name = old_object.name()?.to_owned();
        Ok(Self {
            uid: Uuid::new_v4().to_string(),
            operation: AdmissionOperation::Delete,
            resource: AdmissionResource::config_maps(),
            namespace,
            name,
            object: None,
            old_object: Some(old_object),
            dry_run: false,
            user_info,
        })
    }

    fn write(
        operation: AdmissionOperation,
        user_info: RequestIdentity,
        object: ConfigMap,
        old_object: Option<ConfigMap>,
    ) -> Result<Self, ApiError> {
        let namespace = object.namespace()?.to_owned();
        let name = object.name()?.to_owned();
        Ok(Self {
            uid: Uuid::new_v4().to_string(),
            operation,
            resource: AdmissionResource::config_maps(),
            namespace,
            name,
            object: Some(object),
            old_object,
            dry_run: false,
            user_info,
        })
    }
}

/// Kubernetes-style status represented in an admission response or future webhook review.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmissionStatus {
    pub code: u16,
    pub reason: StatusReason,
    pub message: String,
}

/// Typed decision contract for in-process policy reporting and future webhook replies.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmissionResponse {
    pub uid: String,
    pub allowed: bool,
    pub status: Option<AdmissionStatus>,
    pub warnings: Vec<String>,
}

impl AdmissionResponse {
    pub fn allow(uid: impl Into<String>) -> Self {
        Self {
            uid: uid.into(),
            allowed: true,
            status: None,
            warnings: Vec::new(),
        }
    }

    pub fn deny(uid: impl Into<String>, error: &ApiError) -> Self {
        Self {
            uid: uid.into(),
            allowed: false,
            status: Some(AdmissionStatus {
                code: error.status_code(),
                reason: error.reason(),
                message: error.to_string(),
            }),
            warnings: Vec::new(),
        }
    }
}

/// Async-compatible validating admission boundary.
pub trait AdmissionPlugin: Send + Sync {
    fn validate<'a>(
        &'a self,
        request: &'a AdmissionRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), ApiError>> + Send + 'a>>;
}

/// Ordered, fail-closed validating admission chain.
#[derive(Clone)]
pub struct AdmissionChain {
    plugins: Vec<Arc<dyn AdmissionPlugin>>,
}

impl Default for AdmissionChain {
    fn default() -> Self {
        Self::new(vec![Arc::new(AlwaysAllowPlugin)])
    }
}

impl AdmissionChain {
    pub fn new(plugins: Vec<Arc<dyn AdmissionPlugin>>) -> Self {
        Self { plugins }
    }

    /// Stops at the first rejection. A caller may explicitly construct an empty chain, while the
    /// default development chain runs [`AlwaysAllowPlugin`] through this same boundary.
    pub async fn validate(
        &self,
        request: &AdmissionRequest,
    ) -> Result<AdmissionResponse, ApiError> {
        for plugin in &self.plugins {
            plugin.validate(request).await?;
        }
        Ok(AdmissionResponse::allow(request.uid.clone()))
    }
}

/// Explicit development compatibility plugin.
#[derive(Clone, Debug, Default)]
pub struct AlwaysAllowPlugin;

impl AdmissionPlugin for AlwaysAllowPlugin {
    fn validate<'a>(
        &'a self,
        _request: &'a AdmissionRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), ApiError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

/// Lifecycle state visible to a namespace admission source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NamespacePhase {
    Active,
    Terminating,
    Missing,
}

/// Namespace state source consumed by the lifecycle admission plugin.
///
/// The future Namespaces storage slice implements this at the persistence boundary. Keeping this
/// lookup behind a trait prevents policy semantics from being coupled to ConfigMap handlers.
pub trait NamespaceStateReader: Send + Sync {
    fn phase<'a>(
        &'a self,
        namespace: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<NamespacePhase, ApiError>> + Send + 'a>>;
}

/// In-memory namespace state source for deterministic bootstrap and integration environments.
#[derive(Clone, Debug, Default)]
pub struct StaticNamespaceStateReader {
    phases: BTreeMap<String, NamespacePhase>,
}

impl StaticNamespaceStateReader {
    pub fn new(phases: impl IntoIterator<Item = (String, NamespacePhase)>) -> Self {
        Self {
            phases: phases.into_iter().collect(),
        }
    }
}

impl NamespaceStateReader for StaticNamespaceStateReader {
    fn phase<'a>(
        &'a self,
        namespace: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<NamespacePhase, ApiError>> + Send + 'a>> {
        Box::pin(async move {
            Ok(self
                .phases
                .get(namespace)
                .copied()
                .unwrap_or(NamespacePhase::Missing))
        })
    }
}

/// Kubernetes NamespaceLifecycle-compatible create admission boundary for typed ConfigMaps.
///
/// Kubernetes permits updates and deletes regardless of namespace lifecycle state, but refuses new
/// content in namespaces that do not exist or that are terminating. Cache warm-up and live storage
/// fallback belong to the future Namespaces repository implementation rather than this policy.
#[derive(Clone)]
pub struct NamespaceLifecyclePlugin {
    namespaces: Arc<dyn NamespaceStateReader>,
}

impl NamespaceLifecyclePlugin {
    pub fn new(namespaces: Arc<dyn NamespaceStateReader>) -> Self {
        Self { namespaces }
    }
}

impl AdmissionPlugin for NamespaceLifecyclePlugin {
    fn validate<'a>(
        &'a self,
        request: &'a AdmissionRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), ApiError>> + Send + 'a>> {
        Box::pin(async move {
            if request.operation != AdmissionOperation::Create {
                return Ok(());
            }
            match self.namespaces.phase(&request.namespace).await? {
                NamespacePhase::Active => Ok(()),
                NamespacePhase::Missing => Err(ApiError::NotFound {
                    resource: ResourceReference {
                        group: String::new(),
                        resource: "namespaces".to_owned(),
                        namespace: None,
                        name: Some(request.namespace.clone()),
                    },
                }),
                NamespacePhase::Terminating => Err(ApiError::Forbidden {
                    message: format!(
                        "unable to create new content in namespace {} because it is being terminated",
                        request.namespace
                    ),
                }),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };

    use rusternetes_api_types::{ConfigMap, ObjectMeta, TypeMeta};

    use super::*;

    fn identity() -> RequestIdentity {
        RequestIdentity::authenticated(
            "admission-test",
            None,
            std::iter::empty(),
            Default::default(),
        )
        .expect("identity is valid")
    }

    fn config_map() -> ConfigMap {
        ConfigMap {
            type_meta: TypeMeta::config_map(),
            metadata: ObjectMeta {
                name: Some("settings".to_owned()),
                namespace: Some("development".to_owned()),
                labels: BTreeMap::new(),
                ..ObjectMeta::default()
            },
            ..ConfigMap::default()
        }
    }

    struct RecordingPlugin {
        name: &'static str,
        calls: Arc<Mutex<Vec<&'static str>>>,
        reject: bool,
    }

    impl AdmissionPlugin for RecordingPlugin {
        fn validate<'a>(
            &'a self,
            _request: &'a AdmissionRequest,
        ) -> Pin<Box<dyn Future<Output = Result<(), ApiError>> + Send + 'a>> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .expect("test call log lock")
                    .push(self.name);
                if self.reject {
                    return Err(ApiError::Forbidden {
                        message: self.name.to_owned(),
                    });
                }
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn chain_preserves_order_and_stops_at_first_rejection() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let chain = AdmissionChain::new(vec![
            Arc::new(RecordingPlugin {
                name: "first",
                calls: calls.clone(),
                reject: false,
            }),
            Arc::new(RecordingPlugin {
                name: "second",
                calls: calls.clone(),
                reject: true,
            }),
            Arc::new(RecordingPlugin {
                name: "never",
                calls: calls.clone(),
                reject: false,
            }),
        ]);
        let request = AdmissionRequest::create(identity(), config_map()).expect("request is valid");
        assert!(matches!(
            chain.validate(&request).await,
            Err(ApiError::Forbidden { .. })
        ));
        assert_eq!(
            *calls.lock().expect("test call log lock"),
            vec!["first", "second"]
        );
    }

    #[tokio::test]
    async fn default_chain_returns_uid_correlated_allow_response() {
        let request = AdmissionRequest::create(identity(), config_map()).expect("request is valid");
        let response = AdmissionChain::default()
            .validate(&request)
            .await
            .expect("default development chain allows request");
        assert!(response.allowed);
        assert_eq!(response.uid, request.uid);
        assert!(response.status.is_none());
    }

    #[tokio::test]
    async fn namespace_lifecycle_rejects_missing_and_terminating_namespaces_only_on_create() {
        let namespaces = StaticNamespaceStateReader::new([
            ("development".to_owned(), NamespacePhase::Active),
            ("retiring".to_owned(), NamespacePhase::Terminating),
        ]);
        let plugin = NamespaceLifecyclePlugin::new(Arc::new(namespaces));

        let active = AdmissionRequest::create(identity(), config_map()).expect("request is valid");
        assert!(plugin.validate(&active).await.is_ok());

        let mut missing_map = config_map();
        missing_map.metadata.namespace = Some("missing".to_owned());
        let missing = AdmissionRequest::create(identity(), missing_map).expect("request is valid");
        assert!(matches!(
            plugin.validate(&missing).await,
            Err(ApiError::NotFound { .. })
        ));

        let mut terminating_map = config_map();
        terminating_map.metadata.namespace = Some("retiring".to_owned());
        let terminating =
            AdmissionRequest::create(identity(), terminating_map).expect("request is valid");
        assert!(matches!(
            plugin.validate(&terminating).await,
            Err(ApiError::Forbidden { .. })
        ));

        let update = AdmissionRequest::update(identity(), config_map(), config_map())
            .expect("request is valid");
        assert!(plugin.validate(&update).await.is_ok());
    }

    #[test]
    fn admission_response_preserves_uid_and_typed_error_status() {
        let error = ApiError::Forbidden {
            message: "policy denied the request".to_owned(),
        };
        let response = AdmissionResponse::deny("admission-uid", &error);
        assert!(!response.allowed);
        assert_eq!(response.uid, "admission-uid");
        assert_eq!(response.status.expect("denial carries status").code, 403);
    }
}
