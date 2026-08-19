//! Typed Kubernetes API objects and validation for the first Rusternetes slice.

use std::collections::{BTreeMap, BTreeSet};

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use rusternetes_common::{ApiError, StatusReason};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

pub const CORE_API_VERSION: &str = "v1";
pub const CONFIG_MAP_KIND: &str = "ConfigMap";
pub const POD_KIND: &str = "Pod";
pub const NAMESPACE_KIND: &str = "Namespace";
pub const MAX_CONFIG_MAP_DATA_BYTES: usize = 1024 * 1024;

/// Kubernetes type metadata.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct TypeMeta {
    #[serde(
        rename = "apiVersion",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub api_version: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kind: String,
}

impl TypeMeta {
    pub fn config_map() -> Self {
        Self {
            api_version: CORE_API_VERSION.to_owned(),
            kind: CONFIG_MAP_KIND.to_owned(),
        }
    }

    pub fn config_map_list() -> Self {
        Self {
            api_version: CORE_API_VERSION.to_owned(),
            kind: "ConfigMapList".to_owned(),
        }
    }

    pub fn pod() -> Self {
        Self {
            api_version: CORE_API_VERSION.to_owned(),
            kind: POD_KIND.to_owned(),
        }
    }

    pub fn pod_list() -> Self {
        Self {
            api_version: CORE_API_VERSION.to_owned(),
            kind: "PodList".to_owned(),
        }
    }

    pub fn namespace() -> Self {
        Self {
            api_version: CORE_API_VERSION.to_owned(),
            kind: NAMESPACE_KIND.to_owned(),
        }
    }

    pub fn namespace_list() -> Self {
        Self {
            api_version: CORE_API_VERSION.to_owned(),
            kind: "NamespaceList".to_owned(),
        }
    }

    pub fn status() -> Self {
        Self {
            api_version: CORE_API_VERSION.to_owned(),
            kind: "Status".to_owned(),
        }
    }
}

/// Standard Kubernetes object metadata retained by the typed resources in this slice.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectMeta {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "time::serde::rfc3339::option"
    )]
    pub creation_timestamp: Option<OffsetDateTime>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub owner_references: Vec<OwnerReference>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub finalizers: Vec<String>,
}

/// A typed owner reference. It is retained round-trip in phase 1; ownership processing is deferred.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OwnerReference {
    pub api_version: String,
    pub kind: String,
    pub name: String,
    pub uid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub controller: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_owner_deletion: Option<bool>,
}

/// A strongly typed core/v1 ConfigMap.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConfigMap {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub immutable: Option<bool>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub data: BTreeMap<String, String>,
    /// Values use Kubernetes JSON's base64 encoding for `[]byte`.
    #[serde(
        rename = "binaryData",
        default,
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub binary_data: BTreeMap<String, String>,
}

impl ConfigMap {
    pub fn name(&self) -> Result<&str, ApiError> {
        self.metadata
            .name
            .as_deref()
            .ok_or_else(|| ApiError::Invalid {
                message: "metadata.name is required".to_owned(),
            })
    }

    pub fn namespace(&self) -> Result<&str, ApiError> {
        self.metadata
            .namespace
            .as_deref()
            .ok_or_else(|| ApiError::Invalid {
                message: "metadata.namespace is required".to_owned(),
            })
    }

    pub fn enforce_type_meta(&mut self) -> Result<(), ApiError> {
        if !self.type_meta.api_version.is_empty() && self.type_meta.api_version != CORE_API_VERSION
        {
            return Err(ApiError::Invalid {
                message: format!(
                    "apiVersion must be {CORE_API_VERSION}, got {}",
                    self.type_meta.api_version
                ),
            });
        }
        if !self.type_meta.kind.is_empty() && self.type_meta.kind != CONFIG_MAP_KIND {
            return Err(ApiError::Invalid {
                message: format!(
                    "kind must be {CONFIG_MAP_KIND}, got {}",
                    self.type_meta.kind
                ),
            });
        }
        self.type_meta = TypeMeta::config_map();
        Ok(())
    }

    /// Validates the ConfigMap fields covered by the first vertical slice.
    pub fn validate(&self) -> Result<(), ApiError> {
        validate_dns_subdomain("metadata.name", self.name()?, 253)?;
        validate_dns_label("metadata.namespace", self.namespace()?)?;

        for key in self.data.keys() {
            validate_config_map_key("data", key)?;
            if self.binary_data.contains_key(key) {
                return Err(ApiError::Invalid {
                    message: format!("data[{key:?}] duplicates a key present in binaryData"),
                });
            }
        }

        let mut total_size = self.data.values().map(String::len).sum::<usize>();
        for (key, encoded_value) in &self.binary_data {
            validate_config_map_key("binaryData", key)?;
            let bytes = BASE64
                .decode(encoded_value)
                .map_err(|_| ApiError::Invalid {
                    message: format!("binaryData[{key:?}] must be valid base64"),
                })?;
            total_size = total_size.saturating_add(bytes.len());
        }
        if total_size > MAX_CONFIG_MAP_DATA_BYTES {
            return Err(ApiError::Invalid {
                message: format!("combined data and binaryData must not exceed {MAX_CONFIG_MAP_DATA_BYTES} bytes"),
            });
        }

        for (key, value) in &self.metadata.labels {
            validate_label_key(key)?;
            validate_label_value(value)?;
        }
        Ok(())
    }

    /// Applies ConfigMap update invariants after the storage layer has located the old object.
    pub fn validate_update(&self, previous: &Self) -> Result<(), ApiError> {
        self.validate()?;
        if previous.immutable == Some(true) {
            if self.immutable != Some(true) {
                return Err(ApiError::Invalid {
                    message: "immutable is immutable when it is set to true".to_owned(),
                });
            }
            if self.data != previous.data {
                return Err(ApiError::Invalid {
                    message: "data is immutable when immutable is set to true".to_owned(),
                });
            }
            if self.binary_data != previous.binary_data {
                return Err(ApiError::Invalid {
                    message: "binaryData is immutable when immutable is set to true".to_owned(),
                });
            }
        }
        Ok(())
    }

    pub fn preserve_server_metadata_from(&mut self, previous: &Self, resource_version: String) {
        self.metadata.uid = previous.metadata.uid.clone();
        self.metadata.creation_timestamp = previous.metadata.creation_timestamp;
        self.metadata.generation = previous.metadata.generation;
        self.metadata.resource_version = Some(resource_version);
    }

    pub fn set_create_metadata(
        &mut self,
        uid: String,
        now: OffsetDateTime,
        resource_version: String,
    ) {
        self.metadata.uid = Some(uid);
        self.metadata.creation_timestamp = Some(now);
        self.metadata.generation = Some(1);
        self.metadata.resource_version = Some(resource_version);
    }
}

/// A typed core/v1 container subset suitable for the pre-runtime Pod API slice.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Container {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,
}

impl Container {
    fn validate(&self) -> Result<(), ApiError> {
        validate_dns_label("spec.containers[].name", &self.name)?;
        if self.image.as_deref().is_some_and(str::is_empty) {
            return Err(ApiError::Invalid {
                message: format!(
                    "container {:?} image must not be empty when specified",
                    self.name
                ),
            });
        }
        Ok(())
    }
}

/// Kubernetes Pod restart policy values supported by the core/v1 wire schema.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum RestartPolicy {
    #[default]
    Always,
    OnFailure,
    Never,
}

/// Desired state fields retained by the initial typed Pod API slice.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PodSpec {
    pub containers: Vec<Container>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_policy: Option<RestartPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduler_name: Option<String>,
}

/// High-level Kubernetes Pod lifecycle phases.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum PodPhase {
    Pending,
    Running,
    Succeeded,
    Failed,
    Unknown,
}

/// Server-populated observed state of a Pod in the initial API slice.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct PodStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<PodPhase>,
}

impl PodStatus {
    fn is_empty(&self) -> bool {
        self.phase.is_none()
    }
}

/// Strongly typed core/v1 Pod. Runtime execution and status mutation are deferred to later slices.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Pod {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default)]
    pub spec: PodSpec,
    #[serde(default, skip_serializing_if = "PodStatus::is_empty")]
    pub status: PodStatus,
}

impl Pod {
    pub fn name(&self) -> Result<&str, ApiError> {
        self.metadata
            .name
            .as_deref()
            .ok_or_else(|| ApiError::Invalid {
                message: "metadata.name is required".to_owned(),
            })
    }

    pub fn namespace(&self) -> Result<&str, ApiError> {
        self.metadata
            .namespace
            .as_deref()
            .ok_or_else(|| ApiError::Invalid {
                message: "metadata.namespace is required".to_owned(),
            })
    }

    pub fn enforce_type_meta(&mut self) -> Result<(), ApiError> {
        enforce_type_meta(&mut self.type_meta, POD_KIND)
    }

    pub fn validate(&self) -> Result<(), ApiError> {
        validate_dns_subdomain("metadata.name", self.name()?, 253)?;
        validate_dns_label("metadata.namespace", self.namespace()?)?;
        validate_object_labels(&self.metadata.labels)?;
        if self.spec.containers.is_empty() {
            return Err(ApiError::Invalid {
                message: "spec.containers must contain at least one container".to_owned(),
            });
        }
        let mut container_names = BTreeSet::new();
        for container in &self.spec.containers {
            container.validate()?;
            if !container_names.insert(&container.name) {
                return Err(ApiError::Invalid {
                    message: format!(
                        "spec.containers contains duplicate name {:?}",
                        container.name
                    ),
                });
            }
        }
        if let Some(node_name) = &self.spec.node_name {
            validate_dns_subdomain("spec.nodeName", node_name, 253)?;
        }
        if let Some(scheduler_name) = &self.spec.scheduler_name {
            validate_dns_subdomain("spec.schedulerName", scheduler_name, 63)?;
        }
        Ok(())
    }

    pub fn validate_create(&self) -> Result<(), ApiError> {
        self.validate()?;
        if !self.status.is_empty() {
            return Err(ApiError::Invalid {
                message: "status is server-owned and must not be set during create".to_owned(),
            });
        }
        Ok(())
    }

    /// Applies the immutable fields supported before scheduler and runtime slices exist.
    pub fn validate_update(&self, previous: &Self) -> Result<(), ApiError> {
        self.validate()?;
        if self.spec != previous.spec {
            return Err(ApiError::Invalid {
                message:
                    "Pod spec updates are not supported before the scheduler and runtime slices"
                        .to_owned(),
            });
        }
        if !self.status.is_empty() && self.status != previous.status {
            return Err(ApiError::Invalid {
                message: "status is server-owned; use the future /status subresource".to_owned(),
            });
        }
        Ok(())
    }

    /// Validates and applies one scheduler-owned assignment without exposing general Pod spec mutation.
    pub fn bind_to_node(&mut self, previous: &Self, node_name: &str) -> Result<(), ApiError> {
        validate_dns_subdomain("binding.target.name", node_name, 253)?;
        if previous.spec.node_name.is_some() {
            return Err(ApiError::Conflict {
                resource: rusternetes_common::ResourceReference::pod(
                    previous.namespace()?.to_owned(),
                    previous.name()?.to_owned(),
                ),
            });
        }
        if self.spec != previous.spec
            || self.status != previous.status
            || self.metadata != previous.metadata
        {
            return Err(ApiError::Invalid {
                message: "scheduler binding must start from the current immutable Pod snapshot"
                    .to_owned(),
            });
        }
        self.spec.node_name = Some(node_name.to_owned());
        Ok(())
    }

    pub fn set_create_metadata(
        &mut self,
        uid: String,
        now: OffsetDateTime,
        resource_version: String,
    ) {
        self.metadata.uid = Some(uid);
        self.metadata.creation_timestamp = Some(now);
        self.metadata.generation = Some(1);
        self.metadata.resource_version = Some(resource_version);
        self.spec
            .restart_policy
            .get_or_insert(RestartPolicy::Always);
        self.status = PodStatus {
            phase: Some(PodPhase::Pending),
        };
    }

    pub fn preserve_server_metadata_from(&mut self, previous: &Self, resource_version: String) {
        preserve_server_metadata(&mut self.metadata, &previous.metadata, resource_version);
        self.status = previous.status.clone();
    }
}

/// Namespace desired state retained before finalizer workflow is implemented.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct NamespaceSpec {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub finalizers: Vec<String>,
}

/// Kubernetes Namespace lifecycle phases.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum NamespacePhase {
    Active,
    Terminating,
}

/// Server-populated Namespace lifecycle state.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct NamespaceStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<NamespacePhase>,
}

impl NamespaceStatus {
    fn is_empty(&self) -> bool {
        self.phase.is_none()
    }
}

/// Strongly typed cluster-scoped core/v1 Namespace.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Namespace {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default)]
    pub spec: NamespaceSpec,
    #[serde(default, skip_serializing_if = "NamespaceStatus::is_empty")]
    pub status: NamespaceStatus,
}

impl Namespace {
    pub const NAME_LABEL: &'static str = "kubernetes.io/metadata.name";

    pub fn name(&self) -> Result<&str, ApiError> {
        self.metadata
            .name
            .as_deref()
            .ok_or_else(|| ApiError::Invalid {
                message: "metadata.name is required".to_owned(),
            })
    }

    pub fn enforce_type_meta(&mut self) -> Result<(), ApiError> {
        enforce_type_meta(&mut self.type_meta, NAMESPACE_KIND)
    }

    pub fn validate(&self) -> Result<(), ApiError> {
        validate_dns_label("metadata.name", self.name()?)?;
        if self.metadata.namespace.is_some() {
            return Err(ApiError::Invalid {
                message: "Namespace is cluster-scoped and must not set metadata.namespace"
                    .to_owned(),
            });
        }
        validate_object_labels(&self.metadata.labels)?;
        if let Some(label) = self.metadata.labels.get(Self::NAME_LABEL) {
            if label != self.name()? {
                return Err(ApiError::Invalid {
                    message: format!("{} must equal metadata.name", Self::NAME_LABEL),
                });
            }
        }
        Ok(())
    }

    pub fn validate_create(&self) -> Result<(), ApiError> {
        self.validate()?;
        if !self.status.is_empty() {
            return Err(ApiError::Invalid {
                message: "status is server-owned and must not be set during create".to_owned(),
            });
        }
        Ok(())
    }

    pub fn validate_update(&self, previous: &Self) -> Result<(), ApiError> {
        self.validate()?;
        if self.spec != previous.spec {
            return Err(ApiError::Invalid {
                message: "Namespace spec updates require the future /finalize subresource"
                    .to_owned(),
            });
        }
        if !self.status.is_empty() && self.status != previous.status {
            return Err(ApiError::Invalid {
                message: "status is server-owned; use the future /status subresource".to_owned(),
            });
        }
        Ok(())
    }

    pub fn set_create_metadata(
        &mut self,
        uid: String,
        now: OffsetDateTime,
        resource_version: String,
    ) {
        let name = self.metadata.name.clone().unwrap_or_default();
        self.metadata
            .labels
            .insert(Self::NAME_LABEL.to_owned(), name);
        self.metadata.uid = Some(uid);
        self.metadata.creation_timestamp = Some(now);
        self.metadata.generation = Some(1);
        self.metadata.resource_version = Some(resource_version);
        self.status = NamespaceStatus {
            phase: Some(NamespacePhase::Active),
        };
    }

    pub fn preserve_server_metadata_from(&mut self, previous: &Self, resource_version: String) {
        preserve_server_metadata(&mut self.metadata, &previous.metadata, resource_version);
        self.metadata.labels.insert(
            Self::NAME_LABEL.to_owned(),
            previous.name().unwrap_or_default().to_owned(),
        );
        self.status = previous.status.clone();
    }
}

/// A typed Pod list response.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PodList {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    #[serde(default)]
    pub metadata: ListMeta,
    pub items: Vec<Pod>,
}

impl PodList {
    pub fn new(resource_version: String, items: Vec<Pod>) -> Self {
        Self {
            type_meta: TypeMeta::pod_list(),
            metadata: ListMeta {
                resource_version: Some(resource_version),
            },
            items,
        }
    }
}

/// A typed Namespace list response.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NamespaceList {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    #[serde(default)]
    pub metadata: ListMeta,
    pub items: Vec<Namespace>,
}

impl NamespaceList {
    pub fn new(resource_version: String, items: Vec<Namespace>) -> Self {
        Self {
            type_meta: TypeMeta::namespace_list(),
            metadata: ListMeta {
                resource_version: Some(resource_version),
            },
            items,
        }
    }
}

/// Kubernetes ListMeta for an API list response.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListMeta {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,
}

/// A typed ConfigMap list response.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConfigMapList {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    #[serde(default)]
    pub metadata: ListMeta,
    pub items: Vec<ConfigMap>,
}

impl ConfigMapList {
    pub fn new(resource_version: String, items: Vec<ConfigMap>) -> Self {
        Self {
            type_meta: TypeMeta::config_map_list(),
            metadata: ListMeta {
                resource_version: Some(resource_version),
            },
            items,
        }
    }
}

/// Kubernetes delete preconditions carried by `DeleteOptions`.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Preconditions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,
}

/// The subset of metav1.DeleteOptions needed for conditional deletion.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteOptions {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preconditions: Option<Preconditions>,
}

/// Kubernetes's Status response representation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiStatus {
    #[serde(flatten)]
    pub type_meta: TypeMeta,
    pub status: String,
    pub message: String,
    pub reason: StatusReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<StatusDetails>,
    pub code: u16,
}

/// Identifies the failed resource in an API Status response.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatusDetails {
    pub group: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl ApiStatus {
    pub fn success(message: impl Into<String>) -> Self {
        Self {
            type_meta: TypeMeta::status(),
            status: "Success".to_owned(),
            message: message.into(),
            reason: StatusReason::Success,
            details: None,
            code: 200,
        }
    }
}

impl From<&ApiError> for ApiStatus {
    fn from(error: &ApiError) -> Self {
        let details = error.details().map(|resource| StatusDetails {
            group: resource.group.clone(),
            kind: resource.resource.clone(),
            name: resource.name.clone(),
        });
        Self {
            type_meta: TypeMeta::status(),
            status: "Failure".to_owned(),
            message: error.to_string(),
            reason: error.reason(),
            details,
            code: error.status_code(),
        }
    }
}

/// Kubernetes WATCH event kinds emitted by the ConfigMap resource endpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum WatchEventType {
    #[serde(rename = "ADDED")]
    Added,
    #[serde(rename = "MODIFIED")]
    Modified,
    #[serde(rename = "DELETED")]
    Deleted,
    #[serde(rename = "BOOKMARK")]
    Bookmark,
    #[serde(rename = "ERROR")]
    Error,
}

/// The object carried by a typed ConfigMap watch event.
#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum ConfigMapWatchObject {
    ConfigMap(ConfigMap),
    Status(ApiStatus),
}

/// Kubernetes JSON WATCH event envelope for ConfigMap.
#[derive(Clone, Debug, Serialize)]
pub struct ConfigMapWatchEvent {
    #[serde(rename = "type")]
    pub event_type: WatchEventType,
    pub object: ConfigMapWatchObject,
}

impl ConfigMapWatchEvent {
    pub fn added(resource: ConfigMap) -> Self {
        Self {
            event_type: WatchEventType::Added,
            object: ConfigMapWatchObject::ConfigMap(resource),
        }
    }

    pub fn modified(resource: ConfigMap) -> Self {
        Self {
            event_type: WatchEventType::Modified,
            object: ConfigMapWatchObject::ConfigMap(resource),
        }
    }

    pub fn deleted(resource: ConfigMap) -> Self {
        Self {
            event_type: WatchEventType::Deleted,
            object: ConfigMapWatchObject::ConfigMap(resource),
        }
    }

    pub fn bookmark(resource_version: String) -> Self {
        Self {
            event_type: WatchEventType::Bookmark,
            object: ConfigMapWatchObject::ConfigMap(ConfigMap {
                type_meta: TypeMeta::config_map(),
                metadata: ObjectMeta {
                    resource_version: Some(resource_version),
                    ..ObjectMeta::default()
                },
                ..ConfigMap::default()
            }),
        }
    }

    pub fn error(status: ApiStatus) -> Self {
        Self {
            event_type: WatchEventType::Error,
            object: ConfigMapWatchObject::Status(status),
        }
    }
}

/// The object carried by a typed Pod watch event.
#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum PodWatchObject {
    Pod(Pod),
    Status(ApiStatus),
}

/// Kubernetes JSON WATCH event envelope for Pod.
#[derive(Clone, Debug, Serialize)]
pub struct PodWatchEvent {
    #[serde(rename = "type")]
    pub event_type: WatchEventType,
    pub object: PodWatchObject,
}

impl PodWatchEvent {
    pub fn added(resource: Pod) -> Self {
        Self {
            event_type: WatchEventType::Added,
            object: PodWatchObject::Pod(resource),
        }
    }

    pub fn modified(resource: Pod) -> Self {
        Self {
            event_type: WatchEventType::Modified,
            object: PodWatchObject::Pod(resource),
        }
    }

    pub fn deleted(resource: Pod) -> Self {
        Self {
            event_type: WatchEventType::Deleted,
            object: PodWatchObject::Pod(resource),
        }
    }

    pub fn bookmark(resource_version: String) -> Self {
        Self {
            event_type: WatchEventType::Bookmark,
            object: PodWatchObject::Pod(Pod {
                type_meta: TypeMeta::pod(),
                metadata: ObjectMeta {
                    resource_version: Some(resource_version),
                    ..ObjectMeta::default()
                },
                ..Pod::default()
            }),
        }
    }
}

/// The object carried by a typed Namespace watch event.
#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum NamespaceWatchObject {
    Namespace(Namespace),
    Status(ApiStatus),
}

/// Kubernetes JSON WATCH event envelope for Namespace.
#[derive(Clone, Debug, Serialize)]
pub struct NamespaceWatchEvent {
    #[serde(rename = "type")]
    pub event_type: WatchEventType,
    pub object: NamespaceWatchObject,
}

impl NamespaceWatchEvent {
    pub fn added(resource: Namespace) -> Self {
        Self {
            event_type: WatchEventType::Added,
            object: NamespaceWatchObject::Namespace(resource),
        }
    }

    pub fn modified(resource: Namespace) -> Self {
        Self {
            event_type: WatchEventType::Modified,
            object: NamespaceWatchObject::Namespace(resource),
        }
    }

    pub fn deleted(resource: Namespace) -> Self {
        Self {
            event_type: WatchEventType::Deleted,
            object: NamespaceWatchObject::Namespace(resource),
        }
    }

    pub fn bookmark(resource_version: String) -> Self {
        Self {
            event_type: WatchEventType::Bookmark,
            object: NamespaceWatchObject::Namespace(Namespace {
                type_meta: TypeMeta::namespace(),
                metadata: ObjectMeta {
                    resource_version: Some(resource_version),
                    ..ObjectMeta::default()
                },
                ..Namespace::default()
            }),
        }
    }
}

/// Equality, set-membership, and existence label selector requirements.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LabelRequirement {
    Equals {
        key: String,
        value: String,
    },
    NotEquals {
        key: String,
        value: String,
    },
    In {
        key: String,
        values: BTreeSet<String>,
    },
    NotIn {
        key: String,
        values: BTreeSet<String>,
    },
    Exists {
        key: String,
    },
    DoesNotExist {
        key: String,
    },
}

/// Parsed Kubernetes-style label selector used by ConfigMap list.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LabelSelector {
    requirements: Vec<LabelRequirement>,
}

impl LabelSelector {
    pub fn parse(raw: Option<&str>) -> Result<Self, ApiError> {
        let Some(raw) = raw.filter(|value| !value.trim().is_empty()) else {
            return Ok(Self::default());
        };

        let mut requirements = Vec::new();
        for term in split_selector_terms(raw)? {
            requirements.push(parse_selector_term(term.trim())?);
        }
        Ok(Self { requirements })
    }

    pub fn matches(&self, labels: &BTreeMap<String, String>) -> bool {
        self.requirements
            .iter()
            .all(|requirement| match requirement {
                LabelRequirement::Equals { key, value } => labels.get(key) == Some(value),
                LabelRequirement::NotEquals { key, value } => match labels.get(key) {
                    Some(actual) => actual != value,
                    None => true,
                },
                LabelRequirement::In { key, values } => match labels.get(key) {
                    Some(actual) => values.contains(actual),
                    None => false,
                },
                LabelRequirement::NotIn { key, values } => match labels.get(key) {
                    Some(actual) => !values.contains(actual),
                    None => true,
                },
                LabelRequirement::Exists { key } => labels.contains_key(key),
                LabelRequirement::DoesNotExist { key } => !labels.contains_key(key),
            })
    }
}

/// Equality and inequality field selector requirements supported by ConfigMap list.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FieldRequirement {
    Equals { field: String, value: String },
    NotEquals { field: String, value: String },
}

/// Parsed field selector for the fields indexed by the ConfigMap registry.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FieldSelector {
    requirements: Vec<FieldRequirement>,
}

impl FieldSelector {
    pub fn parse(raw: Option<&str>) -> Result<Self, ApiError> {
        let Some(raw) = raw.filter(|value| !value.trim().is_empty()) else {
            return Ok(Self::default());
        };

        let mut requirements = Vec::new();
        for term in raw.split(',').map(str::trim) {
            if term.is_empty() {
                return Err(invalid_field_selector(raw));
            }
            let (field, value, is_inequality) = if let Some((field, value)) = term.split_once("!=")
            {
                (field, value, true)
            } else if let Some((field, value)) = term.split_once("==") {
                (field, value, false)
            } else if let Some((field, value)) = term.split_once('=') {
                (field, value, false)
            } else {
                return Err(invalid_field_selector(raw));
            };
            let field = field.trim();
            let value = value.trim();
            if !matches!(field, "metadata.name" | "metadata.namespace") {
                return Err(ApiError::Invalid {
                    message: format!(
                        "fieldSelector field {field:?} is not supported for ConfigMap"
                    ),
                });
            }
            let requirement = if is_inequality {
                FieldRequirement::NotEquals {
                    field: field.to_owned(),
                    value: value.to_owned(),
                }
            } else {
                FieldRequirement::Equals {
                    field: field.to_owned(),
                    value: value.to_owned(),
                }
            };
            requirements.push(requirement);
        }
        Ok(Self { requirements })
    }

    pub fn matches(&self, resource: &ConfigMap) -> bool {
        self.matches_metadata(&resource.metadata)
    }

    pub fn matches_pod(&self, resource: &Pod) -> bool {
        self.matches_metadata(&resource.metadata)
    }

    pub fn matches_namespace(&self, resource: &Namespace) -> bool {
        self.matches_metadata(&resource.metadata)
    }

    fn matches_metadata(&self, metadata: &ObjectMeta) -> bool {
        self.requirements.iter().all(|requirement| {
            let (field, expected, is_inequality) = match requirement {
                FieldRequirement::Equals { field, value } => (field, value, false),
                FieldRequirement::NotEquals { field, value } => (field, value, true),
            };
            let actual = match field.as_str() {
                "metadata.name" => metadata.name.as_deref(),
                "metadata.namespace" => metadata.namespace.as_deref(),
                _ => None,
            };
            if is_inequality {
                actual != Some(expected.as_str())
            } else {
                actual == Some(expected.as_str())
            }
        })
    }
}

/// Core API versions discovery response.
#[derive(Clone, Debug, Serialize)]
pub struct ApiVersions {
    pub kind: &'static str,
    #[serde(rename = "apiVersion")]
    pub api_version: &'static str,
    pub versions: Vec<&'static str>,
    #[serde(rename = "serverAddressByClientCIDRs")]
    pub server_address_by_client_cidrs: Vec<ServerAddressByClientCidr>,
}

/// Discovery address entry retained for Kubernetes API response shape compatibility.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerAddressByClientCidr {
    pub client_cidr: String,
    pub server_address: String,
}

/// A discoverable core/v1 resource.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiResource {
    pub name: &'static str,
    pub singular_name: &'static str,
    pub namespaced: bool,
    pub kind: &'static str,
    pub verbs: Vec<&'static str>,
}

/// Core/v1 resource discovery response.
#[derive(Clone, Debug, Serialize)]
pub struct ApiResourceList {
    pub kind: &'static str,
    #[serde(rename = "apiVersion")]
    pub api_version: &'static str,
    #[serde(rename = "groupVersion")]
    pub group_version: &'static str,
    pub resources: Vec<ApiResource>,
}

fn enforce_type_meta(type_meta: &mut TypeMeta, expected_kind: &str) -> Result<(), ApiError> {
    if !type_meta.api_version.is_empty() && type_meta.api_version != CORE_API_VERSION {
        return Err(ApiError::Invalid {
            message: format!(
                "apiVersion must be {CORE_API_VERSION}, got {}",
                type_meta.api_version
            ),
        });
    }
    if !type_meta.kind.is_empty() && type_meta.kind != expected_kind {
        return Err(ApiError::Invalid {
            message: format!("kind must be {expected_kind}, got {}", type_meta.kind),
        });
    }
    type_meta.api_version = CORE_API_VERSION.to_owned();
    type_meta.kind = expected_kind.to_owned();
    Ok(())
}

fn validate_object_labels(labels: &BTreeMap<String, String>) -> Result<(), ApiError> {
    for (key, value) in labels {
        validate_label_key(key)?;
        validate_label_value(value)?;
    }
    Ok(())
}

fn preserve_server_metadata(
    target: &mut ObjectMeta,
    previous: &ObjectMeta,
    resource_version: String,
) {
    target.uid = previous.uid.clone();
    target.creation_timestamp = previous.creation_timestamp;
    target.generation = previous.generation;
    target.resource_version = Some(resource_version);
}

fn validate_config_map_key(field: &str, key: &str) -> Result<(), ApiError> {
    if key.is_empty() || key.len() > 253 {
        return Err(ApiError::Invalid {
            message: format!("{field} key {key:?} must have 1 to 253 characters"),
        });
    }
    let (prefix, name) = match key.rsplit_once('/') {
        Some((prefix, name)) => (Some(prefix), name),
        None => (None, key),
    };
    if let Some(prefix) = prefix {
        validate_dns_subdomain(field, prefix, 253)?;
    }
    if name.is_empty()
        || name.len() > 63
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        || !name.as_bytes()[0].is_ascii_alphanumeric()
        || !name.as_bytes()[name.len() - 1].is_ascii_alphanumeric()
    {
        return Err(ApiError::Invalid {
            message: format!("{field} key {key:?} is not a valid ConfigMap key"),
        });
    }
    Ok(())
}

fn validate_label_key(key: &str) -> Result<(), ApiError> {
    validate_config_map_key("metadata.labels", key)
}

fn validate_label_value(value: &str) -> Result<(), ApiError> {
    if value.len() > 63
        || (!value.is_empty()
            && (!value.as_bytes()[0].is_ascii_alphanumeric()
                || !value.as_bytes()[value.len() - 1].is_ascii_alphanumeric()
                || !value.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')
                })))
    {
        return Err(ApiError::Invalid {
            message: format!("metadata.labels value {value:?} is not valid"),
        });
    }
    Ok(())
}

fn validate_dns_label(field: &str, value: &str) -> Result<(), ApiError> {
    if value.is_empty()
        || value.len() > 63
        || !value.as_bytes()[0].is_ascii_alphanumeric()
        || !value.as_bytes()[value.len() - 1].is_ascii_alphanumeric()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(ApiError::Invalid {
            message: format!("{field} {value:?} is not a DNS label"),
        });
    }
    Ok(())
}

fn validate_dns_subdomain(field: &str, value: &str, max_len: usize) -> Result<(), ApiError> {
    if value.is_empty() || value.len() > max_len {
        return Err(ApiError::Invalid {
            message: format!("{field} {value:?} is not a DNS subdomain"),
        });
    }
    for label in value.split('.') {
        validate_dns_label(field, label)?;
    }
    Ok(())
}

fn split_selector_terms(raw: &str) -> Result<Vec<&str>, ApiError> {
    let mut result = Vec::new();
    let mut nesting = 0_i32;
    let mut start = 0;
    for (index, character) in raw.char_indices() {
        match character {
            '(' => nesting += 1,
            ')' => {
                nesting -= 1;
                if nesting < 0 {
                    return Err(invalid_selector(raw));
                }
            }
            ',' if nesting == 0 => {
                let term = raw[start..index].trim();
                if term.is_empty() {
                    return Err(invalid_selector(raw));
                }
                result.push(term);
                start = index + 1;
            }
            _ => {}
        }
    }
    if nesting != 0 {
        return Err(invalid_selector(raw));
    }
    let term = raw[start..].trim();
    if term.is_empty() {
        return Err(invalid_selector(raw));
    }
    result.push(term);
    Ok(result)
}

fn parse_selector_term(term: &str) -> Result<LabelRequirement, ApiError> {
    if let Some(key) = term.strip_prefix('!') {
        validate_label_key(key)?;
        return Ok(LabelRequirement::DoesNotExist {
            key: key.to_owned(),
        });
    }
    for (operator, constructor) in [
        (" notin ", SelectorOperator::NotIn),
        (" in ", SelectorOperator::In),
    ] {
        if let Some((key, values)) = term.split_once(operator) {
            validate_label_key(key.trim())?;
            let values = parse_set_values(values.trim())?;
            return Ok(match constructor {
                SelectorOperator::In => LabelRequirement::In {
                    key: key.trim().to_owned(),
                    values,
                },
                SelectorOperator::NotIn => LabelRequirement::NotIn {
                    key: key.trim().to_owned(),
                    values,
                },
            });
        }
    }
    if let Some((key, value)) = term.split_once("!=") {
        validate_label_key(key.trim())?;
        validate_label_value(value.trim())?;
        return Ok(LabelRequirement::NotEquals {
            key: key.trim().to_owned(),
            value: value.trim().to_owned(),
        });
    }
    for operator in ["==", "="] {
        if let Some((key, value)) = term.split_once(operator) {
            validate_label_key(key.trim())?;
            validate_label_value(value.trim())?;
            return Ok(LabelRequirement::Equals {
                key: key.trim().to_owned(),
                value: value.trim().to_owned(),
            });
        }
    }
    validate_label_key(term)?;
    Ok(LabelRequirement::Exists {
        key: term.to_owned(),
    })
}

enum SelectorOperator {
    In,
    NotIn,
}

fn parse_set_values(raw: &str) -> Result<BTreeSet<String>, ApiError> {
    let Some(values) = raw
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
    else {
        return Err(invalid_selector(raw));
    };
    let result = values
        .split(',')
        .map(str::trim)
        .map(|value| {
            validate_label_value(value)?;
            Ok(value.to_owned())
        })
        .collect::<Result<BTreeSet<_>, ApiError>>()?;
    if result.is_empty() {
        return Err(invalid_selector(raw));
    }
    Ok(result)
}

fn invalid_selector(value: &str) -> ApiError {
    ApiError::Invalid {
        message: format!("labelSelector {value:?} is not valid"),
    }
}

fn invalid_field_selector(value: &str) -> ApiError {
    ApiError::Invalid {
        message: format!("fieldSelector {value:?} is not valid"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn config_map() -> ConfigMap {
        ConfigMap {
            type_meta: TypeMeta::config_map(),
            metadata: ObjectMeta {
                name: Some("application-config".to_owned()),
                namespace: Some("production".to_owned()),
                ..ObjectMeta::default()
            },
            data: BTreeMap::from([("log.level".to_owned(), "info".to_owned())]),
            ..ConfigMap::default()
        }
    }

    #[test]
    fn rejects_duplicate_data_and_binary_data_keys() {
        let mut resource = config_map();
        resource
            .binary_data
            .insert("log.level".to_owned(), BASE64.encode(b"info"));

        assert!(matches!(resource.validate(), Err(ApiError::Invalid { .. })));
    }

    #[test]
    fn immutable_config_map_rejects_data_change() {
        let mut previous = config_map();
        previous.immutable = Some(true);
        let mut changed = previous.clone();
        changed
            .data
            .insert("log.level".to_owned(), "debug".to_owned());

        assert!(matches!(
            changed.validate_update(&previous),
            Err(ApiError::Invalid { .. })
        ));
    }

    fn pod() -> Pod {
        Pod {
            type_meta: TypeMeta::pod(),
            metadata: ObjectMeta {
                name: Some("web".to_owned()),
                namespace: Some("development".to_owned()),
                ..ObjectMeta::default()
            },
            spec: PodSpec {
                containers: vec![Container {
                    name: "web".to_owned(),
                    image: Some("nginx:stable".to_owned()),
                    ..Container::default()
                }],
                ..PodSpec::default()
            },
            ..Pod::default()
        }
    }

    #[test]
    fn pod_create_defaults_restart_policy_and_pending_status() {
        let mut resource = pod();
        resource.validate_create().expect("Pod create is valid");
        resource.set_create_metadata(
            "pod-uid".to_owned(),
            OffsetDateTime::now_utc(),
            "7".to_owned(),
        );
        assert_eq!(resource.spec.restart_policy, Some(RestartPolicy::Always));
        assert_eq!(resource.status.phase, Some(PodPhase::Pending));

        let mut client_status = pod();
        client_status.status.phase = Some(PodPhase::Running);
        assert!(matches!(
            client_status.validate_create(),
            Err(ApiError::Invalid { .. })
        ));
    }

    #[test]
    fn pod_spec_is_immutable_and_namespace_gets_immutable_name_label() {
        let previous = pod();
        let mut changed = previous.clone();
        changed.spec.containers[0].image = Some("nginx:new".to_owned());
        assert!(matches!(
            changed.validate_update(&previous),
            Err(ApiError::Invalid { .. })
        ));

        let mut namespace = Namespace {
            type_meta: TypeMeta::namespace(),
            metadata: ObjectMeta {
                name: Some("development".to_owned()),
                ..ObjectMeta::default()
            },
            ..Namespace::default()
        };
        namespace
            .validate_create()
            .expect("Namespace create is valid");
        namespace.set_create_metadata(
            "namespace-uid".to_owned(),
            OffsetDateTime::now_utc(),
            "8".to_owned(),
        );
        assert_eq!(
            namespace.metadata.labels.get(Namespace::NAME_LABEL),
            Some(&"development".to_owned())
        );
        assert_eq!(namespace.status.phase, Some(NamespacePhase::Active));
    }

    #[test]
    fn selector_supports_set_and_existence_requirements() {
        let selector = LabelSelector::parse(Some("tier in (api,worker),environment=prod,!retired"))
            .expect("selector is valid");
        let labels = BTreeMap::from([
            ("tier".to_owned(), "api".to_owned()),
            ("environment".to_owned(), "prod".to_owned()),
        ]);

        assert!(selector.matches(&labels));
    }

    #[test]
    fn field_selector_filters_by_name_and_namespace() {
        let selector = FieldSelector::parse(Some(
            "metadata.name=application-config,metadata.namespace!=test",
        ))
        .expect("field selector is valid");
        assert!(selector.matches(&config_map()));
        assert!(matches!(
            FieldSelector::parse(Some("spec.nodeName=worker-a")),
            Err(ApiError::Invalid { .. })
        ));
    }
}
