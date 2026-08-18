//! Typed Kubernetes API objects and validation for the first Rusternetes slice.

use std::collections::{BTreeMap, BTreeSet};

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use rusternetes_common::{ApiError, StatusReason};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

pub const CORE_API_VERSION: &str = "v1";
pub const CONFIG_MAP_KIND: &str = "ConfigMap";
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
        self.requirements.iter().all(|requirement| {
            let (field, expected, is_inequality) = match requirement {
                FieldRequirement::Equals { field, value } => (field, value, false),
                FieldRequirement::NotEquals { field, value } => (field, value, true),
            };
            let actual = match field.as_str() {
                "metadata.name" => resource.metadata.name.as_deref(),
                "metadata.namespace" => resource.metadata.namespace.as_deref(),
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

pub fn core_api_versions() -> ApiVersions {
    ApiVersions {
        kind: "APIVersions",
        api_version: "v1",
        versions: vec![CORE_API_VERSION],
        server_address_by_client_cidrs: Vec::new(),
    }
}

pub fn core_v1_resources() -> ApiResourceList {
    ApiResourceList {
        kind: "APIResourceList",
        api_version: "v1",
        group_version: "v1",
        resources: vec![ApiResource {
            name: "configmaps",
            singular_name: "configmap",
            namespaced: true,
            kind: CONFIG_MAP_KIND,
            verbs: vec!["create", "delete", "get", "list", "update"],
        }],
    }
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
