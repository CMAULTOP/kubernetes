//! Durable etcd v3 persistence for namespaced ConfigMaps.
//!
//! This crate deliberately delegates KV, transactions and transport to `etcd-client`. It owns
//! only the Kubernetes-specific mapping: key layout, JSON encoding, resourceVersion translation,
//! validation and public error semantics.

use std::sync::Arc;

use etcd_client::{Compare, CompareOp, GetOptions, Txn, TxnOp};
use rusternetes_api_types::{
    ConfigMap, ConfigMapList, DeleteOptions, FieldSelector, LabelSelector,
};
use rusternetes_common::{ApiError, ResourceReference};
use rusternetes_storage::DeleteResult;
use time::OffsetDateTime;
use tokio::sync::Mutex;
use uuid::Uuid;

const DEFAULT_PREFIX: &str = "/registry/configmaps";

/// Durable repository mapping one ConfigMap to one etcd key.
///
/// `etcd-client::Client` requires mutable access. The single mutex is therefore the transport
/// boundary only; it does not protect Kubernetes resource state or emulate a storage database.
pub struct EtcdConfigMapRepository {
    client: Arc<Mutex<etcd_client::Client>>,
    key_prefix: String,
}

struct StoredConfigMap {
    resource: ConfigMap,
    mod_revision: i64,
}

impl EtcdConfigMapRepository {
    /// Connects to etcd and validates the key prefix chosen for this API group/resource mapping.
    pub async fn connect(
        endpoints: impl IntoIterator<Item = impl AsRef<str>>,
        key_prefix: Option<&str>,
    ) -> Result<Self, ApiError> {
        let key_prefix = normalize_prefix(key_prefix.unwrap_or(DEFAULT_PREFIX))?;
        let endpoints = endpoints
            .into_iter()
            .map(|endpoint| endpoint.as_ref().to_owned())
            .collect::<Vec<_>>();
        let client = etcd_client::Client::connect(endpoints, None)
            .await
            .map_err(|_| ApiError::Internal)?;
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            key_prefix,
        })
    }

    pub async fn create(&self, mut resource: ConfigMap) -> Result<ConfigMap, ApiError> {
        resource.enforce_type_meta()?;
        resource.validate()?;
        let namespace = resource.namespace()?.to_owned();
        let name = resource.name()?.to_owned();
        let reference = ResourceReference::config_map(namespace.clone(), name.clone());
        let key = self.config_map_key(&namespace, &name)?;

        resource.set_create_metadata(
            Uuid::new_v4().to_string(),
            OffsetDateTime::now_utc(),
            "0".to_owned(),
        );
        let encoded = encode(&resource)?;
        let transaction = Txn::new()
            .when(vec![Compare::version(key.clone(), CompareOp::Equal, 0)])
            .and_then(vec![TxnOp::put(key, encoded, None)]);
        let response = self
            .client
            .lock()
            .await
            .txn(transaction)
            .await
            .map_err(|_| ApiError::Internal)?;
        if !response.succeeded() {
            return Err(ApiError::AlreadyExists {
                resource: reference,
            });
        }
        resource.metadata.resource_version = Some(response_revision(&response)?);
        Ok(resource)
    }

    pub async fn get(&self, namespace: &str, name: &str) -> Result<ConfigMap, ApiError> {
        Ok(self.get_stored(namespace, name).await?.resource)
    }

    pub async fn list(
        &self,
        namespace: &str,
        label_selector: &LabelSelector,
        field_selector: &FieldSelector,
    ) -> Result<ConfigMapList, ApiError> {
        let namespace_prefix = format!("{}/", self.namespace_prefix(namespace)?);
        let response = self
            .client
            .lock()
            .await
            .get(namespace_prefix, Some(GetOptions::new().with_prefix()))
            .await
            .map_err(|_| ApiError::Internal)?;
        let revision = response_revision(&response)?;
        let items = response
            .kvs()
            .iter()
            .map(|key_value| decode(key_value.value(), key_value.mod_revision()))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|resource| {
                label_selector.matches(&resource.metadata.labels)
                    && field_selector.matches(resource)
            })
            .collect();
        Ok(ConfigMapList::new(revision, items))
    }

    /// Replaces a ConfigMap through an atomic mod-revision comparison.
    ///
    /// Kubernetes ConfigMap permits an empty resourceVersion. The repository still reads the
    /// current object and compares its observed etcd revision, so the write never becomes blind.
    pub async fn update(&self, mut resource: ConfigMap) -> Result<ConfigMap, ApiError> {
        resource.enforce_type_meta()?;
        let namespace = resource.namespace()?.to_owned();
        let name = resource.name()?.to_owned();
        let reference = ResourceReference::config_map(namespace.clone(), name.clone());
        let requested_version = resource.metadata.resource_version.clone();
        let stored = self.get_stored(&namespace, &name).await?;

        if let Some(requested_version) = requested_version {
            if requested_version
                != stored
                    .resource
                    .metadata
                    .resource_version
                    .as_deref()
                    .unwrap_or_default()
            {
                return Err(ApiError::Conflict {
                    resource: reference,
                });
            }
        }
        resource.validate_update(&stored.resource)?;
        resource.preserve_server_metadata_from(&stored.resource, "0".to_owned());
        let encoded = encode(&resource)?;
        let key = self.config_map_key(&namespace, &name)?;
        let transaction = Txn::new()
            .when(vec![Compare::mod_revision(
                key.clone(),
                CompareOp::Equal,
                stored.mod_revision,
            )])
            .and_then(vec![TxnOp::put(key, encoded, None)]);
        let response = self
            .client
            .lock()
            .await
            .txn(transaction)
            .await
            .map_err(|_| ApiError::Internal)?;
        if !response.succeeded() {
            return Err(ApiError::Conflict {
                resource: reference,
            });
        }
        resource.metadata.resource_version = Some(response_revision(&response)?);
        Ok(resource)
    }

    pub async fn delete(
        &self,
        namespace: &str,
        name: &str,
        options: DeleteOptions,
    ) -> Result<DeleteResult, ApiError> {
        let reference = ResourceReference::config_map(namespace.to_owned(), name.to_owned());
        let stored = self.get_stored(namespace, name).await?;
        validate_delete_preconditions(&stored.resource, &options, &reference)?;
        let key = self.config_map_key(namespace, name)?;
        let transaction = Txn::new()
            .when(vec![Compare::mod_revision(
                key.clone(),
                CompareOp::Equal,
                stored.mod_revision,
            )])
            .and_then(vec![TxnOp::delete(key, None)]);
        let response = self
            .client
            .lock()
            .await
            .txn(transaction)
            .await
            .map_err(|_| ApiError::Internal)?;
        if !response.succeeded() {
            return Err(ApiError::Conflict {
                resource: reference,
            });
        }
        Ok(DeleteResult {
            resource_version: response_revision(&response)?,
        })
    }

    async fn get_stored(&self, namespace: &str, name: &str) -> Result<StoredConfigMap, ApiError> {
        let reference = ResourceReference::config_map(namespace.to_owned(), name.to_owned());
        let key = self.config_map_key(namespace, name)?;
        let response = self
            .client
            .lock()
            .await
            .get(key, None)
            .await
            .map_err(|_| ApiError::Internal)?;
        let key_value = response.kvs().first().ok_or(ApiError::NotFound {
            resource: reference,
        })?;
        let mod_revision = key_value.mod_revision();
        Ok(StoredConfigMap {
            resource: decode(key_value.value(), mod_revision)?,
            mod_revision,
        })
    }

    fn config_map_key(&self, namespace: &str, name: &str) -> Result<String, ApiError> {
        validate_key_component("name", name)?;
        Ok(format!("{}/{name}", self.namespace_prefix(namespace)?))
    }

    fn namespace_prefix(&self, namespace: &str) -> Result<String, ApiError> {
        validate_key_component("namespace", namespace)?;
        Ok(format!("{}/{}", self.key_prefix, namespace))
    }
}

fn normalize_prefix(prefix: &str) -> Result<String, ApiError> {
    if !prefix.starts_with('/') || prefix.ends_with('/') || prefix.contains('\0') {
        return Err(ApiError::BadRequest {
            message: "etcd key prefix must start with '/', must not end with '/', and must not contain NUL"
                .to_owned(),
        });
    }
    Ok(prefix.to_owned())
}

fn validate_key_component(kind: &str, value: &str) -> Result<(), ApiError> {
    if value.is_empty() || value.contains('/') || value.contains('\0') {
        return Err(ApiError::BadRequest {
            message: format!("{kind} cannot be used safely in the etcd key layout"),
        });
    }
    Ok(())
}

fn encode(resource: &ConfigMap) -> Result<Vec<u8>, ApiError> {
    serde_json::to_vec(resource).map_err(|_| ApiError::Internal)
}

fn decode(raw: &[u8], mod_revision: i64) -> Result<ConfigMap, ApiError> {
    let mut resource = serde_json::from_slice::<ConfigMap>(raw).map_err(|_| ApiError::Internal)?;
    resource.enforce_type_meta()?;
    resource.metadata.resource_version = Some(resource_version(mod_revision)?);
    Ok(resource)
}

fn response_revision(response: &impl HasResponseHeader) -> Result<String, ApiError> {
    let revision = response.header_revision().ok_or(ApiError::Internal)?;
    resource_version(revision)
}

trait HasResponseHeader {
    fn header_revision(&self) -> Option<i64>;
}

impl HasResponseHeader for etcd_client::TxnResponse {
    fn header_revision(&self) -> Option<i64> {
        self.header().map(|header| header.revision())
    }
}

impl HasResponseHeader for etcd_client::GetResponse {
    fn header_revision(&self) -> Option<i64> {
        self.header().map(|header| header.revision())
    }
}

fn resource_version(revision: i64) -> Result<String, ApiError> {
    if revision <= 0 {
        return Err(ApiError::Internal);
    }
    Ok(revision.to_string())
}

fn validate_delete_preconditions(
    resource: &ConfigMap,
    options: &DeleteOptions,
    reference: &ResourceReference,
) -> Result<(), ApiError> {
    if !resource.metadata.finalizers.is_empty() {
        return Err(ApiError::BadRequest {
            message: "deleting resources with finalizers is not implemented in phase 3".to_owned(),
        });
    }
    let Some(preconditions) = options.preconditions.as_ref() else {
        return Ok(());
    };
    if preconditions.uid.is_some() && preconditions.uid != resource.metadata.uid {
        return Err(ApiError::Conflict {
            resource: reference.clone(),
        });
    }
    if preconditions.resource_version.is_some()
        && preconditions.resource_version != resource.metadata.resource_version
    {
        return Err(ApiError::Conflict {
            resource: reference.clone(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_layout_rejects_ambiguous_components() {
        assert!(normalize_prefix("registry/configmaps").is_err());
        assert!(normalize_prefix("/registry/configmaps/").is_err());
        assert!(validate_key_component("namespace", "default/other").is_err());
        assert!(validate_key_component("name", "").is_err());
    }

    #[test]
    fn positive_etcd_revision_becomes_opaque_resource_version() {
        assert_eq!(resource_version(42).expect("positive revision"), "42");
        assert!(resource_version(0).is_err());
    }
}
