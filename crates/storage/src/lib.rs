//! Atomic storage operations for namespaced ConfigMaps.
//!
//! The store owns all mutable ConfigMap state. HTTP layers never access its map directly;
//! every mutation has one linearization point inside the write lock.

use std::collections::BTreeMap;

use rusternetes_api_types::{
    ConfigMap, ConfigMapList, DeleteOptions, FieldSelector, LabelSelector,
};
use rusternetes_common::{ApiError, ResourceReference};
use time::OffsetDateTime;
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ResourceKey {
    namespace: String,
    name: String,
}

impl ResourceKey {
    fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
        }
    }

    fn reference(&self) -> ResourceReference {
        ResourceReference::config_map(self.namespace.clone(), self.name.clone())
    }
}

#[derive(Default)]
struct StoreState {
    revision: u64,
    config_maps: BTreeMap<ResourceKey, ConfigMap>,
}

impl StoreState {
    fn next_resource_version(&mut self) -> String {
        self.revision = self.revision.checked_add(1).unwrap_or(1);
        self.revision.to_string()
    }

    fn current_resource_version(&self) -> String {
        self.revision.to_string()
    }
}

/// Result of a successful delete operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteResult {
    pub resource_version: String,
}

/// In-memory single-process ConfigMap storage.
///
/// This is intentionally a concrete storage implementation for the first slice. A future
/// durable backend will implement the storage boundary only after its consistency and watch
/// semantics have been defined.
#[derive(Default)]
pub struct InMemoryConfigMapStore {
    state: RwLock<StoreState>,
}

impl InMemoryConfigMapStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn create(&self, mut resource: ConfigMap) -> Result<ConfigMap, ApiError> {
        resource.enforce_type_meta()?;
        resource.validate()?;
        let key = ResourceKey::new(
            resource.namespace()?.to_owned(),
            resource.name()?.to_owned(),
        );

        let mut state = self.state.write().await;
        if state.config_maps.contains_key(&key) {
            return Err(ApiError::AlreadyExists {
                resource: key.reference(),
            });
        }
        let resource_version = state.next_resource_version();
        resource.set_create_metadata(
            Uuid::new_v4().to_string(),
            OffsetDateTime::now_utc(),
            resource_version,
        );
        state.config_maps.insert(key, resource.clone());
        Ok(resource)
    }

    pub async fn get(&self, namespace: &str, name: &str) -> Result<ConfigMap, ApiError> {
        let key = ResourceKey::new(namespace, name);
        let state = self.state.read().await;
        state
            .config_maps
            .get(&key)
            .cloned()
            .ok_or_else(|| ApiError::NotFound {
                resource: key.reference(),
            })
    }

    /// Updates a ConfigMap atomically. An explicit resource version is compared with the
    /// current version; an omitted version is deliberately allowed by ConfigMap semantics.
    pub async fn update(&self, mut resource: ConfigMap) -> Result<ConfigMap, ApiError> {
        resource.enforce_type_meta()?;
        let key = ResourceKey::new(
            resource.namespace()?.to_owned(),
            resource.name()?.to_owned(),
        );
        let requested_version = resource.metadata.resource_version.clone();

        let mut state = self.state.write().await;
        let previous = state
            .config_maps
            .get(&key)
            .cloned()
            .ok_or_else(|| ApiError::NotFound {
                resource: key.reference(),
            })?;

        if let Some(requested_version) = requested_version {
            let current_version = previous
                .metadata
                .resource_version
                .as_deref()
                .unwrap_or_default();
            if requested_version != current_version {
                return Err(ApiError::Conflict {
                    resource: key.reference(),
                });
            }
        }
        resource.validate_update(&previous)?;
        let resource_version = state.next_resource_version();
        resource.preserve_server_metadata_from(&previous, resource_version);
        state.config_maps.insert(key, resource.clone());
        Ok(resource)
    }

    pub async fn delete(
        &self,
        namespace: &str,
        name: &str,
        options: DeleteOptions,
    ) -> Result<DeleteResult, ApiError> {
        let key = ResourceKey::new(namespace, name);
        let mut state = self.state.write().await;
        let current = state
            .config_maps
            .get(&key)
            .cloned()
            .ok_or_else(|| ApiError::NotFound {
                resource: key.reference(),
            })?;

        if !current.metadata.finalizers.is_empty() {
            return Err(ApiError::BadRequest {
                message: "deleting resources with finalizers is not implemented in phase 1"
                    .to_owned(),
            });
        }

        if let Some(preconditions) = options.preconditions {
            if preconditions.uid.is_some() && preconditions.uid != current.metadata.uid {
                return Err(ApiError::Conflict {
                    resource: key.reference(),
                });
            }
            if preconditions.resource_version.is_some()
                && preconditions.resource_version != current.metadata.resource_version
            {
                return Err(ApiError::Conflict {
                    resource: key.reference(),
                });
            }
        }

        state.config_maps.remove(&key);
        let resource_version = state.next_resource_version();
        Ok(DeleteResult { resource_version })
    }

    /// Lists either one namespace or all namespaces from one consistent in-memory snapshot.
    pub async fn list(
        &self,
        namespace: Option<&str>,
        label_selector: &LabelSelector,
        field_selector: &FieldSelector,
    ) -> ConfigMapList {
        let state = self.state.read().await;
        let items = state
            .config_maps
            .iter()
            .filter(|(key, resource)| {
                namespace.map_or(true, |requested| key.namespace == requested)
                    && label_selector.matches(&resource.metadata.labels)
                    && field_selector.matches(resource)
            })
            .map(|(_, resource)| resource.clone())
            .collect();
        ConfigMapList::new(state.current_resource_version(), items)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use rusternetes_api_types::{ObjectMeta, TypeMeta};

    use super::*;

    fn config_map(name: &str) -> ConfigMap {
        ConfigMap {
            type_meta: TypeMeta::config_map(),
            metadata: ObjectMeta {
                name: Some(name.to_owned()),
                namespace: Some("default".to_owned()),
                labels: BTreeMap::from([("tier".to_owned(), "api".to_owned())]),
                ..ObjectMeta::default()
            },
            data: BTreeMap::from([("mode".to_owned(), "safe".to_owned())]),
            ..ConfigMap::default()
        }
    }

    #[tokio::test]
    async fn stale_explicit_version_conflicts_but_unconditional_update_succeeds() {
        let store = InMemoryConfigMapStore::new();
        let created = store
            .create(config_map("settings"))
            .await
            .expect("create succeeds");

        let mut first_update = created.clone();
        first_update
            .data
            .insert("mode".to_owned(), "first".to_owned());
        let first_update = store
            .update(first_update)
            .await
            .expect("conditional update succeeds");

        let mut stale_update = created;
        stale_update
            .data
            .insert("mode".to_owned(), "stale".to_owned());
        assert!(matches!(
            store.update(stale_update).await,
            Err(ApiError::Conflict { .. })
        ));

        let mut unconditional = first_update;
        unconditional.metadata.resource_version = None;
        unconditional
            .data
            .insert("mode".to_owned(), "unconditional".to_owned());
        let updated = store
            .update(unconditional)
            .await
            .expect("unconditional ConfigMap update succeeds");
        assert_eq!(updated.data.get("mode"), Some(&"unconditional".to_owned()));
    }

    #[tokio::test]
    async fn delete_preconditions_are_checked_inside_the_mutation() {
        let store = InMemoryConfigMapStore::new();
        let created = store
            .create(config_map("settings"))
            .await
            .expect("create succeeds");
        let options = DeleteOptions {
            preconditions: Some(rusternetes_api_types::Preconditions {
                uid: Some("wrong".to_owned()),
                resource_version: created.metadata.resource_version.clone(),
            }),
            ..DeleteOptions::default()
        };

        assert!(matches!(
            store.delete("default", "settings", options).await,
            Err(ApiError::Conflict { .. })
        ));
        assert!(store.get("default", "settings").await.is_ok());
    }

    #[tokio::test]
    async fn list_filters_resources_by_label_selector() {
        let store = InMemoryConfigMapStore::new();
        store
            .create(config_map("first"))
            .await
            .expect("create first");
        let mut second = config_map("second");
        second
            .metadata
            .labels
            .insert("tier".to_owned(), "worker".to_owned());
        store.create(second).await.expect("create second");

        let selector = LabelSelector::parse(Some("tier=api")).expect("selector is valid");
        let fields =
            FieldSelector::parse(Some("metadata.name=first")).expect("field selector is valid");
        let list = store.list(Some("default"), &selector, &fields).await;
        assert_eq!(list.items.len(), 1);
        assert_eq!(list.items[0].metadata.name.as_deref(), Some("first"));
    }
}
