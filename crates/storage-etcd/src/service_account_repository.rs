//! Durable etcd v3 persistence for namespaced ServiceAccounts.
//!
//! This crate deliberately delegates KV, transactions and transport to `etcd-client`. It owns
//! only the Kubernetes-specific mapping: key layout, JSON encoding, resourceVersion translation,
//! validation and public error semantics.

use std::sync::Arc;

use etcd_client::{
    Compare, CompareOp, EventType as EtcdEventType, GetOptions, Txn, TxnOp, WatchOptions,
};
use rusternetes_api_types::{
    DeleteOptions, FieldSelector, LabelSelector, ServiceAccount, ServiceAccountList,
    ServiceAccountWatchEvent, ServiceAccountWatchObject,
};
use rusternetes_common::{ApiError, ResourceReference};
use rusternetes_storage::{DeleteResult, ServiceAccountWatchRequest};
use time::OffsetDateTime;
use tokio::{
    sync::{mpsc, Mutex},
    task::JoinHandle,
};
use uuid::Uuid;

const DEFAULT_PREFIX: &str = "/registry/serviceaccounts";
const WATCH_CHANNEL_CAPACITY: usize = 256;

/// Durable repository mapping one ServiceAccount to one etcd key.
///
/// `etcd-client::Client` requires mutable access. The single mutex is therefore the transport
/// boundary only; it does not protect Kubernetes resource state or emulate a storage database.
pub struct EtcdServiceAccountRepository {
    client: Arc<Mutex<etcd_client::Client>>,
    key_prefix: String,
}

struct StoredServiceAccount {
    resource: ServiceAccount,
    mod_revision: i64,
}

/// A bounded, cancellation-safe durable ServiceAccount WATCH subscription backed by etcd.
///
/// Dropping the subscription aborts its bridge task, releases the gRPC watch stream and closes the
/// bounded channel. No caller can cause unbounded per-watch buffering in the API Server.
pub struct EtcdServiceAccountWatchSubscription {
    receiver: mpsc::Receiver<rusternetes_api_types::ServiceAccountWatchEvent>,
    bridge_task: JoinHandle<()>,
}

impl EtcdServiceAccountWatchSubscription {
    pub async fn recv(&mut self) -> Option<rusternetes_api_types::ServiceAccountWatchEvent> {
        self.receiver.recv().await
    }
}

impl Drop for EtcdServiceAccountWatchSubscription {
    fn drop(&mut self) {
        self.bridge_task.abort();
    }
}

impl EtcdServiceAccountRepository {
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

    pub async fn create(&self, mut resource: ServiceAccount) -> Result<ServiceAccount, ApiError> {
        resource.enforce_type_meta()?;
        resource.validate_create()?;
        let namespace = resource.namespace()?.to_owned();
        let name = resource.name()?.to_owned();
        let reference = ResourceReference::service_account(namespace.clone(), name.clone());
        let key = self.service_account_key(&namespace, &name)?;

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

    pub async fn get(&self, namespace: &str, name: &str) -> Result<ServiceAccount, ApiError> {
        Ok(self.get_stored(namespace, name).await?.resource)
    }

    pub async fn list(
        &self,
        namespace: &str,
        label_selector: &LabelSelector,
        field_selector: &FieldSelector,
    ) -> Result<ServiceAccountList, ApiError> {
        self.list_prefix(
            format!("{}/", self.namespace_prefix(namespace)?),
            label_selector,
            field_selector,
        )
        .await
    }

    /// Lists ServiceAccounts from every namespace under this repository's dedicated etcd prefix.
    pub async fn list_all(
        &self,
        label_selector: &LabelSelector,
        field_selector: &FieldSelector,
    ) -> Result<ServiceAccountList, ApiError> {
        self.list_prefix(
            format!("{}/", self.key_prefix),
            label_selector,
            field_selector,
        )
        .await
    }

    /// Opens a bounded ServiceAccount watch that replays etcd history and continues with live events.
    ///
    /// A requested historical revision is checked before the HTTP layer starts its response. If
    /// etcd has compacted it, callers receive typed Kubernetes `410 Expired` instead of a stream
    /// that would falsely imply continuity.
    pub async fn watch(
        &self,
        request: ServiceAccountWatchRequest,
    ) -> Result<EtcdServiceAccountWatchSubscription, ApiError> {
        let requested_revision = parse_watch_resource_version(request.resource_version.as_deref())?;
        let prefix = match request.namespace.as_deref() {
            Some(namespace) => format!("{}/", self.namespace_prefix(namespace)?),
            None => format!("{}/", self.key_prefix),
        };

        if let Some(revision) = requested_revision {
            // etcd treats revision 0 as a current read, while Kubernetes resourceVersion=0 asks
            // for every retained change. Probe revision 1 so compaction is surfaced before the
            // watch stream's creation response, which cannot be translated after HTTP starts.
            self.ensure_history_available(&prefix, revision.max(1))
                .await?;
        }

        let mut watch_client = self.client.lock().await.watch_client();
        let options = match requested_revision {
            Some(revision) => WatchOptions::new()
                .with_prefix()
                .with_prev_key()
                .with_start_revision(revision.checked_add(1).ok_or(ApiError::BadRequest {
                    message: "resourceVersion is too large for the etcd watch backend".to_owned(),
                })?),
            None => WatchOptions::new().with_prefix().with_prev_key(),
        };
        let (_watcher, mut stream) = watch_client
            .watch(prefix, Some(options))
            .await
            .map_err(map_etcd_watch_error)?;
        let (sender, receiver) = mpsc::channel(WATCH_CHANNEL_CAPACITY);
        let bridge_task = tokio::spawn(async move {
            loop {
                let response = match stream.message().await {
                    Ok(Some(response)) => response,
                    Ok(None) | Err(_) => return,
                };
                if response.canceled() {
                    return;
                }
                for event in response.events() {
                    let translated = match translate_watch_event(event) {
                        Ok(Some(event)) => event,
                        Ok(None) | Err(_) => continue,
                    };
                    let ServiceAccountWatchObject::ServiceAccount(resource) = &translated.object
                    else {
                        continue;
                    };
                    if !watch_matches(&request, resource) {
                        continue;
                    }
                    match sender.try_send(translated) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_))
                        | Err(mpsc::error::TrySendError::Closed(_)) => return,
                    }
                }
            }
        });
        Ok(EtcdServiceAccountWatchSubscription {
            receiver,
            bridge_task,
        })
    }

    async fn ensure_history_available(&self, prefix: &str, revision: i64) -> Result<(), ApiError> {
        self.client
            .lock()
            .await
            .get(
                prefix,
                Some(GetOptions::new().with_prefix().with_revision(revision)),
            )
            .await
            .map(|_| ())
            .map_err(map_etcd_watch_error)
    }

    /// Replaces a ServiceAccount through an atomic mod-revision comparison.
    ///
    /// Kubernetes ServiceAccount permits an empty resourceVersion. The repository still reads the
    /// current object and compares its observed etcd revision, so the write never becomes blind.
    pub async fn update(&self, mut resource: ServiceAccount) -> Result<ServiceAccount, ApiError> {
        resource.enforce_type_meta()?;
        let namespace = resource.namespace()?.to_owned();
        let name = resource.name()?.to_owned();
        let reference = ResourceReference::service_account(namespace.clone(), name.clone());
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
        let key = self.service_account_key(&namespace, &name)?;
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
        let reference = ResourceReference::service_account(namespace.to_owned(), name.to_owned());
        let stored = self.get_stored(namespace, name).await?;
        validate_delete_preconditions(&stored.resource, &options, &reference)?;
        let key = self.service_account_key(namespace, name)?;
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

    async fn list_prefix(
        &self,
        prefix: String,
        label_selector: &LabelSelector,
        field_selector: &FieldSelector,
    ) -> Result<ServiceAccountList, ApiError> {
        let response = self
            .client
            .lock()
            .await
            .get(prefix, Some(GetOptions::new().with_prefix()))
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
                    && field_selector.matches_service_account(resource)
            })
            .collect();
        Ok(ServiceAccountList::new(revision, items))
    }

    async fn get_stored(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<StoredServiceAccount, ApiError> {
        let reference = ResourceReference::service_account(namespace.to_owned(), name.to_owned());
        let key = self.service_account_key(namespace, name)?;
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
        Ok(StoredServiceAccount {
            resource: decode(key_value.value(), mod_revision)?,
            mod_revision,
        })
    }

    fn service_account_key(&self, namespace: &str, name: &str) -> Result<String, ApiError> {
        validate_key_component("name", name)?;
        Ok(format!("{}/{name}", self.namespace_prefix(namespace)?))
    }

    fn namespace_prefix(&self, namespace: &str) -> Result<String, ApiError> {
        validate_key_component("namespace", namespace)?;
        Ok(format!("{}/{}", self.key_prefix, namespace))
    }
}

fn parse_watch_resource_version(raw: Option<&str>) -> Result<Option<i64>, ApiError> {
    let Some(raw) = raw.filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    raw.parse::<i64>()
        .ok()
        .filter(|revision| *revision >= 0)
        .ok_or_else(|| ApiError::BadRequest {
            message: format!("resourceVersion {raw:?} is not a valid etcd revision"),
        })
        .map(Some)
}

fn map_etcd_watch_error(error: etcd_client::Error) -> ApiError {
    match error {
        etcd_client::Error::GRpcStatus(status)
            if status
                .message()
                .contains("required revision has been compacted") =>
        {
            ApiError::ResourceExpired {
                message: format!("requested etcd revision is compacted: {}", status.message()),
            }
        }
        _ => ApiError::Internal,
    }
}

fn watch_matches(request: &ServiceAccountWatchRequest, resource: &ServiceAccount) -> bool {
    request
        .namespace
        .as_deref()
        .is_none_or(|namespace| resource.metadata.namespace.as_deref() == Some(namespace))
        && request.label_selector.matches(&resource.metadata.labels)
        && request.field_selector.matches_service_account(resource)
}

fn translate_watch_event(
    event: &etcd_client::Event,
) -> Result<Option<ServiceAccountWatchEvent>, ApiError> {
    let key_value = event.kv().ok_or(ApiError::Internal)?;
    match event.event_type() {
        EtcdEventType::Put => {
            let resource = decode(key_value.value(), key_value.mod_revision())?;
            if key_value.version() == 1 {
                Ok(Some(ServiceAccountWatchEvent::added(resource)))
            } else {
                Ok(Some(ServiceAccountWatchEvent::modified(resource)))
            }
        }
        EtcdEventType::Delete => {
            let previous = event.prev_kv().ok_or(ApiError::Internal)?;
            let resource = decode(previous.value(), key_value.mod_revision())?;
            Ok(Some(ServiceAccountWatchEvent::deleted(resource)))
        }
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

fn encode(resource: &ServiceAccount) -> Result<Vec<u8>, ApiError> {
    serde_json::to_vec(resource).map_err(|_| ApiError::Internal)
}

fn decode(raw: &[u8], mod_revision: i64) -> Result<ServiceAccount, ApiError> {
    let mut resource =
        serde_json::from_slice::<ServiceAccount>(raw).map_err(|_| ApiError::Internal)?;
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
    resource: &ServiceAccount,
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
        assert!(normalize_prefix("registry/service_accounts").is_err());
        assert!(normalize_prefix("/registry/serviceaccounts/").is_err());
        assert!(validate_key_component("namespace", "default/other").is_err());
        assert!(validate_key_component("name", "").is_err());
    }

    #[test]
    fn positive_etcd_revision_becomes_opaque_resource_version() {
        assert_eq!(resource_version(42).expect("positive revision"), "42");
        assert!(resource_version(0).is_err());
    }
}
