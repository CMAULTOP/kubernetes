//! Opaque continuation-token pagination over immutable typed LIST snapshots.
//!
//! The HTTP layer performs the initial storage read and places the resulting typed response in this
//! bounded cache. Later pages never re-read a backend, preserving a single snapshot for both the
//! in-memory and etcd implementations.

use std::collections::{BTreeSet, HashMap};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use rusternetes_api_types::{ConfigMap, ListMeta, Namespace, Node, Pod, ServiceAccount};
use rusternetes_common::ApiError;
use serde::{Deserialize, Serialize};
use time::{Duration, OffsetDateTime};
use tokio::sync::Mutex;
use uuid::Uuid;

const CONTINUATION_TOKEN_VERSION: &str = "rusternetes.io/v1";
const SNAPSHOT_TTL: Duration = Duration::minutes(5);
const MAX_ACTIVE_SNAPSHOTS: usize = 1_024;

/// The core/v1 collection bound into a continuation token.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ListResource {
    ConfigMaps,
    Pods,
    ServiceAccounts,
    Namespaces,
    Nodes,
}

/// Exact LIST identity used to bind a cache entry and every continuation request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListPaginationRequest {
    pub resource: ListResource,
    pub namespace: Option<String>,
    pub label_selector: Option<String>,
    pub field_selector: Option<String>,
    /// `None` means a continuation must retain the page limit stored in its token.
    pub limit: Option<usize>,
}

impl ListPaginationRequest {
    pub fn has_selectors(&self) -> bool {
        self.label_selector
            .as_deref()
            .is_some_and(|value| !value.is_empty())
            || self
                .field_selector
                .as_deref()
                .is_some_and(|value| !value.is_empty())
    }
}

/// The strongly typed object values retained by an immutable LIST snapshot.
#[derive(Clone, Debug)]
pub enum SnapshotItems {
    ConfigMaps(Vec<ConfigMap>),
    Pods(Vec<Pod>),
    ServiceAccounts(Vec<ServiceAccount>),
    Namespaces(Vec<Namespace>),
    Nodes(Vec<Node>),
}

impl SnapshotItems {
    fn resource(&self) -> ListResource {
        match self {
            Self::ConfigMaps(_) => ListResource::ConfigMaps,
            Self::Pods(_) => ListResource::Pods,
            Self::ServiceAccounts(_) => ListResource::ServiceAccounts,
            Self::Namespaces(_) => ListResource::Namespaces,
            Self::Nodes(_) => ListResource::Nodes,
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::ConfigMaps(items) => items.len(),
            Self::Pods(items) => items.len(),
            Self::ServiceAccounts(items) => items.len(),
            Self::Namespaces(items) => items.len(),
            Self::Nodes(items) => items.len(),
        }
    }

    fn range(&self, start: usize, end: usize) -> Self {
        match self {
            Self::ConfigMaps(items) => Self::ConfigMaps(items[start..end].to_vec()),
            Self::Pods(items) => Self::Pods(items[start..end].to_vec()),
            Self::ServiceAccounts(items) => Self::ServiceAccounts(items[start..end].to_vec()),
            Self::Namespaces(items) => Self::Namespaces(items[start..end].to_vec()),
            Self::Nodes(items) => Self::Nodes(items[start..end].to_vec()),
        }
    }

    pub fn into_config_maps(self) -> Option<Vec<ConfigMap>> {
        match self {
            Self::ConfigMaps(items) => Some(items),
            _ => None,
        }
    }

    pub fn into_pods(self) -> Option<Vec<Pod>> {
        match self {
            Self::Pods(items) => Some(items),
            _ => None,
        }
    }

    pub fn into_service_accounts(self) -> Option<Vec<ServiceAccount>> {
        match self {
            Self::ServiceAccounts(items) => Some(items),
            _ => None,
        }
    }

    pub fn into_namespaces(self) -> Option<Vec<Namespace>> {
        match self {
            Self::Namespaces(items) => Some(items),
            _ => None,
        }
    }

    pub fn into_nodes(self) -> Option<Vec<Node>> {
        match self {
            Self::Nodes(items) => Some(items),
            _ => None,
        }
    }
}

/// A page of objects and standard Kubernetes list metadata.
#[derive(Clone, Debug)]
pub struct PagedSnapshot {
    pub metadata: ListMeta,
    pub items: SnapshotItems,
}

#[derive(Default)]
pub struct PaginationCache {
    snapshots: Mutex<HashMap<Uuid, ListSnapshot>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ContinuationToken {
    v: String,
    id: Uuid,
    resource: ListResource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    label_selector: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    field_selector: Option<String>,
    offset: usize,
    limit: usize,
    issued_at: i64,
}

struct ListSnapshot {
    request: ListPaginationRequest,
    resource_version: Option<String>,
    items: SnapshotItems,
    issued_at: i64,
    expires_at: OffsetDateTime,
    /// Only cursors previously emitted by this server are accepted. This makes the token opaque
    /// even though it is transport-encoded rather than cryptographically signed.
    issued_offsets: BTreeSet<usize>,
}

impl ListSnapshot {
    fn token(&self, id: Uuid, offset: usize, limit: usize) -> ContinuationToken {
        ContinuationToken {
            v: CONTINUATION_TOKEN_VERSION.to_owned(),
            id,
            resource: self.request.resource,
            namespace: self.request.namespace.clone(),
            label_selector: self.request.label_selector.clone(),
            field_selector: self.request.field_selector.clone(),
            offset,
            limit,
            issued_at: self.issued_at,
        }
    }

    fn validates(&self, token: &ContinuationToken, request: &ListPaginationRequest) -> bool {
        token.v == CONTINUATION_TOKEN_VERSION
            && token.resource == self.request.resource
            && token.resource == request.resource
            && token.namespace == self.request.namespace
            && token.namespace == request.namespace
            && token.label_selector == self.request.label_selector
            && token.label_selector == request.label_selector
            && token.field_selector == self.request.field_selector
            && token.field_selector == request.field_selector
            && token.issued_at == self.issued_at
            && request.limit.is_none_or(|limit| limit == token.limit)
            && token.limit > 0
            && token.offset > 0
            && token.offset <= self.items.len()
            && self.issued_offsets.contains(&token.offset)
    }

    fn page(
        &self,
        start: usize,
        limit: usize,
        continue_token: Option<String>,
    ) -> Result<PagedSnapshot, ApiError> {
        let end = start
            .checked_add(limit)
            .map_or_else(|| self.items.len(), |end| end.min(self.items.len()));
        let remaining = self.items.len().saturating_sub(end);
        let remaining_item_count = if continue_token.is_some() && !self.request.has_selectors() {
            Some(i64::try_from(remaining).map_err(|_| ApiError::Internal)?)
        } else {
            None
        };
        Ok(PagedSnapshot {
            metadata: ListMeta {
                resource_version: self.resource_version.clone(),
                continue_token,
                remaining_item_count,
            },
            items: self.items.range(start, end),
        })
    }
}

impl PaginationCache {
    /// Returns a first LIST page. A non-positive pagination mode is represented by `None` and
    /// returns the supplied typed snapshot without retaining it.
    pub async fn first_page(
        &self,
        request: ListPaginationRequest,
        resource_version: Option<String>,
        items: SnapshotItems,
    ) -> Result<PagedSnapshot, ApiError> {
        if request.resource != items.resource() {
            return Err(ApiError::Internal);
        }
        let Some(limit) = request.limit else {
            return Self::single_response(resource_version, items);
        };
        if limit == 0 {
            return Self::single_response(resource_version, items);
        }

        let now = OffsetDateTime::now_utc();
        let issued_at = now.unix_timestamp();
        let snapshot = ListSnapshot {
            request,
            resource_version,
            items,
            issued_at,
            expires_at: now + SNAPSHOT_TTL,
            issued_offsets: BTreeSet::new(),
        };
        let first_end = snapshot.items.len().min(limit);
        if first_end == snapshot.items.len() {
            return snapshot.page(0, limit, None);
        }

        let mut snapshots = self.snapshots.lock().await;
        Self::prune_expired(&mut snapshots, now);
        Self::evict_oldest_if_full(&mut snapshots);
        let id = Self::new_snapshot_id(&snapshots);
        let token = encode_token(&snapshot.token(id, first_end, limit))?;
        let mut snapshot = snapshot;
        snapshot.issued_offsets.insert(first_end);
        let page = snapshot.page(0, limit, Some(token))?;
        snapshots.insert(id, snapshot);
        Ok(page)
    }

    /// Resolves an opaque continuation token against the retained immutable snapshot.
    pub async fn continue_page(
        &self,
        request: ListPaginationRequest,
        raw_token: &str,
    ) -> Result<PagedSnapshot, ApiError> {
        let token = decode_token(raw_token)?;
        let now = OffsetDateTime::now_utc();
        let mut snapshots = self.snapshots.lock().await;
        Self::prune_expired(&mut snapshots, now);
        let snapshot = snapshots
            .get_mut(&token.id)
            .ok_or_else(|| ApiError::ResourceExpired {
                message: "the provided continue token is no longer valid; restart the list request"
                    .to_owned(),
            })?;
        if !snapshot.validates(&token, &request) {
            return Err(ApiError::BadRequest {
                message: "continue token does not match this list request".to_owned(),
            });
        }
        let limit = token.limit;
        let end = token
            .offset
            .checked_add(limit)
            .map_or_else(|| snapshot.items.len(), |end| end.min(snapshot.items.len()));
        let is_last_page = end == snapshot.items.len();
        let continue_token = if is_last_page {
            None
        } else {
            snapshot.issued_offsets.insert(end);
            Some(encode_token(&snapshot.token(token.id, end, limit))?)
        };
        let page = snapshot.page(token.offset, limit, continue_token)?;
        if is_last_page {
            snapshots.remove(&token.id);
        }
        Ok(page)
    }

    fn single_response(
        resource_version: Option<String>,
        items: SnapshotItems,
    ) -> Result<PagedSnapshot, ApiError> {
        let length = items.len();
        Ok(PagedSnapshot {
            metadata: ListMeta {
                resource_version,
                continue_token: None,
                remaining_item_count: None,
            },
            items: items.range(0, length),
        })
    }

    fn prune_expired(snapshots: &mut HashMap<Uuid, ListSnapshot>, now: OffsetDateTime) {
        snapshots.retain(|_, snapshot| snapshot.expires_at > now);
    }

    fn evict_oldest_if_full(snapshots: &mut HashMap<Uuid, ListSnapshot>) {
        if snapshots.len() < MAX_ACTIVE_SNAPSHOTS {
            return;
        }
        if let Some(oldest) = snapshots
            .iter()
            .min_by_key(|(_, snapshot)| snapshot.issued_at)
            .map(|(id, _)| *id)
        {
            snapshots.remove(&oldest);
        }
    }

    fn new_snapshot_id(snapshots: &HashMap<Uuid, ListSnapshot>) -> Uuid {
        loop {
            let candidate = Uuid::new_v4();
            if !snapshots.contains_key(&candidate) {
                return candidate;
            }
        }
    }
}

fn encode_token(token: &ContinuationToken) -> Result<String, ApiError> {
    let serialized = serde_json::to_vec(token).map_err(|_| ApiError::Internal)?;
    Ok(URL_SAFE_NO_PAD.encode(serialized))
}

fn decode_token(raw_token: &str) -> Result<ContinuationToken, ApiError> {
    let serialized = URL_SAFE_NO_PAD
        .decode(raw_token)
        .map_err(|_| ApiError::BadRequest {
            message: "continue token is not valid URL-safe base64".to_owned(),
        })?;
    let token = serde_json::from_slice::<ContinuationToken>(&serialized).map_err(|_| {
        ApiError::BadRequest {
            message: "continue token has an invalid payload".to_owned(),
        }
    })?;
    if token.v != CONTINUATION_TOKEN_VERSION || token.limit == 0 || token.offset == 0 {
        return Err(ApiError::BadRequest {
            message: "continue token has an unsupported version or invalid cursor".to_owned(),
        });
    }
    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusternetes_api_types::{ObjectMeta, TypeMeta};

    fn config_map(name: &str) -> ConfigMap {
        ConfigMap {
            type_meta: TypeMeta::config_map(),
            metadata: ObjectMeta {
                name: Some(name.to_owned()),
                namespace: Some("default".to_owned()),
                ..ObjectMeta::default()
            },
            ..ConfigMap::default()
        }
    }

    fn request(limit: Option<usize>) -> ListPaginationRequest {
        ListPaginationRequest {
            resource: ListResource::ConfigMaps,
            namespace: Some("default".to_owned()),
            label_selector: None,
            field_selector: None,
            limit,
        }
    }

    #[tokio::test]
    async fn continuation_returns_snapshot_pages_and_rejects_cursor_tampering() {
        let cache = PaginationCache::default();
        let first = cache
            .first_page(
                request(Some(2)),
                Some("7".to_owned()),
                SnapshotItems::ConfigMaps(vec![config_map("a"), config_map("b"), config_map("c")]),
            )
            .await
            .expect("first page succeeds");
        assert_eq!(first.items.into_config_maps().expect("kind").len(), 2);
        assert_eq!(first.metadata.remaining_item_count, Some(1));
        let token = first.metadata.continue_token.expect("continue token");

        let mut decoded: ContinuationToken = serde_json::from_slice(
            &URL_SAFE_NO_PAD
                .decode(token.as_bytes())
                .expect("decode token"),
        )
        .expect("token JSON");
        decoded.offset = 1;
        let tampered = encode_token(&decoded).expect("encode token");
        assert!(matches!(
            cache.continue_page(request(Some(2)), &tampered).await,
            Err(ApiError::BadRequest { .. })
        ));

        let second = cache
            .continue_page(request(None), &token)
            .await
            .expect("second page succeeds");
        assert_eq!(second.items.into_config_maps().expect("kind").len(), 1);
        assert!(second.metadata.continue_token.is_none());
        assert_eq!(second.metadata.resource_version.as_deref(), Some("7"));
    }

    #[tokio::test]
    async fn expired_snapshot_returns_resource_expired_and_is_pruned() {
        let cache = PaginationCache::default();
        let first = cache
            .first_page(
                request(Some(1)),
                Some("9".to_owned()),
                SnapshotItems::ConfigMaps(vec![config_map("a"), config_map("b")]),
            )
            .await
            .expect("first page succeeds");
        let token = first.metadata.continue_token.expect("continue token");
        let decoded: ContinuationToken = serde_json::from_slice(
            &URL_SAFE_NO_PAD
                .decode(token.as_bytes())
                .expect("decode token"),
        )
        .expect("token JSON");
        cache
            .snapshots
            .lock()
            .await
            .get_mut(&decoded.id)
            .expect("snapshot is cached")
            .expires_at = OffsetDateTime::now_utc() - Duration::seconds(1);

        assert!(matches!(
            cache.continue_page(request(None), &token).await,
            Err(ApiError::ResourceExpired { .. })
        ));
        assert!(cache.snapshots.lock().await.is_empty());
    }
}
