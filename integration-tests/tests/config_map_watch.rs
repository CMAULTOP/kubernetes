use std::{sync::Arc, time::Duration};

use futures_util::{Stream, StreamExt};
use rusternetes_api_server::router;
use rusternetes_api_types::{ApiStatus, ConfigMap, ObjectMeta, TypeMeta};
use rusternetes_common::StatusReason;
use rusternetes_storage::{InMemoryConfigMapStore, WATCH_HISTORY_CAPACITY};
use serde_json::Value;
use tokio::{net::TcpListener, task::JoinHandle};

struct TestServer {
    base_url: String,
    task: JoinHandle<()>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn start_server() -> TestServer {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("ephemeral listener binds: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("listener has local address: {error}"));
    let application = router(Arc::new(InMemoryConfigMapStore::new()));
    let task = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, application).await {
            panic!("test API server failed: {error}");
        }
    });
    TestServer {
        base_url: format!("http://{address}"),
        task,
    }
}

fn request_config_map(name: &str, namespace: &str, tier: &str) -> ConfigMap {
    ConfigMap {
        type_meta: TypeMeta::config_map(),
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            namespace: Some(namespace.to_owned()),
            labels: [("tier".to_owned(), tier.to_owned())].into_iter().collect(),
            ..ObjectMeta::default()
        },
        data: [("mode".to_owned(), "safe".to_owned())]
            .into_iter()
            .collect(),
        ..ConfigMap::default()
    }
}

async fn create_config_map(
    client: &reqwest::Client,
    collection: &str,
    resource: &ConfigMap,
) -> ConfigMap {
    let response = client
        .post(collection)
        .json(resource)
        .send()
        .await
        .unwrap_or_else(|error| panic!("create request completes: {error}"));
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    response
        .json()
        .await
        .unwrap_or_else(|error| panic!("create response is ConfigMap JSON: {error}"))
}

async fn next_watch_event<S, B>(stream: &mut S, pending: &mut Vec<u8>) -> Value
where
    S: Stream<Item = Result<B, reqwest::Error>> + Unpin,
    B: AsRef<[u8]>,
{
    loop {
        if let Some(newline) = pending.iter().position(|byte| *byte == b'\n') {
            let encoded = pending.drain(..=newline).collect::<Vec<_>>();
            return serde_json::from_slice(&encoded[..encoded.len() - 1])
                .unwrap_or_else(|error| panic!("watch event is Kubernetes JSON: {error}"));
        }
        let next_chunk = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap_or_else(|_| panic!("watch event arrives before timeout"))
            .unwrap_or_else(|| panic!("watch stream remained open"))
            .unwrap_or_else(|error| panic!("watch body does not fail: {error}"));
        pending.extend_from_slice(next_chunk.as_ref());
    }
}

#[tokio::test]
async fn watch_replays_history_then_streams_live_modification_and_deletion() {
    let server = start_server().await;
    let client = reqwest::Client::new();
    let collection = format!("{}/api/v1/namespaces/default/configmaps", server.base_url);
    let created = create_config_map(
        &client,
        &collection,
        &request_config_map("settings", "default", "api"),
    )
    .await;

    let response = client
        .get(&collection)
        .query(&[("watch", "true"), ("resourceVersion", "0")])
        .send()
        .await
        .unwrap_or_else(|error| panic!("watch request opens: {error}"));
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let mut stream = response.bytes_stream();
    let mut pending = Vec::new();
    let replayed = next_watch_event(&mut stream, &mut pending).await;
    assert_eq!(replayed["type"], "ADDED");
    assert_eq!(replayed["object"]["metadata"]["resourceVersion"], "1");

    let mut update = created;
    update.data.insert("mode".to_owned(), "updated".to_owned());
    let updated_response = client
        .put(format!("{collection}/settings"))
        .json(&update)
        .send()
        .await
        .unwrap_or_else(|error| panic!("update request completes: {error}"));
    assert_eq!(updated_response.status(), reqwest::StatusCode::OK);
    let modified = next_watch_event(&mut stream, &mut pending).await;
    assert_eq!(modified["type"], "MODIFIED");
    assert_eq!(modified["object"]["data"]["mode"], "updated");
    assert_eq!(modified["object"]["metadata"]["resourceVersion"], "2");

    let deleted_response = client
        .delete(format!("{collection}/settings"))
        .send()
        .await
        .unwrap_or_else(|error| panic!("delete request completes: {error}"));
    assert_eq!(deleted_response.status(), reqwest::StatusCode::OK);
    let deleted = next_watch_event(&mut stream, &mut pending).await;
    assert_eq!(deleted["type"], "DELETED");
    assert_eq!(deleted["object"]["metadata"]["resourceVersion"], "3");
}

#[tokio::test]
async fn watch_applies_selectors_and_emits_bookmark_only_when_requested() {
    let server = start_server().await;
    let client = reqwest::Client::new();
    let collection = format!("{}/api/v1/namespaces/default/configmaps", server.base_url);
    create_config_map(
        &client,
        &collection,
        &request_config_map("api-settings", "default", "api"),
    )
    .await;
    create_config_map(
        &client,
        &collection,
        &request_config_map("worker-settings", "default", "worker"),
    )
    .await;

    let response = client
        .get(&collection)
        .query(&[
            ("watch", "true"),
            ("resourceVersion", "0"),
            ("labelSelector", "tier=api"),
            ("allowWatchBookmarks", "true"),
        ])
        .send()
        .await
        .unwrap_or_else(|error| panic!("selected watch opens: {error}"));
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let mut stream = response.bytes_stream();
    let mut pending = Vec::new();
    let selected = next_watch_event(&mut stream, &mut pending).await;
    assert_eq!(selected["type"], "ADDED");
    assert_eq!(selected["object"]["metadata"]["name"], "api-settings");
    let bookmark = next_watch_event(&mut stream, &mut pending).await;
    assert_eq!(bookmark["type"], "BOOKMARK");
    assert_eq!(bookmark["object"]["metadata"]["resourceVersion"], "2");
}

#[tokio::test]
async fn watch_with_compacted_version_returns_kubernetes_expired_status() {
    let server = start_server().await;
    let client = reqwest::Client::new();
    let collection = format!("{}/api/v1/namespaces/default/configmaps", server.base_url);
    for index in 0..=WATCH_HISTORY_CAPACITY {
        create_config_map(
            &client,
            &collection,
            &request_config_map(&format!("settings-{index}"), "default", "api"),
        )
        .await;
    }

    let response = client
        .get(&collection)
        .query(&[("watch", "true"), ("resourceVersion", "0")])
        .send()
        .await
        .unwrap_or_else(|error| panic!("expired watch request completes: {error}"));
    assert_eq!(response.status(), reqwest::StatusCode::GONE);
    let status: ApiStatus = response
        .json()
        .await
        .unwrap_or_else(|error| panic!("expired response is Status JSON: {error}"));
    assert_eq!(status.reason, StatusReason::Expired);
    assert_eq!(status.code, 410);
}
