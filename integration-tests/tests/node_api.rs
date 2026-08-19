use futures_util::StreamExt;
use rusternetes_api_server::{core_backend_from_etcd_config, router_with_core_backend};
use rusternetes_api_types::{ApiStatus, Node, NodeList, NodeSpec, ObjectMeta, TypeMeta};
use rusternetes_common::StatusReason;
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
    let backend = core_backend_from_etcd_config(None, None)
        .await
        .unwrap_or_else(|error| panic!("in-memory core backend initializes: {error}"));
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("ephemeral listener binds: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("listener has local address: {error}"));
    let task = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, router_with_core_backend(backend)).await {
            panic!("test API server failed: {error}");
        }
    });
    TestServer {
        base_url: format!("http://{address}"),
        task,
    }
}

fn node(name: &str) -> Node {
    Node {
        type_meta: TypeMeta::node(),
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            labels: [("role".to_owned(), "worker".to_owned())]
                .into_iter()
                .collect(),
            ..ObjectMeta::default()
        },
        ..Node::default()
    }
}

async fn status(response: reqwest::Response) -> ApiStatus {
    response
        .json()
        .await
        .unwrap_or_else(|error| panic!("Status JSON expected: {error}"))
}

#[tokio::test]
async fn node_watch_replays_typed_creation_event_over_http() {
    let server = start_server().await;
    let client = reqwest::Client::new();
    let collection = format!("{}/api/v1/nodes", server.base_url);
    let created_response = client
        .post(&collection)
        .json(&node("node-a"))
        .send()
        .await
        .expect("create completes");
    assert_eq!(created_response.status(), reqwest::StatusCode::CREATED);

    let response = client
        .get(format!("{collection}?watch=true&resourceVersion=0"))
        .send()
        .await
        .expect("watch opens");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let mut stream = response.bytes_stream();
    let first = stream
        .next()
        .await
        .expect("watch delivers one event")
        .expect("watch payload is valid");
    let event: serde_json::Value = serde_json::from_slice(&first).expect("watch event is JSON");
    assert_eq!(event["type"], "ADDED");
    assert_eq!(event["object"]["metadata"]["name"], "node-a");
}

#[tokio::test]
async fn node_patch_supports_rfc_merge_and_json_patch_without_bypassing_status_or_watch() {
    let server = start_server().await;
    let client = reqwest::Client::new();
    let collection = format!("{}/api/v1/nodes", server.base_url);
    let created_response = client
        .post(&collection)
        .json(&node("node-a"))
        .send()
        .await
        .expect("create completes");
    assert_eq!(created_response.status(), reqwest::StatusCode::CREATED);
    let created: Node = created_response.json().await.expect("typed Node response");
    let resource_version = created
        .metadata
        .resource_version
        .as_deref()
        .expect("created Node has resourceVersion");

    let watch_response = client
        .get(format!(
            "{collection}?watch=true&resourceVersion={resource_version}"
        ))
        .send()
        .await
        .expect("watch opens");
    assert_eq!(watch_response.status(), reqwest::StatusCode::OK);
    let mut watch = watch_response.bytes_stream();

    let merge_response = client
        .patch(format!("{collection}/node-a"))
        .header(
            "content-type",
            "application/merge-patch+json; charset=utf-8",
        )
        .body(r#"{"spec":{"unschedulable":true},"status":{"ready":false}}"#)
        .send()
        .await
        .expect("merge patch completes");
    assert_eq!(merge_response.status(), reqwest::StatusCode::OK);
    let merged: Node = merge_response
        .json()
        .await
        .expect("typed merge patch response");
    assert!(merged.spec.unschedulable);
    assert!(merged.status.ready);
    assert_ne!(
        merged.metadata.resource_version,
        created.metadata.resource_version
    );

    let event = watch
        .next()
        .await
        .expect("PATCH produces a watch event")
        .expect("watch payload is valid");
    let event: serde_json::Value = serde_json::from_slice(&event).expect("watch event is JSON");
    assert_eq!(event["type"], "MODIFIED");
    assert_eq!(event["object"]["spec"]["unschedulable"], true);
    assert_eq!(event["object"]["status"]["ready"], true);

    let json_patch_response = client
        .patch(format!("{collection}/node-a"))
        .header("content-type", "application/json-patch+json")
        .body(r#"[{"op":"replace","path":"/metadata/labels/role","value":"control-plane"}]"#)
        .send()
        .await
        .expect("JSON Patch completes");
    assert_eq!(json_patch_response.status(), reqwest::StatusCode::OK);
    let json_patched: Node = json_patch_response
        .json()
        .await
        .expect("typed JSON Patch response");
    assert_eq!(
        json_patched.metadata.labels.get("role"),
        Some(&"control-plane".to_owned())
    );
    assert!(json_patched.status.ready);

    let unsupported_response = client
        .patch(format!("{collection}/node-a"))
        .header("content-type", "application/strategic-merge-patch+json")
        .body(r#"{"spec":{"unschedulable":false}}"#)
        .send()
        .await
        .expect("unsupported PATCH completes");
    assert_eq!(
        unsupported_response.status(),
        reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    assert_eq!(
        status(unsupported_response).await.reason,
        StatusReason::UnsupportedMediaType
    );
}

#[tokio::test]
async fn node_status_subresource_preserves_spec_enforces_cas_and_emits_watch_update() {
    let server = start_server().await;
    let client = reqwest::Client::new();
    let collection = format!("{}/api/v1/nodes", server.base_url);
    let created_response = client
        .post(&collection)
        .json(&node("node-a"))
        .send()
        .await
        .expect("create completes");
    assert_eq!(created_response.status(), reqwest::StatusCode::CREATED);
    let created: Node = created_response.json().await.expect("typed Node response");
    let resource_version = created
        .metadata
        .resource_version
        .clone()
        .expect("created Node has resourceVersion");

    let watch_response = client
        .get(format!(
            "{collection}?watch=true&resourceVersion={resource_version}"
        ))
        .send()
        .await
        .expect("watch opens");
    assert_eq!(watch_response.status(), reqwest::StatusCode::OK);
    let mut watch = watch_response.bytes_stream();

    let mut status_update = created.clone();
    status_update.spec.unschedulable = true;
    status_update
        .metadata
        .labels
        .insert("untrusted".to_owned(), "ignored".to_owned());
    status_update.status.ready = false;
    let status_response = client
        .put(format!("{collection}/node-a/status"))
        .json(&status_update)
        .send()
        .await
        .expect("status update completes");
    assert_eq!(status_response.status(), reqwest::StatusCode::OK);
    let updated: Node = status_response
        .json()
        .await
        .expect("typed status update response");
    assert!(!updated.status.ready);
    assert!(!updated.spec.unschedulable);
    assert!(!updated.metadata.labels.contains_key("untrusted"));
    assert_ne!(
        updated.metadata.resource_version,
        created.metadata.resource_version
    );

    let event = watch
        .next()
        .await
        .expect("status update produces a watch event")
        .expect("watch event payload is valid");
    let event: serde_json::Value = serde_json::from_slice(&event).expect("watch event is JSON");
    assert_eq!(event["type"], "MODIFIED");
    assert_eq!(event["object"]["status"]["ready"], false);
    assert_eq!(event["object"]["spec"]["unschedulable"], false);

    let status_get = client
        .get(format!("{collection}/node-a/status"))
        .send()
        .await
        .expect("status get completes");
    assert_eq!(status_get.status(), reqwest::StatusCode::OK);
    let observed: Node = status_get.json().await.expect("typed status response");
    assert_eq!(observed, updated);

    let stale_response = client
        .put(format!("{collection}/node-a/status"))
        .json(&created)
        .send()
        .await
        .expect("stale status update completes");
    assert_eq!(stale_response.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(status(stale_response).await.reason, StatusReason::Conflict);
}

#[tokio::test]
async fn core_v1_node_crud_is_executable_with_server_owned_status_and_cas() {
    let server = start_server().await;
    let client = reqwest::Client::new();
    let collection = format!("{}/api/v1/nodes", server.base_url);

    let created_response = client
        .post(&collection)
        .json(&node("node-a"))
        .send()
        .await
        .expect("create completes");
    assert_eq!(created_response.status(), reqwest::StatusCode::CREATED);
    let created: Node = created_response.json().await.expect("typed Node response");
    assert!(created.status.ready);
    assert!(created.metadata.uid.is_some());

    let list_response = client
        .get(format!("{collection}?labelSelector=role=worker"))
        .send()
        .await
        .expect("list completes");
    assert_eq!(list_response.status(), reqwest::StatusCode::OK);
    let list: NodeList = list_response.json().await.expect("typed NodeList response");
    assert_eq!(list.items.len(), 1);

    let mut update = created.clone();
    update.spec = NodeSpec {
        unschedulable: true,
    };
    update.status.ready = false;
    let update_response = client
        .put(format!("{collection}/node-a"))
        .json(&update)
        .send()
        .await
        .expect("update completes");
    assert_eq!(update_response.status(), reqwest::StatusCode::OK);
    let updated: Node = update_response
        .json()
        .await
        .expect("typed Node update response");
    assert!(updated.spec.unschedulable);
    assert!(updated.status.ready);
    assert_ne!(
        updated.metadata.resource_version,
        created.metadata.resource_version
    );

    let stale_response = client
        .put(format!("{collection}/node-a"))
        .json(&update)
        .send()
        .await
        .expect("stale update completes");
    assert_eq!(stale_response.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(status(stale_response).await.reason, StatusReason::Conflict);

    let delete_response = client
        .delete(format!("{collection}/node-a"))
        .send()
        .await
        .expect("delete completes");
    assert_eq!(delete_response.status(), reqwest::StatusCode::OK);
    let missing_response = client
        .get(format!("{collection}/node-a"))
        .send()
        .await
        .expect("get completes");
    assert_eq!(missing_response.status(), reqwest::StatusCode::NOT_FOUND);
}
