use std::sync::Arc;

use rusternetes_api_server::router;
use rusternetes_api_types::{ApiStatus, ConfigMap, ConfigMapList, ObjectMeta, TypeMeta};
use rusternetes_common::StatusReason;
use rusternetes_storage::InMemoryConfigMapStore;
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

fn request_config_map(name: &str, namespace: Option<&str>, tier: &str) -> ConfigMap {
    ConfigMap {
        type_meta: TypeMeta::config_map(),
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            namespace: namespace.map(str::to_owned),
            labels: [("tier".to_owned(), tier.to_owned())].into_iter().collect(),
            ..ObjectMeta::default()
        },
        data: [("mode".to_owned(), "safe".to_owned())]
            .into_iter()
            .collect(),
        ..ConfigMap::default()
    }
}

async fn response_status(response: reqwest::Response) -> ApiStatus {
    response
        .json()
        .await
        .unwrap_or_else(|error| panic!("error response uses Kubernetes Status JSON: {error}"))
}

#[tokio::test]
async fn client_observes_full_config_map_crud_and_concurrency_semantics() {
    let server = start_server().await;
    let client = reqwest::Client::new();
    let collection = format!("{}/api/v1/namespaces/default/configmaps", server.base_url);

    let created_response = client
        .post(&collection)
        .json(&request_config_map("application-settings", None, "api"))
        .send()
        .await
        .unwrap_or_else(|error| panic!("create request succeeds: {error}"));
    assert_eq!(created_response.status(), reqwest::StatusCode::CREATED);
    let created: ConfigMap = created_response
        .json()
        .await
        .unwrap_or_else(|error| panic!("create response is ConfigMap JSON: {error}"));
    assert_eq!(created.metadata.namespace.as_deref(), Some("default"));
    assert!(created.metadata.uid.is_some());
    assert_eq!(created.metadata.resource_version.as_deref(), Some("1"));

    let duplicate_response = client
        .post(&collection)
        .json(&request_config_map(
            "application-settings",
            Some("default"),
            "api",
        ))
        .send()
        .await
        .unwrap_or_else(|error| panic!("duplicate create request completes: {error}"));
    assert_eq!(duplicate_response.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(
        response_status(duplicate_response).await.reason,
        StatusReason::AlreadyExists
    );

    let second_response = client
        .post(&collection)
        .json(&request_config_map(
            "worker-settings",
            Some("default"),
            "worker",
        ))
        .send()
        .await
        .unwrap_or_else(|error| panic!("second create request succeeds: {error}"));
    assert_eq!(second_response.status(), reqwest::StatusCode::CREATED);

    let list_response = client
        .get(&collection)
        .query(&[
            ("labelSelector", "tier in (api,worker)"),
            ("fieldSelector", "metadata.name=application-settings"),
        ])
        .send()
        .await
        .unwrap_or_else(|error| panic!("list request succeeds: {error}"));
    assert_eq!(list_response.status(), reqwest::StatusCode::OK);
    let list: ConfigMapList = list_response
        .json()
        .await
        .unwrap_or_else(|error| panic!("list response is ConfigMapList JSON: {error}"));
    assert_eq!(list.items.len(), 1);
    assert_eq!(
        list.items[0].metadata.name.as_deref(),
        Some("application-settings")
    );
    assert_eq!(list.metadata.resource_version.as_deref(), Some("2"));

    let resource_url = format!("{collection}/application-settings");
    let mut first_update = created.clone();
    first_update
        .data
        .insert("mode".to_owned(), "first".to_owned());
    let first_update_response = client
        .put(&resource_url)
        .json(&first_update)
        .send()
        .await
        .unwrap_or_else(|error| panic!("conditional update request succeeds: {error}"));
    assert_eq!(first_update_response.status(), reqwest::StatusCode::OK);
    let first_update: ConfigMap = first_update_response
        .json()
        .await
        .unwrap_or_else(|error| panic!("update response is ConfigMap JSON: {error}"));
    assert_eq!(first_update.metadata.resource_version.as_deref(), Some("3"));

    let mut stale_update = created;
    stale_update
        .data
        .insert("mode".to_owned(), "stale".to_owned());
    let stale_response = client
        .put(&resource_url)
        .json(&stale_update)
        .send()
        .await
        .unwrap_or_else(|error| panic!("stale update request completes: {error}"));
    assert_eq!(stale_response.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(
        response_status(stale_response).await.reason,
        StatusReason::Conflict
    );

    let mut unconditional_update = first_update;
    unconditional_update.metadata.resource_version = None;
    unconditional_update
        .data
        .insert("mode".to_owned(), "unconditional".to_owned());
    let unconditional_response = client
        .put(&resource_url)
        .json(&unconditional_update)
        .send()
        .await
        .unwrap_or_else(|error| panic!("unconditional update request completes: {error}"));
    assert_eq!(unconditional_response.status(), reqwest::StatusCode::OK);
    let unconditional: ConfigMap = unconditional_response
        .json()
        .await
        .unwrap_or_else(|error| panic!("unconditional response is ConfigMap JSON: {error}"));
    assert_eq!(
        unconditional.data.get("mode"),
        Some(&"unconditional".to_owned())
    );

    let delete_conflict = client
        .delete(&resource_url)
        .json(&serde_json::json!({
            "preconditions": {"resourceVersion": "2"}
        }))
        .send()
        .await
        .unwrap_or_else(|error| panic!("conditional delete request completes: {error}"));
    assert_eq!(delete_conflict.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(
        response_status(delete_conflict).await.reason,
        StatusReason::Conflict
    );

    let deleted_response = client
        .delete(&resource_url)
        .json(&serde_json::json!({
            "preconditions": {
                "uid": unconditional.metadata.uid,
                "resourceVersion": unconditional.metadata.resource_version
            }
        }))
        .send()
        .await
        .unwrap_or_else(|error| panic!("delete request completes: {error}"));
    assert_eq!(deleted_response.status(), reqwest::StatusCode::OK);
    let deleted: ApiStatus = deleted_response
        .json()
        .await
        .unwrap_or_else(|error| panic!("delete response is Status JSON: {error}"));
    assert_eq!(deleted.status, "Success");
    assert_eq!(deleted.reason, StatusReason::Success);

    let missing_response = client
        .get(&resource_url)
        .send()
        .await
        .unwrap_or_else(|error| panic!("missing GET request completes: {error}"));
    assert_eq!(missing_response.status(), reqwest::StatusCode::NOT_FOUND);
    assert_eq!(
        response_status(missing_response).await.reason,
        StatusReason::NotFound
    );
}

#[tokio::test]
async fn client_can_list_all_namespaces() {
    let server = start_server().await;
    let client = reqwest::Client::new();

    for (namespace, name) in [("default", "in-default"), ("other", "in-other")] {
        let response = client
            .post(format!(
                "{}/api/v1/namespaces/{namespace}/configmaps",
                server.base_url
            ))
            .json(&request_config_map(name, Some(namespace), "api"))
            .send()
            .await
            .unwrap_or_else(|error| panic!("create in {namespace} succeeds: {error}"));
        assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    }

    let all_namespaces_response = client
        .get(format!("{}/api/v1/configmaps", server.base_url))
        .query(&[("fieldSelector", "metadata.namespace!=kube-system")])
        .send()
        .await
        .unwrap_or_else(|error| panic!("all namespaces list succeeds: {error}"));
    let list: ConfigMapList = all_namespaces_response
        .json()
        .await
        .unwrap_or_else(|error| panic!("all namespaces list is ConfigMapList JSON: {error}"));
    assert_eq!(list.items.len(), 2);
}
