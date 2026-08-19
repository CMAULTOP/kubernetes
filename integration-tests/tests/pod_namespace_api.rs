use rusternetes_api_server::{core_backend_from_etcd_config, router_with_core_backend};
use rusternetes_api_types::{
    ApiStatus, Container, Namespace, NamespaceList, ObjectMeta, Pod, PodList, PodPhase, PodSpec,
    ServiceAccount, TypeMeta,
};
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
    let application = router_with_core_backend(backend);
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

fn pod(name: &str, namespace: Option<&str>, image: &str) -> Pod {
    Pod {
        type_meta: TypeMeta::pod(),
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            namespace: namespace.map(str::to_owned),
            labels: [("tier".to_owned(), "api".to_owned())]
                .into_iter()
                .collect(),
            ..ObjectMeta::default()
        },
        spec: PodSpec {
            containers: vec![Container {
                name: "web".to_owned(),
                image: Some(image.to_owned()),
                ..Container::default()
            }],
            ..PodSpec::default()
        },
        ..Pod::default()
    }
}

fn service_account(name: &str, namespace: Option<&str>) -> ServiceAccount {
    ServiceAccount {
        type_meta: TypeMeta::service_account(),
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            namespace: namespace.map(str::to_owned),
            labels: [("team".to_owned(), "platform".to_owned())]
                .into_iter()
                .collect(),
            ..ObjectMeta::default()
        },
        ..ServiceAccount::default()
    }
}

fn namespace(name: &str) -> Namespace {
    Namespace {
        type_meta: TypeMeta::namespace(),
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            ..ObjectMeta::default()
        },
        ..Namespace::default()
    }
}

async fn response_status(response: reqwest::Response) -> ApiStatus {
    response
        .json()
        .await
        .unwrap_or_else(|error| panic!("error response uses Kubernetes Status JSON: {error}"))
}

#[tokio::test]
async fn service_account_crud_is_executable_with_cas_and_typed_metadata() {
    let server = start_server().await;
    let client = reqwest::Client::new();
    let collection = format!(
        "{}/api/v1/namespaces/default/serviceaccounts",
        server.base_url
    );
    let created_response = client
        .post(&collection)
        .json(&service_account("build-robot", None))
        .send()
        .await
        .expect("ServiceAccount create completes");
    assert_eq!(created_response.status(), reqwest::StatusCode::CREATED);
    let created: ServiceAccount = created_response.json().await.expect("typed response");
    assert!(created.metadata.uid.is_some());
    assert!(created.metadata.resource_version.is_some());

    let listed_response = client
        .get(&collection)
        .send()
        .await
        .expect("list completes");
    assert_eq!(listed_response.status(), reqwest::StatusCode::OK);
    let listed: serde_json::Value = listed_response.json().await.expect("list is JSON");
    assert_eq!(listed["items"].as_array().map(Vec::len), Some(1));

    let mut update = created.clone();
    update.automount_service_account_token = Some(false);
    let update_response = client
        .put(format!("{collection}/build-robot"))
        .json(&update)
        .send()
        .await
        .expect("update completes");
    assert_eq!(update_response.status(), reqwest::StatusCode::OK);
    let updated: ServiceAccount = update_response.json().await.expect("typed update response");
    assert_eq!(updated.automount_service_account_token, Some(false));
    assert_ne!(
        updated.metadata.resource_version,
        created.metadata.resource_version
    );

    let stale_response = client
        .put(format!("{collection}/build-robot"))
        .json(&created)
        .send()
        .await
        .expect("stale update completes");
    assert_eq!(stale_response.status(), reqwest::StatusCode::CONFLICT);
}

#[tokio::test]
async fn namespace_lifecycle_admission_and_typed_pod_api_are_executable_end_to_end() {
    let server = start_server().await;
    let client = reqwest::Client::new();

    let missing_response = client
        .post(format!(
            "{}/api/v1/namespaces/missing/pods",
            server.base_url
        ))
        .json(&pod("blocked", None, "example:v1"))
        .send()
        .await
        .unwrap_or_else(|error| panic!("missing namespace request completes: {error}"));
    assert_eq!(missing_response.status(), reqwest::StatusCode::NOT_FOUND);
    let missing_status = response_status(missing_response).await;
    assert_eq!(missing_status.reason, StatusReason::NotFound);

    let namespace_response = client
        .post(format!("{}/api/v1/namespaces", server.base_url))
        .json(&namespace("development"))
        .send()
        .await
        .unwrap_or_else(|error| panic!("Namespace create request completes: {error}"));
    assert_eq!(namespace_response.status(), reqwest::StatusCode::CREATED);
    let created_namespace: Namespace = namespace_response
        .json()
        .await
        .unwrap_or_else(|error| panic!("Namespace response is typed JSON: {error}"));
    assert_eq!(
        created_namespace
            .metadata
            .labels
            .get(Namespace::NAME_LABEL)
            .map(String::as_str),
        Some("development")
    );

    let collection = format!("{}/api/v1/namespaces/development/pods", server.base_url);
    let created_response = client
        .post(&collection)
        .json(&pod("web", None, "example:v1"))
        .send()
        .await
        .unwrap_or_else(|error| panic!("Pod create request completes: {error}"));
    assert_eq!(created_response.status(), reqwest::StatusCode::CREATED);
    let created: Pod = created_response
        .json()
        .await
        .unwrap_or_else(|error| panic!("Pod response is typed JSON: {error}"));
    assert_eq!(created.metadata.namespace.as_deref(), Some("development"));
    assert_eq!(created.status.phase, Some(PodPhase::Pending));
    assert!(created.metadata.uid.is_some());

    let list_response = client
        .get(format!(
            "{}/api/v1/pods?labelSelector=tier=api",
            server.base_url
        ))
        .send()
        .await
        .unwrap_or_else(|error| panic!("all-namespaces Pod list completes: {error}"));
    assert_eq!(list_response.status(), reqwest::StatusCode::OK);
    let list: PodList = list_response
        .json()
        .await
        .unwrap_or_else(|error| panic!("Pod list is typed JSON: {error}"));
    assert_eq!(list.items.len(), 1);
    assert_eq!(list.items[0].metadata.name.as_deref(), Some("web"));

    let mut invalid_update = created;
    invalid_update.spec.containers[0].image = Some("example:v2".to_owned());
    let update_response = client
        .put(format!("{collection}/web"))
        .json(&invalid_update)
        .send()
        .await
        .unwrap_or_else(|error| panic!("Pod update request completes: {error}"));
    assert_eq!(
        update_response.status(),
        reqwest::StatusCode::UNPROCESSABLE_ENTITY
    );
    let update_status = response_status(update_response).await;
    assert_eq!(update_status.reason, StatusReason::Invalid);

    let namespaces_response = client
        .get(format!("{}/api/v1/namespaces", server.base_url))
        .send()
        .await
        .unwrap_or_else(|error| panic!("Namespace list completes: {error}"));
    assert_eq!(namespaces_response.status(), reqwest::StatusCode::OK);
    let namespaces: NamespaceList = namespaces_response
        .json()
        .await
        .unwrap_or_else(|error| panic!("Namespace list is typed JSON: {error}"));
    assert!(namespaces
        .items
        .iter()
        .any(|item| item.metadata.name.as_deref() == Some("default")));
    assert!(namespaces
        .items
        .iter()
        .any(|item| item.metadata.name.as_deref() == Some("development")));
}
