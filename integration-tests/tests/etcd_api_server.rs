use std::{
    fs,
    net::TcpListener as StdTcpListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::Arc,
    thread,
    time::Duration,
};

use rusternetes_api_server::{router_with_backend, ConfigMapBackend};
use rusternetes_api_types::{ApiStatus, ConfigMap, ConfigMapList, ObjectMeta, TypeMeta};
use rusternetes_common::StatusReason;
use rusternetes_storage_etcd::EtcdConfigMapRepository;
use tokio::{net::TcpListener, task::JoinHandle};
use uuid::Uuid;

struct EphemeralEtcd {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for EphemeralEtcd {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = fs::remove_dir_all(&self.data_dir);
    }
}

struct TestServer {
    base_url: String,
    task: JoinHandle<()>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn unused_port() -> u16 {
    let listener = StdTcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("ephemeral TCP port binds: {error}"));
    listener
        .local_addr()
        .unwrap_or_else(|error| panic!("listener reports local address: {error}"))
        .port()
}

async fn start_etcd() -> (EphemeralEtcd, String) {
    let client_port = unused_port();
    let peer_port = unused_port();
    let client_url = format!("http://127.0.0.1:{client_port}");
    let peer_url = format!("http://127.0.0.1:{peer_port}");
    let data_dir = std::env::temp_dir().join(format!("rusternetes-http-etcd-{}", Uuid::new_v4()));
    let child = Command::new("etcd")
        .args([
            "--name",
            "rusternetes-http-test",
            "--data-dir",
            data_dir
                .to_str()
                .unwrap_or_else(|| panic!("temporary etcd data path is UTF-8")),
            "--listen-client-urls",
            &client_url,
            "--advertise-client-urls",
            &client_url,
            "--listen-peer-urls",
            &peer_url,
            "--initial-advertise-peer-urls",
            &peer_url,
            "--initial-cluster",
            &format!("rusternetes-http-test={peer_url}"),
            "--initial-cluster-state",
            "new",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap_or_else(|error| panic!("etcd process starts: {error}"));
    let guard = EphemeralEtcd { child, data_dir };

    for _ in 0..50 {
        if let Ok(repository) = EtcdConfigMapRepository::connect([client_url.as_str()], None).await
        {
            if repository
                .list_all(
                    &rusternetes_api_types::LabelSelector::default(),
                    &rusternetes_api_types::FieldSelector::default(),
                )
                .await
                .is_ok()
            {
                return (guard, client_url);
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("ephemeral etcd accepts real KV operations before timeout");
}

async fn start_server(repository: Arc<EtcdConfigMapRepository>) -> TestServer {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("HTTP listener binds: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("HTTP listener reports address: {error}"));
    let application = router_with_backend(ConfigMapBackend::Etcd(repository));
    let task = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, application).await {
            panic!("etcd-backed API Server fails: {error}");
        }
    });
    TestServer {
        base_url: format!("http://{address}"),
        task,
    }
}

fn config_map(name: &str, namespace: &str, tier: &str) -> ConfigMap {
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

#[tokio::test]
async fn http_crud_uses_real_etcd_storage_without_falling_back_to_memory() {
    let (_etcd, endpoint) = start_etcd().await;
    let prefix = format!("/rusternetes-http-tests/{}", Uuid::new_v4());
    let repository = Arc::new(
        EtcdConfigMapRepository::connect([endpoint.as_str()], Some(&prefix))
            .await
            .expect("etcd repository connects"),
    );
    let server = start_server(repository.clone()).await;
    let client = reqwest::Client::new();
    let collection = format!("{}/api/v1/namespaces/default/configmaps", server.base_url);

    let created_response = client
        .post(&collection)
        .json(&config_map("settings", "default", "api"))
        .send()
        .await
        .unwrap_or_else(|error| panic!("HTTP create completes: {error}"));
    assert_eq!(created_response.status(), reqwest::StatusCode::CREATED);
    let created: ConfigMap = created_response
        .json()
        .await
        .unwrap_or_else(|error| panic!("create response is ConfigMap: {error}"));
    assert!(
        created
            .metadata
            .resource_version
            .as_deref()
            .expect("server assigns resourceVersion")
            .parse::<u64>()
            .expect("etcd revision is numeric")
            > 0
    );

    let persisted = repository
        .get("default", "settings")
        .await
        .expect("direct etcd repository reads API-created resource");
    assert_eq!(persisted.metadata.uid, created.metadata.uid);

    let other_response = client
        .post(format!(
            "{}/api/v1/namespaces/other/configmaps",
            server.base_url
        ))
        .json(&config_map("other-settings", "other", "worker"))
        .send()
        .await
        .unwrap_or_else(|error| panic!("cross-namespace create completes: {error}"));
    assert_eq!(other_response.status(), reqwest::StatusCode::CREATED);

    let all_response = client
        .get(format!("{}/api/v1/configmaps", server.base_url))
        .query(&[("labelSelector", "tier in (api,worker)")])
        .send()
        .await
        .unwrap_or_else(|error| panic!("all-namespaces list completes: {error}"));
    assert_eq!(all_response.status(), reqwest::StatusCode::OK);
    let list: ConfigMapList = all_response
        .json()
        .await
        .unwrap_or_else(|error| panic!("list response is ConfigMapList: {error}"));
    assert_eq!(list.items.len(), 2);

    let resource_url = format!("{collection}/settings");
    let mut updated_request = created.clone();
    updated_request
        .data
        .insert("mode".to_owned(), "durable".to_owned());
    let updated_response = client
        .put(&resource_url)
        .json(&updated_request)
        .send()
        .await
        .unwrap_or_else(|error| panic!("HTTP update completes: {error}"));
    assert_eq!(updated_response.status(), reqwest::StatusCode::OK);
    let updated: ConfigMap = updated_response
        .json()
        .await
        .unwrap_or_else(|error| panic!("update response is ConfigMap: {error}"));
    assert_ne!(
        updated.metadata.resource_version,
        created.metadata.resource_version
    );

    let mut stale = created;
    stale.data.insert("mode".to_owned(), "stale".to_owned());
    let stale_response = client
        .put(&resource_url)
        .json(&stale)
        .send()
        .await
        .unwrap_or_else(|error| panic!("stale update completes: {error}"));
    assert_eq!(stale_response.status(), reqwest::StatusCode::CONFLICT);

    let watch_response = client
        .get(&collection)
        .query(&[("watch", "true")])
        .send()
        .await
        .unwrap_or_else(|error| panic!("watch request completes: {error}"));
    assert_eq!(watch_response.status(), reqwest::StatusCode::BAD_REQUEST);
    let watch_status: ApiStatus = watch_response
        .json()
        .await
        .unwrap_or_else(|error| panic!("watch rejection is Kubernetes Status: {error}"));
    assert_eq!(watch_status.reason, StatusReason::BadRequest);

    let deleted_response = client
        .delete(&resource_url)
        .send()
        .await
        .unwrap_or_else(|error| panic!("delete completes: {error}"));
    assert_eq!(deleted_response.status(), reqwest::StatusCode::OK);
    assert!(repository.get("default", "settings").await.is_err());
}
