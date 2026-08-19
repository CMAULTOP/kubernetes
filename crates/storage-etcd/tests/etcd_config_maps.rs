use std::{
    collections::BTreeMap,
    fs,
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
    thread,
    time::Duration,
};

use rusternetes_api_types::{
    ConfigMap, DeleteOptions, FieldSelector, LabelSelector, ObjectMeta, TypeMeta,
};
use rusternetes_common::ApiError;
use rusternetes_storage::ConfigMapWatchRequest;
use rusternetes_storage_etcd::EtcdConfigMapRepository;
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

fn unused_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0")
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
    let data_dir = std::env::temp_dir().join(format!("rusternetes-etcd-{}", Uuid::new_v4()));
    let child = Command::new("etcd")
        .args([
            "--name",
            "rusternetes-test",
            "--data-dir",
            data_dir
                .to_str()
                .unwrap_or_else(|| panic!("temporary etcd path is valid UTF-8")),
            "--listen-client-urls",
            &client_url,
            "--advertise-client-urls",
            &client_url,
            "--listen-peer-urls",
            &peer_url,
            "--initial-advertise-peer-urls",
            &peer_url,
            "--initial-cluster",
            &format!("rusternetes-test={peer_url}"),
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
                .list(
                    "readiness",
                    &LabelSelector::default(),
                    &FieldSelector::default(),
                )
                .await
                .is_ok()
            {
                return (guard, client_url);
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("ephemeral etcd accepts gRPC connections before timeout");
}

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
async fn persists_config_maps_with_etcd_revisions_and_transactional_conflicts() {
    let (_etcd, endpoint) = start_etcd().await;
    let prefix = format!("/rusternetes-integration-tests/{}", Uuid::new_v4());
    let repository = EtcdConfigMapRepository::connect([endpoint.as_str()], Some(&prefix))
        .await
        .expect("repository connects through real etcd gRPC API");

    let created = repository
        .create(config_map("settings"))
        .await
        .expect("create persists to etcd");
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
    assert!(matches!(
        repository.create(config_map("settings")).await,
        Err(ApiError::AlreadyExists { .. })
    ));

    let retrieved = repository
        .get("default", "settings")
        .await
        .expect("new repository read decodes persisted JSON");
    assert_eq!(retrieved.metadata.uid, created.metadata.uid);
    assert_eq!(
        retrieved.metadata.resource_version,
        created.metadata.resource_version
    );

    let label_selector = LabelSelector::parse(Some("tier=api")).expect("valid selector");
    let field_selector =
        FieldSelector::parse(Some("metadata.name=settings")).expect("valid selector");
    let list = repository
        .list("default", &label_selector, &field_selector)
        .await
        .expect("namespaced prefix list succeeds");
    assert_eq!(list.items.len(), 1);
    assert_eq!(list.items[0].metadata.name.as_deref(), Some("settings"));

    let mut updated_request = retrieved.clone();
    updated_request
        .data
        .insert("mode".to_owned(), "durable".to_owned());
    let updated = repository
        .update(updated_request)
        .await
        .expect("mod-revision compare protects update");
    assert_ne!(
        updated.metadata.resource_version,
        created.metadata.resource_version
    );
    assert_eq!(updated.data.get("mode"), Some(&"durable".to_owned()));

    let mut stale_request = created;
    stale_request
        .data
        .insert("mode".to_owned(), "stale".to_owned());
    assert!(matches!(
        repository.update(stale_request).await,
        Err(ApiError::Conflict { .. })
    ));

    let delete = repository
        .delete("default", "settings", DeleteOptions::default())
        .await
        .expect("conditional delete persists through etcd transaction");
    assert!(
        delete
            .resource_version
            .parse::<u64>()
            .expect("delete revision is numeric")
            > updated
                .metadata
                .resource_version
                .as_deref()
                .expect("update has revision")
                .parse::<u64>()
                .expect("update revision is numeric")
    );
    assert!(matches!(
        repository.get("default", "settings").await,
        Err(ApiError::NotFound { .. })
    ));
}

#[tokio::test]
async fn compacted_etcd_history_is_reported_as_kubernetes_resource_expired() {
    let (_etcd, endpoint) = start_etcd().await;
    let prefix = format!("/rusternetes-watch-compaction-tests/{}", Uuid::new_v4());
    let repository = EtcdConfigMapRepository::connect([endpoint.as_str()], Some(&prefix))
        .await
        .expect("repository connects through real etcd gRPC API");
    let created = repository
        .create(config_map("settings"))
        .await
        .expect("create persists one revision");
    let revision = created
        .metadata
        .resource_version
        .as_deref()
        .expect("created object has etcd revision")
        .parse::<i64>()
        .expect("resource version is numeric");

    let mut raw_client = etcd_client::Client::connect([endpoint.as_str()], None)
        .await
        .expect("raw maintenance client connects");
    raw_client
        .compact(revision, None)
        .await
        .expect("etcd compacts historical revisions");

    assert!(matches!(
        repository
            .watch(ConfigMapWatchRequest {
                resource_version: Some("0".to_owned()),
                ..ConfigMapWatchRequest::default()
            })
            .await,
        Err(ApiError::ResourceExpired { .. })
    ));
}
