use std::{
    fs,
    net::TcpListener as StdTcpListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::Arc,
    thread,
    time::Duration,
};

use rusternetes_admission::{
    AdmissionChain, NamespaceLifecyclePlugin, NamespacePhase, StaticNamespaceStateReader,
};
use rusternetes_api_server::{
    core_backend_from_etcd_config, router_with_backend_auth_authorization_and_admission,
    router_with_core_backend_and_auth, AuthorizationMode, ConfigMapBackend, CoreApiBackend,
};
use rusternetes_api_types::{
    ApiStatus, ConfigMap, ConfigMapList, Container, Namespace, Node, ObjectMeta, Pod, PodPhase,
    PodSpec, ServiceAccount, TypeMeta,
};
use rusternetes_authn::{
    AnonymousPolicy, AuthenticationChain, RequestIdentity, ServiceAccountJwtKey,
    ServiceAccountJwtVerifier, ServiceAccountTokenIssuer, StaticBearerToken,
};
use rusternetes_authz_rbac::{
    ClusterRole, ClusterRoleBinding, PolicyRule, RbacAuthorizer, Role, RoleBinding, RoleRef,
    Subject,
};
use rusternetes_common::StatusReason;
use rusternetes_storage_etcd::{
    EtcdConfigMapRepository, EtcdNamespaceRepository, EtcdNodeRepository, EtcdPodRepository,
    EtcdServiceAccountRepository,
};
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

async fn start_core_server(backend: CoreApiBackend) -> TestServer {
    start_core_server_with_auth(backend, AuthenticationChain::default()).await
}

async fn start_core_server_with_auth(
    backend: CoreApiBackend,
    authentication: AuthenticationChain,
) -> TestServer {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("core HTTP listener binds: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("core HTTP listener reports address: {error}"));
    let application = router_with_core_backend_and_auth(backend, authentication);
    let task = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, application).await {
            panic!("etcd-backed core API Server fails: {error}");
        }
    });
    TestServer {
        base_url: format!("http://{address}"),
        task,
    }
}

async fn start_server(repository: Arc<EtcdConfigMapRepository>) -> TestServer {
    start_server_with_auth(repository, AuthenticationChain::default()).await
}

async fn start_server_with_auth(
    repository: Arc<EtcdConfigMapRepository>,
    authentication: AuthenticationChain,
) -> TestServer {
    start_server_with_authorization(repository, authentication, AuthorizationMode::AlwaysAllow)
        .await
}

async fn start_server_with_authorization(
    repository: Arc<EtcdConfigMapRepository>,
    authentication: AuthenticationChain,
    authorization: AuthorizationMode,
) -> TestServer {
    start_server_with_authorization_and_admission(
        repository,
        authentication,
        authorization,
        AdmissionChain::default(),
    )
    .await
}

async fn start_server_with_authorization_and_admission(
    repository: Arc<EtcdConfigMapRepository>,
    authentication: AuthenticationChain,
    authorization: AuthorizationMode,
    admission: AdmissionChain,
) -> TestServer {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap_or_else(|error| panic!("HTTP listener binds: {error}"));
    let address = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("HTTP listener reports address: {error}"));
    let application = router_with_backend_auth_authorization_and_admission(
        ConfigMapBackend::Etcd(repository),
        authentication,
        authorization,
        admission,
    );
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

fn pod(name: &str, namespace: &str) -> Pod {
    Pod {
        type_meta: TypeMeta::pod(),
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            namespace: Some(namespace.to_owned()),
            ..ObjectMeta::default()
        },
        spec: PodSpec {
            containers: vec![Container {
                name: "app".to_owned(),
                image: Some("example:v1".to_owned()),
                ..Container::default()
            }],
            ..PodSpec::default()
        },
        ..Pod::default()
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

    let mut stale = created.clone();
    stale.data.insert("mode".to_owned(), "stale".to_owned());
    let stale_response = client
        .put(&resource_url)
        .json(&stale)
        .send()
        .await
        .unwrap_or_else(|error| panic!("stale update completes: {error}"));
    assert_eq!(stale_response.status(), reqwest::StatusCode::CONFLICT);

    let mut watch_response = client
        .get(&collection)
        .query(&[
            ("watch", "true"),
            (
                "resourceVersion",
                created
                    .metadata
                    .resource_version
                    .as_deref()
                    .expect("created resource has a version"),
            ),
            ("labelSelector", "tier=api"),
            ("fieldSelector", "metadata.name=settings"),
        ])
        .send()
        .await
        .unwrap_or_else(|error| panic!("durable watch request completes: {error}"));
    assert_eq!(watch_response.status(), reqwest::StatusCode::OK);

    let replayed_chunk = watch_response
        .chunk()
        .await
        .unwrap_or_else(|error| panic!("historical watch event arrives: {error}"))
        .expect("historical watch body contains an event");
    let replayed: serde_json::Value = serde_json::from_slice(&replayed_chunk)
        .unwrap_or_else(|error| panic!("historical watch event is JSON: {error}"));
    assert_eq!(replayed["type"], "MODIFIED");
    assert_eq!(
        replayed["object"]["metadata"]["resourceVersion"],
        updated
            .metadata
            .resource_version
            .as_deref()
            .expect("updated resource has a version")
    );

    let deleted_response = client
        .delete(&resource_url)
        .send()
        .await
        .unwrap_or_else(|error| panic!("delete completes: {error}"));
    assert_eq!(deleted_response.status(), reqwest::StatusCode::OK);
    let live_chunk = watch_response
        .chunk()
        .await
        .unwrap_or_else(|error| panic!("live watch event arrives: {error}"))
        .expect("live watch body contains a delete event");
    let live: serde_json::Value = serde_json::from_slice(&live_chunk)
        .unwrap_or_else(|error| panic!("live watch event is JSON: {error}"));
    assert_eq!(live["type"], "DELETED");
    assert_eq!(live["object"]["metadata"]["name"], "settings");
    assert!(repository.get("default", "settings").await.is_err());
}

#[tokio::test]
async fn node_status_update_is_durable_cas_guarded_and_watch_visible() {
    let (_etcd, endpoint) = start_etcd().await;
    let prefix = format!("/rusternetes-http-node-status/{}", Uuid::new_v4());
    let backend = core_backend_from_etcd_config(Some(&endpoint), Some(&prefix))
        .await
        .expect("durable core backend initializes");
    let nodes = EtcdNodeRepository::connect([endpoint.as_str()], Some(&format!("{prefix}/nodes")))
        .await
        .expect("direct Node repository connects");
    let server = start_core_server(backend).await;
    let client = reqwest::Client::new();
    let collection = format!("{}/api/v1/nodes", server.base_url);

    let created_response = client
        .post(&collection)
        .json(&node("node-a"))
        .send()
        .await
        .expect("Node create completes");
    assert_eq!(created_response.status(), reqwest::StatusCode::CREATED);
    let created: Node = created_response.json().await.expect("typed Node response");
    let resource_version = created
        .metadata
        .resource_version
        .clone()
        .expect("created Node has resourceVersion");

    let mut watch = client
        .get(&collection)
        .query(&[
            ("watch", "true"),
            ("resourceVersion", resource_version.as_str()),
        ])
        .send()
        .await
        .expect("durable Node watch opens");
    assert_eq!(watch.status(), reqwest::StatusCode::OK);

    let mut status_update = created.clone();
    status_update.status.ready = false;
    status_update.spec.unschedulable = true;
    let update_response = client
        .put(format!("{collection}/node-a/status"))
        .json(&status_update)
        .send()
        .await
        .expect("durable status update completes");
    assert_eq!(update_response.status(), reqwest::StatusCode::OK);
    let updated: Node = update_response
        .json()
        .await
        .expect("typed status update response");
    assert!(!updated.status.ready);
    assert!(!updated.spec.unschedulable);
    assert_ne!(
        updated.metadata.resource_version,
        created.metadata.resource_version
    );

    let event = watch
        .chunk()
        .await
        .expect("durable status watch event arrives")
        .expect("durable status watch body has event");
    let event: serde_json::Value = serde_json::from_slice(&event).expect("watch event is JSON");
    assert_eq!(event["type"], "MODIFIED");
    assert_eq!(event["object"]["status"]["ready"], false);

    let persisted = nodes.get("node-a").await.expect("direct etcd Node read");
    assert_eq!(persisted, updated);

    let stale_response = client
        .put(format!("{collection}/node-a/status"))
        .json(&created)
        .send()
        .await
        .expect("stale durable status update completes");
    assert_eq!(stale_response.status(), reqwest::StatusCode::CONFLICT);
}

#[tokio::test]
async fn node_patch_is_durable_and_preserves_status_projection() {
    let (_etcd, endpoint) = start_etcd().await;
    let prefix = format!("/rusternetes-http-node-patch/{}", Uuid::new_v4());
    let backend = core_backend_from_etcd_config(Some(&endpoint), Some(&prefix))
        .await
        .expect("durable core backend initializes");
    let nodes = EtcdNodeRepository::connect([endpoint.as_str()], Some(&format!("{prefix}/nodes")))
        .await
        .expect("direct Node repository connects");
    let server = start_core_server(backend).await;
    let client = reqwest::Client::new();
    let collection = format!("{}/api/v1/nodes", server.base_url);

    let created_response = client
        .post(&collection)
        .json(&node("node-a"))
        .send()
        .await
        .expect("Node create completes");
    assert_eq!(created_response.status(), reqwest::StatusCode::CREATED);
    let created: Node = created_response.json().await.expect("typed Node response");

    let patch_response = client
        .patch(format!("{collection}/node-a"))
        .header("content-type", "application/merge-patch+json")
        .body(
            r#"{"metadata":{"labels":{"topology.kubernetes.io/zone":"east-a"}},"spec":{"unschedulable":true},"status":{"ready":false}}"#,
        )
        .send()
        .await
        .expect("durable Node PATCH completes");
    assert_eq!(patch_response.status(), reqwest::StatusCode::OK);
    let patched: Node = patch_response.json().await.expect("typed PATCH response");
    assert!(patched.spec.unschedulable);
    assert_eq!(
        patched.metadata.labels.get("topology.kubernetes.io/zone"),
        Some(&"east-a".to_owned())
    );
    assert!(patched.status.ready);
    assert_ne!(
        patched.metadata.resource_version,
        created.metadata.resource_version
    );

    let json_patch_response = client
        .patch(format!("{collection}/node-a"))
        .header("content-type", "application/json-patch+json")
        .body(
            r#"[{"op":"replace","path":"/metadata/labels/topology.kubernetes.io~1zone","value":"east-b"}]"#,
        )
        .send()
        .await
        .expect("durable Node JSON Patch completes");
    assert_eq!(json_patch_response.status(), reqwest::StatusCode::OK);
    let json_patched: Node = json_patch_response
        .json()
        .await
        .expect("typed JSON Patch response");
    assert_eq!(
        json_patched
            .metadata
            .labels
            .get("topology.kubernetes.io/zone"),
        Some(&"east-b".to_owned())
    );
    assert!(json_patched.spec.unschedulable);
    assert!(json_patched.status.ready);

    let persisted = nodes.get("node-a").await.expect("direct etcd Node read");
    assert_eq!(persisted, json_patched);
}

#[tokio::test]
async fn http_watch_reports_410_when_etcd_history_is_compacted() {
    let (_etcd, endpoint) = start_etcd().await;
    let prefix = format!("/rusternetes-http-watch-compaction/{}", Uuid::new_v4());
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
    let created: ConfigMap = created_response
        .json()
        .await
        .unwrap_or_else(|error| panic!("create response is ConfigMap: {error}"));
    let revision = created
        .metadata
        .resource_version
        .as_deref()
        .expect("create response has etcd revision")
        .parse::<i64>()
        .expect("revision is numeric");

    let mut raw_client = etcd_client::Client::connect([endpoint.as_str()], None)
        .await
        .expect("raw maintenance client connects");
    raw_client
        .compact(revision, None)
        .await
        .expect("etcd compacts old history");

    let response = client
        .get(&collection)
        .query(&[("watch", "true"), ("resourceVersion", "0")])
        .send()
        .await
        .unwrap_or_else(|error| panic!("compacted watch request completes: {error}"));
    assert_eq!(response.status(), reqwest::StatusCode::GONE);
    let status: ApiStatus = response
        .json()
        .await
        .unwrap_or_else(|error| panic!("410 response uses Kubernetes Status JSON: {error}"));
    assert_eq!(status.reason, StatusReason::Expired);
    assert_eq!(status.code, 410);
}

#[tokio::test]
async fn http_api_enforces_configured_bearer_authentication_before_etcd_mutation() {
    let (_etcd, endpoint) = start_etcd().await;
    let prefix = format!("/rusternetes-http-authn/{}", Uuid::new_v4());
    let repository = Arc::new(
        EtcdConfigMapRepository::connect([endpoint.as_str()], Some(&prefix))
            .await
            .expect("etcd repository connects"),
    );
    let identity = RequestIdentity::authenticated(
        "cluster-bootstrap",
        Some("bootstrap-uid".to_owned()),
        ["system:bootstrappers".to_owned()],
        Default::default(),
    )
    .expect("identity is valid");
    let token = StaticBearerToken::new("bootstrap-secret", identity).expect("token is valid");
    let server = start_server_with_auth(
        repository.clone(),
        AuthenticationChain::new(AnonymousPolicy::Deny, vec![token]),
    )
    .await;
    let client = reqwest::Client::new();
    let collection = format!("{}/api/v1/namespaces/default/configmaps", server.base_url);

    let anonymous = client
        .post(&collection)
        .json(&config_map("settings", "default", "api"))
        .send()
        .await
        .unwrap_or_else(|error| panic!("unauthenticated request completes: {error}"));
    assert_eq!(anonymous.status(), reqwest::StatusCode::UNAUTHORIZED);
    let status: ApiStatus = anonymous
        .json()
        .await
        .unwrap_or_else(|error| panic!("401 response is Kubernetes Status: {error}"));
    assert_eq!(status.reason, StatusReason::Unauthorized);
    assert!(repository.get("default", "settings").await.is_err());

    let created = client
        .post(&collection)
        .header("authorization", "Bearer bootstrap-secret")
        .json(&config_map("settings", "default", "api"))
        .send()
        .await
        .unwrap_or_else(|error| panic!("authenticated request completes: {error}"));
    assert_eq!(created.status(), reqwest::StatusCode::CREATED);

    let invalid = client
        .get(format!("{collection}/settings"))
        .header("authorization", "Bearer invalid")
        .send()
        .await
        .unwrap_or_else(|error| panic!("invalid bearer request completes: {error}"));
    assert_eq!(invalid.status(), reqwest::StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn http_rbac_allows_only_bound_namespace_after_authentication() {
    let (_etcd, endpoint) = start_etcd().await;
    let prefix = format!("/rusternetes-http-rbac/{}", Uuid::new_v4());
    let repository = Arc::new(
        EtcdConfigMapRepository::connect([endpoint.as_str()], Some(&prefix))
            .await
            .expect("etcd repository connects"),
    );
    let identity = RequestIdentity::authenticated(
        "namespace-editor",
        None,
        std::iter::empty(),
        Default::default(),
    )
    .expect("identity is valid");
    let token = StaticBearerToken::new("editor-secret", identity).expect("token is valid");
    let policy = RbacAuthorizer::new(
        vec![Role {
            namespace: "development".to_owned(),
            name: "configmap-editor".to_owned(),
            rules: vec![PolicyRule {
                api_groups: vec![String::new()],
                resources: vec!["configmaps".to_owned()],
                verbs: vec!["create".to_owned(), "get".to_owned(), "list".to_owned()],
                ..PolicyRule::default()
            }],
        }],
        Vec::new(),
        vec![RoleBinding {
            namespace: "development".to_owned(),
            name: "editor-binding".to_owned(),
            subjects: vec![Subject::User("namespace-editor".to_owned())],
            role_ref: RoleRef::Role("configmap-editor".to_owned()),
        }],
        Vec::new(),
    );
    let server = start_server_with_authorization(
        repository.clone(),
        AuthenticationChain::new(AnonymousPolicy::Deny, vec![token]),
        AuthorizationMode::Rbac(policy),
    )
    .await;
    let client = reqwest::Client::new();

    let allowed = client
        .post(format!(
            "{}/api/v1/namespaces/development/configmaps",
            server.base_url
        ))
        .header("authorization", "Bearer editor-secret")
        .json(&config_map("settings", "development", "api"))
        .send()
        .await
        .unwrap_or_else(|error| panic!("bound namespace request completes: {error}"));
    assert_eq!(allowed.status(), reqwest::StatusCode::CREATED);
    assert!(repository.get("development", "settings").await.is_ok());

    let denied = client
        .post(format!(
            "{}/api/v1/namespaces/production/configmaps",
            server.base_url
        ))
        .header("authorization", "Bearer editor-secret")
        .json(&config_map("settings", "production", "api"))
        .send()
        .await
        .unwrap_or_else(|error| panic!("unbound namespace request completes: {error}"));
    assert_eq!(denied.status(), reqwest::StatusCode::FORBIDDEN);
    let status: ApiStatus = denied
        .json()
        .await
        .unwrap_or_else(|error| panic!("403 response is Kubernetes Status: {error}"));
    assert_eq!(status.reason, StatusReason::Forbidden);
    assert!(repository.get("production", "settings").await.is_err());
}

#[tokio::test]
async fn http_admission_rejects_rbac_authorized_create_in_missing_namespace_before_etcd_mutation() {
    let (_etcd, endpoint) = start_etcd().await;
    let prefix = format!("/rusternetes-http-admission/{}", Uuid::new_v4());
    let repository = Arc::new(
        EtcdConfigMapRepository::connect([endpoint.as_str()], Some(&prefix))
            .await
            .expect("etcd repository connects"),
    );
    let identity = RequestIdentity::authenticated(
        "namespace-writer",
        None,
        std::iter::empty(),
        Default::default(),
    )
    .expect("identity is valid");
    let token = StaticBearerToken::new("writer-secret", identity).expect("token is valid");
    let policy = RbacAuthorizer::new(
        Vec::new(),
        vec![ClusterRole {
            name: "configmap-writer".to_owned(),
            rules: vec![PolicyRule {
                api_groups: vec![String::new()],
                resources: vec!["configmaps".to_owned()],
                verbs: vec!["create".to_owned()],
                ..PolicyRule::default()
            }],
        }],
        Vec::new(),
        vec![ClusterRoleBinding {
            name: "writer-binding".to_owned(),
            subjects: vec![Subject::User("namespace-writer".to_owned())],
            role_ref: "configmap-writer".to_owned(),
        }],
    );
    let namespaces =
        StaticNamespaceStateReader::new([("development".to_owned(), NamespacePhase::Active)]);
    let admission = AdmissionChain::new(vec![Arc::new(NamespaceLifecyclePlugin::new(Arc::new(
        namespaces,
    )))]);
    let server = start_server_with_authorization_and_admission(
        repository.clone(),
        AuthenticationChain::new(AnonymousPolicy::Deny, vec![token]),
        AuthorizationMode::Rbac(policy),
        admission,
    )
    .await;
    let client = reqwest::Client::new();

    let missing = client
        .post(format!(
            "{}/api/v1/namespaces/missing/configmaps",
            server.base_url
        ))
        .header("authorization", "Bearer writer-secret")
        .json(&config_map("missing-settings", "missing", "api"))
        .send()
        .await
        .unwrap_or_else(|error| {
            panic!("RBAC-authorized missing namespace create completes: {error}")
        });
    assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);
    let status: ApiStatus = missing
        .json()
        .await
        .unwrap_or_else(|error| panic!("admission rejection is Kubernetes Status JSON: {error}"));
    assert_eq!(status.reason, StatusReason::NotFound);
    assert!(repository.get("missing", "missing-settings").await.is_err());

    let allowed = client
        .post(format!(
            "{}/api/v1/namespaces/development/configmaps",
            server.base_url
        ))
        .header("authorization", "Bearer writer-secret")
        .json(&config_map("settings", "development", "api"))
        .send()
        .await
        .unwrap_or_else(|error| panic!("active namespace create completes: {error}"));
    assert_eq!(allowed.status(), reqwest::StatusCode::CREATED);
    assert!(repository.get("development", "settings").await.is_ok());
}

#[tokio::test]
async fn http_core_backend_persists_pods_in_real_etcd_without_memory_fallback() {
    let (_etcd, endpoint) = start_etcd().await;
    let root = format!("/rusternetes-core-http-tests/{}", Uuid::new_v4());
    let backend = core_backend_from_etcd_config(Some(&endpoint), Some(&root))
        .await
        .unwrap_or_else(|error| panic!("all-resource etcd backend initializes: {error}"));
    let server = start_core_server(backend).await;
    let client = reqwest::Client::new();

    let namespace_response = client
        .post(format!("{}/api/v1/namespaces", server.base_url))
        .json(&namespace("development"))
        .send()
        .await
        .unwrap_or_else(|error| panic!("Namespace create completes: {error}"));
    assert_eq!(namespace_response.status(), reqwest::StatusCode::CREATED);

    let pod_response = client
        .post(format!(
            "{}/api/v1/namespaces/development/pods",
            server.base_url
        ))
        .json(&pod("web", "development"))
        .send()
        .await
        .unwrap_or_else(|error| panic!("Pod create completes: {error}"));
    assert_eq!(pod_response.status(), reqwest::StatusCode::CREATED);
    let created: Pod = pod_response
        .json()
        .await
        .unwrap_or_else(|error| panic!("Pod create response is typed JSON: {error}"));
    assert_eq!(created.status.phase, Some(PodPhase::Pending));
    assert!(created
        .metadata
        .resource_version
        .as_deref()
        .is_some_and(|value| value.parse::<u64>().is_ok_and(|revision| revision > 0)));

    let pod_prefix = format!("{root}/pods");
    let repository = EtcdPodRepository::connect([endpoint.as_str()], Some(&pod_prefix))
        .await
        .unwrap_or_else(|error| panic!("direct Pod etcd repository connects: {error}"));
    let persisted = repository
        .get("development", "web")
        .await
        .unwrap_or_else(|error| panic!("Pod is durable in real etcd: {error}"));
    assert_eq!(persisted.metadata.name.as_deref(), Some("web"));
    assert_eq!(persisted.status.phase, Some(PodPhase::Pending));
}

#[tokio::test]
async fn namespace_finalizer_lifecycle_persists_through_configured_etcd_backend() {
    let (_etcd, endpoint) = start_etcd().await;
    let root = format!(
        "/rusternetes-namespace-finalizer-http-tests/{}",
        Uuid::new_v4()
    );
    let backend = core_backend_from_etcd_config(Some(&endpoint), Some(&root))
        .await
        .expect("durable core backend initializes");
    let repository =
        EtcdNamespaceRepository::connect([endpoint.as_str()], Some(&format!("{root}/namespaces")))
            .await
            .expect("direct Namespace etcd repository connects");
    let server = start_core_server(backend).await;
    let client = reqwest::Client::new();
    let mut resource = namespace("terminating-development");
    resource
        .spec
        .finalizers
        .push("example.com/cleanup".to_owned());
    let created_response = client
        .post(format!("{}/api/v1/namespaces", server.base_url))
        .json(&resource)
        .send()
        .await
        .expect("Namespace create completes");
    assert_eq!(created_response.status(), reqwest::StatusCode::CREATED);

    let delete_response = client
        .delete(format!(
            "{}/api/v1/namespaces/terminating-development",
            server.base_url
        ))
        .send()
        .await
        .expect("Namespace deletion request completes");
    assert_eq!(delete_response.status(), reqwest::StatusCode::ACCEPTED);
    let pending = repository
        .get("terminating-development")
        .await
        .expect("deletion-pending Namespace is durable");
    assert!(pending.metadata.deletion_timestamp.is_some());
    assert_eq!(
        pending.status.phase,
        Some(rusternetes_api_types::NamespacePhase::Terminating)
    );

    let mut finalize = pending.clone();
    finalize.spec.finalizers.clear();
    let finalize_response = client
        .put(format!(
            "{}/api/v1/namespaces/terminating-development/finalize",
            server.base_url
        ))
        .json(&finalize)
        .send()
        .await
        .expect("Namespace finalization completes");
    assert_eq!(finalize_response.status(), reqwest::StatusCode::OK);
    assert!(matches!(
        repository.get("terminating-development").await,
        Err(rusternetes_common::ApiError::NotFound { .. })
    ));
}

#[tokio::test]
async fn pod_finalizer_lifecycle_persists_through_configured_etcd_backend() {
    let (_etcd, endpoint) = start_etcd().await;
    let root = format!("/rusternetes-pod-finalizer-http-tests/{}", Uuid::new_v4());
    let backend = core_backend_from_etcd_config(Some(&endpoint), Some(&root))
        .await
        .expect("durable core backend initializes");
    let repository = EtcdPodRepository::connect([endpoint.as_str()], Some(&format!("{root}/pods")))
        .await
        .expect("direct Pod etcd repository connects");
    let server = start_core_server(backend).await;
    let client = reqwest::Client::new();
    let namespace_name = "finalizer-development";
    let namespace_response = client
        .post(format!("{}/api/v1/namespaces", server.base_url))
        .json(&namespace(namespace_name))
        .send()
        .await
        .expect("Pod finalizer test Namespace creates");
    assert_eq!(namespace_response.status(), reqwest::StatusCode::CREATED);

    let mut resource = pod("terminating", namespace_name);
    resource
        .metadata
        .finalizers
        .push("example.com/cleanup".to_owned());
    let collection = format!(
        "{}/api/v1/namespaces/{namespace_name}/pods",
        server.base_url
    );
    let created_response = client
        .post(&collection)
        .json(&resource)
        .send()
        .await
        .expect("finalizer-bearing Pod creates");
    assert_eq!(created_response.status(), reqwest::StatusCode::CREATED);

    let delete_response = client
        .delete(format!("{collection}/terminating"))
        .send()
        .await
        .expect("Pod deletion request completes");
    assert_eq!(delete_response.status(), reqwest::StatusCode::ACCEPTED);
    let pending = repository
        .get(namespace_name, "terminating")
        .await
        .expect("deletion-pending Pod is durable");
    assert!(pending.metadata.deletion_timestamp.is_some());
    assert_eq!(
        pending.metadata.finalizers,
        vec!["example.com/cleanup".to_owned()]
    );
    assert_eq!(pending.status.phase, Some(PodPhase::Pending));

    let finalize_response = client
        .patch(format!("{collection}/terminating"))
        .header("content-type", "application/json-patch+json")
        .body(r#"[{"op":"remove","path":"/metadata/finalizers/0"}]"#)
        .send()
        .await
        .expect("Pod finalizer-removal patch completes");
    assert_eq!(finalize_response.status(), reqwest::StatusCode::OK);
    assert!(matches!(
        repository.get(namespace_name, "terminating").await,
        Err(rusternetes_common::ApiError::NotFound { .. })
    ));
}

#[tokio::test]
async fn namespace_status_persists_through_configured_etcd_backend_without_spec_mutation() {
    let (_etcd, endpoint) = start_etcd().await;
    let root = format!(
        "/rusternetes-namespace-status-http-tests/{}",
        Uuid::new_v4()
    );
    let backend = core_backend_from_etcd_config(Some(&endpoint), Some(&root))
        .await
        .expect("durable core backend initializes");
    let repository =
        EtcdNamespaceRepository::connect([endpoint.as_str()], Some(&format!("{root}/namespaces")))
            .await
            .expect("direct Namespace etcd repository connects");
    let server = start_core_server(backend).await;
    let client = reqwest::Client::new();
    let created_response = client
        .post(format!("{}/api/v1/namespaces", server.base_url))
        .json(&namespace("status-development"))
        .send()
        .await
        .expect("Namespace create completes");
    assert_eq!(created_response.status(), reqwest::StatusCode::CREATED);
    let created: Namespace = created_response
        .json()
        .await
        .expect("typed Namespace response");

    let mut status_update = created.clone();
    status_update
        .spec
        .finalizers
        .push("attempted-finalizer".to_owned());
    status_update.status.phase = Some(rusternetes_api_types::NamespacePhase::Terminating);
    let updated_response = client
        .put(format!(
            "{}/api/v1/namespaces/status-development/status",
            server.base_url
        ))
        .json(&status_update)
        .send()
        .await
        .expect("Namespace status update completes");
    assert_eq!(updated_response.status(), reqwest::StatusCode::OK);
    let updated: Namespace = updated_response
        .json()
        .await
        .expect("typed status response");
    assert_eq!(
        updated.status.phase,
        Some(rusternetes_api_types::NamespacePhase::Terminating)
    );
    assert_eq!(updated.spec, created.spec);
    assert_ne!(
        updated.metadata.resource_version,
        created.metadata.resource_version
    );
    let persisted = repository
        .get("status-development")
        .await
        .expect("status update is durable in real etcd");
    assert_eq!(persisted, updated);
}

#[tokio::test]
async fn pod_status_persists_through_configured_etcd_backend_without_spec_mutation() {
    let (_etcd, endpoint) = start_etcd().await;
    let root = format!("/rusternetes-pod-status-http-tests/{}", Uuid::new_v4());
    let backend = core_backend_from_etcd_config(Some(&endpoint), Some(&root))
        .await
        .expect("durable core backend initializes");
    let repository = EtcdPodRepository::connect([endpoint.as_str()], Some(&format!("{root}/pods")))
        .await
        .expect("direct Pod etcd repository connects");
    let server = start_core_server(backend).await;
    let client = reqwest::Client::new();
    let created_response = client
        .post(format!(
            "{}/api/v1/namespaces/default/pods",
            server.base_url
        ))
        .json(&pod("status-web", "default"))
        .send()
        .await
        .expect("Pod create completes");
    assert_eq!(created_response.status(), reqwest::StatusCode::CREATED);
    let created: Pod = created_response.json().await.expect("typed Pod response");

    let mut status_update = created.clone();
    status_update.spec.node_name = Some("attempted-spec-mutation".to_owned());
    status_update.status.phase = Some(PodPhase::Running);
    let updated_response = client
        .put(format!(
            "{}/api/v1/namespaces/default/pods/status-web/status",
            server.base_url
        ))
        .json(&status_update)
        .send()
        .await
        .expect("Pod status update completes");
    assert_eq!(updated_response.status(), reqwest::StatusCode::OK);
    let updated: Pod = updated_response
        .json()
        .await
        .expect("typed status response");
    assert_eq!(updated.status.phase, Some(PodPhase::Running));
    assert_eq!(updated.spec, created.spec);
    assert_ne!(
        updated.metadata.resource_version,
        created.metadata.resource_version
    );

    let persisted = repository
        .get("default", "status-web")
        .await
        .expect("status update is durable in real etcd");
    assert_eq!(persisted, updated);
}

#[tokio::test]
async fn service_account_api_persists_through_configured_etcd_backend() {
    let (_etcd, endpoint) = start_etcd().await;
    let prefix = format!("/rusternetes-http-serviceaccounts/{}", Uuid::new_v4());
    let backend = core_backend_from_etcd_config(Some(&endpoint), Some(&prefix))
        .await
        .expect("durable core backend initializes");
    let repository = EtcdServiceAccountRepository::connect(
        [endpoint.as_str()],
        Some(&format!("{prefix}/serviceaccounts")),
    )
    .await
    .expect("direct ServiceAccount repository connects");
    let server = start_core_server(backend).await;
    let client = reqwest::Client::new();
    let collection = format!(
        "{}/api/v1/namespaces/default/serviceaccounts",
        server.base_url
    );
    let resource = ServiceAccount {
        type_meta: TypeMeta::service_account(),
        metadata: ObjectMeta {
            name: Some("build-robot".to_owned()),
            ..ObjectMeta::default()
        },
        ..ServiceAccount::default()
    };
    let created_response = client
        .post(&collection)
        .json(&resource)
        .send()
        .await
        .expect("ServiceAccount create completes");
    assert_eq!(created_response.status(), reqwest::StatusCode::CREATED);
    let created: ServiceAccount = created_response.json().await.expect("typed response");

    let mut update = created.clone();
    update.automount_service_account_token = Some(false);
    let updated_response = client
        .put(format!("{collection}/build-robot"))
        .json(&update)
        .send()
        .await
        .expect("ServiceAccount update completes");
    assert_eq!(updated_response.status(), reqwest::StatusCode::OK);
    let updated: ServiceAccount = updated_response.json().await.expect("typed response");
    assert_ne!(
        updated.metadata.resource_version,
        created.metadata.resource_version
    );

    let persisted = repository
        .get("default", "build-robot")
        .await
        .expect("direct etcd read succeeds");
    assert_eq!(persisted, updated);
}

fn token_review_authentication() -> AuthenticationChain {
    const PRIVATE_KEY: &str =
        include_str!("../../crates/api-server/testdata/tokenrequest-test-private.pem");
    const PUBLIC_KEY: &str =
        include_str!("../../crates/api-server/testdata/tokenrequest-test-public.pem");
    let verifier = ServiceAccountJwtVerifier::new(
        "https://issuer.example",
        vec!["api".to_owned()],
        vec![ServiceAccountJwtKey {
            key_id: Some("tokenreview-etcd-test".to_owned()),
            rsa_public_key_pem: PUBLIC_KEY.to_owned(),
        }],
    )
    .expect("ServiceAccount JWT verifier is valid");
    let issuer = ServiceAccountTokenIssuer::new(
        "https://issuer.example",
        vec!["api".to_owned()],
        PRIVATE_KEY,
        "tokenreview-etcd-test",
        60,
        120,
    )
    .expect("ServiceAccount JWT issuer is valid");
    let caller = RequestIdentity::authenticated(
        "tokenreview-admin",
        None,
        ["system:masters".to_owned()],
        Default::default(),
    )
    .expect("TokenReview caller identity is valid");
    AuthenticationChain::new(
        AnonymousPolicy::Deny,
        vec![StaticBearerToken::new("tokenreview-admin-secret", caller)
            .expect("TokenReview caller token is valid")],
    )
    .with_service_account_jwt_verifier(verifier)
    .with_service_account_token_issuer(issuer)
    .expect("issuer matches TokenReview verifier")
}

#[tokio::test]
async fn token_review_validates_a_durable_serviceaccount_jwt_through_real_etcd() {
    let (_etcd, endpoint) = start_etcd().await;
    let prefix = format!("/rusternetes-tokenreview-tests/{}", Uuid::new_v4());
    let backend = core_backend_from_etcd_config(Some(&endpoint), Some(&prefix))
        .await
        .expect("durable core backend initializes");
    let repository = EtcdServiceAccountRepository::connect(
        [endpoint.as_str()],
        Some(&format!("{prefix}/serviceaccounts")),
    )
    .await
    .expect("direct ServiceAccount repository connects");
    let server = start_core_server_with_auth(backend, token_review_authentication()).await;
    let client = reqwest::Client::new();
    let collection = format!(
        "{}/api/v1/namespaces/default/serviceaccounts",
        server.base_url
    );
    let created_response = client
        .post(&collection)
        .header("authorization", "Bearer tokenreview-admin-secret")
        .json(&ServiceAccount {
            type_meta: TypeMeta::service_account(),
            metadata: ObjectMeta {
                name: Some("review-robot".to_owned()),
                ..ObjectMeta::default()
            },
            ..ServiceAccount::default()
        })
        .send()
        .await
        .expect("durable ServiceAccount create completes");
    assert_eq!(created_response.status(), reqwest::StatusCode::CREATED);
    let created: ServiceAccount = created_response
        .json()
        .await
        .expect("created ServiceAccount is typed JSON");
    let persisted = repository
        .get("default", "review-robot")
        .await
        .expect("created ServiceAccount is durable in etcd");
    assert_eq!(persisted, created);

    let token_response = client
        .post(format!("{collection}/review-robot/token"))
        .header("authorization", "Bearer tokenreview-admin-secret")
        .json(&serde_json::json!({
            "apiVersion": "authentication.k8s.io/v1",
            "kind": "TokenRequest",
            "spec": { "audiences": ["api"] }
        }))
        .send()
        .await
        .expect("durable TokenRequest completes");
    assert_eq!(token_response.status(), reqwest::StatusCode::OK);
    let token_response: serde_json::Value = token_response
        .json()
        .await
        .expect("TokenRequest response is JSON");
    let token = token_response["status"]["token"]
        .as_str()
        .expect("TokenRequest returns JWT");

    let review_response = client
        .post(format!(
            "{}/apis/authentication.k8s.io/v1/tokenreviews",
            server.base_url
        ))
        .header("authorization", "Bearer tokenreview-admin-secret")
        .json(&serde_json::json!({
            "apiVersion": "authentication.k8s.io/v1",
            "kind": "TokenReview",
            "spec": { "token": token, "audiences": ["api"] }
        }))
        .send()
        .await
        .expect("durable TokenReview completes");
    assert_eq!(review_response.status(), reqwest::StatusCode::OK);
    let review: serde_json::Value = review_response
        .json()
        .await
        .expect("TokenReview response is JSON");
    assert_eq!(review["status"]["authenticated"], true);
    assert_eq!(
        review["status"]["user"]["username"],
        "system:serviceaccount:default:review-robot"
    );
    assert_eq!(
        review["status"]["user"]["uid"].as_str(),
        created.metadata.uid.as_deref()
    );
    assert_eq!(review["status"]["audiences"], serde_json::json!(["api"]));
}

#[tokio::test]
async fn config_map_pagination_preserves_an_etcd_snapshot_across_continuation_requests() {
    let (_etcd, endpoint) = start_etcd().await;
    let prefix = format!("/rusternetes-pagination-tests/{}", Uuid::new_v4());
    let repository = Arc::new(
        EtcdConfigMapRepository::connect([endpoint.as_str()], Some(&prefix))
            .await
            .unwrap_or_else(|error| panic!("etcd ConfigMap repository connects: {error}")),
    );
    let server = start_server(repository).await;
    let client = reqwest::Client::new();
    let collection = format!("{}/api/v1/namespaces/default/configmaps", server.base_url);

    for name in ["a", "b", "c"] {
        let response = client
            .post(&collection)
            .json(&config_map(name, "default", "frontend"))
            .send()
            .await
            .unwrap_or_else(|error| panic!("{name} ConfigMap create completes: {error}"));
        assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    }

    let first_response = client
        .get(format!("{}/api/v1/configmaps", server.base_url))
        .query(&[("limit", "2")])
        .send()
        .await
        .unwrap_or_else(|error| panic!("first paginated list completes: {error}"));
    assert_eq!(first_response.status(), reqwest::StatusCode::OK);
    let first: ConfigMapList = first_response
        .json()
        .await
        .unwrap_or_else(|error| panic!("first page is ConfigMapList JSON: {error}"));
    assert_eq!(first.items.len(), 2);
    assert_eq!(first.metadata.remaining_item_count, Some(1));
    let resource_version = first.metadata.resource_version.clone();
    let continue_token = first
        .metadata
        .continue_token
        .clone()
        .expect("first etcd page supplies continue token");

    let late_response = client
        .post(&collection)
        .json(&config_map("late", "default", "frontend"))
        .send()
        .await
        .expect("post-snapshot ConfigMap create completes");
    assert_eq!(late_response.status(), reqwest::StatusCode::CREATED);

    let final_response = client
        .get(format!("{}/api/v1/configmaps", server.base_url))
        .query(&[("continue", continue_token.as_str())])
        .send()
        .await
        .expect("etcd continuation request completes");
    assert_eq!(final_response.status(), reqwest::StatusCode::OK);
    let final_page: ConfigMapList = final_response
        .json()
        .await
        .expect("final page is ConfigMapList JSON");
    assert_eq!(final_page.items.len(), 1);
    assert_eq!(final_page.items[0].metadata.name.as_deref(), Some("c"));
    assert_eq!(final_page.metadata.resource_version, resource_version);
    assert!(final_page.metadata.continue_token.is_none());
}

#[tokio::test]
async fn service_account_patch_persists_through_configured_etcd_backend() {
    let (_etcd, endpoint) = start_etcd().await;
    let prefix = format!("/rusternetes-http-serviceaccount-patch/{}", Uuid::new_v4());
    let backend = core_backend_from_etcd_config(Some(&endpoint), Some(&prefix))
        .await
        .expect("durable core backend initializes");
    let repository = EtcdServiceAccountRepository::connect(
        [endpoint.as_str()],
        Some(&format!("{prefix}/serviceaccounts")),
    )
    .await
    .expect("direct ServiceAccount repository connects");
    let server = start_core_server(backend).await;
    let client = reqwest::Client::new();
    let collection = format!(
        "{}/api/v1/namespaces/default/serviceaccounts",
        server.base_url
    );
    let created_response = client
        .post(&collection)
        .json(&ServiceAccount {
            type_meta: TypeMeta::service_account(),
            metadata: ObjectMeta {
                name: Some("patch-robot".to_owned()),
                ..ObjectMeta::default()
            },
            ..ServiceAccount::default()
        })
        .send()
        .await
        .expect("ServiceAccount create completes");
    assert_eq!(created_response.status(), reqwest::StatusCode::CREATED);
    let created: ServiceAccount = created_response.json().await.expect("typed response");
    let endpoint = format!("{collection}/patch-robot");

    let merged_response = client
        .patch(&endpoint)
        .header(
            reqwest::header::CONTENT_TYPE,
            "application/merge-patch+json",
        )
        .body(r#"{"automountServiceAccountToken":false}"#)
        .send()
        .await
        .expect("merge patch completes");
    assert_eq!(merged_response.status(), reqwest::StatusCode::OK);
    let merged: ServiceAccount = merged_response.json().await.expect("typed response");
    assert_eq!(merged.automount_service_account_token, Some(false));
    assert_eq!(merged.metadata.uid, created.metadata.uid);

    let json_response = client
        .patch(&endpoint)
        .header(reqwest::header::CONTENT_TYPE, "application/json-patch+json")
        .body(r#"[{"op":"replace","path":"/automountServiceAccountToken","value":true}]"#)
        .send()
        .await
        .expect("JSON patch completes");
    assert_eq!(json_response.status(), reqwest::StatusCode::OK);
    let updated: ServiceAccount = json_response.json().await.expect("typed response");
    assert_eq!(updated.automount_service_account_token, Some(true));
    assert_ne!(
        updated.metadata.resource_version,
        merged.metadata.resource_version
    );

    let persisted = repository
        .get("default", "patch-robot")
        .await
        .expect("direct etcd read succeeds");
    assert_eq!(persisted, updated);
}
