use std::{
    fs,
    net::TcpListener as StdTcpListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
    thread,
    time::Duration,
};

use rusternetes_controller_runtime::{EtcdLeaseStore, Leadership};
use tokio::time::sleep;
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
    let listener = StdTcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("ephemeral port binds: {error}"));
    listener
        .local_addr()
        .unwrap_or_else(|error| panic!("listener has address: {error}"))
        .port()
}

async fn start_etcd() -> (EphemeralEtcd, String) {
    let client_port = unused_port();
    let peer_port = unused_port();
    let client_url = format!("http://127.0.0.1:{client_port}");
    let peer_url = format!("http://127.0.0.1:{peer_port}");
    let data_dir =
        std::env::temp_dir().join(format!("rusternetes-election-etcd-{}", Uuid::new_v4()));
    let child = Command::new("etcd")
        .args([
            "--name",
            "rusternetes-election-test",
            "--data-dir",
            data_dir
                .to_str()
                .unwrap_or_else(|| panic!("temporary data path is UTF-8")),
            "--listen-client-urls",
            &client_url,
            "--advertise-client-urls",
            &client_url,
            "--listen-peer-urls",
            &peer_url,
            "--initial-advertise-peer-urls",
            &peer_url,
            "--initial-cluster",
            &format!("rusternetes-election-test={peer_url}"),
            "--initial-cluster-state",
            "new",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap_or_else(|error| panic!("etcd starts: {error}"));
    let guard = EphemeralEtcd { child, data_dir };
    for _ in 0..50 {
        if let Ok(store) =
            EtcdLeaseStore::connect([client_url.as_str()], "/rusternetes-test", "controller").await
        {
            if store.observe().await.is_ok() {
                return (guard, client_url);
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("etcd readiness timeout");
}

#[tokio::test]
async fn durable_lease_allows_one_holder_then_cas_takeover_after_expiry() {
    let (_etcd, endpoint) = start_etcd().await;
    let prefix = format!("/rusternetes-leader-tests/{}", Uuid::new_v4());
    let first = EtcdLeaseStore::connect([endpoint.as_str()], &prefix, "controller-manager")
        .await
        .expect("first client connects");
    let second = EtcdLeaseStore::connect([endpoint.as_str()], &prefix, "controller-manager")
        .await
        .expect("second client connects");
    assert!(matches!(
        first
            .acquire_or_renew("first", Duration::from_secs(1))
            .await,
        Ok(Leadership::Acquired)
    ));
    assert!(
        matches!(second.acquire_or_renew("second", Duration::from_secs(1)).await, Ok(Leadership::Standby { holder_identity }) if holder_identity == "first")
    );
    sleep(Duration::from_millis(1_100)).await;
    assert!(matches!(
        second
            .acquire_or_renew("second", Duration::from_secs(1))
            .await,
        Ok(Leadership::Acquired)
    ));
    let observed = second
        .observe()
        .await
        .expect("observe succeeds")
        .expect("record exists");
    assert_eq!(observed.record.holder_identity, "second");
    assert_eq!(observed.record.lease_transitions, 1);
}
