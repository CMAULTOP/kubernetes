use std::{env, io};

use rusternetes_api_server::{backend_from_etcd_config, router_with_backend};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> io::Result<()> {
    let bind_address =
        env::var("RUSTERNETES_BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());
    let backend = backend_from_etcd_config(
        env::var("RUSTERNETES_ETCD_ENDPOINTS").ok().as_deref(),
        env::var("RUSTERNETES_ETCD_PREFIX").ok().as_deref(),
    )
    .await
    .map_err(io::Error::other)?;
    let listener = TcpListener::bind(&bind_address).await?;
    axum::serve(listener, router_with_backend(backend)).await
}
