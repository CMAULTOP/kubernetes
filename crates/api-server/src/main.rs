use std::{env, io, sync::Arc};

use rusternetes_api_server::router;
use rusternetes_storage::InMemoryConfigMapStore;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> io::Result<()> {
    let bind_address = match env::var("RUSTERNETES_BIND_ADDRESS") {
        Ok(address) => address,
        Err(_) => "127.0.0.1:8080".to_owned(),
    };
    let listener = TcpListener::bind(&bind_address).await?;
    let application = router(Arc::new(InMemoryConfigMapStore::new()));
    axum::serve(listener, application).await
}
