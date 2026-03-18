use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use lsmdb::catalog::Catalog;
use lsmdb::mvcc::MvccStore;
use lsmdb::server::{ServerOptions, start_server_with_options};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bind_addr: SocketAddr = "127.0.0.1:7878".parse()?;

    // Persistent MVCC store (creates ./data if it does not exist).
    let store = Arc::new(MvccStore::open_persistent("./data")?);
    let catalog = Arc::new(Catalog::open((*store).clone())?);

    // Insecure local-dev mode. Do not use this mode for production.
    let options = ServerOptions::insecure_for_local_dev();
    let _server = start_server_with_options(bind_addr, catalog, store, options).await?;

    println!("lsmdb server listening on {}", bind_addr);
    println!("Connect from another terminal:");
    println!("  cargo run --bin lsmdb-cli -- --addr {}", bind_addr);
    println!("Press Ctrl+C to stop.");

    loop {
        tokio::time::sleep(Duration::from_secs(3600)).await;
    }

    #[allow(unreachable_code)]
    Ok(())
}
