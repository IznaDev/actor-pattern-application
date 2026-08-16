use device_hub::connection::handle_connection;
use device_hub::handlers::hub_dispatch;
use device_hub::hub_actor::HubHandle;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(true)
        .init();

    let listener = TcpListener::bind("127.0.0.1:8089").await?;

    info!("device-hub listening on 127.0.0.1:8089");

    let hub = HubHandle::new();

    let (light_tx, _) = broadcast::channel(32);

    loop {
        let (stream, _) = listener.accept().await?;
        let hub = hub.clone();
        let light_tx = light_tx.clone();
        tokio::spawn(async move {
            match handle_connection(stream).await {
                Ok(device) => {
                    if let Err(e) = hub_dispatch(device, &hub, &light_tx).await {
                        warn!("handler error: {e}");
                    }
                }
                Err(e) => warn!("handshake failed: {e}"),
            }
        });
    }
}
