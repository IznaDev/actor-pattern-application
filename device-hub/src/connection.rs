use super::error::ConnectionError;
use super::protocol::*;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{instrument, warn};

/// represent a connected devices after the handshake
#[derive(Debug)]
pub struct Device {
    pub stream: TcpStream,
    pub dev_type: DeviceType,
    pub dev_id: u8,
    pub connexion_id: u64,
}

static NEXT_CONN: AtomicU64 = AtomicU64::new(1);

/// handshake with a TCP client
/// check the magic bit, send the ACK, read the type & the ID of the device.
/// Return a [`Device`]
#[instrument(skip(stream), fields(peer = ?stream.peer_addr()))]
pub async fn handle_connection(mut stream: TcpStream) -> Result<Device, ConnectionError> {
    let mut magic = [0u8; 1];
    stream.read_exact(&mut magic).await?;

    if magic[0] != HANDSHAKE_MAGIC {
        warn!("bad handshake magic: 0x{:02X}", magic[0]);
        return Err(ConnectionError::HandshakeError);
    }

    stream.write_all(&[HANDSHAKE_ACK]).await?;
    stream.flush().await?;

    let mut dev_type = [0u8; 1];
    stream.read_exact(&mut dev_type).await?;

    let device_type =
        DeviceType::from_byte(dev_type[0]).ok_or(ConnectionError::DevicetypeError(dev_type[0]));

    let mut dev_id = [0u8; 1];
    stream.read_exact(&mut dev_id).await?;

    let connexion_id = NEXT_CONN.fetch_add(1, Ordering::Relaxed);

    let device = Device {
        stream: stream,
        dev_type: device_type.unwrap(),
        dev_id: dev_id[0],
        connexion_id: connexion_id,
    };

    Ok(device)
}
