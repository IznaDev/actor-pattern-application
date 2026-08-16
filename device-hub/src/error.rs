use thiserror::Error;

/// initial handshake errors
#[derive(Debug, Error)]
pub enum ConnectionError {
    #[error("Handshake error")]
    HandshakeError,
    #[error("I/O Error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Unknown device type: {0}")]
    DevicetypeError(u8),
}

/// devices handlers errors
#[derive(Debug, Error)]
pub enum HubError {
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Connection closed")]
    Disconnected,
}
