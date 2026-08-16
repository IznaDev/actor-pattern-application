use thiserror::Error;

/// Erreurs pouvant survenir lors du handshake initial.
#[derive(Debug, Error)]
pub enum ConnectionError {
    #[error("Handshake error")]
    HandshakeError,
    #[error("I/O Error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Unknown device type: {0}")]
    DevicetypeError(u8),
}
