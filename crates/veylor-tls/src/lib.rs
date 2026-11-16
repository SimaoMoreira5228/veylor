// NOTE: this is the only crate allowed to name the TLS library for the TCP stack; no backend type
// may appear in a public signature. veylor-h3 is the one exception (quinn needs the backend config).

mod client;
mod pem;
mod pin;
mod pump;
mod stream;
mod types;

pub use client::{TlsConnector, TlsConnectorBuilder};
pub use pem::{load_cert_chain, load_private_key};
pub use pump::{
    WireIo, decrypt_from, decrypt_from_vec, drain_plaintext, feed_ciphertext, feed_ciphertext_into,
    flush_ciphertext, plaintext_queued, queue_close_notify,
};
pub use stream::TlsStream;
pub use types::{Certificate, PrivateKey, ProtocolVersion};

use std::io;
use std::sync::Arc;

const DEFAULT_ALPN: [&[u8]; 2] = [b"h2", b"http/1.1"];

pub const MAX_TLS_CONN_BYTES: usize = 2048;

pub struct TlsAcceptor {
    config: Arc<rustls::ServerConfig>,
    alpn: Vec<Vec<u8>>,
    max_handshakes: usize,
    active: usize,
}

impl TlsAcceptor {
    pub fn new(cert_der: &[u8], key_der: &[u8], max_handshakes: usize) -> Result<Self, TlsError> {
        let cert = Certificate::from_der(cert_der.to_vec());
        let key = PrivateKey::Pkcs8(key_der.to_vec());

        Self::from_chain(vec![cert], key, max_handshakes)
    }

    pub fn from_pem(
        cert_pem: &[u8],
        key_pem: &[u8],
        max_handshakes: usize,
    ) -> Result<Self, TlsError> {
        Self::from_chain(
            load_cert_chain(cert_pem)?,
            load_private_key(key_pem)?,
            max_handshakes,
        )
    }

    pub fn from_chain(
        chain: Vec<Certificate>,
        key: PrivateKey,
        max_handshakes: usize,
    ) -> Result<Self, TlsError> {
        let alpn = DEFAULT_ALPN.iter().map(|p| p.to_vec()).collect::<Vec<_>>();

        Ok(Self {
            config: Arc::new(Self::build_config(&chain, &key, &alpn)?),
            alpn,
            max_handshakes,
            active: 0,
        })
    }

    pub fn rotate(&mut self, chain: Vec<Certificate>, key: PrivateKey) -> Result<(), TlsError> {
        self.config = Arc::new(Self::build_config(&chain, &key, &self.alpn)?);

        Ok(())
    }

    pub fn with_alpn(mut self, protocols: &[&[u8]]) -> Self {
        self.alpn = protocols.iter().map(|p| p.to_vec()).collect();

        Arc::make_mut(&mut self.config).alpn_protocols = self.alpn.clone();

        self
    }

    fn build_config(
        chain: &[Certificate],
        key: &PrivateKey,
        alpn: &[Vec<u8>],
    ) -> Result<rustls::ServerConfig, TlsError> {
        let mut config =
            rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_no_client_auth()
                .with_single_cert(
                    chain.iter().map(Certificate::to_backend).collect(),
                    key.to_backend(),
                )
                .map_err(|_| TlsError::InvalidInput)?;

        config.alpn_protocols = alpn.to_vec();

        debug_assert_eq!(config.max_early_data_size, 0);

        Ok(config)
    }

    pub fn admit(&mut self) -> Result<TlsConn, TlsError> {
        if self.active >= self.max_handshakes {
            return Err(TlsError::SlotsFull);
        }

        let inner = rustls::ServerConnection::new(Arc::clone(&self.config))
            .map_err(|_| TlsError::Handshake)?;

        self.active += 1;

        Ok(TlsConn::new(rustls::Connection::Server(inner)))
    }

    pub fn release(&mut self) {
        self.active = self.active.saturating_sub(1);
    }

    pub fn active(&self) -> usize {
        self.active
    }

    pub fn max_handshakes(&self) -> usize {
        self.max_handshakes
    }

    pub fn max_early_data_size(&self) -> u32 {
        self.config.max_early_data_size
    }
}

pub struct TlsConn {
    inner: rustls::Connection,
    bytes_encrypted: u64,
    bytes_decrypted: u64,
    peer_closed: bool,
}

impl TlsConn {
    fn new(inner: rustls::Connection) -> Self {
        Self {
            inner,
            bytes_encrypted: 0,
            bytes_decrypted: 0,
            peer_closed: false,
        }
    }

    pub fn bytes_encrypted(&self) -> u64 {
        self.bytes_encrypted
    }

    pub fn bytes_decrypted(&self) -> u64 {
        self.bytes_decrypted
    }

    pub fn peer_closed(&self) -> bool {
        self.peer_closed
    }

    pub fn is_handshaking(&self) -> bool {
        self.inner.is_handshaking()
    }
}

pub fn alpn(conn: &TlsConn) -> Option<&[u8]> {
    conn.inner.alpn_protocol()
}

pub fn negotiated_version(conn: &TlsConn) -> Option<ProtocolVersion> {
    conn.inner
        .protocol_version()
        .and_then(ProtocolVersion::from_backend)
}

#[derive(Debug)]
pub enum TlsError {
    SlotsFull,
    InvalidInput,
    Handshake,
    Certificate,
    Crypto,
    Io(io::Error),
}

impl std::fmt::Display for TlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SlotsFull => write!(f, "handshake slot budget exhausted"),
            Self::InvalidInput => write!(f, "invalid certificate or key input"),
            Self::Handshake => write!(f, "TLS handshake failure"),
            Self::Certificate => write!(f, "TLS peer certificate rejected"),
            Self::Crypto => write!(f, "TLS cryptographic failure"),
            Self::Io(e) => write!(f, "TLS transport I/O failure: {e}"),
        }
    }
}

impl std::error::Error for TlsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for TlsError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<rustls::Error> for TlsError {
    fn from(e: rustls::Error) -> Self {
        match e {
            rustls::Error::InvalidCertificate(_) | rustls::Error::InvalidCertRevocationList(_) => {
                Self::Certificate
            }
            rustls::Error::DecryptError | rustls::Error::EncryptError => Self::Crypto,
            _ => Self::Handshake,
        }
    }
}
