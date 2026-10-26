mod pump;

pub use pump::{drain_plaintext, feed_ciphertext};

use std::io;
use std::sync::Arc;

pub const MAX_TLS_CONN_BYTES: usize = 2048;

pub struct TlsAcceptor {
    config: Arc<rustls::ServerConfig>,
    max_handshakes: usize,
    active: usize,
}

impl TlsAcceptor {
    pub fn new(cert_der: &[u8], key_der: &[u8], max_handshakes: usize) -> Result<Self, TlsError> {
        let cert = rustls::pki_types::CertificateDer::from(cert_der.to_vec());

        let key = rustls::pki_types::PrivateKeyDer::Pkcs8(key_der.to_vec().into());

        let mut config =
            rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_no_client_auth()
                .with_single_cert(vec![cert], key)
                .map_err(|_| TlsError::InvalidInput)?;

        config.alpn_protocols = vec![b"http/1.1".to_vec()];

        debug_assert_eq!(config.max_early_data_size, 0);

        Ok(Self {
            config: Arc::new(config),
            max_handshakes,
            active: 0,
        })
    }

    pub fn admit(&mut self) -> Result<TlsConn, TlsError> {
        if self.active >= self.max_handshakes {
            return Err(TlsError::SlotsFull);
        }

        let inner = rustls::ServerConnection::new(Arc::clone(&self.config))
            .map_err(|_| TlsError::Handshake)?;

        self.active += 1;

        Ok(TlsConn {
            inner,
            bytes_encrypted: 0,
            bytes_decrypted: 0,
        })
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
    inner: rustls::ServerConnection,
    bytes_encrypted: u64,
    bytes_decrypted: u64,
}

impl TlsConn {
    pub fn bytes_encrypted(&self) -> u64 {
        self.bytes_encrypted
    }

    pub fn bytes_decrypted(&self) -> u64 {
        self.bytes_decrypted
    }

    pub fn is_handshaking(&self) -> bool {
        self.inner.is_handshaking()
    }
}

pub fn alpn(conn: &TlsConn) -> Option<&[u8]> {
    conn.inner.alpn_protocol()
}

pub fn negotiated_version(conn: &TlsConn) -> Option<rustls::ProtocolVersion> {
    conn.inner.protocol_version()
}

#[derive(Debug)]
pub enum TlsError {
    SlotsFull,
    InvalidInput,
    Handshake,
    Crypto,
    Io(io::Error),
}

impl std::fmt::Display for TlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SlotsFull => write!(f, "handshake slot budget exhausted"),
            Self::InvalidInput => write!(f, "invalid certificate or key input"),
            Self::Handshake => write!(f, "TLS handshake failure"),
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
            rustls::Error::DecryptError | rustls::Error::EncryptError => Self::Crypto,
            _ => Self::Handshake,
        }
    }
}
