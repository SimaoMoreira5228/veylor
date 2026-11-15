use std::sync::Arc;

use rustls::RootCertStore;
use rustls::pki_types::{CertificateDer, ServerName};

use crate::{TlsConn, TlsError, load_cert_chain};

const DEFAULT_ALPN: [&[u8]; 2] = [b"h2", b"http/1.1"];

pub struct TlsConnectorBuilder {
    default_roots: bool,
    extra_roots: Vec<CertificateDer<'static>>,
    alpn: Vec<Vec<u8>>,
    tls13_only: bool,
}

impl TlsConnectorBuilder {
    pub fn default_roots(mut self, enabled: bool) -> Self {
        self.default_roots = enabled;

        self
    }

    pub fn add_root(mut self, pem_or_der: &[u8]) -> Result<Self, TlsError> {
        self.extra_roots.extend(load_cert_chain(pem_or_der)?);

        Ok(self)
    }

    pub fn alpn(mut self, protocols: &[&[u8]]) -> Self {
        self.alpn = protocols.iter().map(|p| p.to_vec()).collect();

        self
    }

    pub fn tls13_only(mut self, enabled: bool) -> Self {
        self.tls13_only = enabled;

        self
    }

    pub fn build(self) -> Result<TlsConnector, TlsError> {
        let mut roots = RootCertStore::empty();

        if self.default_roots {
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }

        for cert in self.extra_roots {
            roots.add(cert).map_err(|_| TlsError::InvalidInput)?;
        }

        if roots.is_empty() {
            return Err(TlsError::InvalidInput);
        }

        let versions: &[&rustls::SupportedProtocolVersion] = if self.tls13_only {
            &[&rustls::version::TLS13]
        } else {
            rustls::ALL_VERSIONS
        };

        let mut config = rustls::ClientConfig::builder_with_protocol_versions(versions)
            .with_root_certificates(roots)
            .with_no_client_auth();

        config.alpn_protocols = self.alpn;
        config.enable_early_data = false;

        Ok(TlsConnector {
            config: Arc::new(config),
        })
    }
}

#[derive(Clone)]
pub struct TlsConnector {
    config: Arc<rustls::ClientConfig>,
}

impl TlsConnector {
    pub fn builder() -> TlsConnectorBuilder {
        TlsConnectorBuilder {
            default_roots: true,
            extra_roots: Vec::new(),
            alpn: DEFAULT_ALPN.iter().map(|p| p.to_vec()).collect(),
            tls13_only: false,
        }
    }

    pub fn connect(&self, server_name: &str) -> Result<TlsConn, TlsError> {
        let name =
            ServerName::try_from(server_name.to_owned()).map_err(|_| TlsError::InvalidInput)?;
        let inner = rustls::ClientConnection::new(Arc::clone(&self.config), name)?;

        Ok(TlsConn::new(rustls::Connection::Client(inner)))
    }

    pub fn early_data_enabled(&self) -> bool {
        self.config.enable_early_data
    }
}
