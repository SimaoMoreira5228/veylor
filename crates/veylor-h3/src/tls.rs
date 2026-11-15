use std::sync::Arc;

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use veylor_tls::{Certificate, PrivateKey};

// NOTE: the only module that names the TLS library. QUIC carries TLS 1.3 inside its own packet
// protection, so quinn needs the library's config object directly and veylor-tls cannot hide it.
pub(crate) fn server_crypto(
    certs: &[Certificate],
    key: &PrivateKey,
) -> Result<Arc<dyn quinn::crypto::ServerConfig>, &'static str> {
    let chain = certs.iter().map(cert_der).collect();
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut tls = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|_| "tls 1.3 unsupported")?
        .with_no_client_auth()
        .with_single_cert(chain, key_der(key))
        .map_err(|_| "invalid certificate or key")?;

    tls.alpn_protocols = vec![b"h3".to_vec()];
    tls.max_early_data_size = 0;
    tls.send_half_rtt_data = false;

    let crypto = QuicServerConfig::try_from(tls).map_err(|_| "no quic cipher suite")?;

    Ok(Arc::new(crypto))
}

pub(crate) fn client_crypto(
    roots: &[Certificate],
) -> Result<Arc<dyn quinn::crypto::ClientConfig>, &'static str> {
    let mut store = rustls::RootCertStore::empty();

    for root in roots {
        store
            .add(cert_der(root))
            .map_err(|_| "invalid root certificate")?;
    }

    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|_| "tls 1.3 unsupported")?
        .with_root_certificates(store)
        .with_no_client_auth();

    tls.alpn_protocols = vec![b"h3".to_vec()];

    let crypto = QuicClientConfig::try_from(tls).map_err(|_| "no quic cipher suite")?;

    Ok(Arc::new(crypto))
}

fn cert_der(cert: &Certificate) -> CertificateDer<'static> {
    CertificateDer::from(cert.as_der().to_vec())
}

fn key_der(key: &PrivateKey) -> PrivateKeyDer<'static> {
    let der = key.as_der().to_vec();

    match key {
        PrivateKey::Pkcs8(_) => PrivateKeyDer::Pkcs8(der.into()),
        PrivateKey::Pkcs1(_) => PrivateKeyDer::Pkcs1(der.into()),
        PrivateKey::Sec1(_) => PrivateKeyDer::Sec1(der.into()),
    }
}
