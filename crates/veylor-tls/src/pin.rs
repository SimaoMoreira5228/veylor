use std::sync::Arc;

use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{CertificateError, DigitallySignedStruct, RootCertStore, SignatureScheme};

use crate::{Certificate, TlsError};

#[derive(Debug)]
pub(crate) struct PinVerifier {
    expected: Vec<u8>,
    inner: Arc<WebPkiServerVerifier>,
}

impl PinVerifier {
    pub(crate) fn new(pinned: &Certificate) -> Result<Self, TlsError> {
        let mut roots = RootCertStore::empty();

        roots
            .add(pinned.to_backend())
            .map_err(|_| TlsError::InvalidInput)?;

        let inner = WebPkiServerVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|_| TlsError::InvalidInput)?;

        Ok(Self {
            expected: pinned.as_der().to_vec(),
            inner,
        })
    }
}

impl ServerCertVerifier for PinVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        if end_entity.as_ref() == self.expected.as_slice() {
            return Ok(ServerCertVerified::assertion());
        }

        Err(rustls::Error::InvalidCertificate(
            CertificateError::UnknownIssuer,
        ))
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}
