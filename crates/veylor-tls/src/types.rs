use std::fmt;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};

#[derive(Clone, PartialEq, Eq)]
pub struct Certificate(Vec<u8>);

impl Certificate {
    pub fn from_der(der: Vec<u8>) -> Self {
        Self(der)
    }

    pub fn as_der(&self) -> &[u8] {
        &self.0
    }

    pub(crate) fn to_backend(&self) -> CertificateDer<'static> {
        CertificateDer::from(self.0.clone())
    }

    pub(crate) fn from_backend(der: &CertificateDer<'_>) -> Self {
        Self(der.as_ref().to_vec())
    }
}

impl From<Vec<u8>> for Certificate {
    fn from(der: Vec<u8>) -> Self {
        Self(der)
    }
}

impl AsRef<[u8]> for Certificate {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for Certificate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Certificate({} bytes)", self.0.len())
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum PrivateKey {
    Pkcs8(Vec<u8>),
    Pkcs1(Vec<u8>),
    Sec1(Vec<u8>),
}

impl PrivateKey {
    pub fn as_der(&self) -> &[u8] {
        match self {
            Self::Pkcs8(der) | Self::Pkcs1(der) | Self::Sec1(der) => der,
        }
    }

    pub(crate) fn to_backend(&self) -> PrivateKeyDer<'static> {
        match self {
            Self::Pkcs8(der) => PrivateKeyDer::Pkcs8(der.clone().into()),
            Self::Pkcs1(der) => PrivateKeyDer::Pkcs1(der.clone().into()),
            Self::Sec1(der) => PrivateKeyDer::Sec1(der.clone().into()),
        }
    }

    pub(crate) fn from_backend(key: &PrivateKeyDer<'_>) -> Option<Self> {
        let der = key.secret_der().to_vec();

        match key {
            PrivateKeyDer::Pkcs8(_) => Some(Self::Pkcs8(der)),
            PrivateKeyDer::Pkcs1(_) => Some(Self::Pkcs1(der)),
            PrivateKeyDer::Sec1(_) => Some(Self::Sec1(der)),
            _ => None,
        }
    }
}

impl fmt::Debug for PrivateKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PrivateKey(..)")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtocolVersion {
    Tls12,
    Tls13,
}

impl ProtocolVersion {
    pub(crate) fn from_backend(version: rustls::ProtocolVersion) -> Option<Self> {
        match version {
            rustls::ProtocolVersion::TLSv1_2 => Some(Self::Tls12),
            rustls::ProtocolVersion::TLSv1_3 => Some(Self::Tls13),
            _ => None,
        }
    }
}
