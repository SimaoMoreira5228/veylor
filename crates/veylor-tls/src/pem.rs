use rustls::pki_types::pem::SliceIter;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::TlsError;

pub fn load_cert_chain(input: &[u8]) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    if is_pem(input) {
        let mut chain = Vec::new();

        for entry in SliceIter::<CertificateDer<'static>>::new(input) {
            chain.push(entry.map_err(|_| TlsError::InvalidInput)?);
        }

        if chain.is_empty() {
            return Err(TlsError::InvalidInput);
        }

        return Ok(chain);
    }

    if input.is_empty() {
        return Err(TlsError::InvalidInput);
    }

    Ok(vec![CertificateDer::from(input.to_vec())])
}

pub fn load_private_key(input: &[u8]) -> Result<PrivateKeyDer<'static>, TlsError> {
    if is_pem(input) {
        return match SliceIter::<PrivateKeyDer<'static>>::new(input).next() {
            Some(Ok(key)) => Ok(key),
            _ => Err(TlsError::InvalidInput),
        };
    }

    if input.is_empty() {
        return Err(TlsError::InvalidInput);
    }

    Ok(PrivateKeyDer::Pkcs8(input.to_vec().into()))
}

fn is_pem(input: &[u8]) -> bool {
    let mut prefix = 0;

    for byte in input.iter() {
        if *byte == b'-' {
            break;
        }

        if !byte.is_ascii_whitespace() {
            return false;
        }

        prefix += 1;

        if prefix > 64 {
            return false;
        }
    }

    input[prefix..].starts_with(b"-----BEGIN ")
}

#[cfg(test)]
mod tests {
    use super::{load_cert_chain, load_private_key};
    use rustls::pki_types::PrivateKeyDer;

    const CERT_DER: &[u8] = include_bytes!("../tests/testdata/server_cert.der");
    const KEY_DER: &[u8] = include_bytes!("../tests/testdata/server_key.der");
    const CERT_PEM: &[u8] = include_bytes!("../tests/testdata/server_cert.pem");
    const KEY_PEM: &[u8] = include_bytes!("../tests/testdata/server_key.pem");
    const RSA_KEY_PEM: &[u8] = include_bytes!("../tests/testdata/rsa_key.pem");
    const EC_KEY_PEM: &[u8] = include_bytes!("../tests/testdata/ec_key.pem");

    #[test]
    fn pem_chain_decodes_to_pinned_der() {
        let chain = load_cert_chain(CERT_PEM).expect("parse cert pem");

        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].as_ref(), CERT_DER);
    }

    #[test]
    fn pem_key_decodes_to_pinned_der() {
        let key = load_private_key(KEY_PEM).expect("parse key pem");
        let PrivateKeyDer::Pkcs8(inner) = key else {
            panic!("pkcs8 key expected");
        };

        assert_eq!(inner.secret_pkcs8_der(), KEY_DER);
    }

    #[test]
    fn der_inputs_pass_through() {
        let chain = load_cert_chain(CERT_DER).expect("parse cert der");

        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].as_ref(), CERT_DER);

        let key = load_private_key(KEY_DER).expect("parse key der");
        let PrivateKeyDer::Pkcs8(inner) = key else {
            panic!("pkcs8 key expected");
        };

        assert_eq!(inner.secret_pkcs8_der(), KEY_DER);
    }

    #[test]
    fn rsa_and_ec_pem_keys_map_to_formats() {
        let rsa = load_private_key(RSA_KEY_PEM).expect("parse rsa pem");

        assert!(matches!(rsa, PrivateKeyDer::Pkcs1(_)));

        let ec = load_private_key(EC_KEY_PEM).expect("parse ec pem");

        assert!(matches!(ec, PrivateKeyDer::Sec1(_)));
    }

    #[test]
    fn garbage_and_empty_inputs_reject() {
        assert!(load_cert_chain(&[]).is_err());
        assert!(load_private_key(&[]).is_err());
        assert!(
            load_cert_chain(b"-----BEGIN GARBAGE-----\n!!!!\n-----END GARBAGE-----\n").is_err()
        );
        assert!(load_private_key(KEY_DER).is_ok());
    }

    #[test]
    fn garbage_der_fails_acceptor_build() {
        use crate::TlsAcceptor;

        assert!(TlsAcceptor::new(b"not a cert", b"not a key", 1).is_err());
        assert!(TlsAcceptor::from_pem(b"not a cert", b"not a key", 1).is_err());
    }
}
