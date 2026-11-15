use std::io;
use std::sync::Arc;
use std::time::Duration;

use veylor_client::Field;
use veylor_tls::TlsConnector;

use crate::connect::ConnectTimeouts;
use crate::h1::{Deadlines, INITIAL_BUFFER};
use crate::h2::{H2Config, H2Error, H2Request, Upload, UploadSender};
use crate::pool::{PoolConfig, Protocol};
use crate::url::ParsedUrl;

#[derive(Clone)]
pub struct ClientConfig {
    pub connect_timeout: Duration,
    pub attempt_timeout: Duration,
    pub handshake_timeout: Duration,
    pub write_timeout: Duration,
    pub head_timeout: Duration,
    pub body_timeout: Duration,
    pub max_head: usize,
    pub max_body: usize,
    pub pool: PoolConfig,
    pub h2: H2Config,
    pub h2c_prior_knowledge: bool,
    pub tls: Option<TlsConnector>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            attempt_timeout: Duration::from_secs(3),
            handshake_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
            head_timeout: Duration::from_secs(30),
            body_timeout: Duration::from_secs(30),
            max_head: 64 * 1024,
            max_body: 1024 * 1024,
            pool: PoolConfig::default(),
            h2: H2Config::default(),
            h2c_prior_knowledge: false,
            tls: None,
        }
    }
}

impl ClientConfig {
    pub fn connector(&self) -> io::Result<TlsConnector> {
        match self.tls.clone() {
            Some(tls) => Ok(tls),
            None => TlsConnector::builder()
                .alpn(&[b"h2", b"http/1.1"])
                .build()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e)),
        }
    }

    pub fn timeouts(&self) -> ConnectTimeouts {
        ConnectTimeouts {
            connect: self.connect_timeout,
            attempt: self.attempt_timeout,
            handshake: self.handshake_timeout,
        }
    }

    pub fn deadlines(&self) -> Deadlines {
        Deadlines {
            write: self.write_timeout,
            head: self.head_timeout,
            body: self.body_timeout,
            max_head: self.max_head.max(INITIAL_BUFFER),
        }
    }

    pub fn protocol_hint(&self, tls: bool) -> Option<Protocol> {
        match (tls, self.h2c_prior_knowledge) {
            (true, _) => None,
            (false, true) => Some(Protocol::Http2),
            (false, false) => Some(Protocol::Http1),
        }
    }
}

pub struct StreamingBody {
    upload: Arc<Upload>,
}

impl StreamingBody {
    pub fn new(capacity: usize) -> (Self, UploadSender) {
        let (upload, sender) = Upload::pair(capacity);

        (Self { upload }, sender)
    }
}

pub struct ClientRequest<'a> {
    pub method: &'a [u8],
    pub url: &'a str,
    pub headers: &'a [Field<'a>],
    pub body: &'a [u8],
    pub streaming: Option<&'a StreamingBody>,
}

impl<'a> ClientRequest<'a> {
    pub fn new(method: &'a [u8], url: &'a str) -> Self {
        Self {
            method,
            url,
            headers: &[],
            body: &[],
            streaming: None,
        }
    }

    pub fn get(url: &'a str) -> Self {
        Self::new(b"GET", url)
    }

    pub fn headers(mut self, headers: &'a [Field<'a>]) -> Self {
        self.headers = headers;

        self
    }

    pub fn body(mut self, body: &'a [u8]) -> Self {
        self.body = body;

        self
    }

    pub fn streaming(mut self, body: &'a StreamingBody) -> Self {
        self.streaming = Some(body);

        self
    }

    pub fn h2(&self, url: &ParsedUrl) -> H2Request {
        let authority = url.origin.host_header();
        let scheme: &[u8] = if url.origin.tls { b"https" } else { b"http" };
        let mut h2 =
            H2Request::new(self.method, authority.as_bytes(), url.target.as_bytes()).scheme(scheme);

        for field in self.headers {
            if is_connection_specific(field.name) {
                continue;
            }

            h2 = h2.header(field.name, field.value);
        }

        match self.streaming {
            Some(streaming) => h2.with_upload(Arc::clone(&streaming.upload)),
            None => h2.body(self.body.to_vec()),
        }
    }
}

pub fn is_retryable(error: &io::Error) -> bool {
    error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<H2Error>())
        .is_some_and(H2Error::is_retryable)
}

pub fn streaming_unsupported() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "streaming request bodies need HTTP/2",
    )
}

pub fn fields_with_host<'a>(headers: &'a [Field<'a>], host: &'a str) -> Vec<Field<'a>> {
    let mut fields = Vec::with_capacity(headers.len() + 1);

    if !headers.iter().any(|f| f.name.eq_ignore_ascii_case(b"host")) {
        fields.push(Field {
            name: b"host",
            value: host.as_bytes(),
        });
    }

    fields.extend_from_slice(headers);

    fields
}

fn is_connection_specific(name: &[u8]) -> bool {
    [
        b"host".as_slice(),
        b"connection",
        b"keep-alive",
        b"proxy-connection",
        b"transfer-encoding",
        b"upgrade",
        b"te",
    ]
    .iter()
    .any(|blocked| name.eq_ignore_ascii_case(blocked))
}
