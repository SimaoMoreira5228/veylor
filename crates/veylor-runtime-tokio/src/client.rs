use std::io;
use std::sync::Arc;
use std::time::Duration;

use veylor_client::Field;
use veylor_tls::TlsConnector;

use crate::client_conn::{Deadlines, INITIAL_BUFFER};
use crate::client_connect::{ConnectPlan, ConnectTimeouts, Connected, connect};
use crate::client_pool::{Grant, H2Ticket, Lease, Pool, PoolConfig, PoolStats, Protocol};
use crate::client_response::{ClientResponse, ResponseStream};
use crate::client_url::{ParsedUrl, parse_url};
use crate::h2_client::{H2Config, H2Error, H2Request, UploadSender};

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

pub struct StreamingBody {
    pub(crate) upload: Arc<crate::h2_client::Upload>,
}

impl StreamingBody {
    pub fn new(capacity: usize) -> (Self, UploadSender) {
        let (upload, sender) = crate::h2_client::Upload::pair(capacity);

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
}

struct Shared {
    config: ClientConfig,
    tls: TlsConnector,
    pool: Arc<Pool>,
}

#[derive(Clone)]
pub struct Client {
    shared: Arc<Shared>,
}

impl Client {
    pub fn new(mut config: ClientConfig) -> io::Result<Self> {
        let tls = match config.tls.clone() {
            Some(tls) => tls,
            None => TlsConnector::builder()
                .alpn(&[b"h2", b"http/1.1"])
                .build()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
        };

        config.h2.idle_timeout = Some(config.pool.idle_timeout);

        let pool = Pool::new(config.pool.clone());

        Ok(Self {
            shared: Arc::new(Shared { config, tls, pool }),
        })
    }

    pub fn pool_stats(&self) -> PoolStats {
        self.shared.pool.stats()
    }

    pub fn close_idle(&self) {
        self.shared.pool.close_idle();
    }

    pub fn is_retryable(error: &io::Error) -> bool {
        error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<H2Error>())
            .is_some_and(H2Error::is_retryable)
    }

    pub async fn send(&self, request: &ClientRequest<'_>) -> io::Result<ResponseStream> {
        let config = &self.shared.config;
        let url = parse_url(request.url)?;
        let hint = match (url.origin.tls, config.h2c_prior_knowledge) {
            (true, _) => None,
            (false, true) => Some(Protocol::Http2),
            (false, false) => Some(Protocol::Http1),
        };

        if request.streaming.is_some() && hint == Some(Protocol::Http1) {
            return Err(streaming_unsupported());
        }

        match self.shared.pool.acquire(&url.origin, hint).await? {
            Grant::Idle(lease) => self.send_h1(lease, &url, request).await,
            Grant::H2(ticket) => self.send_h2(ticket, &url, request).await,
            Grant::Dial(dial) => {
                let plan = ConnectPlan {
                    tls: &self.shared.tls,
                    timeouts: ConnectTimeouts {
                        connect: config.connect_timeout,
                        attempt: config.attempt_timeout,
                        handshake: config.handshake_timeout,
                    },
                    deadlines: Deadlines {
                        write: config.write_timeout,
                        head: config.head_timeout,
                        body: config.body_timeout,
                        max_head: config.max_head.max(INITIAL_BUFFER),
                    },
                    h2: &config.h2,
                    prior_knowledge_h2: config.h2c_prior_knowledge,
                };

                match connect(&url.origin, &plan).await? {
                    Connected::Http1(conn) => {
                        let lease = dial.finish_h1(conn);

                        self.send_h1(lease, &url, request).await
                    }
                    Connected::Http2(client) => {
                        let ticket = dial.finish_h2(client);

                        self.send_h2(ticket, &url, request).await
                    }
                }
            }
        }
    }

    pub async fn request(&self, request: &ClientRequest<'_>) -> io::Result<ClientResponse> {
        let stream = self.send(request).await?;

        stream.into_buffered(self.shared.config.max_body).await
    }

    async fn send_h1(
        &self,
        mut lease: Lease,
        url: &ParsedUrl,
        request: &ClientRequest<'_>,
    ) -> io::Result<ResponseStream> {
        if request.streaming.is_some() {
            return Err(streaming_unsupported());
        }

        let host = url.origin.host_header();
        let fields = fields_with_host(request.headers, &host);
        let conn = lease.conn();

        conn.send_request(request.method, url.target.as_bytes(), &fields, request.body)
            .await?;

        let head = conn.read_head().await?;

        Ok(ResponseStream::http1(head.status, head.headers, lease))
    }

    async fn send_h2(
        &self,
        ticket: H2Ticket,
        url: &ParsedUrl,
        request: &ClientRequest<'_>,
    ) -> io::Result<ResponseStream> {
        let authority = url.origin.host_header();
        let scheme: &[u8] = if url.origin.tls { b"https" } else { b"http" };
        let mut h2 = H2Request::new(request.method, authority.as_bytes(), url.target.as_bytes())
            .scheme(scheme);

        for field in request.headers {
            if is_connection_specific(field.name) {
                continue;
            }

            h2 = h2.header(field.name, field.value);
        }

        h2 = match request.streaming {
            Some(streaming) => h2.with_upload(Arc::clone(&streaming.upload)),
            None => h2.body(request.body.to_vec()),
        };

        let response = ticket.client().send(h2).await?;

        Ok(ResponseStream::http2(
            response.status,
            response.headers,
            response.body,
            ticket,
        ))
    }
}

fn streaming_unsupported() -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "streaming request bodies need HTTP/2",
    )
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

fn fields_with_host<'a>(headers: &'a [Field<'a>], host: &'a str) -> Vec<Field<'a>> {
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
