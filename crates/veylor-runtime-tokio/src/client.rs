use std::io;
use std::sync::Arc;
use std::time::Duration;

use veylor_client::Field;
use veylor_tls::TlsConnector;

use crate::client_conn::{Deadlines, INITIAL_BUFFER, protocol};
use crate::client_connect::{ConnectTimeouts, connect};
use crate::client_pool::{Lease, Pool, PoolConfig, PoolStats};
use crate::client_url::parse_url;

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
            tls: None,
        }
    }
}

pub struct ClientRequest<'a> {
    pub method: &'a [u8],
    pub url: &'a str,
    pub headers: &'a [Field<'a>],
    pub body: &'a [u8],
}

impl<'a> ClientRequest<'a> {
    pub fn new(method: &'a [u8], url: &'a str) -> Self {
        Self {
            method,
            url,
            headers: &[],
            body: &[],
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
}

pub struct ClientResponse {
    pub status: u16,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Vec<u8>,
}

pub struct ResponseStream {
    lease: Lease,
    status: u16,
    headers: Vec<(Vec<u8>, Vec<u8>)>,
}

impl ResponseStream {
    pub fn status(&self) -> u16 {
        self.status
    }

    pub fn headers(&self) -> &[(Vec<u8>, Vec<u8>)] {
        &self.headers
    }

    pub fn header(&self, name: &str) -> Option<&[u8]> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name.as_bytes()))
            .map(|(_, v)| v.as_slice())
    }

    pub fn alpn(&mut self) -> Option<&[u8]> {
        self.lease.conn().alpn()
    }

    pub async fn chunk(&mut self) -> io::Result<Option<&[u8]>> {
        self.lease.conn().next_chunk().await
    }

    pub async fn collect(mut self, max: usize) -> io::Result<Vec<u8>> {
        let mut body = Vec::new();

        while let Some(chunk) = self.chunk().await? {
            if body.len() + chunk.len() > max {
                return Err(protocol("response body exceeds the limit"));
            }

            body.extend_from_slice(chunk);
        }

        Ok(body)
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
    pub fn new(config: ClientConfig) -> io::Result<Self> {
        let tls = match config.tls.clone() {
            Some(tls) => tls,
            None => TlsConnector::builder()
                .alpn(&[b"http/1.1"])
                .build()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
        };
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

    pub async fn send(&self, request: &ClientRequest<'_>) -> io::Result<ResponseStream> {
        let config = &self.shared.config;
        let url = parse_url(request.url)?;
        let mut lease = self.shared.pool.acquire(&url.origin).await?;

        if !lease.has_conn() {
            let conn = connect(
                &url.origin,
                &self.shared.tls,
                ConnectTimeouts {
                    connect: config.connect_timeout,
                    attempt: config.attempt_timeout,
                    handshake: config.handshake_timeout,
                },
                Deadlines {
                    write: config.write_timeout,
                    head: config.head_timeout,
                    body: config.body_timeout,
                    max_head: config.max_head.max(INITIAL_BUFFER),
                },
            )
            .await?;

            lease.set(conn);
        }

        let host = url.origin.host_header();
        let fields = fields_with_host(request.headers, &host);
        let conn = lease.conn();

        conn.send_request(request.method, url.target.as_bytes(), &fields, request.body)
            .await?;

        let head = conn.read_head().await?;

        Ok(ResponseStream {
            lease,
            status: head.status,
            headers: head.headers,
        })
    }

    pub async fn request(&self, request: &ClientRequest<'_>) -> io::Result<ClientResponse> {
        let stream = self.send(request).await?;
        let status = stream.status;
        let headers = stream.headers.clone();
        let body = stream.collect(self.shared.config.max_body).await?;

        Ok(ClientResponse {
            status,
            headers,
            body,
        })
    }
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
