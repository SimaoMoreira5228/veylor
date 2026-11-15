use std::io;
use std::sync::Arc;
use std::time::Instant;

use smol::Timer;
use smol::future::or;
use veylor_client_driver::{
    ClientConfig, ClientRequest, ClientResponse, Grant, H2Ticket, Lease, ParsedUrl, Pool,
    PoolStats, Protocol, TlsConnector, WaitGuard, fields_with_host, is_retryable, parse_url,
    streaming_unsupported, timed_out,
};

use crate::client_conn::Connection;
use crate::client_connect::{ConnectPlan, Connected, connect};

pub type ResponseStream = veylor_client_driver::ResponseStream<Connection>;

struct Shared {
    config: ClientConfig,
    tls: TlsConnector,
    pool: Arc<Pool<Connection>>,
}

#[derive(Clone)]
pub struct Client {
    shared: Arc<Shared>,
}

impl Client {
    pub fn new(mut config: ClientConfig) -> io::Result<Self> {
        let tls = config.connector()?;

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
        is_retryable(error)
    }

    pub async fn send(&self, request: &ClientRequest<'_>) -> io::Result<ResponseStream> {
        let config = &self.shared.config;
        let url = parse_url(request.url)?;
        let hint = config.protocol_hint(url.origin.tls);

        if request.streaming.is_some() && hint == Some(Protocol::Http1) {
            return Err(streaming_unsupported());
        }

        match self.acquire(&url, hint).await? {
            Grant::Idle(lease) => self.send_h1(lease, &url, request).await,
            Grant::H2(ticket) => self.send_h2(ticket, &url, request).await,
            Grant::Dial(dial) => {
                let plan = ConnectPlan {
                    tls: &self.shared.tls,
                    timeouts: config.timeouts(),
                    deadlines: config.deadlines(),
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

    async fn acquire(
        &self,
        url: &ParsedUrl,
        hint: Option<Protocol>,
    ) -> io::Result<Grant<Connection>> {
        let pool = &self.shared.pool;
        let deadline = Instant::now() + pool.acquire_timeout();
        let mut guard: Option<WaitGuard> = None;

        loop {
            match pool.poll_acquire(&url.origin, hint, &mut guard)? {
                Ok(grant) => return Ok(grant),
                Err(listener) => {
                    let notified = async {
                        listener.await;

                        true
                    };
                    let expired = async {
                        Timer::at(deadline).await;

                        false
                    };

                    if !or(notified, expired).await {
                        return Err(timed_out("waiting for a pooled connection"));
                    }
                }
            }
        }
    }

    async fn send_h1(
        &self,
        mut lease: Lease<Connection>,
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
        let response = ticket.client().send(request.h2(url)).await?;

        Ok(ResponseStream::http2(
            response.status,
            response.headers,
            response.body,
            ticket,
        ))
    }
}
