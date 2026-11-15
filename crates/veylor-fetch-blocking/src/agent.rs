use std::io;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use veylor_tls::TlsConnector;

use crate::budget::Budget;
use crate::config::{AgentBuilder, Config};
use crate::error::{Error, TimeoutKind};
use crate::h1::H1Conn;
use crate::h2::{H2Handle, H2Settings};
use crate::outgoing::Outgoing;
use crate::pool::{Pool, PoolStats};
use crate::request::Request;
use crate::response::{Body, H1Body, H2Body, Protocol, Response};
use crate::url::{Origin, Url};
use crate::wire::Wire;

const H2_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_ATTEMPTS: usize = 3;

struct Inner {
    config: Config,
    pool: Pool,
    connector: OnceLock<Result<TlsConnector, Error>>,
}

#[derive(Clone)]
pub struct Agent {
    inner: Arc<Inner>,
}

enum Conn {
    H1(Box<H1Conn>),
    H2(Arc<H2Handle>),
}

struct Failed {
    error: Error,
    retry: bool,
}

impl Agent {
    pub fn new() -> Self {
        Self::from_config(Config::default())
    }

    pub fn builder() -> AgentBuilder {
        AgentBuilder::new()
    }

    pub(crate) fn from_config(config: Config) -> Self {
        let pool = Pool::new(
            config.idle_timeout,
            config.max_idle_per_host,
            config.max_idle_total,
        );

        Self {
            inner: Arc::new(Inner {
                config,
                pool,
                connector: OnceLock::new(),
            }),
        }
    }

    pub fn request(&self, method: &str, url: &str) -> Request {
        Request::new(self.clone(), method, url)
    }

    pub fn get(&self, url: &str) -> Request {
        self.request("GET", url)
    }

    pub fn post(&self, url: &str) -> Request {
        self.request("POST", url)
    }

    pub fn put(&self, url: &str) -> Request {
        self.request("PUT", url)
    }

    pub fn patch(&self, url: &str) -> Request {
        self.request("PATCH", url)
    }

    pub fn delete(&self, url: &str) -> Request {
        self.request("DELETE", url)
    }

    pub fn head(&self, url: &str) -> Request {
        self.request("HEAD", url)
    }

    pub fn pool_stats(&self) -> PoolStats {
        self.inner.pool.stats()
    }

    pub(crate) fn pool(&self) -> &Pool {
        &self.inner.pool
    }

    pub(crate) fn execute(
        &self,
        method: &str,
        url: Url,
        mut headers: Vec<(String, String)>,
        body: &[u8],
        timeout: Option<Duration>,
    ) -> Result<Response, Error> {
        let config = &self.inner.config;
        let overall = timeout.or(config.overall_timeout);
        let budget = Budget {
            read: config.read_timeout,
            write: config.write_timeout,
            overall: overall.map(|limit| Instant::now() + limit),
        };

        self.apply_defaults(&mut headers);

        let mut method = method.to_owned();
        let mut url = url;
        let mut body = body;
        let mut redirects = 0;

        loop {
            let mut response = self.round_trip(&method, &url, &headers, body, &budget)?;
            let location = redirect_location(&response);

            let Some(location) = location.filter(|_| config.max_redirects > 0) else {
                return self.finish(response);
            };

            if redirects >= config.max_redirects {
                return Err(Error::TooManyRedirects);
            }

            redirects += 1;

            let next = url.resolve(&location)?;
            let status = response.status();

            response.discard();
            drop(response);

            if drops_body(status, &method) {
                if status == 303 || method == "POST" {
                    "GET".clone_into(&mut method);
                }

                body = &[];
                headers.retain(|(name, _)| !is_body_header(name));
            }

            // SECURITY: credentials must not follow a redirect to another origin (RFC 9110 15.4).
            if next.origin != url.origin {
                headers.retain(|(name, _)| !is_credential_header(name));
            }

            url = next;
        }
    }

    fn finish(&self, response: Response) -> Result<Response, Error> {
        let status = response.status();

        if status >= 400 && self.inner.config.status_as_error {
            return Err(Error::Status(status, Box::new(response)));
        }

        Ok(response)
    }

    fn apply_defaults(&self, headers: &mut Vec<(String, String)>) {
        let config = &self.inner.config;

        for (name, value) in &config.default_headers {
            if !headers
                .iter()
                .any(|(existing, _)| existing.eq_ignore_ascii_case(name))
            {
                headers.push((name.clone(), value.clone()));
            }
        }

        if !headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("user-agent"))
        {
            headers.push(("user-agent".to_owned(), config.user_agent.clone()));
        }
    }

    fn round_trip(
        &self,
        method: &str,
        url: &Url,
        headers: &[(String, String)],
        body: &[u8],
        budget: &Budget,
    ) -> Result<Response, Error> {
        let host = url.origin.host_header();
        let request = Outgoing {
            method,
            host: &host,
            target: &url.target,
            headers,
            body,
        };

        for attempt in 0..MAX_ATTEMPTS {
            let reuse = attempt + 1 < MAX_ATTEMPTS;
            let (conn, reused) = self.checkout(&url.origin, budget, reuse)?;

            match self.exchange(conn, &request, url, budget) {
                Ok(response) => return Ok(response),
                Err(failed) if reused && failed.retry && request.is_idempotent() => {}
                Err(failed) => return Err(failed.error),
            }
        }

        Err(Error::Protocol("no usable connection".to_owned()))
    }

    fn checkout(
        &self,
        origin: &Origin,
        budget: &Budget,
        reuse: bool,
    ) -> Result<(Conn, bool), Error> {
        if reuse {
            if origin.tls
                && let Some(handle) = self.inner.pool.take_h2(origin)
            {
                return Ok((Conn::H2(handle), true));
            }

            if let Some(conn) = self.inner.pool.take_h1(origin) {
                return Ok((Conn::H1(conn), true));
            }
        }

        Ok((self.connect(origin, budget)?, false))
    }

    fn connect(&self, origin: &Origin, budget: &Budget) -> Result<Conn, Error> {
        let config = &self.inner.config;
        let connector = if origin.tls {
            Some(self.connector()?)
        } else {
            None
        };
        let wire = Wire::connect(origin, connector, config.connect_timeout, budget)?;

        if wire.alpn() == Some(b"h2".as_slice()) {
            let settings = H2Settings {
                max_head: config.max_head_size,
                idle_timeout: config.idle_timeout,
                handshake_timeout: H2_HANDSHAKE_TIMEOUT,
            };
            let handle = H2Handle::spawn(wire, settings)?;

            self.inner.pool.add_h2(origin, &handle);

            return Ok(Conn::H2(handle));
        }

        Ok(Conn::H1(Box::new(H1Conn::new(wire, config.max_head_size))))
    }

    fn connector(&self) -> Result<&TlsConnector, Error> {
        let built = self
            .inner
            .connector
            .get_or_init(|| self.inner.config.connector());

        match built {
            Ok(connector) => Ok(connector),
            Err(error) => Err(Error::Tls(error.to_string())),
        }
    }

    fn exchange(
        &self,
        conn: Conn,
        request: &Outgoing<'_>,
        url: &Url,
        budget: &Budget,
    ) -> Result<Response, Failed> {
        match conn {
            Conn::H1(conn) => self.exchange_h1(conn, request, url, budget),
            Conn::H2(handle) => self.exchange_h2(handle, request, url, budget),
        }
    }

    fn exchange_h1(
        &self,
        mut conn: Box<H1Conn>,
        request: &Outgoing<'_>,
        url: &Url,
        budget: &Budget,
    ) -> Result<Response, Failed> {
        if let Err(error) = conn.send(request, budget) {
            let retry = is_stale_error(&error);

            return Err(failed(error, TimeoutKind::Write, budget, retry));
        }

        let (status, headers) = match conn.head(budget) {
            Ok(head) => head,
            Err(error) => {
                let retry = conn.received_nothing() && is_stale_error(&error);

                return Err(failed(error, TimeoutKind::Read, budget, retry));
            }
        };
        let body = Body::H1(H1Body {
            conn: Some(conn),
            agent: self.clone(),
            origin: url.origin.clone(),
            budget: *budget,
        });

        Ok(self.response(status, Protocol::Http11, headers, url, body))
    }

    fn exchange_h2(
        &self,
        handle: Arc<H2Handle>,
        request: &Outgoing<'_>,
        url: &Url,
        budget: &Budget,
    ) -> Result<Response, Failed> {
        let authority = url.origin.host_header();

        match handle.request(request, &authority, budget) {
            Ok((status, headers)) => {
                let body = Body::H2(H2Body {
                    handle,
                    budget: *budget,
                    released: false,
                });

                Ok(self.response(status, Protocol::Http2, headers, url, body))
            }
            Err(refused) => {
                handle.abandon();
                handle.release();

                Err(failed(
                    refused.error,
                    TimeoutKind::Read,
                    budget,
                    refused.retryable,
                ))
            }
        }
    }

    fn response(
        &self,
        status: u16,
        protocol: Protocol,
        headers: crate::headers::Headers,
        url: &Url,
        body: Body,
    ) -> Response {
        let max_body = self.inner.config.max_body_size;

        Response::new(status, protocol, headers, url.display(), max_body, body)
    }
}

impl Default for Agent {
    fn default() -> Self {
        Self::new()
    }
}

fn failed(error: io::Error, phase: TimeoutKind, budget: &Budget, retry: bool) -> Failed {
    Failed {
        error: Error::from_io(error, phase, budget.expired()),
        retry,
    }
}

fn is_stale_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::UnexpectedEof
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::NotConnected
    )
}

fn redirect_location(response: &Response) -> Option<String> {
    if !matches!(response.status(), 301 | 302 | 303 | 307 | 308) {
        return None;
    }

    response.header("location").map(str::to_owned)
}

fn drops_body(status: u16, method: &str) -> bool {
    match status {
        303 => method != "HEAD",
        301 | 302 => method == "POST",
        _ => false,
    }
}

fn is_body_header(name: &str) -> bool {
    name.eq_ignore_ascii_case("content-type") || name.eq_ignore_ascii_case("content-encoding")
}

fn is_credential_header(name: &str) -> bool {
    name.eq_ignore_ascii_case("authorization")
        || name.eq_ignore_ascii_case("proxy-authorization")
        || name.eq_ignore_ascii_case("cookie")
}
