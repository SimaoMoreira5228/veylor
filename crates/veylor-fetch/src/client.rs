use std::sync::Arc;
use std::time::Duration;

use veylor_client_driver::{ClientConfig, PoolStats, TlsConnector};

use crate::error::{Error, Result};
use crate::headers::Headers;
use crate::method::Method;
use crate::policy::Policy;
use crate::request::RequestBuilder;
use crate::runtime::{Runtime, Transport};
use crate::url::IntoUrl;

const DEFAULT_MAX_BODY: usize = 16 * 1024 * 1024;

pub(crate) struct Inner {
    pub transport: Transport,
    pub headers: Headers,
    pub policy: Policy,
    pub timeout: Option<Duration>,
    pub max_body: usize,
}

#[derive(Clone)]
pub struct Client {
    pub(crate) inner: Arc<Inner>,
}

impl Client {
    pub fn new() -> Self {
        Self::builder()
            .build()
            .expect("default client config is valid")
    }

    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    pub fn request(&self, method: Method, url: impl IntoUrl) -> RequestBuilder {
        RequestBuilder::new(self.clone(), method, url.into_url())
    }

    pub fn get(&self, url: impl IntoUrl) -> RequestBuilder {
        self.request(Method::Get, url)
    }

    pub fn post(&self, url: impl IntoUrl) -> RequestBuilder {
        self.request(Method::Post, url)
    }

    pub fn put(&self, url: impl IntoUrl) -> RequestBuilder {
        self.request(Method::Put, url)
    }

    pub fn patch(&self, url: impl IntoUrl) -> RequestBuilder {
        self.request(Method::Patch, url)
    }

    pub fn delete(&self, url: impl IntoUrl) -> RequestBuilder {
        self.request(Method::Delete, url)
    }

    pub fn head(&self, url: impl IntoUrl) -> RequestBuilder {
        self.request(Method::Head, url)
    }

    pub fn pool_stats(&self) -> PoolStats {
        self.inner.transport.stats()
    }

    pub fn close_idle(&self) {
        self.inner.transport.close_idle();
    }

    pub fn runtime(&self) -> Runtime {
        self.inner.transport.runtime()
    }
}

impl Default for Client {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ClientBuilder {
    config: ClientConfig,
    runtime: Runtime,
    headers: Headers,
    error: Option<Error>,
    roots: Vec<Vec<u8>>,
    builtin_roots: bool,
    policy: Policy,
    timeout: Option<Duration>,
    max_body: usize,
    user_agent: Option<String>,
}

impl ClientBuilder {
    pub fn new() -> Self {
        Self {
            config: ClientConfig {
                max_body: DEFAULT_MAX_BODY,
                ..ClientConfig::default()
            },
            runtime: Runtime::default(),
            headers: Headers::default(),
            error: None,
            roots: Vec::new(),
            builtin_roots: true,
            policy: Policy::default(),
            timeout: None,
            max_body: DEFAULT_MAX_BODY,
            user_agent: None,
        }
    }

    pub fn runtime(mut self, runtime: Runtime) -> Self {
        self.runtime = runtime;

        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);

        self
    }

    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.config.connect_timeout = timeout;

        self
    }

    pub fn read_timeout(mut self, timeout: Duration) -> Self {
        self.config.head_timeout = timeout;
        self.config.body_timeout = timeout;

        self
    }

    pub fn pool_idle_timeout(mut self, timeout: Duration) -> Self {
        self.config.pool.idle_timeout = timeout;

        self
    }

    pub fn pool_max_idle_per_host(mut self, max: usize) -> Self {
        self.config.pool.max_idle_per_origin = max;

        self
    }

    pub fn pool_max_idle(mut self, max: usize) -> Self {
        self.config.pool.max_idle_total = max;

        self
    }

    pub fn max_connections_per_host(mut self, max: usize) -> Self {
        self.config.pool.max_per_origin = max;

        self
    }

    pub fn redirect(mut self, policy: Policy) -> Self {
        self.policy = policy;

        self
    }

    pub fn max_body(mut self, max: usize) -> Self {
        self.max_body = max;
        self.config.max_body = max;

        self
    }

    pub fn user_agent(mut self, value: impl Into<String>) -> Self {
        self.user_agent = Some(value.into());

        self
    }

    pub fn default_header(mut self, name: impl AsRef<str>, value: impl AsRef<[u8]>) -> Self {
        if let Err(error) = self.headers.push(name.as_ref(), value.as_ref()) {
            self.error.get_or_insert(error);
        }

        self
    }

    pub fn default_headers<I, K, V>(mut self, headers: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<[u8]>,
    {
        for (name, value) in headers {
            self = self.default_header(name, value);
        }

        self
    }

    pub fn add_root_certificate(mut self, pem_or_der: &[u8]) -> Self {
        self.roots.push(pem_or_der.to_vec());

        self
    }

    pub fn builtin_root_certificates(mut self, enabled: bool) -> Self {
        self.builtin_roots = enabled;

        self
    }

    pub fn http2_prior_knowledge(mut self) -> Self {
        self.config.h2c_prior_knowledge = true;

        self
    }

    pub fn build(mut self) -> Result<Client> {
        if let Some(error) = self.error.take() {
            return Err(error);
        }

        if !self.headers.contains("user-agent") {
            let agent = self
                .user_agent
                .take()
                .unwrap_or_else(|| concat!("veylor-fetch/", env!("CARGO_PKG_VERSION")).to_owned());

            self.headers.push("user-agent", agent.as_bytes())?;
        }

        if !self.roots.is_empty() || !self.builtin_roots {
            self.config.tls = Some(self.connector()?);
        }

        let transport =
            Transport::new(self.runtime, self.config).map_err(|e| Error::builder(e.to_string()))?;

        Ok(Client {
            inner: Arc::new(Inner {
                transport,
                headers: self.headers,
                policy: self.policy,
                timeout: self.timeout,
                max_body: self.max_body,
            }),
        })
    }

    fn connector(&self) -> Result<TlsConnector> {
        let mut builder = TlsConnector::builder()
            .default_roots(self.builtin_roots)
            .alpn(&[b"h2", b"http/1.1"]);

        for root in &self.roots {
            builder = builder
                .add_root(root)
                .map_err(|e| Error::builder(format!("bad root certificate: {e}")))?;
        }

        builder
            .build()
            .map_err(|e| Error::builder(format!("tls setup failed: {e}")))
    }
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}
