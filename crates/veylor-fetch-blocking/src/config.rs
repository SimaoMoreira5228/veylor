use std::time::Duration;

use veylor_tls::{Certificate, TlsConnector, load_cert_chain};

use crate::agent::Agent;
use crate::error::Error;
use crate::outgoing::check_header;

#[derive(Clone, Debug)]
pub(crate) struct Config {
    pub(crate) connect_timeout: Option<Duration>,
    pub(crate) read_timeout: Option<Duration>,
    pub(crate) write_timeout: Option<Duration>,
    pub(crate) overall_timeout: Option<Duration>,
    pub(crate) idle_timeout: Duration,
    pub(crate) max_idle_per_host: usize,
    pub(crate) max_idle_total: usize,
    pub(crate) max_redirects: usize,
    pub(crate) user_agent: String,
    pub(crate) default_headers: Vec<(String, String)>,
    pub(crate) max_body_size: usize,
    pub(crate) max_head_size: usize,
    pub(crate) status_as_error: bool,
    pub(crate) http1_only: bool,
    pub(crate) roots: Vec<Certificate>,
    pub(crate) pinned: Option<Certificate>,
    pub(crate) default_roots: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            connect_timeout: Some(Duration::from_secs(30)),
            read_timeout: Some(Duration::from_secs(30)),
            write_timeout: Some(Duration::from_secs(30)),
            overall_timeout: None,
            idle_timeout: Duration::from_secs(60),
            max_idle_per_host: 8,
            max_idle_total: 64,
            max_redirects: 5,
            user_agent: concat!("veylor-fetch-blocking/", env!("CARGO_PKG_VERSION")).to_owned(),
            default_headers: Vec::new(),
            max_body_size: 10 * 1024 * 1024,
            max_head_size: 64 * 1024,
            status_as_error: true,
            http1_only: false,
            roots: Vec::new(),
            pinned: None,
            default_roots: true,
        }
    }
}

impl Config {
    pub(crate) fn connector(&self) -> Result<TlsConnector, Error> {
        let tls = |error: veylor_tls::TlsError| Error::Tls(error.to_string());
        let mut builder = TlsConnector::builder().default_roots(self.default_roots);

        if self.http1_only {
            builder = builder.alpn(&[b"http/1.1"]);
        }

        for root in &self.roots {
            builder = builder.add_root(root.as_der()).map_err(tls)?;
        }

        if let Some(pinned) = &self.pinned {
            builder = builder.pin_certificate(pinned.as_der()).map_err(tls)?;
        }

        builder.build().map_err(tls)
    }
}

#[derive(Clone, Debug, Default)]
pub struct AgentBuilder {
    config: Config,
    error: Option<String>,
}

impl AgentBuilder {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub fn timeout_connect(mut self, timeout: Duration) -> Self {
        self.config.connect_timeout = Some(timeout);
        self
    }

    pub fn timeout_read(mut self, timeout: Duration) -> Self {
        self.config.read_timeout = Some(timeout);
        self
    }

    pub fn timeout_write(mut self, timeout: Duration) -> Self {
        self.config.write_timeout = Some(timeout);
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.config.overall_timeout = Some(timeout);
        self
    }

    pub fn no_timeouts(mut self) -> Self {
        self.config.connect_timeout = None;
        self.config.read_timeout = None;
        self.config.write_timeout = None;
        self.config.overall_timeout = None;
        self
    }

    pub fn max_idle_connections(mut self, total: usize) -> Self {
        self.config.max_idle_total = total;
        self
    }

    pub fn max_idle_connections_per_host(mut self, per_host: usize) -> Self {
        self.config.max_idle_per_host = per_host;
        self
    }

    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.config.idle_timeout = timeout;
        self
    }

    pub fn redirects(mut self, limit: usize) -> Self {
        self.config.max_redirects = limit;
        self
    }

    pub fn user_agent(mut self, user_agent: &str) -> Self {
        self.config.user_agent = user_agent.to_owned();
        self
    }

    pub fn header(mut self, name: &str, value: &str) -> Self {
        if let Err(error) = check_header(name, value) {
            self.error.get_or_insert(error.to_string());

            return self;
        }

        self.config
            .default_headers
            .push((name.to_ascii_lowercase(), value.to_owned()));
        self
    }

    pub fn max_response_body(mut self, limit: usize) -> Self {
        self.config.max_body_size = limit;
        self
    }

    pub fn max_response_head(mut self, limit: usize) -> Self {
        self.config.max_head_size = limit.clamp(4 * 1024, 1024 * 1024);
        self
    }

    pub fn http_status_as_error(mut self, enabled: bool) -> Self {
        self.config.status_as_error = enabled;
        self
    }

    pub fn http1_only(mut self, enabled: bool) -> Self {
        self.config.http1_only = enabled;
        self
    }

    pub fn default_roots(mut self, enabled: bool) -> Self {
        self.config.default_roots = enabled;
        self
    }

    pub fn add_root_certificate(mut self, pem_or_der: &[u8]) -> Result<Self, Error> {
        let chain = load_cert_chain(pem_or_der).map_err(|error| Error::Tls(error.to_string()))?;

        self.config.roots.extend(chain);

        Ok(self)
    }

    // SECURITY: a pinned certificate replaces chain and name validation for every https origin of
    // the agent; only that exact end-entity certificate is accepted.
    pub fn pin_certificate(mut self, pem_or_der: &[u8]) -> Result<Self, Error> {
        let mut chain =
            load_cert_chain(pem_or_der).map_err(|error| Error::Tls(error.to_string()))?;

        if chain.len() != 1 {
            return Err(Error::Tls(
                "pinning needs exactly one certificate".to_owned(),
            ));
        }

        self.config.pinned = chain.pop();

        Ok(self)
    }

    pub fn build(self) -> Result<Agent, Error> {
        if let Some(message) = self.error {
            return Err(Error::InvalidRequest(message));
        }

        Ok(Agent::from_config(self.config))
    }
}
