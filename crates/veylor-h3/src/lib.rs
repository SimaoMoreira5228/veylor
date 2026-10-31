// NOTE: the HTTP/3 connection layer is Veylor-owned and runs on the in-repo
// veylor-http3 frame/QPACK codec. QPACK dynamic state is disabled (capacity 0).

use std::fmt;
use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use http::header::CONTENT_LENGTH;
use http::{Request, Response, StatusCode};
use quinn::VarInt;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

pub use bytes;
pub use bytes::Bytes;
pub use http;

mod connection;

const MAX_UNI_STREAMS: u64 = 8;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Tls(veylor_tls::TlsError),
    Config(&'static str),
    Runtime(std::io::Error),
    Thread,
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "io: {error}"),
            Self::Tls(error) => write!(formatter, "tls: {error}"),
            Self::Config(reason) => write!(formatter, "config: {reason}"),
            Self::Runtime(error) => write!(formatter, "runtime: {error}"),
            Self::Thread => write!(formatter, "h3 thread panicked"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<veylor_tls::TlsError> for Error {
    fn from(error: veylor_tls::TlsError) -> Self {
        Self::Tls(error)
    }
}

pub struct Tls {
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
}

impl Tls {
    pub fn new(certs: Vec<CertificateDer<'static>>, key: PrivateKeyDer<'static>) -> Self {
        Self { certs, key }
    }

    pub fn from_files(cert: &std::path::Path, key: &std::path::Path) -> Result<Self, Error> {
        let chain = veylor_tls::load_cert_chain(&fs::read(cert)?)?;
        let key = veylor_tls::load_private_key(&fs::read(key)?)?;
        Ok(Self { certs: chain, key })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_connections: usize,
    pub max_streams: u64,
    pub idle_timeout: Duration,
    pub max_field_section: u64,
    pub max_request_body: usize,
    pub max_response_body: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_connections: 256,
            max_streams: 100,
            idle_timeout: Duration::from_secs(30),
            max_field_section: 16 * 1024,
            max_request_body: 1024 * 1024,
            max_response_body: 4 * 1024 * 1024,
        }
    }
}

pub struct Config {
    pub tls: Tls,
    pub limits: Limits,
}

pub trait Handler: Send + Sync + 'static {
    fn handle(&self, request: Request<()>, body: Bytes) -> Response<Bytes>;
}

impl<F> Handler for F
where
    F: Fn(Request<()>, Bytes) -> Response<Bytes> + Send + Sync + 'static,
{
    fn handle(&self, request: Request<()>, body: Bytes) -> Response<Bytes> {
        self(request, body)
    }
}

pub struct Server {
    address: SocketAddr,
    config: Config,
}

impl Server {
    pub fn bind(address: SocketAddr, config: Config) -> Self {
        Self { address, config }
    }

    pub fn start<H: Handler>(self, handler: H) -> Result<RunningServer, Error> {
        let server_config = server_config(
            self.config.tls.certs.clone(),
            self.config.tls.key.clone_key(),
            self.config.limits,
        )?;
        let limits = self.config.limits;
        let handler: Arc<dyn Handler> = Arc::new(handler);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(Error::Runtime)?;
        let entered = runtime.enter();
        let endpoint = quinn::Endpoint::server(server_config, self.address)?;
        let address = endpoint.local_addr()?;
        drop(entered);
        let accept_endpoint = endpoint.clone();
        let thread = std::thread::Builder::new()
            .name("veylor-h3".to_owned())
            .spawn(move || {
                runtime.block_on(accept_loop(accept_endpoint, handler, limits));
            })?;
        Ok(RunningServer {
            address,
            endpoint,
            limits,
            thread: Some(thread),
        })
    }
}

pub struct RunningServer {
    address: SocketAddr,
    endpoint: quinn::Endpoint,
    limits: Limits,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl RunningServer {
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn rotate_tls(
        &self,
        certs: Vec<rustls::pki_types::CertificateDer<'static>>,
        key: rustls::pki_types::PrivateKeyDer<'static>,
    ) -> Result<(), Error> {
        let config = server_config(certs, key, self.limits)?;
        self.endpoint.set_server_config(Some(config));
        Ok(())
    }

    pub fn stop(&self) {
        self.endpoint.close(VarInt::from_u32(0), b"server shutdown");
    }

    pub fn join(mut self) -> Result<(), Error> {
        self.stop();
        if let Some(thread) = self.thread.take() {
            thread.join().map_err(|_| Error::Thread)?;
        }
        Ok(())
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        self.stop();
    }
}

fn server_config(
    certs: Vec<rustls::pki_types::CertificateDer<'static>>,
    key: rustls::pki_types::PrivateKeyDer<'static>,
    limits: Limits,
) -> Result<quinn::ServerConfig, Error> {
    let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .map_err(|_| Error::Config("tls 1.3 unsupported"))?
    .with_no_client_auth()
    .with_single_cert(certs, key)
    .map_err(|_| Error::Config("invalid certificate or key"))?;
    tls.alpn_protocols = vec![b"h3".to_vec()];
    tls.max_early_data_size = 0;
    tls.send_half_rtt_data = false;

    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls)
        .map_err(|_| Error::Config("no quic cipher suite"))?;
    let mut server = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    let mut transport = quinn::TransportConfig::default();
    let streams =
        VarInt::from_u64(limits.max_streams).map_err(|_| Error::Config("invalid stream limit"))?;
    let uni =
        VarInt::from_u64(MAX_UNI_STREAMS).map_err(|_| Error::Config("invalid stream limit"))?;
    transport.max_concurrent_bidi_streams(streams);
    transport.max_concurrent_uni_streams(uni);
    transport.max_idle_timeout(Some(
        limits
            .idle_timeout
            .try_into()
            .map_err(|_| Error::Config("invalid idle timeout"))?,
    ));
    server.transport_config(Arc::new(transport));
    Ok(server)
}

async fn accept_loop(endpoint: quinn::Endpoint, handler: Arc<dyn Handler>, limits: Limits) {
    while let Some(incoming) = endpoint.accept().await {
        if endpoint.open_connections() >= limits.max_connections {
            incoming.refuse();
            continue;
        }
        let handler = Arc::clone(&handler);
        tokio::spawn(async move {
            let Ok(connection) = incoming.await else {
                return;
            };
            connection::serve(connection, handler, limits).await;
        });
    }
}

pub fn text_response(status: StatusCode, body: impl Into<Bytes>) -> Response<Bytes> {
    let body = body.into();
    Response::builder()
        .status(status)
        .header(CONTENT_LENGTH, body.len())
        .body(body)
        .expect("valid response")
}
