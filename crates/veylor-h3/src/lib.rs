// NOTE: the h3 0.0.8 mapping layer leaves 13 h3spec error-path cases unhandled (QPACK control
// streams, push-stream creation, duplicate/ordered pseudo-headers, SETTINGS framing). Normal
// requests pass; patching or replacing the mapping layer is tracked in the whitepaper.

use std::fmt;
use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, Bytes};
use http::header::{CONTENT_LENGTH, TE};
use http::{Method, Request, Response, StatusCode};
use quinn::VarInt;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

pub use bytes;
pub use http;

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
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_connections: 256,
            max_streams: 100,
            idle_timeout: Duration::from_secs(30),
            max_field_section: 16 * 1024,
            max_request_body: 1024 * 1024,
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
        let server_config = server_config(&self.config)?;
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
            thread: Some(thread),
        })
    }
}

pub struct RunningServer {
    address: SocketAddr,
    endpoint: quinn::Endpoint,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl RunningServer {
    pub fn address(&self) -> SocketAddr {
        self.address
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

fn server_config(config: &Config) -> Result<quinn::ServerConfig, Error> {
    let certs = config.tls.certs.clone();
    let key = config.tls.key.clone_key();
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
    let streams = VarInt::from_u64(config.limits.max_streams)
        .map_err(|_| Error::Config("invalid stream limit"))?;
    let uni =
        VarInt::from_u64(MAX_UNI_STREAMS).map_err(|_| Error::Config("invalid stream limit"))?;
    transport.max_concurrent_bidi_streams(streams);
    transport.max_concurrent_uni_streams(uni);
    transport.max_idle_timeout(Some(
        config
            .limits
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
            serve_connection(
                connection,
                handler,
                limits.max_field_section,
                limits.max_request_body,
            )
            .await;
        });
    }
}

async fn serve_connection(
    connection: quinn::Connection,
    handler: Arc<dyn Handler>,
    max_field_section: u64,
    max_request_body: usize,
) {
    let mut builder = h3::server::builder();
    builder.max_field_section_size(max_field_section);
    let quic = connection.clone();
    let connection = h3_quinn::Connection::new(connection);
    let Ok(mut h3) = builder.build(connection).await else {
        return;
    };

    loop {
        match h3.accept().await {
            Ok(Some(resolver)) => {
                let handler = Arc::clone(&handler);
                let connection = quic.clone();
                tokio::spawn(async move {
                    let _ = handle_request(resolver, handler, connection, max_request_body).await;
                });
            }
            Ok(None) => return,
            Err(_) => return,
        }
    }
}

fn malformed_request(request: &Request<()>) -> bool {
    let headers = request.headers();
    for name in headers.keys() {
        if matches!(
            name.as_str(),
            "connection" | "keep-alive" | "proxy-connection" | "transfer-encoding" | "upgrade"
        ) {
            return true;
        }
    }
    headers
        .get(TE)
        .is_some_and(|value| value.as_bytes() != b"trailers")
}

async fn handle_request(
    resolver: h3::server::RequestResolver<h3_quinn::Connection, Bytes>,
    handler: Arc<dyn Handler>,
    connection: quinn::Connection,
    max_request_body: usize,
) -> Result<(), h3::error::StreamError> {
    let (request, mut stream) = resolver.resolve_request().await?;
    if malformed_request(&request) {
        let code = VarInt::from_u64(h3::error::Code::H3_MESSAGE_ERROR.value())
            .unwrap_or(VarInt::from_u32(0x10e));
        connection.close(code, b"malformed request");
        return Ok(());
    }
    let is_head = request.method() == Method::HEAD;

    let mut body = Vec::new();
    loop {
        let Some(chunk) = stream.recv_data().await? else {
            break;
        };
        let remaining = max_request_body.saturating_sub(body.len());
        if chunk.remaining() > remaining {
            let mut response = Response::new(());
            *response.status_mut() = StatusCode::PAYLOAD_TOO_LARGE;
            response.headers_mut().insert(CONTENT_LENGTH, 0.into());
            stream.send_response(response).await?;
            stream.finish().await?;
            return Ok(());
        }
        body.extend_from_slice(chunk.chunk());
    }

    let response = handler.handle(request, Bytes::from(body));
    let (parts, body) = response.into_parts();
    let mut response = Response::from_parts(parts, ());
    response
        .headers_mut()
        .entry(CONTENT_LENGTH)
        .or_insert_with(|| body.len().into());
    stream.send_response(response).await?;
    if !is_head && !body.is_empty() {
        stream.send_data(body).await?;
    }
    stream.finish().await?;
    Ok(())
}

pub fn text_response(status: StatusCode, body: impl Into<Bytes>) -> Response<Bytes> {
    let body = body.into();
    Response::builder()
        .status(status)
        .header(CONTENT_LENGTH, body.len())
        .body(body)
        .expect("valid response")
}
