// NOTE: the HTTP/3 connection layer is Veylor-owned and runs on the in-repo
// veylor-http3 frame/QPACK codec. QPACK dynamic state is disabled (capacity 0).

use std::fmt;
use std::fs;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use http::header::CONTENT_LENGTH;
use http::{Request, Response, StatusCode};
use quinn::VarInt;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

pub use bytes;
pub use bytes::Bytes;
pub use client::{Client, ClientConfig, ClientError, ClientResponse, ClientTls, ClientTunnel};
pub use http;
pub use response_writer::ResponseWriter;

mod client;
mod connection;
mod frame_io;
mod response_writer;

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
    // NOTE: QUIC termination is userspace CPU work; a single-thread runtime serialises
    // every connection's crypto and stream state on one core. The server passes the
    // host's parallelism so independent connections spread like plod's per-CPU loop.
    pub runtime_threads: usize,
}

#[derive(Clone, Debug, Default)]
pub struct RequestTrailers(pub Vec<(Vec<u8>, Vec<u8>)>);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectProtocol(pub Vec<u8>);

pub struct RequestStream {
    recv: quinn::RecvStream,
    data_remaining: u64,
}

impl RequestStream {
    pub(crate) fn new(recv: quinn::RecvStream) -> Self {
        Self {
            recv,
            data_remaining: 0,
        }
    }

    pub async fn read(&mut self, max: usize) -> io::Result<Option<Bytes>> {
        match self.recv.read_chunk(max, true).await {
            Ok(Some(chunk)) => Ok(Some(chunk.bytes)),
            Ok(None) => Ok(None),
            Err(error) => Err(io::Error::other(error)),
        }
    }

    pub async fn read_data(&mut self, max: usize) -> io::Result<Option<Bytes>> {
        if max == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "tunnel chunk must be nonzero",
            ));
        }
        loop {
            if self.data_remaining > 0 {
                let take = self.data_remaining.min(max as u64) as usize;
                let mut buffer = vec![0u8; take];
                self.recv
                    .read_exact(&mut buffer)
                    .await
                    .map_err(io::Error::other)?;
                self.data_remaining -= take as u64;
                return Ok(Some(Bytes::from(buffer)));
            }
            let kind = match crate::frame_io::read_varint(&mut self.recv).await {
                Ok(Some(kind)) => kind,
                Ok(None) => return Ok(None),
                Err(()) => return Err(io::Error::other("invalid tunnel frame")),
            };
            let length = match crate::frame_io::read_varint(&mut self.recv).await {
                Ok(Some(length)) => {
                    usize::try_from(length).map_err(|_| io::Error::other("tunnel frame length"))?
                }
                Ok(None) => return Ok(None),
                Err(()) => return Err(io::Error::other("invalid tunnel frame")),
            };
            if kind == veylor_http3::frame::DATA {
                self.data_remaining = length as u64;
                continue;
            }
            crate::frame_io::discard(&mut self.recv, length)
                .await
                .map_err(|_| io::Error::other("truncated tunnel frame"))?;
        }
    }

    pub fn stop(&mut self, code: u64) {
        if let Ok(code) = quinn::VarInt::from_u64(code) {
            let _ = self.recv.stop(code);
        }
    }
}

pub enum ManagedOutcome {
    Handled(Response<Body>),
    Pass(Request<()>, Bytes),
    Streamed,
}

pub trait BodySource: Send {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize>;
}

pub struct StreamBody {
    content_length: Option<u64>,
    source: Box<dyn BodySource>,
}

impl StreamBody {
    pub fn new(content_length: Option<u64>, source: impl BodySource + 'static) -> Self {
        Self {
            content_length,
            source: Box::new(source),
        }
    }

    pub fn content_length(&self) -> Option<u64> {
        self.content_length
    }

    pub fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        self.source.read(out)
    }
}

pub enum Body {
    Bytes(Bytes),
    Stream(StreamBody),
}

impl Body {
    pub fn content_length(&self) -> Option<u64> {
        match self {
            Self::Bytes(body) => Some(body.len() as u64),
            Self::Stream(body) => body.content_length(),
        }
    }
}

impl From<Bytes> for Body {
    fn from(body: Bytes) -> Self {
        Self::Bytes(body)
    }
}

impl From<Vec<u8>> for Body {
    fn from(body: Vec<u8>) -> Self {
        Self::Bytes(Bytes::from(body))
    }
}

impl From<&'static [u8]> for Body {
    fn from(body: &'static [u8]) -> Self {
        Self::Bytes(Bytes::from_static(body))
    }
}

impl From<&'static str> for Body {
    fn from(body: &'static str) -> Self {
        Self::Bytes(Bytes::from_static(body.as_bytes()))
    }
}

pub trait Handler: Send + Sync + 'static {
    fn handle(&self, request: Request<()>, body: Bytes) -> Response<Body>;

    fn managed<'a>(
        &'a self,
        request: Request<()>,
        body: Bytes,
        _writer: &'a mut ResponseWriter<'_>,
    ) -> Pin<Box<dyn Future<Output = ManagedOutcome> + Send + 'a>> {
        Box::pin(async move { ManagedOutcome::Pass(request, body) })
    }

    fn connect<'a>(
        &'a self,
        _request: Request<()>,
        _stream: RequestStream,
        _writer: &'a mut ResponseWriter<'_>,
    ) -> Pin<Box<dyn Future<Output = ManagedOutcome> + Send + 'a>> {
        Box::pin(async move {
            ManagedOutcome::Handled(text_response(
                StatusCode::NOT_IMPLEMENTED,
                "connect unsupported",
            ))
        })
    }
}

impl<F> Handler for F
where
    F: Fn(Request<()>, Bytes) -> Response<Body> + Send + Sync + 'static,
{
    fn handle(&self, request: Request<()>, body: Bytes) -> Response<Body> {
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
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(self.config.runtime_threads.max(1))
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

pub fn text_response(status: StatusCode, body: impl Into<Bytes>) -> Response<Body> {
    let body = body.into();
    Response::builder()
        .status(status)
        .header(CONTENT_LENGTH, body.len())
        .body(Body::Bytes(body))
        .expect("valid response")
}
