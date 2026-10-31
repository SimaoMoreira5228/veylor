use std::fmt;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc::{SyncSender, sync_channel};
use std::thread::{self, JoinHandle};

use bytes::Bytes;
use http::header::{CONTENT_LENGTH, HOST};
use http::{HeaderMap, HeaderName, HeaderValue, Request, StatusCode};
use quinn::crypto::rustls::QuicClientConfig;
use rustls::pki_types::CertificateDer;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use veylor_http3::{
    CONTROL_STREAM, QPACK_DECODER_STREAM, QPACK_ENCODER_STREAM, SETTINGS_MAX_FIELD_SECTION_SIZE,
    SETTINGS_QPACK_BLOCKED_STREAMS, SETTINGS_QPACK_MAX_TABLE_CAPACITY, frame, qpack,
};

use crate::frame_io::{discard, read_byte, read_varint};
use crate::{Error, Limits};

pub struct ClientTls {
    roots: Vec<CertificateDer<'static>>,
}

impl ClientTls {
    pub fn new(roots: Vec<CertificateDer<'static>>) -> Self {
        Self { roots }
    }

    pub fn from_file(path: &Path) -> Result<Self, Error> {
        let roots = veylor_tls::load_cert_chain(&std::fs::read(path)?)?;
        Ok(Self { roots })
    }

    pub fn from_der(chain: &[u8]) -> Result<Self, Error> {
        let roots = veylor_tls::load_cert_chain(chain)?;
        Ok(Self { roots })
    }
}

pub struct ClientConfig {
    pub server: SocketAddr,
    pub server_name: String,
    pub tls: ClientTls,
    pub limits: Limits,
}

#[derive(Debug)]
pub enum ClientError {
    Io(std::io::Error),
    Config(&'static str),
    Connect(String),
    Closed,
    Protocol(&'static str),
    TooLarge,
}

impl fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "io: {error}"),
            Self::Config(reason) => write!(formatter, "config: {reason}"),
            Self::Connect(reason) => write!(formatter, "connect: {reason}"),
            Self::Closed => write!(formatter, "client closed"),
            Self::Protocol(reason) => write!(formatter, "protocol: {reason}"),
            Self::TooLarge => write!(formatter, "response too large"),
        }
    }
}

impl std::error::Error for ClientError {}

impl From<std::io::Error> for ClientError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

pub struct ClientResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub trailers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Bytes,
}

pub struct Client {
    commands: UnboundedSender<Command>,
    thread: Option<JoinHandle<()>>,
}

#[allow(clippy::large_enum_variant)]
enum Command {
    Request {
        request: Request<()>,
        body: Bytes,
        reply: SyncSender<Result<ClientResponse, ClientError>>,
    },
    Connect {
        request: Request<()>,
        protocol: Vec<u8>,
        reply: SyncSender<Result<ClientTunnel, ClientError>>,
    },
    Shutdown,
}

enum TunnelOp {
    Write(Vec<u8>, SyncSender<Result<(), ClientError>>),
    Read(SyncSender<Result<Option<Vec<u8>>, ClientError>>),
    Close,
}

pub struct ClientTunnel {
    commands: UnboundedSender<TunnelOp>,
}

impl Client {
    pub fn connect(config: ClientConfig) -> Result<Self, ClientError> {
        let (commands, receiver) = unbounded_channel();
        let (ready_tx, ready_rx) = sync_channel(0);
        let thread = thread::Builder::new()
            .name("veylor-h3-client".to_owned())
            .spawn(move || run(config, receiver, ready_tx))
            .map_err(|_| ClientError::Config("client thread"))?;
        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Self {
                commands,
                thread: Some(thread),
            }),
            Ok(Err(error)) => {
                let _ = thread.join();
                Err(error)
            }
            Err(_) => {
                let _ = thread.join();
                Err(ClientError::Closed)
            }
        }
    }

    pub fn request(
        &self,
        request: Request<()>,
        body: Bytes,
    ) -> Result<ClientResponse, ClientError> {
        let (reply, receiver) = sync_channel(1);
        self.commands
            .send(Command::Request {
                request,
                body,
                reply,
            })
            .map_err(|_| ClientError::Closed)?;
        receiver.recv().map_err(|_| ClientError::Closed)?
    }

    pub fn connect_tunnel(
        &self,
        request: Request<()>,
        protocol: &[u8],
    ) -> Result<ClientTunnel, ClientError> {
        if request.method() != http::Method::CONNECT || protocol.is_empty() {
            return Err(ClientError::Protocol("invalid connect request"));
        }
        let (reply, receiver) = sync_channel(1);
        self.commands
            .send(Command::Connect {
                request,
                protocol: protocol.to_vec(),
                reply,
            })
            .map_err(|_| ClientError::Closed)?;
        receiver.recv().map_err(|_| ClientError::Closed)?
    }
}

impl ClientTunnel {
    pub fn write(&self, data: &[u8]) -> Result<(), ClientError> {
        let (reply, receiver) = sync_channel(1);
        self.commands
            .send(TunnelOp::Write(data.to_vec(), reply))
            .map_err(|_| ClientError::Closed)?;
        receiver.recv().map_err(|_| ClientError::Closed)?
    }

    pub fn read(&self) -> Result<Option<Vec<u8>>, ClientError> {
        let (reply, receiver) = sync_channel(1);
        self.commands
            .send(TunnelOp::Read(reply))
            .map_err(|_| ClientError::Closed)?;
        receiver.recv().map_err(|_| ClientError::Closed)?
    }

    pub fn close(&self) {
        let _ = self.commands.send(TunnelOp::Close);
    }
}

impl Drop for ClientTunnel {
    fn drop(&mut self) {
        let _ = self.commands.send(TunnelOp::Close);
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.commands.send(Command::Shutdown);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn run(
    config: ClientConfig,
    mut commands: UnboundedReceiver<Command>,
    ready: SyncSender<Result<(), ClientError>>,
) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => {
            let _ = ready.send(Err(ClientError::Config("client runtime")));
            return;
        }
    };
    runtime.block_on(async move {
        let limits = config.limits;
        let (endpoint, client_config) = match build_client(&config) {
            Ok(client) => client,
            Err(error) => {
                let _ = ready.send(Err(error));
                return;
            }
        };
        let connecting =
            match endpoint.connect_with(client_config, config.server, &config.server_name) {
                Ok(connecting) => connecting,
                Err(error) => {
                    let _ = ready.send(Err(ClientError::Connect(error.to_string())));
                    return;
                }
            };
        let connection = match connecting.await {
            Ok(connection) => connection,
            Err(error) => {
                let _ = ready.send(Err(ClientError::Connect(error.to_string())));
                return;
            }
        };
        if let Err(error) = setup_control(&connection, limits).await {
            let _ = ready.send(Err(error));
            return;
        }
        let accepted = connection.clone();
        tokio::spawn(async move { accept_uni(accepted).await });
        let _ = ready.send(Ok(()));

        while let Some(command) = commands.recv().await {
            match command {
                Command::Request {
                    request,
                    body,
                    reply,
                } => {
                    let connection = connection.clone();
                    tokio::spawn(async move {
                        let result = request_once(&connection, request, body, limits).await;
                        let _ = reply.send(result);
                    });
                }
                Command::Connect {
                    request,
                    protocol,
                    reply,
                } => {
                    let connection = connection.clone();
                    tokio::spawn(async move {
                        let result = connect_tunnel(&connection, request, &protocol, limits).await;
                        let _ = reply.send(result);
                    });
                }
                Command::Shutdown => break,
            }
        }
        endpoint.close(0u32.into(), b"client shutdown");
        endpoint.wait_idle().await;
    });
}

fn build_client(
    config: &ClientConfig,
) -> Result<(quinn::Endpoint, quinn::ClientConfig), ClientError> {
    let mut roots = rustls::RootCertStore::empty();
    for root in &config.tls.roots {
        roots
            .add(root.clone())
            .map_err(|_| ClientError::Config("invalid root certificate"))?;
    }
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|_| ClientError::Config("tls 1.3"))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let crypto = QuicClientConfig::try_from(tls).map_err(|_| ClientError::Config("quic"))?;
    let mut transport = quinn::TransportConfig::default();
    transport.max_idle_timeout(Some(
        config
            .limits
            .idle_timeout
            .try_into()
            .map_err(|_| ClientError::Config("idle timeout"))?,
    ));
    let mut client = quinn::ClientConfig::new(Arc::new(crypto));
    client.transport_config(Arc::new(transport));
    let bind = if config.server.is_ipv4() {
        SocketAddr::from(([0, 0, 0, 0], 0))
    } else {
        SocketAddr::from(([0u16; 8], 0))
    };
    let endpoint = quinn::Endpoint::client(bind)?;
    Ok((endpoint, client))
}

async fn setup_control(connection: &quinn::Connection, limits: Limits) -> Result<(), ClientError> {
    let mut control = connection
        .open_uni()
        .await
        .map_err(|_| ClientError::Closed)?;
    control
        .write_all(&[CONTROL_STREAM as u8])
        .await
        .map_err(|_| ClientError::Closed)?;
    let settings = frame::encode_settings(&[
        (SETTINGS_QPACK_MAX_TABLE_CAPACITY, 0),
        (SETTINGS_QPACK_BLOCKED_STREAMS, 0),
        (SETTINGS_MAX_FIELD_SECTION_SIZE, limits.max_field_section),
    ]);
    let mut encoded = Vec::with_capacity(settings.len() + 16);
    frame::encode_frame(frame::SETTINGS, &settings, &mut encoded);
    control
        .write_all(&encoded)
        .await
        .map_err(|_| ClientError::Closed)?;

    let mut encoder = connection
        .open_uni()
        .await
        .map_err(|_| ClientError::Closed)?;
    encoder
        .write_all(&[QPACK_ENCODER_STREAM as u8])
        .await
        .map_err(|_| ClientError::Closed)?;
    let mut decoder = connection
        .open_uni()
        .await
        .map_err(|_| ClientError::Closed)?;
    decoder
        .write_all(&[QPACK_DECODER_STREAM as u8])
        .await
        .map_err(|_| ClientError::Closed)?;

    let connection = connection.clone();
    tokio::spawn(async move {
        let _ = connection.closed().await;
        drop((control, encoder, decoder));
    });
    Ok(())
}

async fn accept_uni(connection: quinn::Connection) {
    loop {
        let Ok(mut recv) = connection.accept_uni().await else {
            return;
        };
        tokio::spawn(async move {
            let Some(kind) = read_byte(&mut recv).await else {
                return;
            };
            if u64::from(kind) == CONTROL_STREAM {
                read_server_control(&mut recv).await;
            } else {
                while read_byte(&mut recv).await.is_some() {}
            }
        });
    }
}

async fn read_server_control(recv: &mut quinn::RecvStream) {
    let mut first = true;
    loop {
        let Some(kind) = read_varint(recv).await.ok().flatten() else {
            return;
        };
        let Some(length) = read_varint(recv).await.ok().flatten() else {
            return;
        };
        let Ok(length) = usize::try_from(length) else {
            return;
        };
        if first {
            if kind != frame::SETTINGS || length > 1024 {
                return;
            }
            let mut payload = vec![0u8; length];
            if recv.read_exact(&mut payload).await.is_err() {
                return;
            }
            if frame::decode_settings(&payload).is_err() {
                return;
            }
            first = false;
            continue;
        }
        if kind == frame::SETTINGS {
            return;
        }
        if discard(recv, length).await.is_err() {
            return;
        }
    }
}

async fn request_once(
    connection: &quinn::Connection,
    request: Request<()>,
    body: Bytes,
    limits: Limits,
) -> Result<ClientResponse, ClientError> {
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|_| ClientError::Closed)?;
    let fields = request_fields(&request, None)?;
    let block = qpack::encode(&fields);
    let mut encoded = Vec::with_capacity(block.len() + 16);
    frame::encode_frame(frame::HEADERS, &block, &mut encoded);
    send.write_all(&encoded)
        .await
        .map_err(|_| ClientError::Closed)?;
    if !body.is_empty() {
        let mut data = Vec::with_capacity(body.len() + 16);
        frame::encode_frame(frame::DATA, &body, &mut data);
        send.write_all(&data)
            .await
            .map_err(|_| ClientError::Closed)?;
    }
    send.finish().map_err(|_| ClientError::Closed)?;

    let mut status = None;
    let mut headers = HeaderMap::new();
    let mut trailers = Vec::new();
    let mut response_body = Vec::new();
    loop {
        let kind = read_varint(&mut recv)
            .await
            .map_err(|_| ClientError::Protocol("response frame"))?;
        let Some(kind) = kind else {
            break;
        };
        let length = read_varint(&mut recv)
            .await
            .map_err(|_| ClientError::Protocol("response length"))?
            .ok_or(ClientError::Protocol("truncated response"))?;
        let length =
            usize::try_from(length).map_err(|_| ClientError::Protocol("response length"))?;
        match kind {
            frame::HEADERS => {
                if length > limits.max_field_section as usize {
                    return Err(ClientError::TooLarge);
                }
                let mut payload = vec![0u8; length];
                recv.read_exact(&mut payload)
                    .await
                    .map_err(|_| ClientError::Protocol("header block"))?;
                let fields = qpack::decode(&payload, limits.max_field_section as usize)
                    .map_err(|_| ClientError::Protocol("qpack"))?;
                if status.is_none() {
                    status = Some(response_fields(&fields, &mut headers)?);
                } else {
                    append_trailers(&mut trailers, fields)?;
                }
            }
            frame::DATA => {
                if response_body.len().saturating_add(length) > limits.max_response_body {
                    return Err(ClientError::TooLarge);
                }
                let start = response_body.len();
                response_body.resize(start + length, 0);
                recv.read_exact(&mut response_body[start..])
                    .await
                    .map_err(|_| ClientError::Protocol("response body"))?;
            }
            frame::SETTINGS
            | frame::GOAWAY
            | frame::PUSH_PROMISE
            | frame::CANCEL_PUSH
            | frame::MAX_PUSH_ID => {
                return Err(ClientError::Protocol("unexpected response frame"));
            }
            _ => discard(&mut recv, length)
                .await
                .map_err(|_| ClientError::Protocol("unknown frame"))?,
        }
    }
    let status = status.ok_or(ClientError::Protocol("missing status"))?;
    Ok(ClientResponse {
        status,
        headers,
        trailers,
        body: Bytes::from(response_body),
    })
}

async fn connect_tunnel(
    connection: &quinn::Connection,
    request: Request<()>,
    protocol: &[u8],
    limits: Limits,
) -> Result<ClientTunnel, ClientError> {
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|_| ClientError::Closed)?;
    let fields = request_fields(&request, Some(protocol))?;
    let block = qpack::encode(&fields);
    let mut encoded = Vec::with_capacity(block.len() + 16);
    frame::encode_frame(frame::HEADERS, &block, &mut encoded);
    send.write_all(&encoded)
        .await
        .map_err(|_| ClientError::Closed)?;

    let Some((kind, payload)) =
        crate::frame_io::read_frame(&mut recv, limits.max_field_section as usize)
            .await
            .map_err(|_| ClientError::Protocol("connect response"))?
    else {
        return Err(ClientError::Protocol("missing connect response"));
    };
    if kind != frame::HEADERS {
        return Err(ClientError::Protocol("unexpected connect response"));
    }
    let fields = qpack::decode(&payload, limits.max_field_section as usize)
        .map_err(|_| ClientError::Protocol("connect qpack"))?;
    let mut headers = HeaderMap::new();
    let status = response_fields(&fields, &mut headers)?;
    if status != StatusCode::OK {
        let _ = send.finish();
        let _ = recv.stop(0u32.into());
        return Err(ClientError::Protocol("connect rejected"));
    }

    let (commands, operations) = unbounded_channel();
    tokio::spawn(tunnel_task(send, recv, operations));
    Ok(ClientTunnel { commands })
}

async fn tunnel_task(
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    mut operations: UnboundedReceiver<TunnelOp>,
) {
    while let Some(operation) = operations.recv().await {
        match operation {
            TunnelOp::Write(data, reply) => {
                let result = write_tunnel_data(&mut send, &data).await;
                let _ = reply.send(result);
            }
            TunnelOp::Read(reply) => {
                let result = read_tunnel_data(&mut recv).await;
                let _ = reply.send(result);
            }
            TunnelOp::Close => {
                let _ = send.finish();
                let _ = recv.stop(0u32.into());
                break;
            }
        }
    }
}

const TUNNEL_FRAME_BYTES: usize = 64 * 1024;

async fn read_tunnel_data(recv: &mut quinn::RecvStream) -> Result<Option<Vec<u8>>, ClientError> {
    loop {
        match crate::frame_io::read_frame(recv, TUNNEL_FRAME_BYTES)
            .await
            .map_err(|_| ClientError::Protocol("tunnel frame"))?
        {
            Some((kind, payload)) if kind == frame::DATA => return Ok(Some(payload)),
            Some((frame::HEADERS, _)) => {}
            Some(_) => {}
            None => return Ok(None),
        }
    }
}

async fn write_tunnel_data(send: &mut quinn::SendStream, data: &[u8]) -> Result<(), ClientError> {
    let mut head = [0u8; 16];
    let used = frame::encode_frame_header(frame::DATA, data.len() as u64, &mut head);
    send.write_all(&head[..used])
        .await
        .map_err(|_| ClientError::Closed)?;
    send.write_all(data).await.map_err(|_| ClientError::Closed)
}

type RequestFields<'a> = Vec<(&'a [u8], &'a [u8])>;

fn request_fields<'a>(
    request: &'a Request<()>,
    protocol: Option<&'a [u8]>,
) -> Result<RequestFields<'a>, ClientError> {
    let uri = request.uri();
    let scheme = uri.scheme_str().unwrap_or("https");
    let authority = uri
        .authority()
        .map(|authority| authority.as_str())
        .ok_or(ClientError::Protocol("request authority"))?;
    let target = uri
        .path_and_query()
        .map(|target| target.as_str())
        .unwrap_or("/");
    let method = request.method().as_str();
    let mut fields = Vec::with_capacity(request.headers().len() + 4);
    fields.push((b":method".as_slice(), method.as_bytes()));
    fields.push((b":scheme".as_slice(), scheme.as_bytes()));
    fields.push((b":authority".as_slice(), authority.as_bytes()));
    fields.push((b":path".as_slice(), target.as_bytes()));
    if let Some(protocol) = protocol {
        fields.push((b":protocol".as_slice(), protocol));
    }
    for (name, value) in request.headers() {
        if name == HOST || name == CONTENT_LENGTH || prohibited(name.as_str().as_bytes()) {
            continue;
        }
        fields.push((name.as_str().as_bytes(), value.as_bytes()));
    }
    Ok(fields)
}

fn response_fields(
    fields: &[qpack::Field],
    headers: &mut HeaderMap,
) -> Result<StatusCode, ClientError> {
    let mut status = None;
    let mut regular = false;
    for (name, value) in fields {
        if name.first() == Some(&b':') {
            if regular || name.as_slice() != b":status" || status.is_some() {
                return Err(ClientError::Protocol("response pseudo-header"));
            }
            let code = std::str::from_utf8(value)
                .map_err(|_| ClientError::Protocol("status"))?
                .parse::<u16>()
                .map_err(|_| ClientError::Protocol("status"))?;
            status = Some(StatusCode::from_u16(code).map_err(|_| ClientError::Protocol("status"))?);
            continue;
        }
        regular = true;
        if prohibited(name) {
            return Err(ClientError::Protocol("response header"));
        }
        let name =
            HeaderName::from_bytes(name).map_err(|_| ClientError::Protocol("header name"))?;
        let value =
            HeaderValue::from_bytes(value).map_err(|_| ClientError::Protocol("header value"))?;
        headers.append(name, value);
    }
    status.ok_or(ClientError::Protocol("missing status"))
}

fn append_trailers(
    trailers: &mut Vec<(Vec<u8>, Vec<u8>)>,
    fields: Vec<(Vec<u8>, Vec<u8>)>,
) -> Result<(), ClientError> {
    if trailers.len().saturating_add(fields.len()) > 8 {
        return Err(ClientError::Protocol("too many trailers"));
    }
    let mut total: usize = trailers
        .iter()
        .map(|(name, value)| name.len() + value.len())
        .sum();
    for (name, value) in fields {
        if name.first() == Some(&b':') || prohibited(&name) {
            return Err(ClientError::Protocol("trailer name"));
        }
        let valid_name = !name.is_empty()
            && name.iter().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(
                        byte,
                        b'!' | b'#'
                            | b'$'
                            | b'%'
                            | b'&'
                            | b'\''
                            | b'*'
                            | b'+'
                            | b'-'
                            | b'.'
                            | b'^'
                            | b'_'
                            | b'`'
                            | b'|'
                            | b'~'
                    )
            });
        if !valid_name {
            return Err(ClientError::Protocol("trailer name"));
        }
        total = total.saturating_add(name.len()).saturating_add(value.len());
        if total > 1024 {
            return Err(ClientError::Protocol("trailers too large"));
        }
        trailers.push((name, value));
    }
    Ok(())
}

fn prohibited(name: &[u8]) -> bool {
    [
        b"connection".as_slice(),
        b"keep-alive",
        b"proxy-connection",
        b"transfer-encoding",
        b"upgrade",
    ]
    .iter()
    .any(|banned| name.eq_ignore_ascii_case(banned))
}
