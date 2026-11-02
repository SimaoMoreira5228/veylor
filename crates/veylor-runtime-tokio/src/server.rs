use std::borrow::Cow;
use std::future::Future;
use std::io;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio::time::{Instant, timeout, timeout_at};
use veylor_server_conn::{
    Admission, BodyKind, Field, Header, RequestHead, ResponseFraming, ResponseHeadParams,
    ServerConnection, ServerError, Span,
};

const INITIAL_HEAD: usize = 8 * 1024;
const INITIAL_OUTPUT: usize = 8 * 1024;
const STAGING: usize = 8 * 1024;
const MAX_STAGING: usize = 64 * 1024;
const HEADER_SLOTS: usize = 64;
const TRAILER_SLOTS: usize = 16;
const MAX_RESPONSE_HEADERS: usize = 32;

#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub max_head: usize,
    pub max_body: usize,
    pub max_output: usize,
    pub head_timeout: Duration,
    pub body_timeout: Duration,
    pub write_timeout: Duration,
    pub drain_timeout: Duration,
    pub max_connections: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_head: 64 * 1024,
            max_body: 1024 * 1024,
            max_output: 64 * 1024,
            head_timeout: Duration::from_secs(10),
            body_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            drain_timeout: Duration::from_secs(5),
            max_connections: 1024,
        }
    }
}

pub struct Request<'a> {
    input: &'a [u8],
    head: RequestHead,
    headers: &'a [Header],
    body: &'a [u8],
}

impl<'a> Request<'a> {
    pub fn method(&self) -> &'a [u8] {
        self.head.method(self.input).unwrap_or(&[])
    }

    pub fn target(&self) -> &'a [u8] {
        self.head.target(self.input).unwrap_or(&[])
    }

    pub fn body(&self) -> &'a [u8] {
        self.body
    }

    pub fn header(&self, name: &str) -> Option<&'a [u8]> {
        self.headers().find_map(|(field, value)| {
            if field.eq_ignore_ascii_case(name.as_bytes()) {
                Some(value)
            } else {
                None
            }
        })
    }

    pub fn headers(&self) -> impl Iterator<Item = (&'a [u8], &'a [u8])> + 'a {
        let input = self.input;
        let count = self.head.header_count.min(self.headers.len());
        self.headers[..count]
            .iter()
            .filter_map(move |header| Some((span(input, header.name)?, span(input, header.value)?)))
    }
}

pub struct Response<'a> {
    status: u16,
    reason: &'static [u8],
    headers: Vec<(Vec<u8>, Vec<u8>)>,
    body: Cow<'a, [u8]>,
}

impl<'a> Response<'a> {
    pub fn new(status: u16, reason: &'static [u8], body: impl Into<Cow<'a, [u8]>>) -> Self {
        Self {
            status,
            reason,
            headers: Vec::new(),
            body: body.into(),
        }
    }

    pub fn ok(body: impl Into<Cow<'a, [u8]>>) -> Self {
        Self::new(200, b"OK", body)
    }

    pub fn text(body: impl Into<Cow<'a, [u8]>>) -> Self {
        Self::ok(body).header("content-type", "text/plain; charset=utf-8")
    }

    pub fn header(mut self, name: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

pub trait Handler: Send + Sync + 'static {
    fn handle<'a>(&self, request: Request<'a>) -> Response<'a>;
}

impl<F> Handler for F
where
    F: for<'a> Fn(Request<'a>) -> Response<'a> + Send + Sync + 'static,
{
    fn handle<'a>(&self, request: Request<'a>) -> Response<'a> {
        (self)(request)
    }
}

pub async fn serve<H: Handler + Clone>(
    listener: TcpListener,
    handler: H,
    config: Config,
) -> io::Result<()> {
    serve_until(listener, handler, config, std::future::pending()).await
}

pub async fn serve_until<H, F>(
    listener: TcpListener,
    handler: H,
    config: Config,
    shutdown: F,
) -> io::Result<()>
where
    H: Handler + Clone,
    F: Future<Output = ()>,
{
    let mut tasks = JoinSet::new();
    tokio::pin!(shutdown);
    let limit = config.max_connections.max(1);

    loop {
        if tasks.len() >= limit {
            let _ = tasks.join_next().await;
            continue;
        }

        tokio::select! {
            _ = &mut shutdown => break,
            accepted = listener.accept() => {
                let (socket, _) = accepted?;
                let handler = handler.clone();
                tasks.spawn(async move {
                    let _ = serve_connection(socket, handler, config).await;
                });
            }
            _ = tasks.join_next(), if !tasks.is_empty() => {}
        }
    }

    drop(listener);
    let deadline = Instant::now() + config.drain_timeout;
    while !tasks.is_empty() {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => {
                tasks.abort_all();
                break;
            }
            _ = tasks.join_next() => {}
        }
    }

    Ok(())
}

pub async fn serve_connection<H: Handler>(
    mut socket: TcpStream,
    handler: H,
    config: Config,
) -> io::Result<()> {
    socket.set_nodelay(true)?;
    let mut head = vec![0u8; INITIAL_HEAD.min(config.max_head)];
    let mut staging = vec![0u8; STAGING];
    let mut output = vec![0u8; INITIAL_OUTPUT.min(config.max_output)];
    let mut headers = [Header::default(); HEADER_SLOTS];
    let mut trailers = [Header::default(); TRAILER_SLOTS];
    // NOTE: the body buffer keeps its peak capacity for the connection; a large upload is
    // released only when the connection closes.
    let mut body = Vec::new();

    loop {
        let mut filled = 0usize;
        let head_deadline = Instant::now() + config.head_timeout;
        let (request_head, mut connection) = loop {
            if filled == head.len() {
                if head.len() >= config.max_head {
                    write_error_status(
                        &mut socket,
                        431,
                        b"Request Header Fields Too Large",
                        &config,
                    )
                    .await?;
                    return Ok(());
                }
                let next = (head.len() * 2).min(config.max_head);
                head.resize(next, 0);
            }

            let read = match timeout_at(head_deadline, socket.read(&mut head[filled..])).await {
                Ok(result) => result?,
                Err(_) => return Err(timed_out("request head timeout")),
            };
            if read == 0 {
                if filled == 0 {
                    return Ok(());
                }
                return Err(protocol("eof inside request head"));
            }
            filled += read;

            let mut probe = ServerConnection::new();
            match probe.parse_head(&head[..filled], &mut headers) {
                Ok(head) => break (head, probe),
                Err(ServerError::NeedMore) => continue,
                Err(ServerError::HeaderCapacity { .. })
                | Err(ServerError::BufferTooSmall { .. }) => {
                    write_error_status(
                        &mut socket,
                        431,
                        b"Request Header Fields Too Large",
                        &config,
                    )
                    .await?;
                    return Ok(());
                }
                Err(_) => {
                    write_error_status(&mut socket, 400, b"Bad Request", &config).await?;
                    return Ok(());
                }
            }
        };

        let admission = connection.admit_body(&head[..filled], &headers, &[], &[], true);
        match admission {
            Ok(Admission::Ready) => {}
            Ok(Admission::SendContinue) => {
                write_continue(
                    &mut socket,
                    &mut connection,
                    &head[..filled],
                    &headers,
                    &mut output,
                    &config,
                )
                .await?;
                connection
                    .mark_continue_sent()
                    .map_err(|error| protocol(&format!("continue: {error:?}")))?;
            }
            Ok(Admission::Rejected) => {
                write_payload_too_large(
                    &mut socket,
                    &mut connection,
                    &head[..filled],
                    &headers,
                    &mut output,
                    &config,
                )
                .await?;
                return Ok(());
            }
            Err(_) => {
                write_error_status(&mut socket, 400, b"Bad Request", &config).await?;
                return Ok(());
            }
        }

        if request_head
            .content_length
            .is_some_and(|length| length > config.max_body as u64)
        {
            write_payload_too_large(
                &mut socket,
                &mut connection,
                &head[..filled],
                &headers,
                &mut output,
                &config,
            )
            .await?;
            return Ok(());
        }

        let tail = filled - request_head.length;
        if tail > staging.len() {
            staging.resize(tail.min(MAX_STAGING), 0);
        }
        let mut bfilled = tail;
        staging[..bfilled].copy_from_slice(&head[request_head.length..filled]);
        let mut cursor = 0usize;
        body.clear();

        loop {
            let event = connection
                .read_body(&staging[cursor..bfilled], false, &mut trailers)
                .map_err(|error| protocol(&format!("body: {error:?}")))?;
            let base = cursor;
            cursor += event.consumed;

            match event.kind {
                BodyKind::Data => {
                    let start = base + event.data.offset;
                    let end = start + event.data.length;
                    body.extend_from_slice(&staging[start..end]);
                    if body.len() > config.max_body {
                        write_payload_too_large(
                            &mut socket,
                            &mut connection,
                            &head[..filled],
                            &headers,
                            &mut output,
                            &config,
                        )
                        .await?;
                        return Ok(());
                    }
                }
                BodyKind::Complete => break,
                BodyKind::NeedMore | BodyKind::Trailers => {}
            }

            if event.kind == BodyKind::NeedMore && (cursor == bfilled || event.consumed == 0) {
                if cursor > 0 {
                    staging.copy_within(cursor..bfilled, 0);
                    bfilled -= cursor;
                    cursor = 0;
                }
                if bfilled == staging.len() {
                    if staging.len() >= MAX_STAGING {
                        return Err(protocol("body staging overflow"));
                    }
                    let next = (staging.len() * 2).min(MAX_STAGING);
                    staging.resize(next, 0);
                }

                let read = match timeout(config.body_timeout, socket.read(&mut staging[bfilled..]))
                    .await
                {
                    Ok(result) => result?,
                    Err(_) => return Err(timed_out("request body timeout")),
                };
                if read == 0 {
                    return Err(protocol("eof inside request body"));
                }
                bfilled += read;
            }
        }

        let request = Request {
            input: &head[..filled],
            head: request_head,
            headers: &headers[..],
            body: &body,
        };
        let response = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            handler.handle(request)
        })) {
            Ok(response) => response,
            Err(_) => Response::new(500, b"Internal Server Error", &[][..]),
        };
        let reusable = write_response(
            &mut socket,
            &mut connection,
            &head[..filled],
            &headers,
            response,
            &mut output,
            &config,
        )
        .await?;

        if !reusable {
            return Ok(());
        }
    }
}

async fn write_response(
    socket: &mut TcpStream,
    connection: &mut ServerConnection<'_>,
    input: &[u8],
    request_headers: &[Header],
    response: Response<'_>,
    output: &mut Vec<u8>,
    config: &Config,
) -> io::Result<bool> {
    if response.headers.len() > MAX_RESPONSE_HEADERS {
        return Err(protocol("too many response headers"));
    }

    let mut fields = [Field {
        name: b"",
        value: b"",
    }; MAX_RESPONSE_HEADERS];
    for (slot, (name, value)) in fields.iter_mut().zip(response.headers.iter()) {
        *slot = Field { name, value };
    }
    let fields = &fields[..response.headers.len()];

    let head = loop {
        let params = ResponseHeadParams {
            status: response.status,
            reason: response.reason,
            fields,
            framing: ResponseFraming::FixedLength,
            content_length: response.body.len() as u64,
            request_body_complete: true,
            response_close: false,
        };
        match connection.encode_response_head(input, request_headers, params, output) {
            Ok(head) => break head,
            Err(ServerError::BufferTooSmall { required }) => {
                grow(output, required, config.max_output)?
            }
            Err(error) => return Err(protocol(&format!("response head: {error:?}"))),
        }
    };
    connection
        .advance_response_write(head.written)
        .map_err(|error| protocol(&format!("advance head: {error:?}")))?;
    let mut used = head.written;

    if head.body_allowed {
        let mut offset = 0usize;
        while offset < response.body.len() {
            match connection.encode_response_body(&response.body[offset..], &mut output[used..]) {
                Ok(encoded) => {
                    if encoded.written == 0 {
                        return Err(protocol("response body made no progress"));
                    }
                    used += encoded.written;
                    offset += encoded.written;
                    connection
                        .advance_response_write(encoded.written)
                        .map_err(|error| protocol(&format!("advance body: {error:?}")))?;
                    if used == output.len() {
                        write_bytes(socket, &output[..used], config.write_timeout).await?;
                        used = 0;
                    }
                }
                Err(ServerError::BufferTooSmall { required }) => {
                    if used > 0 {
                        write_bytes(socket, &output[..used], config.write_timeout).await?;
                        used = 0;
                    }
                    if required > output.len() {
                        grow(output, required, config.max_output)?;
                    }
                }
                Err(error) => return Err(protocol(&format!("response body: {error:?}"))),
            }
        }
    }

    loop {
        match connection.finish_response_body(&[], &mut output[used..]) {
            Ok(tail) => {
                used += tail.written;
                if tail.written > 0 {
                    connection
                        .advance_response_write(tail.written)
                        .map_err(|error| protocol(&format!("advance tail: {error:?}")))?;
                }
                break;
            }
            Err(ServerError::BufferTooSmall { required }) => {
                if used > 0 {
                    write_bytes(socket, &output[..used], config.write_timeout).await?;
                    used = 0;
                }
                if required > output.len() {
                    grow(output, required, config.max_output)?;
                }
            }
            Err(error) => return Err(protocol(&format!("finish response: {error:?}"))),
        }
    }

    if used > 0 {
        write_bytes(socket, &output[..used], config.write_timeout).await?;
    }

    Ok(connection.can_reuse())
}

async fn write_continue(
    socket: &mut TcpStream,
    connection: &mut ServerConnection<'_>,
    input: &[u8],
    request_headers: &[Header],
    output: &mut Vec<u8>,
    config: &Config,
) -> io::Result<()> {
    let head = loop {
        let params = ResponseHeadParams {
            status: 100,
            reason: b"Continue",
            fields: &[],
            framing: ResponseFraming::FixedLength,
            content_length: 0,
            request_body_complete: false,
            response_close: false,
        };
        match connection.encode_response_head(input, request_headers, params, output) {
            Ok(head) => break head,
            Err(ServerError::BufferTooSmall { required }) => {
                grow(output, required, config.max_output)?
            }
            Err(error) => return Err(protocol(&format!("interim head: {error:?}"))),
        }
    };
    write_bytes(socket, &output[..head.written], config.write_timeout).await?;
    connection
        .advance_response_write(head.written)
        .map_err(|error| protocol(&format!("advance interim: {error:?}")))?;
    Ok(())
}

async fn write_payload_too_large(
    socket: &mut TcpStream,
    connection: &mut ServerConnection<'_>,
    input: &[u8],
    request_headers: &[Header],
    output: &mut Vec<u8>,
    config: &Config,
) -> io::Result<()> {
    let response = Response {
        status: 413,
        reason: b"Payload Too Large",
        headers: Vec::new(),
        body: Cow::Borrowed(&[]),
    };
    let _ = write_response(
        socket,
        connection,
        input,
        request_headers,
        response,
        output,
        config,
    )
    .await?;
    Ok(())
}

async fn write_error_status(
    socket: &mut TcpStream,
    status: u16,
    reason: &[u8],
    config: &Config,
) -> io::Result<()> {
    let head = format!(
        "HTTP/1.1 {status} {}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        String::from_utf8_lossy(reason)
    );
    write_bytes(socket, head.as_bytes(), config.write_timeout).await
}

async fn write_bytes(socket: &mut TcpStream, bytes: &[u8], limit: Duration) -> io::Result<()> {
    match timeout(limit, socket.write_all(bytes)).await {
        Ok(result) => result,
        Err(_) => Err(timed_out("write timeout")),
    }
}

fn timed_out(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, message.to_owned())
}

fn grow(output: &mut Vec<u8>, required: usize, max_output: usize) -> io::Result<()> {
    if required > max_output {
        return Err(protocol("response exceeds max_output"));
    }
    output.resize(required.max(output.len() * 2).min(max_output), 0);
    Ok(())
}

fn span(input: &[u8], span: Span) -> Option<&[u8]> {
    input.get(span.offset..span.offset.checked_add(span.length)?)
}

fn protocol(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_owned())
}
