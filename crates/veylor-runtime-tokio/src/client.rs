use std::io;
use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use veylor_client::{
    BodyFraming, ClientConnection, ClientError, ClientReadKind, Field, Header, RequestMethod, Span,
};

const INITIAL_BUFFER: usize = 8 * 1024;
const MAX_HEAD: usize = 64 * 1024;
const MAX_BODY: usize = 1024 * 1024;
const HEADER_SLOTS: usize = 64;
const TRAILER_SLOTS: usize = 16;

pub struct ClientResponse {
    pub status: u16,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Vec<u8>,
}

pub struct Client {
    socket: TcpStream,
    connection: ClientConnection<'static>,
    read: Vec<u8>,
    write: Vec<u8>,
    headers: [Header; HEADER_SLOTS],
    trailers: [Header; TRAILER_SLOTS],
    reusable: bool,
}

impl Client {
    pub async fn connect(addr: SocketAddr) -> io::Result<Self> {
        let socket = TcpStream::connect(addr).await?;
        socket.set_nodelay(true)?;

        Ok(Self {
            socket,
            connection: ClientConnection::new(RequestMethod::Other, b""),
            read: vec![0u8; INITIAL_BUFFER],
            write: vec![0u8; INITIAL_BUFFER],
            headers: [Header::default(); HEADER_SLOTS],
            trailers: [Header::default(); TRAILER_SLOTS],
            reusable: true,
        })
    }

    pub fn is_reusable(&self) -> bool {
        self.reusable
    }

    pub async fn request(
        &mut self,
        method: &[u8],
        target: &[u8],
        headers: &[Field<'_>],
        body: &[u8],
    ) -> io::Result<ClientResponse> {
        if !self.reusable {
            return Err(protocol("client connection is not reusable"));
        }

        let framing = if body.is_empty() {
            BodyFraming::None
        } else {
            BodyFraming::Fixed(body.len() as u64)
        };

        loop {
            match self.connection.encode_request_head(
                method,
                target,
                headers,
                framing,
                &mut self.write,
            ) {
                Ok(head) => {
                    self.socket.write_all(&self.write[..head.written]).await?;
                    self.connection
                        .advance_request_write(head.written)
                        .map_err(client_error)?;
                    break;
                }
                Err(ClientError::BufferTooSmall { required }) => {
                    grow(&mut self.write, required, MAX_HEAD)?;
                }
                Err(error) => return Err(client_error(error)),
            }
        }

        let mut offset = 0usize;
        while offset < body.len() {
            let written = loop {
                match self
                    .connection
                    .encode_request_body(&body[offset..], &mut self.write)
                {
                    Ok(encoded) => break encoded.written,
                    Err(ClientError::BufferTooSmall { required }) => {
                        grow(&mut self.write, required, MAX_HEAD)?;
                    }
                    Err(error) => return Err(client_error(error)),
                }
            };
            if written == 0 {
                return Err(protocol("request body made no progress"));
            }
            self.socket.write_all(&self.write[..written]).await?;
            self.connection
                .advance_request_write(written)
                .map_err(client_error)?;
            offset += written;
        }

        if !body.is_empty() {
            let tail = loop {
                match self.connection.finish_request_body(&[], &mut self.write) {
                    Ok(tail) => break tail,
                    Err(ClientError::BufferTooSmall { required }) => {
                        grow(&mut self.write, required, MAX_HEAD)?;
                    }
                    Err(error) => return Err(client_error(error)),
                }
            };
            if tail.written > 0 {
                self.socket.write_all(&self.write[..tail.written]).await?;
                self.connection
                    .advance_request_write(tail.written)
                    .map_err(client_error)?;
            }
        }

        self.read_response().await
    }

    async fn read_response(&mut self) -> io::Result<ClientResponse> {
        let mut filled = 0usize;
        let mut cursor = 0usize;
        let mut status = 0u16;
        let mut headers = Vec::new();
        let mut body = Vec::new();

        loop {
            let input = &self.read[cursor..filled];
            let event = self
                .connection
                .read_response(input, false, &mut self.headers, &mut self.trailers)
                .map_err(client_error)?;
            let base = cursor;
            cursor += event.consumed;

            match event.kind {
                ClientReadKind::Head => {
                    status = event.head.map(|head| head.status).unwrap_or(0);
                    let count = event
                        .head
                        .map(|head| head.header_count)
                        .unwrap_or(0)
                        .min(self.headers.len());
                    headers.clear();
                    for header in &self.headers[..count] {
                        let (Some(name), Some(value)) =
                            (slice(input, header.name), slice(input, header.value))
                        else {
                            return Err(protocol("invalid response header span"));
                        };
                        headers.push((name.to_vec(), value.to_vec()));
                    }
                }
                ClientReadKind::BodyData => {
                    let start = base + event.body_data.offset;
                    let end = start + event.body_data.length;
                    body.extend_from_slice(&self.read[start..end]);
                    if body.len() > MAX_BODY {
                        return Err(protocol("response body exceeds the limit"));
                    }
                }
                ClientReadKind::Complete => {
                    self.reusable = event.can_reuse && self.connection.reset().is_ok();
                    return Ok(ClientResponse {
                        status,
                        headers,
                        body,
                    });
                }
                ClientReadKind::Upgrade => {
                    return Err(protocol("response upgrade is not supported"));
                }
                ClientReadKind::NeedMore
                | ClientReadKind::Informational
                | ClientReadKind::Trailers => {}
            }

            if event.kind == ClientReadKind::NeedMore && (cursor == filled || event.consumed == 0) {
                if cursor > 0 {
                    self.read.copy_within(cursor..filled, 0);
                    filled -= cursor;
                    cursor = 0;
                }
                if filled == self.read.len() {
                    let next = self.read.len() * 2;
                    grow(&mut self.read, next, MAX_HEAD)?;
                }

                let read = self.socket.read(&mut self.read[filled..]).await?;
                if read == 0 {
                    return Err(protocol("eof inside response"));
                }
                filled += read;
            }
        }
    }
}

fn slice(input: &[u8], span: Span) -> Option<&[u8]> {
    input.get(span.offset..span.offset.checked_add(span.length)?)
}

fn grow(buffer: &mut Vec<u8>, required: usize, max: usize) -> io::Result<()> {
    if required > max {
        return Err(protocol("message exceeds the buffer limit"));
    }
    buffer.resize(required.max(buffer.len() * 2).min(max), 0);
    Ok(())
}

fn client_error(error: ClientError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("client: {error:?}"))
}

fn protocol(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_owned())
}
