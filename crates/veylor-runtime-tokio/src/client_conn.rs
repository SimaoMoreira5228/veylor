use std::io;
use std::ops::Range;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::timeout;
use veylor_client::{
    BodyFraming, ClientConnection, ClientError, ClientReadKind, Field, Header, RequestMethod, Span,
};
use veylor_tls::{
    TlsConn, TlsError, drain_plaintext, feed_ciphertext, flush_ciphertext, queue_close_notify,
};

use crate::h2_client::HeaderList;

pub const INITIAL_BUFFER: usize = 8 * 1024;
const RETAINED_BUFFER: usize = 32 * 1024;
const READ_CHUNK: usize = 16 * 1024 + 512;
const HEADER_SLOTS: usize = 64;
const TRAILER_SLOTS: usize = 16;

#[derive(Clone, Copy)]
pub struct Deadlines {
    pub write: Duration,
    pub head: Duration,
    pub body: Duration,
    pub max_head: usize,
}

pub struct ResponseHead {
    pub status: u16,
    pub headers: HeaderList,
}

pub enum Event {
    Head(ResponseHead),
    Body(Range<usize>),
    Done,
}

pub struct Connection {
    socket: TcpStream,
    tls: Option<Box<TlsConn>>,
    exchange: ClientConnection<'static>,
    read: Vec<u8>,
    cursor: usize,
    write: Vec<u8>,
    cipher_in: Vec<u8>,
    cipher_out: Vec<u8>,
    headers: Box<[Header]>,
    trailers: Box<[Header]>,
    deadlines: Deadlines,
    alpn: Option<Vec<u8>>,
    trailer_list: Option<HeaderList>,
    eof: bool,
    busy: bool,
    reusable: bool,
}

impl Connection {
    pub fn new(
        socket: TcpStream,
        tls: Option<Box<TlsConn>>,
        alpn: Option<Vec<u8>>,
        deadlines: Deadlines,
    ) -> Self {
        Self {
            socket,
            tls,
            exchange: ClientConnection::new(RequestMethod::Other, b""),
            read: Vec::new(),
            cursor: 0,
            write: vec![0u8; INITIAL_BUFFER],
            cipher_in: Vec::new(),
            cipher_out: Vec::new(),
            headers: vec![Header::default(); HEADER_SLOTS].into_boxed_slice(),
            trailers: vec![Header::default(); TRAILER_SLOTS].into_boxed_slice(),
            deadlines,
            alpn,
            trailer_list: None,
            eof: false,
            busy: false,
            reusable: true,
        }
    }

    pub fn alpn(&self) -> Option<&[u8]> {
        self.alpn.as_deref()
    }

    pub fn take_trailers(&mut self) -> Option<HeaderList> {
        self.trailer_list.take()
    }

    pub fn is_idle_reusable(&self) -> bool {
        self.reusable && !self.busy
    }

    pub fn trim(&mut self) {
        if self.read.capacity() > RETAINED_BUFFER {
            self.read = Vec::new();
        }

        if self.write.len() > RETAINED_BUFFER {
            self.write.truncate(INITIAL_BUFFER);
            self.write.shrink_to_fit();
        }

        self.cipher_in = Vec::new();
        self.cipher_out = Vec::new();
    }

    pub fn is_alive(&mut self) -> bool {
        if !self.is_idle_reusable() {
            return false;
        }

        let mut cx = Context::from_waker(Waker::noop());
        let mut probe = [0u8; 1024];

        loop {
            match self.socket.poll_read_ready(&mut cx) {
                Poll::Pending => return true,
                Poll::Ready(Err(_)) => return false,
                Poll::Ready(Ok(())) => {}
            }

            match self.socket.try_read(&mut probe) {
                Ok(0) => return false,
                Ok(read) => {
                    let Some(tls) = self.tls.as_deref_mut() else {
                        return false;
                    };

                    if feed_ciphertext(tls, &probe[..read], &mut self.read).is_err() {
                        return false;
                    }

                    if !self.read.is_empty() {
                        return false;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return true,
                Err(_) => return false,
            }
        }
    }

    pub async fn send_request(
        &mut self,
        method: &[u8],
        target: &[u8],
        headers: &[Field<'_>],
        body: &[u8],
    ) -> io::Result<()> {
        self.busy = true;
        self.eof = false;
        self.trailer_list = None;
        self.read.clear();
        self.cursor = 0;
        self.exchange = ClientConnection::new(request_method(method)?, b"");

        let framing = if body.is_empty() {
            BodyFraming::None
        } else {
            BodyFraming::Fixed(body.len() as u64)
        };
        let max = self.deadlines.max_head;

        let head = loop {
            match self.exchange.encode_request_head(
                method,
                target,
                headers,
                framing,
                &mut self.write,
            ) {
                Ok(head) => break head.written,
                Err(ClientError::BufferTooSmall { required }) => {
                    grow(&mut self.write, required, max)?;
                }
                Err(error) => return Err(client_error(error)),
            }
        };

        self.write_pending(head).await?;

        let mut offset = 0usize;

        while offset < body.len() {
            let written = loop {
                match self
                    .exchange
                    .encode_request_body(&body[offset..], &mut self.write)
                {
                    Ok(encoded) => break encoded.written,
                    Err(ClientError::BufferTooSmall { required }) => {
                        grow(&mut self.write, required, max)?;
                    }
                    Err(error) => return Err(client_error(error)),
                }
            };

            if written == 0 {
                return Err(protocol("request body made no progress"));
            }

            self.write_pending(written).await?;

            offset += written;
        }

        if body.is_empty() {
            return Ok(());
        }

        let tail = loop {
            match self.exchange.finish_request_body(&[], &mut self.write) {
                Ok(tail) => break tail.written,
                Err(ClientError::BufferTooSmall { required }) => {
                    grow(&mut self.write, required, max)?;
                }
                Err(error) => return Err(client_error(error)),
            }
        };

        if tail > 0 {
            self.write_pending(tail).await?;
        }

        Ok(())
    }

    pub async fn read_head(&mut self) -> io::Result<ResponseHead> {
        let deadline = self.deadlines.head;

        match timeout(deadline, self.next_event(true)).await {
            Ok(Ok(Event::Head(head))) => Ok(head),
            Ok(Ok(_)) => Err(protocol("response ended before its head")),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(timed_out("response head")),
        }
    }

    pub async fn next_chunk(&mut self) -> io::Result<Option<&[u8]>> {
        let deadline = self.deadlines.body;

        let event = match timeout(deadline, self.next_event(false)).await {
            Ok(event) => event?,
            Err(_) => return Err(timed_out("response body")),
        };

        match event {
            Event::Body(range) => Ok(Some(&self.read[range])),
            Event::Done => Ok(None),
            Event::Head(_) => Err(protocol("unexpected second response head")),
        }
    }

    async fn next_event(&mut self, want_head: bool) -> io::Result<Event> {
        loop {
            let input = &self.read[self.cursor..];
            let event = self
                .exchange
                .read_response(input, self.eof, &mut self.headers, &mut self.trailers)
                .map_err(client_error)?;
            let base = self.cursor;

            self.cursor += event.consumed;

            match event.kind {
                ClientReadKind::Head if want_head => {
                    let head = event.head;
                    let status = head.map(|h| h.status).unwrap_or(0);
                    let count = head
                        .map(|h| h.header_count)
                        .unwrap_or(0)
                        .min(self.headers.len());
                    let mut headers = HeaderList::default();

                    for header in &self.headers[..count] {
                        let (Some(name), Some(value)) =
                            (slice(input, header.name), slice(input, header.value))
                        else {
                            return Err(protocol("invalid response header span"));
                        };

                        headers.push(name, value);
                    }

                    return Ok(Event::Head(ResponseHead { status, headers }));
                }

                ClientReadKind::Head => return Err(protocol("unexpected second response head")),

                ClientReadKind::BodyData => {
                    let start = base + event.body_data.offset;

                    return Ok(Event::Body(start..start + event.body_data.length));
                }

                ClientReadKind::Complete => {
                    self.busy = false;
                    self.reusable = event.can_reuse
                        && self.cursor == self.read.len()
                        && self.exchange.reset().is_ok();
                    self.read.clear();
                    self.cursor = 0;

                    return Ok(Event::Done);
                }

                ClientReadKind::Upgrade => {
                    return Err(protocol("response upgrade is not supported"));
                }

                ClientReadKind::Trailers => {
                    let count = event.trailer_count.min(self.trailers.len());
                    let mut list = HeaderList::default();

                    for header in &self.trailers[..count] {
                        let (Some(name), Some(value)) =
                            (slice(input, header.name), slice(input, header.value))
                        else {
                            return Err(protocol("invalid trailer span"));
                        };

                        list.push(name, value);
                    }

                    self.trailer_list = Some(list);
                }

                ClientReadKind::Informational => {}

                ClientReadKind::NeedMore => {
                    if self.cursor < self.read.len() && event.consumed > 0 {
                        continue;
                    }

                    if self.eof {
                        return Err(protocol("eof inside response"));
                    }

                    self.fill().await?;
                }
            }
        }
    }

    async fn fill(&mut self) -> io::Result<()> {
        if self.cursor > 0 {
            self.read.drain(..self.cursor);
            self.cursor = 0;
        }

        if self.read.len() >= self.deadlines.max_head.max(READ_CHUNK) {
            return Err(protocol("response message exceeds the buffer limit"));
        }

        if self.tls.is_none() {
            self.read.reserve(READ_CHUNK);

            let read = tokio::io::AsyncReadExt::read_buf(&mut self.socket, &mut self.read).await?;

            self.eof = read == 0;

            return Ok(());
        }

        self.cipher_in.clear();
        self.cipher_in.reserve(READ_CHUNK);

        let read = tokio::io::AsyncReadExt::read_buf(&mut self.socket, &mut self.cipher_in).await?;

        if read == 0 {
            self.eof = true;

            return Ok(());
        }

        self.feed_tls().await
    }

    async fn feed_tls(&mut self) -> io::Result<()> {
        let Some(tls) = self.tls.as_deref_mut() else {
            return Ok(());
        };

        let mut offset = 0usize;

        while offset < self.cipher_in.len() {
            let used = feed_ciphertext(tls, &self.cipher_in[offset..], &mut self.read)
                .map_err(tls_error)?;

            if used == 0 {
                return Err(protocol("tls stalled on input"));
            }

            offset += used;
        }

        self.cipher_out.clear();
        flush_ciphertext(tls, &mut self.cipher_out).map_err(tls_error)?;

        if self.cipher_out.is_empty() {
            return Ok(());
        }

        self.write_cipher().await
    }

    async fn write_pending(&mut self, len: usize) -> io::Result<()> {
        let limit = self.deadlines.write;

        let result = match self.tls.as_deref_mut() {
            None => timeout(limit, self.socket.write_all(&self.write[..len])).await,
            Some(tls) => {
                self.cipher_out.clear();
                drain_plaintext(tls, &self.write[..len], &mut self.cipher_out)
                    .map_err(tls_error)?;

                timeout(limit, self.socket.write_all(&self.cipher_out)).await
            }
        };

        match result {
            Ok(done) => done?,
            Err(_) => return Err(timed_out("request write")),
        }

        self.exchange
            .advance_request_write(len)
            .map_err(client_error)?;

        Ok(())
    }

    async fn write_cipher(&mut self) -> io::Result<()> {
        match timeout(
            self.deadlines.write,
            self.socket.write_all(&self.cipher_out),
        )
        .await
        {
            Ok(done) => done,
            Err(_) => Err(timed_out("tls write")),
        }
    }
}

fn request_method(method: &[u8]) -> io::Result<RequestMethod> {
    match method {
        b"HEAD" => Ok(RequestMethod::Head),
        b"CONNECT" => Err(protocol("CONNECT is not supported")),
        _ => Ok(RequestMethod::Other),
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

pub fn client_error(error: ClientError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("client: {error:?}"))
}

pub fn tls_error(error: TlsError) -> io::Error {
    match error {
        TlsError::Io(e) => e,
        other => io::Error::new(io::ErrorKind::InvalidData, other),
    }
}

pub fn protocol(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_owned())
}

pub fn timed_out(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, format!("{what} timed out"))
}

impl Drop for Connection {
    fn drop(&mut self) {
        let Some(tls) = self.tls.as_deref_mut() else {
            return;
        };
        let mut out = Vec::new();

        if queue_close_notify(tls, &mut out).is_ok() {
            let _ = self.socket.try_write(&out);
        }
    }
}
