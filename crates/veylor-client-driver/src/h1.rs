use std::future::Future;
use std::io;
use std::ops::Range;
use std::time::Duration;

use veylor_client::{
    BodyFraming, ClientConnection, ClientError, ClientReadKind, Field, Header, RequestMethod, Span,
};
use veylor_tls::{TlsConn, TlsError, drain_plaintext, feed_ciphertext, flush_ciphertext};

use crate::h2::HeaderList;

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

pub enum Step {
    Event(Event),
    NeedRead,
}

pub trait H1Conn: Send + 'static {
    fn exchange(&self) -> &H1Exchange;

    fn exchange_mut(&mut self) -> &mut H1Exchange;

    fn is_alive(&mut self) -> bool;

    fn next_chunk(&mut self) -> impl Future<Output = io::Result<Option<&[u8]>>> + Send;
}

pub struct H1Exchange {
    tls: Option<Box<TlsConn>>,
    exchange: ClientConnection<'static>,
    read: Vec<u8>,
    cursor: usize,
    write: Vec<u8>,
    cipher_in: Vec<u8>,
    cipher_out: Vec<u8>,
    headers: Box<[Header]>,
    trailers: Box<[Header]>,
    max_head: usize,
    alpn: Option<Vec<u8>>,
    trailer_list: Option<HeaderList>,
    eof: bool,
    busy: bool,
    reusable: bool,
}

impl H1Exchange {
    pub fn new(tls: Option<Box<TlsConn>>, alpn: Option<Vec<u8>>, max_head: usize) -> Self {
        Self {
            tls,
            exchange: ClientConnection::new(RequestMethod::Other, b""),
            read: Vec::new(),
            cursor: 0,
            write: vec![0u8; INITIAL_BUFFER],
            cipher_in: Vec::new(),
            cipher_out: Vec::new(),
            headers: vec![Header::default(); HEADER_SLOTS].into_boxed_slice(),
            trailers: vec![Header::default(); TRAILER_SLOTS].into_boxed_slice(),
            max_head,
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

    pub fn tls_mut(&mut self) -> Option<&mut TlsConn> {
        self.tls.as_deref_mut()
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

    pub fn probe_fed(&mut self, ciphertext: &[u8]) -> bool {
        let Some(tls) = self.tls.as_deref_mut() else {
            return false;
        };

        if feed_ciphertext(tls, ciphertext, &mut self.read).is_err() {
            return false;
        }

        self.read.is_empty()
    }

    pub fn begin(&mut self, method: &[u8]) -> io::Result<()> {
        self.busy = true;
        self.eof = false;
        self.trailer_list = None;
        self.read.clear();
        self.cursor = 0;
        self.exchange = ClientConnection::new(request_method(method)?, b"");

        Ok(())
    }

    pub fn encode_head(
        &mut self,
        method: &[u8],
        target: &[u8],
        headers: &[Field<'_>],
        body_len: usize,
    ) -> io::Result<usize> {
        let framing = if body_len == 0 {
            BodyFraming::None
        } else {
            BodyFraming::Fixed(body_len as u64)
        };

        loop {
            match self.exchange.encode_request_head(
                method,
                target,
                headers,
                framing,
                &mut self.write,
            ) {
                Ok(head) => return Ok(head.written),
                Err(ClientError::BufferTooSmall { required }) => {
                    grow(&mut self.write, required, self.max_head)?;
                }
                Err(error) => return Err(client_error(error)),
            }
        }
    }

    pub fn encode_body(&mut self, body: &[u8]) -> io::Result<usize> {
        loop {
            match self.exchange.encode_request_body(body, &mut self.write) {
                Ok(encoded) if encoded.written == 0 => {
                    return Err(protocol("request body made no progress"));
                }
                Ok(encoded) => return Ok(encoded.written),
                Err(ClientError::BufferTooSmall { required }) => {
                    grow(&mut self.write, required, self.max_head)?;
                }
                Err(error) => return Err(client_error(error)),
            }
        }
    }

    pub fn encode_tail(&mut self) -> io::Result<usize> {
        loop {
            match self.exchange.finish_request_body(&[], &mut self.write) {
                Ok(tail) => return Ok(tail.written),
                Err(ClientError::BufferTooSmall { required }) => {
                    grow(&mut self.write, required, self.max_head)?;
                }
                Err(error) => return Err(client_error(error)),
            }
        }
    }

    pub fn wire_out(&mut self, len: usize) -> io::Result<&[u8]> {
        let Some(tls) = self.tls.as_deref_mut() else {
            return Ok(&self.write[..len]);
        };

        self.cipher_out.clear();
        drain_plaintext(tls, &self.write[..len], &mut self.cipher_out).map_err(tls_error)?;

        Ok(&self.cipher_out)
    }

    pub fn cipher_out(&self) -> &[u8] {
        &self.cipher_out
    }

    pub fn wrote(&mut self, len: usize) -> io::Result<()> {
        self.exchange
            .advance_request_write(len)
            .map(drop)
            .map_err(client_error)
    }

    pub fn chunk(&self, range: Range<usize>) -> &[u8] {
        &self.read[range]
    }

    pub fn next_step(&mut self, want_head: bool) -> io::Result<Step> {
        let mut retried_at = None;

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

                    return Ok(Step::Event(Event::Head(ResponseHead { status, headers })));
                }

                ClientReadKind::Head => return Err(protocol("unexpected second response head")),

                ClientReadKind::BodyData => {
                    let start = base + event.body_data.offset;

                    return Ok(Step::Event(Event::Body(
                        start..start + event.body_data.length,
                    )));
                }

                ClientReadKind::Complete => {
                    self.busy = false;
                    self.reusable = event.can_reuse
                        && self.cursor == self.read.len()
                        && self.exchange.reset().is_ok();
                    self.read.clear();
                    self.cursor = 0;

                    return Ok(Step::Event(Event::Done));
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

                    // NOTE: the parser can finish a chunk-size line without consuming
                    // bytes, so buffered input gets one more pass before reading.
                    if self.cursor < self.read.len() && retried_at != Some(self.cursor) {
                        retried_at = Some(self.cursor);

                        continue;
                    }

                    if self.eof {
                        return Err(protocol("eof inside response"));
                    }

                    return Ok(Step::NeedRead);
                }
            }
        }
    }

    pub fn read_slot(&mut self) -> io::Result<&mut [u8]> {
        if self.cursor > 0 {
            self.read.drain(..self.cursor);
            self.cursor = 0;
        }

        if self.read.len() >= self.max_head.max(READ_CHUNK) {
            return Err(protocol("response message exceeds the buffer limit"));
        }

        if self.tls.is_none() {
            let base = self.read.len();

            self.read.resize(base + READ_CHUNK, 0);

            return Ok(&mut self.read[base..]);
        }

        self.cipher_in.resize(READ_CHUNK, 0);

        Ok(&mut self.cipher_in)
    }

    pub fn filled(&mut self, read: usize) -> io::Result<bool> {
        let Some(tls) = self.tls.as_deref_mut() else {
            let base = self.read.len() - READ_CHUNK;

            self.read.truncate(base + read);
            self.eof = read == 0;

            return Ok(false);
        };

        if read == 0 {
            self.eof = true;

            return Ok(false);
        }

        let mut offset = 0usize;

        while offset < read {
            let used = feed_ciphertext(tls, &self.cipher_in[offset..read], &mut self.read)
                .map_err(tls_error)?;

            if used == 0 {
                return Err(protocol("tls stalled on input"));
            }

            offset += used;
        }

        self.cipher_out.clear();
        flush_ciphertext(tls, &mut self.cipher_out).map_err(tls_error)?;

        Ok(!self.cipher_out.is_empty())
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
