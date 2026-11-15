use std::io;
use std::ops::Range;

use veylor_client::{
    BodyFraming, ClientConnection, ClientError, ClientReadKind, Field, Header, RequestMethod, Span,
};

use crate::budget::Budget;
use crate::headers::Headers;
use crate::outgoing::Outgoing;
use crate::wire::Wire;

const INITIAL_WRITE: usize = 8 * 1024;
const BODY_WRITE: usize = 32 * 1024;
const RETAINED_BUFFER: usize = 64 * 1024;
const READ_CHUNK: usize = 16 * 1024;
const HEADER_SLOTS: usize = 64;
const TRAILER_SLOTS: usize = 8;
const MAX_HEADER_SLOTS: usize = 1024;

pub(crate) enum Step {
    Head { status: u16, headers: Headers },
    Body(Range<usize>),
    Done,
    NeedRead,
}

pub(crate) struct H1Conn {
    wire: Wire,
    exchange: ClientConnection<'static>,
    read: Vec<u8>,
    cursor: usize,
    write: Vec<u8>,
    headers: Box<[Header]>,
    trailers: Box<[Header]>,
    max_head: usize,
    eof: bool,
    pending: Range<usize>,
    finished: bool,
    reusable: bool,
}

impl H1Conn {
    pub(crate) fn new(wire: Wire, max_head: usize) -> Self {
        Self {
            wire,
            exchange: ClientConnection::new(RequestMethod::Other, b""),
            read: Vec::new(),
            cursor: 0,
            write: vec![0; INITIAL_WRITE],
            headers: vec![Header::default(); HEADER_SLOTS].into_boxed_slice(),
            trailers: vec![Header::default(); TRAILER_SLOTS].into_boxed_slice(),
            max_head,
            eof: false,
            pending: 0..0,
            finished: false,
            reusable: false,
        }
    }

    pub(crate) fn is_stale(&self) -> bool {
        self.wire.is_stale()
    }

    pub(crate) fn received_nothing(&self) -> bool {
        self.read.is_empty() && self.cursor == 0 && !self.finished
    }

    pub(crate) fn is_reusable(&self) -> bool {
        self.finished && self.reusable
    }

    pub(crate) fn trim(&mut self) {
        if self.read.capacity() > RETAINED_BUFFER {
            self.read = Vec::new();
        }

        if self.write.len() > RETAINED_BUFFER {
            self.write.truncate(INITIAL_WRITE);
            self.write.shrink_to_fit();
        }
    }

    pub(crate) fn send(&mut self, request: &Outgoing<'_>, budget: &Budget) -> io::Result<()> {
        self.read.clear();
        self.cursor = 0;
        self.eof = false;
        self.pending = 0..0;
        self.finished = false;
        self.reusable = false;

        let kind = if request.method == "HEAD" {
            RequestMethod::Head
        } else {
            RequestMethod::Other
        };

        self.exchange = ClientConnection::new(kind, b"");

        if request.body.len() > INITIAL_WRITE && self.write.len() < BODY_WRITE {
            self.write.resize(BODY_WRITE, 0);
        }

        let mut fields = Vec::with_capacity(request.headers.len() + 1);

        fields.push(Field {
            name: b"host",
            value: request.host.as_bytes(),
        });

        for (name, value) in request.headers {
            fields.push(Field {
                name: name.as_bytes(),
                value: value.as_bytes(),
            });
        }

        let framing = if request.expects_body() {
            BodyFraming::Fixed(request.body.len() as u64)
        } else {
            BodyFraming::None
        };
        let mut used = self.encode_head(request, &fields, framing)?;

        self.exchange
            .advance_request_write(used)
            .map_err(client_error)?;
        self.wire.arm_write(budget)?;

        let mut offset = 0;

        while offset < request.body.len() {
            if used == self.write.len() {
                self.wire.write_plain(&self.write[..used])?;
                used = 0;
            }

            let encoded = self
                .exchange
                .encode_request_body(&request.body[offset..], &mut self.write[used..])
                .map_err(client_error)?;

            if encoded.written == 0 {
                return Err(protocol("request body made no progress"));
            }

            self.exchange
                .advance_request_write(encoded.written)
                .map_err(client_error)?;

            used += encoded.written;
            offset += encoded.written;
        }

        if !request.body.is_empty() {
            self.exchange
                .finish_request_body(&[], &mut self.write[used..])
                .map_err(client_error)?;
        }

        self.wire.write_plain(&self.write[..used])
    }

    fn encode_head(
        &mut self,
        request: &Outgoing<'_>,
        fields: &[Field<'_>],
        framing: BodyFraming,
    ) -> io::Result<usize> {
        loop {
            let encoded = self.exchange.encode_request_head(
                request.method.as_bytes(),
                request.target.as_bytes(),
                fields,
                framing,
                &mut self.write,
            );

            match encoded {
                Ok(head) => return Ok(head.written),
                Err(ClientError::BufferTooSmall { required }) => {
                    if required > self.max_head {
                        return Err(protocol("request head exceeds the buffer limit"));
                    }

                    self.write
                        .resize(required.max(self.write.len() * 2).min(self.max_head), 0);
                }
                Err(error) => return Err(client_error(error)),
            }
        }
    }

    pub(crate) fn step(&mut self, want_head: bool, budget: Option<&Budget>) -> io::Result<Step> {
        loop {
            let base = self.cursor;
            let read = self.exchange.read_response(
                &self.read[base..],
                self.eof,
                &mut self.headers,
                &mut self.trailers,
            );
            let event = match read {
                Ok(event) => event,
                Err(ClientError::HeaderCapacity {
                    headers_required,
                    trailers_required,
                }) => {
                    self.grow_slots(headers_required, trailers_required)?;

                    continue;
                }
                Err(ClientError::ProtocolError) if self.eof && self.cursor >= self.read.len() => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "connection closed inside the response",
                    ));
                }
                Err(error) => return Err(client_error(error)),
            };

            self.cursor += event.consumed;

            match event.kind {
                ClientReadKind::Head if want_head => {
                    let head = event.head;
                    let status = head.map_or(0, |head| head.status);
                    let count = head
                        .map_or(0, |head| head.header_count)
                        .min(self.headers.len());
                    let headers = self.collect_headers(base, count)?;

                    return Ok(Step::Head { status, headers });
                }
                ClientReadKind::Head => return Err(protocol("unexpected second response head")),
                ClientReadKind::BodyData => {
                    let start = base + event.body_data.offset;

                    return Ok(Step::Body(start..start + event.body_data.length));
                }
                ClientReadKind::Complete => {
                    self.reusable = event.can_reuse
                        && self.cursor == self.read.len()
                        && self.exchange.reset().is_ok();
                    self.finished = true;
                    self.read.clear();
                    self.cursor = 0;

                    return Ok(Step::Done);
                }
                ClientReadKind::Upgrade => {
                    return Err(protocol("response upgrade is not supported"));
                }
                ClientReadKind::Trailers | ClientReadKind::Informational => {}
                ClientReadKind::NeedMore => {
                    if event.consumed > 0 && self.cursor < self.read.len() {
                        continue;
                    }

                    if self.eof {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "connection closed inside the response",
                        ));
                    }

                    let Some(budget) = budget else {
                        return Ok(Step::NeedRead);
                    };

                    self.fill(budget)?;
                }
            }
        }
    }

    fn collect_headers(&self, base: usize, count: usize) -> io::Result<Headers> {
        let input = &self.read[base..];
        let mut headers = Headers::default();

        for header in &self.headers[..count] {
            let (Some(name), Some(value)) = (slice(input, header.name), slice(input, header.value))
            else {
                return Err(protocol("invalid response header span"));
            };

            headers.push(name, value);
        }

        Ok(headers)
    }

    fn grow_slots(&mut self, headers: usize, trailers: usize) -> io::Result<()> {
        if headers > MAX_HEADER_SLOTS || trailers > MAX_HEADER_SLOTS {
            return Err(protocol("response has too many header fields"));
        }

        if headers > self.headers.len() {
            self.headers = vec![Header::default(); headers].into_boxed_slice();
        }

        if trailers > self.trailers.len() {
            self.trailers = vec![Header::default(); trailers].into_boxed_slice();
        }

        Ok(())
    }

    fn fill(&mut self, budget: &Budget) -> io::Result<()> {
        if self.cursor > 0 {
            self.read.drain(..self.cursor);
            self.cursor = 0;
        }

        if self.read.len() >= self.max_head.max(READ_CHUNK) {
            return Err(protocol("response message exceeds the buffer limit"));
        }

        self.wire.arm_read(budget)?;

        let read = self.wire.read_plain(&mut self.read, READ_CHUNK)?;

        self.eof = read == 0;

        Ok(())
    }

    pub(crate) fn fill_buf(&mut self, budget: &Budget) -> io::Result<&[u8]> {
        while self.pending.is_empty() {
            if self.finished {
                return Ok(&[]);
            }

            match self.step(false, Some(budget))? {
                Step::Body(range) => self.pending = range,
                Step::Done => return Ok(&[]),
                Step::Head { .. } | Step::NeedRead => {}
            }
        }

        Ok(&self.read[self.pending.clone()])
    }

    pub(crate) fn consume(&mut self, amount: usize) {
        self.pending.start = (self.pending.start + amount).min(self.pending.end);
    }

    pub(crate) fn head(&mut self, budget: &Budget) -> io::Result<(u16, Headers)> {
        match self.step(true, Some(budget))? {
            Step::Head { status, headers } => Ok((status, headers)),
            _ => Err(protocol("response ended without a head")),
        }
    }

    pub(crate) fn drain(&mut self, budget: &Budget, limit: usize) -> bool {
        let mut seen = 0usize;

        loop {
            match self.fill_buf(budget) {
                Ok([]) => return self.is_reusable(),
                Ok(data) => {
                    seen += data.len();

                    let length = data.len();

                    self.consume(length);

                    if seen > limit {
                        return false;
                    }
                }
                Err(_) => return false,
            }
        }
    }

    pub(crate) fn drain_buffered(&mut self) -> bool {
        self.pending = 0..0;

        loop {
            match self.step(false, None) {
                Ok(Step::Body(_)) => {}
                Ok(Step::Done) => return self.is_reusable(),
                _ => return false,
            }
        }
    }
}

fn slice(input: &[u8], span: Span) -> Option<&[u8]> {
    input.get(span.offset..span.offset.checked_add(span.length)?)
}

fn client_error(error: ClientError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("client: {error:?}"))
}

fn protocol(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_owned())
}
