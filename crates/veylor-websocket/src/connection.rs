use crate::close::{self, CloseError, CloseFrame};
use crate::frame::{self, EncodeError, FrameError, FrameHeader, OpCode};
use crate::utf8::Utf8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Role {
    Client,
    Server,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Limits {
    pub max_frame_bytes: usize,
    pub max_message_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 64 * 1024,
            max_message_bytes: 16 * 1024 * 1024,
        }
    }
}

impl Limits {
    pub const fn new(max_frame_bytes: usize, max_message_bytes: usize) -> Self {
        Self {
            max_frame_bytes,
            max_message_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Protocol(&'static str),
    FrameTooLarge,
    MessageTooLarge,
    InvalidUtf8,
    ControlTooLong,
    FragmentedControl,
    Close(CloseError),
    Closed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Event<'a> {
    Text { payload: &'a [u8], fin: bool },
    Binary { payload: &'a [u8], fin: bool },
    Continuation { payload: &'a [u8], fin: bool },
    Ping(&'a [u8]),
    Pong(&'a [u8]),
    Close(CloseFrame<'a>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Feed<'a> {
    pub consumed: usize,
    pub event: Option<Event<'a>>,
    pub need_more: bool,
}

impl Feed<'_> {
    fn need_more() -> Self {
        Self {
            consumed: 0,
            event: None,
            need_more: true,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum State {
    Open,
    Closed,
}

pub struct WebSocket {
    role: Role,
    limits: Limits,
    state: State,
    fragmented: Option<OpCode>,
    text: bool,
    message_bytes: usize,
    utf8: Utf8,
}

impl WebSocket {
    pub fn new(role: Role, limits: Limits) -> Self {
        Self {
            role,
            limits,
            state: State::Open,
            fragmented: None,
            text: false,
            message_bytes: 0,
            utf8: Utf8::default(),
        }
    }

    pub fn server(limits: Limits) -> Self {
        Self::new(Role::Server, limits)
    }

    pub fn client(limits: Limits) -> Self {
        Self::new(Role::Client, limits)
    }

    pub const fn role(&self) -> Role {
        self.role
    }

    pub fn is_closed(&self) -> bool {
        self.state == State::Closed
    }

    pub fn close(&mut self) {
        self.state = State::Closed;
    }

    pub fn feed<'a>(&mut self, input: &'a mut [u8]) -> Result<Feed<'a>, Error> {
        if self.state == State::Closed {
            return Err(Error::Closed);
        }

        let Some(header) = frame::parse_frame_header(input).map_err(map_frame_error)? else {
            return Ok(Feed::need_more());
        };
        let payload_len = usize::try_from(header.payload_len).map_err(|_| Error::FrameTooLarge)?;
        if payload_len > self.limits.max_frame_bytes {
            return Err(Error::FrameTooLarge);
        }
        let end = header
            .header_len
            .checked_add(payload_len)
            .ok_or(Error::FrameTooLarge)?;
        if input.len() < end {
            return Ok(Feed::need_more());
        }

        self.check_header(&header, payload_len)?;

        let payload = &mut input[header.header_len..end];
        if let Some(mask) = header.mask {
            frame::apply_mask(payload, mask, 0);
        }
        let payload: &'a [u8] = payload;

        if header.opcode.is_data() {
            self.accept_data(&header, payload, payload_len)?;
        }

        let event = match header.opcode {
            OpCode::Text => Event::Text {
                payload,
                fin: header.fin,
            },
            OpCode::Binary => Event::Binary {
                payload,
                fin: header.fin,
            },
            OpCode::Continuation => Event::Continuation {
                payload,
                fin: header.fin,
            },
            OpCode::Ping => Event::Ping(payload),
            OpCode::Pong => Event::Pong(payload),
            OpCode::Close => {
                let close = close::parse_close_payload(payload).map_err(Error::Close)?;
                self.state = State::Closed;
                Event::Close(close)
            }
        };

        Ok(Feed {
            consumed: end,
            event: Some(event),
            need_more: false,
        })
    }

    pub fn encode(
        &self,
        fin: bool,
        opcode: OpCode,
        mask: Option<[u8; 4]>,
        payload: &[u8],
        out: &mut [u8],
    ) -> Result<usize, EncodeError> {
        self.check_mask(mask)?;
        frame::encode_frame(fin, opcode, mask, payload, out)
    }

    pub fn encode_close(
        &self,
        code: Option<u16>,
        reason: &[u8],
        mask: Option<[u8; 4]>,
        out: &mut [u8],
    ) -> Result<usize, EncodeError> {
        self.check_mask(mask)?;
        let mut payload = [0u8; 125];
        let length =
            close::encode_close_payload(code, reason, &mut payload).map_err(EncodeError::Close)?;
        frame::encode_frame(true, OpCode::Close, mask, &payload[..length], out)
    }

    fn check_header(&self, header: &FrameHeader, payload_len: usize) -> Result<(), Error> {
        match (self.role, header.mask) {
            (Role::Server, None) => return Err(Error::Protocol("client frame is not masked")),
            (Role::Client, Some(_)) => return Err(Error::Protocol("server frame is masked")),
            _ => {}
        }

        match header.opcode {
            OpCode::Text | OpCode::Binary => {
                if self.fragmented.is_some() {
                    return Err(Error::Protocol("expected a continuation frame"));
                }
            }
            OpCode::Continuation => {
                if self.fragmented.is_none() {
                    return Err(Error::Protocol("unexpected continuation frame"));
                }
            }
            OpCode::Close | OpCode::Ping | OpCode::Pong => {}
        }

        if header.opcode.is_data() {
            let total = self
                .message_bytes
                .checked_add(payload_len)
                .ok_or(Error::MessageTooLarge)?;
            if total > self.limits.max_message_bytes {
                return Err(Error::MessageTooLarge);
            }
        }
        Ok(())
    }

    fn accept_data(
        &mut self,
        header: &FrameHeader,
        payload: &[u8],
        payload_len: usize,
    ) -> Result<(), Error> {
        let text = match header.opcode {
            OpCode::Text => true,
            OpCode::Binary => false,
            OpCode::Continuation => self.text,
            _ => return Ok(()),
        };

        if matches!(header.opcode, OpCode::Text | OpCode::Binary) {
            self.utf8.reset();
        }
        if text && !self.utf8.feed(payload) {
            return Err(Error::InvalidUtf8);
        }

        self.text = text;
        self.message_bytes += payload_len;

        if header.fin {
            if text && !self.utf8.finish() {
                return Err(Error::InvalidUtf8);
            }
            self.fragmented = None;
            self.message_bytes = 0;
        } else if matches!(header.opcode, OpCode::Text | OpCode::Binary) {
            self.fragmented = Some(header.opcode);
        }
        Ok(())
    }

    fn check_mask(&self, mask: Option<[u8; 4]>) -> Result<(), EncodeError> {
        match (self.role, mask) {
            (Role::Server, Some(_)) => {
                Err(EncodeError::Protocol("server frames must not be masked"))
            }
            (Role::Client, None) => Err(EncodeError::Protocol("client frames must be masked")),
            _ => Ok(()),
        }
    }
}

fn map_frame_error(error: FrameError) -> Error {
    match error {
        FrameError::ReservedBits => Error::Protocol("reserved bits are set"),
        FrameError::ReservedOpcode(_) => Error::Protocol("reserved opcode"),
        FrameError::ControlTooLong => Error::ControlTooLong,
        FrameError::FragmentedControl => Error::FragmentedControl,
        FrameError::NonMinimalLength => Error::Protocol("non-minimal length"),
        FrameError::LengthHighBit => Error::Protocol("length high bit is set"),
    }
}
