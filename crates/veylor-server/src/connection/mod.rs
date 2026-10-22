use veylor_http1::{Header, Limits, ParseError, Parser};
use veylor_router::RouteTable;
use veylor_runtime::{
    CloseReason, ConnectionToken, IoBatch, ReadStatus, TcpStream, WriteStatus, dispatch,
};

use crate::static_routes::{
    EncodedStaticResponse, StaticRoutes, respond_bad_request, respond_to_head,
};

pub const INPUT_CAP: usize = 8192;
pub const OUTPUT_CAP: usize = 8192;
pub const HEADER_SLOTS: usize = 100;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConnPhase {
    ReadingHead,
    SendingResponse,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnClose {
    PeerClosed,
    Reset,
    Timeout,
    ProtocolError,
    ResponseOverflow,
    TransportError,
    Shutdown,
    Completed,
}

pub struct ConnState {
    token: ConnectionToken,
    stream: TcpStream,
    parser: Parser,
    headers: Box<[Header; HEADER_SLOTS]>,
    input: Box<[u8; INPUT_CAP]>,
    len: usize,
    head_len: usize,
    output: Box<[u8; OUTPUT_CAP]>,
    out_len: usize,
    out_sent: usize,
    phase: ConnPhase,
    read_ready: bool,
    write_blocked: bool,
    closing: bool,
    close_reason: ConnClose,
    error_hint: bool,
}

impl ConnState {
    pub fn new(token: ConnectionToken, stream: TcpStream) -> Self {
        Self {
            token,
            stream,
            parser: Parser::new(),
            headers: Box::new([Header::default(); HEADER_SLOTS]),
            input: Box::new([0; INPUT_CAP]),
            len: 0,
            head_len: 0,
            output: Box::new([0; OUTPUT_CAP]),
            out_len: 0,
            out_sent: 0,
            phase: ConnPhase::ReadingHead,
            read_ready: true,
            write_blocked: false,
            closing: false,
            close_reason: ConnClose::Completed,
            error_hint: false,
        }
    }

    pub fn token(&self) -> ConnectionToken {
        self.token
    }

    pub fn mark_readable(&mut self) {
        self.read_ready = true;
    }

    pub fn mark_writable(&mut self) {
        self.write_blocked = false;
    }

    pub fn mark_error(&mut self) {
        self.error_hint = true;
    }

    pub fn has_pending_output(&self) -> bool {
        self.out_sent < self.out_len
    }

    pub fn has_unparsed_input(&self) -> bool {
        self.phase == ConnPhase::ReadingHead && self.len > 0
    }

    pub fn is_idle(&self) -> bool {
        self.phase == ConnPhase::ReadingHead && !self.has_pending_output() && self.len == 0
    }

    pub fn begin_drain(&mut self) {
        self.closing = true;
    }

    pub fn into_stream(self) -> TcpStream {
        self.stream
    }
}

pub enum PumpOutcome {
    Keep,
    Close(ConnClose),
}

pub fn pump_connection(
    conn: &mut ConnState,
    routes: &StaticRoutes,
    table: &RouteTable<'_, '_>,
    byte_budget: u64,
) -> PumpOutcome {
    if conn.error_hint {
        return PumpOutcome::Close(ConnClose::TransportError);
    }
    let reading = conn.phase == ConnPhase::ReadingHead && conn.len < INPUT_CAP;
    let token = conn.token;
    let read_ready = conn.read_ready;
    let out_sent = conn.out_sent;
    let out_len = conn.out_len;
    let write_ready = out_sent < out_len && !conn.write_blocked;
    let read_scratch = if reading {
        &mut conn.input[conn.len..]
    } else {
        &mut [] as &mut [u8]
    };
    let mut batch = [IoBatch::new(
        token,
        &mut conn.stream,
        read_scratch,
        read_ready,
        &conn.output[out_sent..out_len],
        write_ready,
    )];
    dispatch(byte_budget, &mut batch);
    let entry = &batch[0];
    conn.read_ready = false;
    match entry.read {
        ReadStatus::Progress(bytes) => {
            conn.len += bytes;
        }
        ReadStatus::Eof => return PumpOutcome::Close(ConnClose::PeerClosed),
        ReadStatus::Closed(CloseReason::PeerClosed) => {
            return PumpOutcome::Close(ConnClose::PeerClosed);
        }
        ReadStatus::Closed(CloseReason::Reset) => return PumpOutcome::Close(ConnClose::Reset),
        ReadStatus::Closed(CloseReason::Error(_)) | ReadStatus::Error(_) => {
            return PumpOutcome::Close(ConnClose::TransportError);
        }
        ReadStatus::Idle
        | ReadStatus::WouldBlock
        | ReadStatus::BudgetExhausted
        | ReadStatus::PausedForWrite
        | ReadStatus::Interrupted => {}
    }
    let accepted = match entry.write {
        WriteStatus::Progress(accepted)
        | WriteStatus::WouldBlock(accepted)
        | WriteStatus::BudgetExhausted(accepted)
        | WriteStatus::Interrupted(accepted) => {
            if matches!(entry.write, WriteStatus::WouldBlock(_)) {
                conn.write_blocked = true;
            }
            accepted
        }
        WriteStatus::Closed(CloseReason::PeerClosed) => {
            return PumpOutcome::Close(ConnClose::PeerClosed);
        }
        WriteStatus::Closed(CloseReason::Reset) => return PumpOutcome::Close(ConnClose::Reset),
        WriteStatus::Closed(CloseReason::Error(_)) | WriteStatus::Error(_) => {
            return PumpOutcome::Close(ConnClose::TransportError);
        }
        WriteStatus::Idle => 0,
    };
    conn.out_sent += accepted;
    if entry.closed.is_some() && !conn.has_pending_output() && conn.phase == ConnPhase::ReadingHead
    {
        return PumpOutcome::Close(ConnClose::PeerClosed);
    }

    if conn.phase == ConnPhase::ReadingHead {
        match parse_exchange(conn, routes, table) {
            ExchangeOutcome::NeedMore => {}
            ExchangeOutcome::Responded => {}
            ExchangeOutcome::Close(reason) => return PumpOutcome::Close(reason),
        }
    }

    if conn.phase == ConnPhase::SendingResponse && !conn.has_pending_output() {
        if conn.closing {
            return PumpOutcome::Close(conn.close_reason);
        }
        reuse_keep_alive(conn);
    }
    PumpOutcome::Keep
}

enum ExchangeOutcome {
    NeedMore,
    Responded,
    Close(ConnClose),
}

fn parse_exchange(
    conn: &mut ConnState,
    routes: &StaticRoutes,
    table: &RouteTable<'_, '_>,
) -> ExchangeOutcome {
    let parsed = conn.parser.parse(
        &conn.input[..conn.len],
        &mut conn.headers[..],
        Limits::default(),
    );
    match parsed {
        Ok(Some(head)) => {
            conn.head_len = head.length;
            match respond_to_head(
                routes,
                table,
                head,
                &conn.input[..conn.len],
                &mut conn.output[..],
            ) {
                Ok(EncodedStaticResponse { length, keep_open }) => {
                    conn.out_len = length;
                    conn.out_sent = 0;
                    conn.phase = ConnPhase::SendingResponse;
                    conn.write_blocked = false;
                    conn.closing = !keep_open;
                    conn.close_reason = ConnClose::Completed;
                    ExchangeOutcome::Responded
                }
                Err(_) => ExchangeOutcome::Close(ConnClose::ResponseOverflow),
            }
        }
        Ok(None) => {
            if conn.len == INPUT_CAP {
                send_bad_request(conn);
                ExchangeOutcome::Responded
            } else {
                ExchangeOutcome::NeedMore
            }
        }
        Err(ParseError::HeaderCapacity) => {
            send_bad_request(conn);
            ExchangeOutcome::Responded
        }
        Err(_) => {
            send_bad_request(conn);
            ExchangeOutcome::Responded
        }
    }
}

fn send_bad_request(conn: &mut ConnState) {
    conn.close_reason = ConnClose::ProtocolError;
    match respond_bad_request(&mut conn.output[..]) {
        Ok(length) => {
            conn.out_len = length;
            conn.out_sent = 0;
            conn.phase = ConnPhase::SendingResponse;
            conn.write_blocked = false;
            conn.closing = true;
        }
        Err(_) => {
            conn.out_len = 0;
            conn.out_sent = 0;
            conn.phase = ConnPhase::SendingResponse;
            conn.closing = true;
        }
    }
}

fn reuse_keep_alive(conn: &mut ConnState) {
    let tail = conn.len - conn.head_len;
    conn.input.copy_within(conn.head_len..conn.len, 0);
    conn.len = tail;
    conn.head_len = 0;
    conn.out_len = 0;
    conn.out_sent = 0;
    conn.phase = ConnPhase::ReadingHead;
    conn.parser.reset();
    conn.read_ready = tail > 0;
}
