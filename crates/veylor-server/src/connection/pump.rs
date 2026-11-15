use super::tls::{tls_alpn_is_h2, tls_encrypt_output, tls_turn_active};

use veylor_router::RouteTable;
use veylor_runtime::{CloseReason, IoBatch, ReadStatus, WriteStatus, dispatch};

use super::INPUT_CAP;
use super::exchange::{ExchangeOutcome, parse_exchange};
#[cfg(target_os = "linux")]
use super::file::sendfile_file_chunk;
use super::file::stage_file_chunk;
use super::state::{ConnClose, ConnPhase, ConnState};
use crate::host::HostShared;
use crate::static_routes::StaticRoutes;

pub enum PumpOutcome {
    Keep(PumpProgress),
    Close(ConnClose),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PumpProgress {
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub resumed_idle: bool,
    pub entered_body: bool,
    pub entered_response: bool,
    pub entered_park: bool,
    pub response_flushed: bool,
    pub tunnel_upgraded: bool,
    pub tunnel_draining: bool,
}

pub fn pump_connection(
    conn: &mut ConnState,
    routes: &StaticRoutes,
    table: &RouteTable<'_, '_>,
    byte_budget: u64,
    worker_id: u32,
    host: &HostShared,
    draining: bool,
) -> PumpOutcome {
    if conn.error_hint {
        return PumpOutcome::Close(ConnClose::TransportError);
    }
    if conn.phase == ConnPhase::Tunnel {
        return super::tunnel::pump_tunnel(conn, host, byte_budget);
    }
    if conn.is_parked() {
        return super::parked::pump_parked(conn, host);
    }
    // NOTE: zero-copy staging split: Linux moves file bytes straight to the socket with
    // sendfile while other targets stage one read_chunk slice through the output buffer;
    // both cap a turn at the same byte width off the same transfer offset, so keep-alive
    // reuse, pipelined tail, and shutdown drain stay byte-identical while a short send
    // still fails closed as truncation. Rust ignores SIGPIPE process-wide, so a closed
    // receiver surfaces here as EPIPE/PeerClosed, never as a signal; EAGAIN backpressures
    // through write_blocked exactly like a blocked buffer flush.
    #[cfg(target_os = "linux")]
    let mut file_sent: u64 = 0;
    #[cfg(not(target_os = "linux"))]
    let file_sent: u64 = 0;
    #[cfg(target_os = "linux")]
    if conn.phase == ConnPhase::SendingFile && !conn.has_pending_output() {
        if conn.has_tls() {
            if let Err(reason) = stage_file_chunk(conn) {
                return PumpOutcome::Close(reason);
            }
        } else {
            match sendfile_file_chunk(conn, byte_budget) {
                Ok(sent) => file_sent = sent,
                Err(reason) => return PumpOutcome::Close(reason),
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    if conn.phase == ConnPhase::SendingFile && !conn.has_pending_output() {
        if let Err(reason) = stage_file_chunk(conn) {
            return PumpOutcome::Close(reason);
        }
    }
    if conn.has_tls() {
        return pump_tls_full(
            conn,
            routes,
            table,
            byte_budget,
            worker_id,
            host,
            draining,
            file_sent,
        );
    }
    let draining_body = matches!(conn.phase, ConnPhase::DrainingBody { .. });
    let reading = (conn.phase == ConnPhase::ReadingHead || draining_body) && conn.len < INPUT_CAP;
    let token = conn.token;
    let batched_read = conn.batched_read;
    conn.batched_read = 0;
    let mut progress = PumpProgress {
        bytes_read: batched_read as u64,
        ..PumpProgress::default()
    };
    let read_ready = conn.read_ready;
    let out_sent = conn.out_sent;
    let out_len = conn.out_len;
    // NOTE: the encoded final response is held while draining and no final byte is written until
    // the declared body is fully skipped, so a drained exchange keeps single-outstanding-response
    // order. An admitted 100-continue interim is staged ahead of the final and stays writable
    // during the drain; dispatch serves the write first and pauses reads while it is pending, so
    // the interim always flushes before further drain bytes are consumed from the wire.
    // NOTE: this flush-before-drain order is the pump's transport half; the verdicts (who gets
    // an interim, the 100 shape) are shared policy in veylor_http1. The host-driven conn crate
    // mirrors the order via its return-to-Idle 100 state, so only the mechanism differs.
    let sending = conn.phase == ConnPhase::SendingResponse || conn.phase == ConnPhase::SendingFile;
    let flushing_interim =
        matches!(conn.phase, ConnPhase::DrainingBody { .. }) && out_sent < out_len;
    let wants_write = (sending || flushing_interim) && out_sent < out_len && !conn.write_blocked;
    let defer = conn.defer_write && conn.phase == ConnPhase::SendingResponse;
    if defer && wants_write {
        conn.write_deferred = true;
    }
    let write_ready = wants_write && !defer;
    // NOTE: slabs materialize on activity, not on phase: a quiet conn (reads wanted but
    // nothing signaled) keeps zero slabs through pump turns; the empty scratch dispatches
    // an Idle read that attempts nothing.
    if reading && read_ready {
        ConnState::materialize_input(&mut conn.input);
    }
    let len = conn.len;
    let read_scratch: &mut [u8] = match conn.input.as_mut() {
        Some(input) => &mut input[len..],
        None => &mut [],
    };
    let write_src: &[u8] = match conn.output.as_ref() {
        Some(output) => &output[out_sent..out_len],
        None => &[],
    };
    let (read, write, closed) = if read_scratch.is_empty() && write_src.is_empty() {
        (ReadStatus::Idle, WriteStatus::Idle, None)
    } else {
        let mut batch = [IoBatch::new(
            token,
            &mut conn.stream,
            read_scratch,
            read_ready,
            write_src,
            write_ready,
        )];
        dispatch(byte_budget, &mut batch);
        (batch[0].read, batch[0].write, batch[0].closed)
    };
    // NOTE: edge-triggered readiness fires only on new arrivals, so a read that stopped on
    // partial progress or budget keeps its claim; a drained socket (WouldBlock) waits for the
    // next edge instead. Without the re-arm, budget-split heads stall until more data arrives.
    conn.read_ready = matches!(read, ReadStatus::Progress(_) | ReadStatus::BudgetExhausted);
    let read_drained = matches!(read, ReadStatus::WouldBlock);
    match read {
        ReadStatus::Progress(bytes) => {
            conn.len += bytes;
            progress.bytes_read += bytes as u64;
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
    let accepted = match write {
        WriteStatus::Progress(accepted)
        | WriteStatus::WouldBlock(accepted)
        | WriteStatus::BudgetExhausted(accepted)
        | WriteStatus::Interrupted(accepted) => {
            if matches!(write, WriteStatus::WouldBlock(_)) {
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
    progress.bytes_written = accepted as u64 + file_sent;
    if closed.is_some() && !conn.has_pending_output() && conn.phase == ConnPhase::ReadingHead {
        return PumpOutcome::Close(ConnClose::PeerClosed);
    }
    // NOTE: resume candidate only: local head phase plus new bytes or an armed buffered tail.
    // Quiet turns (no bytes, drained tail) stay silent so the hooks skip the phase lookup.
    if conn.phase == ConnPhase::ReadingHead
        && (progress.bytes_read > 0 || (conn.len > 0 && read_ready))
    {
        progress.resumed_idle = true;
    }

    if let ConnPhase::DrainingBody {
        remaining,
        responded,
    } = conn.phase
    {
        let skip = (conn.len as u64).min(remaining) as usize;
        if let Some(input) = conn.input.as_mut() {
            input.copy_within(skip..conn.len, 0);
        }
        conn.len -= skip;
        let left = remaining - skip as u64;
        if left == 0 {
            conn.out_len = conn.pending_len;
            conn.pending_len = 0;
            conn.phase = if conn.file.is_some() {
                ConnPhase::SendingFile
            } else {
                ConnPhase::SendingResponse
            };
            conn.write_blocked = false;
            // NOTE: a parked respond already moved the worker to WritingResponse, so the
            // drain-complete hook must not transition twice; static drains still report it.
            progress.entered_response = !responded;
        } else {
            conn.phase = ConnPhase::DrainingBody {
                remaining: left,
                responded,
            };
            // NOTE: edge-triggered readiness fires only on new arrivals, so a drain that stopped
            // short of EAGAIN re-arms its read; a drained socket waits for the next edge instead.
            // An Idle read attempted nothing (no readiness claim), so it leaves the flag alone.
            if !matches!(read, ReadStatus::Idle) {
                conn.read_ready = !read_drained;
            }
        }
    }

    if conn.phase == ConnPhase::ReadingHead {
        match parse_exchange(conn, routes, table, worker_id, host, draining) {
            ExchangeOutcome::NeedMore | ExchangeOutcome::Responded => {}
            ExchangeOutcome::Parked => progress.entered_park = true,
            ExchangeOutcome::Close(reason) => return PumpOutcome::Close(reason),
        }
        if conn.phase == ConnPhase::SendingResponse || conn.phase == ConnPhase::SendingFile {
            progress.entered_response = true;
        } else if matches!(conn.phase, ConnPhase::DrainingBody { .. }) {
            progress.entered_body = true;
        }
    }

    // NOTE: the encode above happens after this turn's dispatch, so without this flush the
    // response would wait a whole poll turn for its write. A response just encoded into an
    // empty socket writes immediately; blocked/oversize tails keep the normal next-turn path.
    if progress.entered_response
        && conn.phase == ConnPhase::SendingResponse
        && conn.file.is_none()
        && !conn.write_blocked
        && progress.bytes_written == 0
    {
        if conn.defer_write {
            conn.write_deferred = true;
        } else {
            let out_sent = conn.out_sent;
            let out_len = conn.out_len;
            if out_len > out_sent {
                let write_src = match conn.output.as_ref() {
                    Some(output) => &output[out_sent..out_len],
                    None => &[][..],
                };
                let budget = byte_budget.saturating_sub(progress.bytes_read);
                if !write_src.is_empty() && budget > 0 {
                    let mut batch = [IoBatch::new(
                        token,
                        &mut conn.stream,
                        &mut [],
                        false,
                        write_src,
                        true,
                    )];
                    dispatch(budget, &mut batch);
                    match batch[0].write {
                        WriteStatus::Progress(accepted)
                        | WriteStatus::WouldBlock(accepted)
                        | WriteStatus::BudgetExhausted(accepted)
                        | WriteStatus::Interrupted(accepted) => {
                            if matches!(batch[0].write, WriteStatus::WouldBlock(_)) {
                                conn.write_blocked = true;
                            }
                            conn.out_sent += accepted;
                            progress.bytes_written = accepted as u64;
                        }
                        WriteStatus::Closed(CloseReason::PeerClosed) => {
                            return PumpOutcome::Close(ConnClose::PeerClosed);
                        }
                        WriteStatus::Closed(CloseReason::Reset) => {
                            return PumpOutcome::Close(ConnClose::Reset);
                        }
                        WriteStatus::Closed(CloseReason::Error(_)) | WriteStatus::Error(_) => {
                            return PumpOutcome::Close(ConnClose::TransportError);
                        }
                        WriteStatus::Idle => {}
                    }
                }
            }
        }
    }

    // NOTE: a file transfer completes only when its staged offset reaches plan size, so
    // partial flushes keep their WritingResponse deadline via byte progress; the re-arm
    // keeps the poll wait at zero between chunks instead of paying a cadence per slice.
    // Shutdown mid-transfer drains the same way: begin_drain set closing, so completion
    // closes while the force deadline still force-closes through the normal cleanup.
    let file_done = matches!(&conn.file, Some(transfer) if transfer.offset >= transfer.plan.size);
    let response_done = conn.phase == ConnPhase::SendingResponse
        || (conn.phase == ConnPhase::SendingFile && file_done);
    if response_done && !conn.has_pending_output() {
        if conn.closing {
            return PumpOutcome::Close(conn.close_reason);
        }
        conn.file = None;
        reuse_keep_alive(conn);
        progress.response_flushed = true;
    } else if conn.phase == ConnPhase::SendingFile && !conn.has_pending_output() {
        conn.read_ready = true;
    }
    PumpOutcome::Keep(progress)
}

#[allow(clippy::too_many_arguments)]
fn pump_tls_full(
    conn: &mut ConnState,
    routes: &StaticRoutes,
    table: &RouteTable<'_, '_>,
    byte_budget: u64,
    worker_id: u32,
    host: &HostShared,
    draining: bool,
    file_sent: u64,
) -> PumpOutcome {
    let mut progress = PumpProgress::default();
    let read_ready_before = conn.read_ready;

    if conn.len < INPUT_CAP {
        ConnState::materialize_input(&mut conn.input);
    }

    if let Err(reason) = tls_turn_active(conn, byte_budget, &mut progress) {
        return PumpOutcome::Close(reason);
    }

    progress.bytes_written += file_sent;

    if conn.phase == ConnPhase::ReadingHead
        && (progress.bytes_read > 0 || (conn.len > 0 && read_ready_before))
    {
        progress.resumed_idle = true;
    }

    if let ConnPhase::DrainingBody {
        remaining,
        responded,
    } = conn.phase
    {
        let skip = (conn.len as u64).min(remaining) as usize;

        if let Some(input) = conn.input.as_mut() {
            input.copy_within(skip..conn.len, 0);
        }

        conn.len -= skip;
        let left = remaining - skip as u64;

        if left == 0 {
            conn.out_len = conn.pending_len;
            conn.pending_len = 0;
            conn.phase = if conn.file.is_some() {
                ConnPhase::SendingFile
            } else {
                ConnPhase::SendingResponse
            };
            conn.write_blocked = false;
            progress.entered_response = !responded;
        } else {
            conn.phase = ConnPhase::DrainingBody {
                remaining: left,
                responded,
            };
        }
    }

    if tls_alpn_is_h2(conn) {
        return PumpOutcome::Keep(progress);
    }

    if conn.phase == ConnPhase::ReadingHead {
        match parse_exchange(conn, routes, table, worker_id, host, draining) {
            ExchangeOutcome::NeedMore | ExchangeOutcome::Responded => {}
            ExchangeOutcome::Parked => progress.entered_park = true,
            ExchangeOutcome::Close(reason) => return PumpOutcome::Close(reason),
        }

        if conn.phase == ConnPhase::SendingResponse || conn.phase == ConnPhase::SendingFile {
            progress.entered_response = true;
        } else if matches!(conn.phase, ConnPhase::DrainingBody { .. }) {
            progress.entered_body = true;
        }
    }

    if !conn.tls.as_ref().is_some_and(|tls| tls.is_handshaking())
        && conn.out_sent < conn.out_len
        && tls_encrypt_output(conn).is_err()
    {
        return PumpOutcome::Close(ConnClose::TransportError);
    }

    let file_done = matches!(&conn.file, Some(transfer) if transfer.offset >= transfer.plan.size);
    let response_done = conn.phase == ConnPhase::SendingResponse
        || (conn.phase == ConnPhase::SendingFile && file_done);

    if response_done && !conn.has_pending_output() {
        if conn.closing {
            return PumpOutcome::Close(conn.close_reason);
        }

        conn.file = None;
        reuse_keep_alive(conn);
        progress.response_flushed = true;
    } else if conn.phase == ConnPhase::SendingFile && !conn.has_pending_output() {
        conn.read_ready = true;
    }

    PumpOutcome::Keep(progress)
}

pub(crate) fn reuse_keep_alive(conn: &mut ConnState) {
    // NOTE: mechanics only, not the reuse decision: admission is the shared
    // veylor_http1::response_permits_reuse verdict folded into conn.closing at the encode
    // sites (static/parked keep_open) with request-body drain enforced by the DrainingBody
    // phase; this function only shifts the pipelined tail, resets the parser, and re-arms
    // read_ready for the next exchange.
    let tail = conn.len - conn.head_len;
    if tail > 0
        && let Some(input) = conn.input.as_mut()
    {
        input.copy_within(conn.head_len..conn.len, 0);
    }
    conn.len = tail;
    conn.head_len = 0;
    conn.out_len = 0;
    conn.out_sent = 0;
    conn.phase = ConnPhase::ReadingHead;
    conn.close_drain_remaining = 0;
    conn.parser.reset();
    conn.read_ready = tail > 0;
}

#[cfg(test)]
mod tests {
    use super::*;
    use veylor_router::RouteSlot;
    use veylor_runtime::{ConnectionToken, TcpStream};

    fn test_token() -> ConnectionToken {
        ConnectionToken {
            worker_id: 0,
            slot: 0,
            request_seq: 0,
            stream_id: 0,
            worker_generation: 1,
            generation: 1,
        }
    }

    fn test_conn() -> ConnState {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind rig");
        let addr = listener.local_addr().expect("rig addr");
        let stream = TcpStream::connect(addr).expect("rig connect");
        ConnState::new(test_token(), stream)
    }

    #[test]
    fn new_conn_holds_no_slabs() {
        let conn = test_conn();
        assert!(conn.input.is_none());
        assert!(conn.headers.is_none());
        assert!(conn.output.is_none());
        assert!(conn.is_idle());
    }

    #[test]
    fn first_exchange_materializes_and_reuse_retains_slabs_while_quiet() {
        let mut conn = test_conn();
        let request = b"GET /nope HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n";
        let input = ConnState::materialize_input(&mut conn.input);
        input[..request.len()].copy_from_slice(request);
        conn.len = request.len();
        let routes = StaticRoutes::new();
        let mut slots = [RouteSlot::EMPTY; 8];
        let table = routes.build_table(&mut slots).expect("empty table");
        let host = HostShared::new(1);
        match parse_exchange(&mut conn, &routes, &table, 0, &host, false) {
            ExchangeOutcome::Responded => {}
            ExchangeOutcome::NeedMore => panic!("full head must parse"),
            ExchangeOutcome::Parked => panic!("no managed routes registered"),
            ExchangeOutcome::Close(_) => panic!("unknown path must encode, not close"),
        }
        assert!(conn.input.is_some());
        assert!(conn.headers.is_some());
        assert!(conn.output.is_some());
        assert!(conn.out_len > 0);
        conn.out_sent = conn.out_len;
        reuse_keep_alive(&mut conn);
        assert!(conn.input.is_some());
        assert!(conn.headers.is_some());
        assert!(conn.output.is_some());
        assert!(conn.is_idle());
        assert!(!conn.read_ready);
        let input_ptr = conn.input.as_ref().map(|slab| slab.as_ptr());
        let headers_ptr = conn.headers.as_ref().map(|slab| slab.as_ptr());
        let output_ptr = conn.output.as_ref().map(|slab| slab.as_ptr());
        let _ = pump_connection(&mut conn, &routes, &table, u64::MAX, 0, &host, false);
        assert_eq!(conn.input.as_ref().map(|slab| slab.as_ptr()), input_ptr);
        assert_eq!(conn.headers.as_ref().map(|slab| slab.as_ptr()), headers_ptr);
        assert_eq!(conn.output.as_ref().map(|slab| slab.as_ptr()), output_ptr);
        assert!(conn.is_idle());
    }

    #[test]
    fn hot_reuse_cycles_never_realloc_slabs() {
        let mut conn = test_conn();
        let request = b"GET /nope HTTP/1.1\r\nHost: x\r\nConnection: keep-alive\r\n\r\n";
        let routes = StaticRoutes::new();
        let mut slots = [RouteSlot::EMPTY; 8];
        let table = routes.build_table(&mut slots).expect("empty table");
        let host = HostShared::new(1);
        for _ in 0..8 {
            let input = ConnState::materialize_input(&mut conn.input);
            input[..request.len()].copy_from_slice(request);
            conn.len = request.len();
            conn.read_ready = true;
            match parse_exchange(&mut conn, &routes, &table, 0, &host, false) {
                ExchangeOutcome::Responded => {}
                ExchangeOutcome::NeedMore => panic!("full head must parse"),
                ExchangeOutcome::Parked => panic!("no managed routes registered"),
                ExchangeOutcome::Close(_) => panic!("unknown path must encode, not close"),
            }
            conn.out_sent = conn.out_len;
            reuse_keep_alive(&mut conn);
        }
        let input_ptr = conn.input.as_ref().map(|slab| slab.as_ptr());
        let headers_ptr = conn.headers.as_ref().map(|slab| slab.as_ptr());
        let output_ptr = conn.output.as_ref().map(|slab| slab.as_ptr());
        assert!(input_ptr.is_some());
        assert!(headers_ptr.is_some());
        assert!(output_ptr.is_some());
        for _ in 0..8 {
            let input = ConnState::materialize_input(&mut conn.input);
            input[..request.len()].copy_from_slice(request);
            conn.len = request.len();
            conn.read_ready = true;
            match parse_exchange(&mut conn, &routes, &table, 0, &host, false) {
                ExchangeOutcome::Responded => {}
                ExchangeOutcome::NeedMore => panic!("full head must parse"),
                ExchangeOutcome::Parked => panic!("no managed routes registered"),
                ExchangeOutcome::Close(_) => panic!("unknown path must encode, not close"),
            }
            conn.out_sent = conn.out_len;
            reuse_keep_alive(&mut conn);
            assert_eq!(conn.input.as_ref().map(|slab| slab.as_ptr()), input_ptr);
            assert_eq!(conn.headers.as_ref().map(|slab| slab.as_ptr()), headers_ptr);
            assert_eq!(conn.output.as_ref().map(|slab| slab.as_ptr()), output_ptr);
        }
    }
}
