use veylor_runtime::{CloseReason, ConnectionToken, IoBatch, ReadStatus, WriteStatus, dispatch};

use veylor_websocket::{Event as WsEvent, OpCode};

use crate::host::HostShared;

use super::tls::{tls_encrypt_output, tls_flush_pending, tls_read_available};
use super::{ConnClose, ConnPhase, ConnState, INPUT_CAP, PumpOutcome, PumpProgress};

fn flush_inbound(conn: &mut ConnState, host: &HostShared, token: ConnectionToken) -> bool {
    if conn.len == 0 {
        return true;
    }
    let accepted = match conn.input.as_ref() {
        Some(input) => host.push_tunnel_in(token, &input[..conn.len]),
        None => 0,
    };
    if accepted > 0 {
        if let Some(input) = conn.input.as_mut() {
            input.copy_within(accepted..conn.len, 0);
        }
        conn.len -= accepted;
    }
    conn.len == 0
}

fn write_pending(conn: &mut ConnState, budget: u64) -> Result<(u64, bool), ConnClose> {
    let out_sent = conn.out_sent;
    let out_len = conn.out_len;
    let Some(output) = conn.output.as_ref() else {
        conn.out_sent = conn.out_len;
        return Ok((0, true));
    };
    let token = conn.tunnel_token;
    let mut batch = [IoBatch::new(
        token,
        &mut conn.stream,
        &mut [],
        false,
        &output[out_sent..out_len],
        !conn.write_blocked,
    )];
    dispatch(budget, &mut batch);
    let accepted = match batch[0].write {
        WriteStatus::Progress(accepted)
        | WriteStatus::WouldBlock(accepted)
        | WriteStatus::BudgetExhausted(accepted)
        | WriteStatus::Interrupted(accepted) => {
            if matches!(batch[0].write, WriteStatus::WouldBlock(_)) {
                conn.write_blocked = true;
            }
            accepted
        }
        WriteStatus::Closed(CloseReason::PeerClosed) => {
            return Err(ConnClose::PeerClosed);
        }
        WriteStatus::Closed(CloseReason::Reset) => {
            return Err(ConnClose::Reset);
        }
        WriteStatus::Closed(CloseReason::Error(_)) | WriteStatus::Error(_) => {
            return Err(ConnClose::TransportError);
        }
        WriteStatus::Idle => 0,
    };
    conn.out_sent += accepted;
    if conn.out_sent < conn.out_len {
        return Ok((accepted as u64, false));
    }
    conn.out_len = 0;
    conn.out_sent = 0;
    Ok((accepted as u64, true))
}

fn mark_eof(
    conn: &mut ConnState,
    host: &HostShared,
    token: ConnectionToken,
    progress: &mut PumpProgress,
) {
    if conn.tunnel_eof {
        return;
    }
    conn.tunnel_eof = true;
    conn.read_ready = false;
    if conn.tunnel_notified {
        return;
    }
    host.mark_tunnel_eof(token);
    conn.tunnel_notified = host.stage_tunnel_closed(token);
    if conn.phase == ConnPhase::Tunnel {
        progress.tunnel_draining = true;
    }
}

fn pull_output(conn: &mut ConnState, host: &HostShared, token: ConnectionToken) -> usize {
    ConnState::materialize_output(&mut conn.output);
    let copied = match conn.output.as_mut() {
        Some(output) => host.take_tunnel_out(token, &mut output[..]),
        None => 0,
    };
    if copied > 0 {
        conn.out_len = copied;
        conn.out_sent = 0;
        host.stage_writable(token);
    }
    copied
}

fn settle_eof(
    conn: &mut ConnState,
    host: &HostShared,
    token: ConnectionToken,
    progress: PumpProgress,
) -> Option<PumpOutcome> {
    if !conn.tunnel_eof {
        return None;
    }
    if conn.out_len == 0 && pull_output(conn, host, token) > 0 {
        return Some(PumpOutcome::Keep(progress));
    }
    if host.tunnel_input_done(token) {
        return Some(PumpOutcome::Close(ConnClose::PeerClosed));
    }
    Some(PumpOutcome::Keep(progress))
}

fn pump_echo(conn: &mut ConnState, byte_budget: u64) -> PumpOutcome {
    let mut progress = PumpProgress {
        bytes_read: std::mem::take(&mut conn.batched_read) as u64,
        ..PumpProgress::default()
    };
    if conn.tunnel_close && conn.out_sent >= conn.out_len {
        return PumpOutcome::Close(ConnClose::Completed);
    }
    if conn.out_sent < conn.out_len {
        if conn.defer_write {
            conn.write_deferred = true;
            return PumpOutcome::Keep(progress);
        }
        match write_pending(conn, byte_budget) {
            Ok((wrote, flushed)) => {
                progress.bytes_written = wrote;
                if !flushed {
                    return PumpOutcome::Keep(progress);
                }
            }
            Err(reason) => return PumpOutcome::Close(reason),
        }
    }
    if progress.bytes_read == 0 && conn.read_ready {
        if conn.len >= INPUT_CAP {
            conn.read_ready = false;
        } else {
            ConnState::materialize_input(&mut conn.input);
            let len = conn.len;
            let read_scratch: &mut [u8] = match conn.input.as_mut() {
                Some(input) => &mut input[len..],
                None => &mut [],
            };
            let mut batch = [IoBatch::new(
                conn.tunnel_token,
                &mut conn.stream,
                read_scratch,
                true,
                &[],
                false,
            )];
            dispatch(byte_budget, &mut batch);
            match batch[0].read {
                ReadStatus::Progress(bytes) => {
                    conn.len += bytes;
                    conn.read_ready = true;
                    progress.bytes_read += bytes as u64;
                }
                ReadStatus::Eof => return PumpOutcome::Close(ConnClose::PeerClosed),
                ReadStatus::Closed(CloseReason::PeerClosed) => {
                    return PumpOutcome::Close(ConnClose::PeerClosed);
                }
                ReadStatus::Closed(CloseReason::Reset) => {
                    return PumpOutcome::Close(ConnClose::Reset);
                }
                ReadStatus::Closed(CloseReason::Error(_)) | ReadStatus::Error(_) => {
                    return PumpOutcome::Close(ConnClose::TransportError);
                }
                ReadStatus::BudgetExhausted => conn.read_ready = true,
                ReadStatus::Idle
                | ReadStatus::WouldBlock
                | ReadStatus::PausedForWrite
                | ReadStatus::Interrupted => conn.read_ready = false,
            }
        }
    }
    if let Err(reason) = echo_frames(conn, &mut progress, byte_budget) {
        return PumpOutcome::Close(reason);
    }
    PumpOutcome::Keep(progress)
}

fn echo_frames(
    conn: &mut ConnState,
    progress: &mut PumpProgress,
    budget: u64,
) -> Result<(), ConnClose> {
    let capacity = ConnState::materialize_output(&mut conn.output).len();
    let mut staged = 0usize;
    let mut close_after = false;

    loop {
        if staged > 0 && staged + conn.len + FRAME_OVERHEAD > capacity {
            if !flush_staged(conn, staged, budget, progress)? {
                return Ok(());
            }
            staged = 0;
        }
        if conn.len == 0 {
            break;
        }

        let len = conn.len;
        let (consumed, frame_len) = {
            let Some(ws) = conn.tunnel_echo.as_mut() else {
                return Err(ConnClose::TransportError);
            };
            let Some(input) = conn.input.as_mut() else {
                break;
            };
            let feed = ws
                .feed(&mut input[..len])
                .map_err(|_| ConnClose::ProtocolError)?;
            if feed.need_more {
                break;
            }
            let consumed = feed.consumed;
            let Some(event) = feed.event else {
                break;
            };
            let (fin, opcode, payload) = match event {
                WsEvent::Text { payload, fin } => (fin, OpCode::Text, payload),
                WsEvent::Binary { payload, fin } => (fin, OpCode::Binary, payload),
                WsEvent::Continuation { payload, fin } => (fin, OpCode::Continuation, payload),
                WsEvent::Ping(payload) => (true, OpCode::Pong, payload),
                WsEvent::Pong(_) => (true, OpCode::Pong, &[][..]),
                WsEvent::Close(_) => {
                    close_after = true;
                    conn.tunnel_close = true;
                    (true, OpCode::Close, &[][..])
                }
            };
            let output = ConnState::materialize_output(&mut conn.output);
            let frame_len = ws
                .encode(fin, opcode, None, payload, &mut output[staged..])
                .map_err(|_| ConnClose::ProtocolError)?;
            (consumed, frame_len)
        };

        if consumed > 0 {
            if let Some(input) = conn.input.as_mut() {
                input.copy_within(consumed..len, 0);
            }
            conn.len -= consumed;
        }
        staged += frame_len;
        if close_after {
            break;
        }
    }

    if staged > 0 && !flush_staged(conn, staged, budget, progress)? {
        return Ok(());
    }
    if close_after {
        return Err(ConnClose::Completed);
    }
    Ok(())
}

fn flush_staged(
    conn: &mut ConnState,
    staged: usize,
    budget: u64,
    progress: &mut PumpProgress,
) -> Result<bool, ConnClose> {
    stage_len(conn, staged);
    if conn.defer_write {
        conn.write_deferred = true;
        return Ok(false);
    }
    match write_pending(conn, budget) {
        Ok((wrote, flushed)) => {
            progress.bytes_written += wrote;
            Ok(flushed)
        }
        Err(reason) => Err(reason),
    }
}

const FRAME_OVERHEAD: usize = 14;

fn stage_len(conn: &mut ConnState, len: usize) {
    conn.out_len = len;
    conn.out_sent = 0;
}

pub(crate) fn pump_tunnel(
    conn: &mut ConnState,
    host: &HostShared,
    byte_budget: u64,
) -> PumpOutcome {
    if conn.tunnel_echo.is_some() {
        return pump_echo(conn, byte_budget);
    }
    let token = conn.tunnel_token;
    let mut progress = PumpProgress::default();

    if conn.out_sent < conn.out_len || conn.tls_unflushed() > 0 {
        if conn.has_tls() {
            if conn.out_sent < conn.out_len {
                if let Err(reason) = tls_encrypt_output(conn) {
                    return PumpOutcome::Close(reason);
                }
            }
            match tls_flush_pending(conn, byte_budget) {
                Ok(wrote) => {
                    progress.bytes_written = wrote;
                    if conn.tls_unflushed() > 0 {
                        return PumpOutcome::Keep(progress);
                    }
                    conn.out_len = 0;
                    conn.out_sent = 0;
                }
                Err(reason) => return PumpOutcome::Close(reason),
            }
        } else {
            match write_pending(conn, byte_budget) {
                Ok((wrote, flushed)) => {
                    progress.bytes_written = wrote;
                    if !flushed {
                        return PumpOutcome::Keep(progress);
                    }
                }
                Err(reason) => return PumpOutcome::Close(reason),
            }
        }
    }

    if conn.out_len == 0 && pull_output(conn, host, token) > 0 {
        return PumpOutcome::Keep(progress);
    }

    if host.tunnel_close_requested(token) {
        return PumpOutcome::Close(ConnClose::Completed);
    }

    if !flush_inbound(conn, host, token) {
        if host.tunnel_close_requested(token) {
            return PumpOutcome::Close(ConnClose::Completed);
        }
        conn.read_ready = false;
        return PumpOutcome::Keep(progress);
    }

    if let Some(outcome) = settle_eof(conn, host, token, progress) {
        return outcome;
    }

    conn.read_ready = true;
    if conn.has_tls() {
        match tls_read_available(conn, byte_budget) {
            Ok((wire, _)) => progress.bytes_read = wire,
            Err(ConnClose::PeerClosed) => mark_eof(conn, host, token, &mut progress),
            Err(reason) => return PumpOutcome::Close(reason),
        }
    } else {
        if conn.len >= INPUT_CAP {
            conn.read_ready = false;
            return PumpOutcome::Keep(progress);
        }
        ConnState::materialize_input(&mut conn.input);
        let len = conn.len;
        let read_scratch: &mut [u8] = match conn.input.as_mut() {
            Some(input) => &mut input[len..],
            None => &mut [],
        };
        let mut batch = [IoBatch::new(
            token,
            &mut conn.stream,
            read_scratch,
            true,
            &[],
            false,
        )];
        dispatch(byte_budget, &mut batch);
        match batch[0].read {
            ReadStatus::Progress(bytes) => {
                conn.len += bytes;
                conn.read_ready = true;
                progress.bytes_read = bytes as u64;
            }
            ReadStatus::Eof => mark_eof(conn, host, token, &mut progress),
            ReadStatus::Closed(CloseReason::PeerClosed) => {
                return PumpOutcome::Close(ConnClose::PeerClosed);
            }
            ReadStatus::Closed(CloseReason::Reset) => {
                return PumpOutcome::Close(ConnClose::Reset);
            }
            ReadStatus::Closed(CloseReason::Error(_)) | ReadStatus::Error(_) => {
                return PumpOutcome::Close(ConnClose::TransportError);
            }
            ReadStatus::BudgetExhausted => conn.read_ready = true,
            ReadStatus::Idle
            | ReadStatus::WouldBlock
            | ReadStatus::PausedForWrite
            | ReadStatus::Interrupted => {
                conn.read_ready = false;
            }
        }
    }

    if !flush_inbound(conn, host, token) {
        conn.read_ready = false;
        return PumpOutcome::Keep(progress);
    }

    if let Some(outcome) = settle_eof(conn, host, token, progress) {
        return outcome;
    }

    PumpOutcome::Keep(progress)
}
