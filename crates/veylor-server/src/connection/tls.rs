use veylor_runtime::TcpStream;
use veylor_tls::{TlsConn, TlsError, WireIo, alpn, decrypt_from, drain_plaintext};

use super::{ConnClose, ConnPhase, ConnState, INPUT_CAP, OUTPUT_CAP, PumpProgress};

// NOTE: TLS scratch floors, not bounds: encrypted records may exceed OUTPUT_CAP, so the
// pending buffer pre-reserves 16 KiB while the plaintext side matches INPUT_CAP. Floors,
// not caps — growth beyond them is normal and unbounded by these values.
pub const TLS_PENDING_MIN_BYTES: usize = 16384;

// NOTE: plaintext delivered to one turn's read spare, matching the old ciphertext read
// quantum; a larger record leaves its remainder queued in the provider for the next turn.
const TLS_PLAIN_CHUNK: usize = 4096;

pub(crate) fn tls_flush_pending(conn: &mut ConnState, budget: u64) -> Result<u64, ConnClose> {
    let unflushed = conn.tls_pending.len().saturating_sub(conn.tls_pending_sent);

    if unflushed == 0 {
        return Ok(0);
    }

    if conn.write_blocked {
        return Ok(0);
    }

    if budget == 0 {
        return Ok(0);
    }

    let mut total = 0usize;
    let mut attempts = 0;

    while total < unflushed {
        let remaining_budget = budget.saturating_sub(total as u64);

        if remaining_budget == 0 {
            break;
        }

        let start = conn.tls_pending_sent + total;
        let end = conn.tls_pending.len();
        let available = end - start;
        let limit = (remaining_budget as usize).min(available);

        if limit == 0 {
            break;
        }

        let chunk: &[u8] = &conn.tls_pending[start..start + limit];

        let wrote = tls_write_once(&mut conn.stream, chunk);

        match wrote {
            TlsWriteOutcome::Accepted(accepted) => {
                total += accepted;

                if accepted == 0 {
                    break;
                }

                if accepted < limit {
                    break;
                }
            }
            TlsWriteOutcome::Blocked => {
                conn.write_blocked = true;

                break;
            }
            TlsWriteOutcome::Interrupted => {
                attempts += 1;

                if attempts >= 2 {
                    break;
                }
            }
            TlsWriteOutcome::Closed(reason) => {
                conn.tls_pending_sent += total;
                return Err(reason);
            }
        }
    }

    conn.tls_pending_sent += total;

    if conn.tls_pending_sent >= conn.tls_pending.len() {
        conn.tls_pending.clear();
        conn.tls_pending_sent = 0;
    }

    Ok(total as u64)
}

pub(crate) fn tls_encrypt_output(conn: &mut ConnState) -> Result<(), ConnClose> {
    if conn.tls_pending.len() > conn.tls_pending_sent {
        return Ok(());
    }

    let plain_start = conn.out_sent;
    let plain_end = conn.out_len;

    if plain_start >= plain_end {
        return Ok(());
    }

    let Some(output) = conn.output.as_ref() else {
        return Err(ConnClose::TransportError);
    };

    let width = plain_end - plain_start;

    if width > OUTPUT_CAP || plain_end > output.len() {
        return Err(ConnClose::TransportError);
    }

    let Some(tls) = conn.tls.as_mut() else {
        return Err(ConnClose::TransportError);
    };

    conn.tls_pending.clear();
    conn.tls_pending_sent = 0;

    drain_plaintext(tls, &output[plain_start..plain_end], &mut conn.tls_pending)
        .map_err(|_| ConnClose::TransportError)?;

    conn.out_sent = conn.out_len;
    conn.write_blocked = false;

    Ok(())
}

pub(crate) fn tls_generate_flight(conn: &mut ConnState) -> Result<(), ConnClose> {
    let unflushed = conn.tls_pending.len().saturating_sub(conn.tls_pending_sent);

    if unflushed > 0 {
        return Ok(());
    }

    let handshaking = conn.tls.as_ref().is_some_and(|tls| tls.is_handshaking());

    if !handshaking {
        return Ok(());
    }

    if conn.tls.is_none() {
        return Err(ConnClose::TransportError);
    }

    conn.tls_pending.clear();
    conn.tls_pending_sent = 0;

    let Some(tls) = conn.tls.as_mut() else {
        return Err(ConnClose::TransportError);
    };

    drain_plaintext(tls, &[], &mut conn.tls_pending).map_err(|_| ConnClose::TransportError)?;

    if conn.tls_pending.is_empty() {
        return Ok(());
    }

    conn.write_blocked = false;

    Ok(())
}

pub(crate) fn tls_read_available(
    conn: &mut ConnState,
    budget: u64,
) -> Result<(u64, u64), ConnClose> {
    if !conn.read_ready {
        return Ok((0, 0));
    }

    if conn.len >= INPUT_CAP {
        return Ok((0, 0));
    }

    if budget == 0 {
        return Ok((0, 0));
    }

    let room = (INPUT_CAP - conn.len)
        .min(TLS_PLAIN_CHUNK)
        .min(budget as usize);

    if room == 0 {
        return Ok((0, 0));
    }

    let input = ConnState::materialize_input(&mut conn.input);

    let Some(tls) = conn.tls.as_mut() else {
        return Err(ConnClose::TransportError);
    };

    let start = conn.len;
    let spare = &mut input[start..start + room];

    match decrypt_from(tls, &mut conn.stream, spare) {
        Ok(WireIo::Progress { wire, plain }) => {
            conn.len += plain;
            conn.read_ready = true;

            Ok((wire as u64, plain as u64))
        }
        Ok(WireIo::WouldBlock) => {
            conn.read_ready = false;

            Ok((0, 0))
        }
        Ok(WireIo::Eof) => Err(ConnClose::PeerClosed),
        Err(TlsError::Io(error)) => Err(close_for(&error)),
        Err(_) => Err(ConnClose::TransportError),
    }
}

pub(crate) fn tls_alpn_is_h2(conn: &ConnState) -> bool {
    conn.tls
        .as_ref()
        .is_some_and(|tls| !tls.is_handshaking() && alpn(tls) == Some(b"h2".as_slice()))
}

pub(crate) fn tls_encrypt_bytes(
    tls: &mut TlsConn,
    plain: &[u8],
    out: &mut Vec<u8>,
) -> Result<(), ConnClose> {
    drain_plaintext(tls, plain, out).map_err(|_| ConnClose::TransportError)
}

pub(crate) fn tls_read_decrypted(
    stream: &mut TcpStream,
    tls: &mut TlsConn,
    spare: &mut [u8],
    budget: u64,
    _scratch: &mut Vec<u8>,
) -> Result<(u64, usize, bool), ConnClose> {
    // NOTE: h2/turn.rs still passes its tls_plain slot for this call; plaintext lands
    // straight in the caller's spare, so that slot stays empty until its owner drops it.
    if budget == 0 || spare.is_empty() {
        return Ok((0, 0, false));
    }

    let room = spare.len().min(budget as usize);

    match decrypt_from(tls, stream, &mut spare[..room]) {
        Ok(WireIo::Progress { wire, plain }) => Ok((wire as u64, plain, false)),
        Ok(WireIo::WouldBlock) => Ok((0, 0, true)),
        Ok(WireIo::Eof) => Err(ConnClose::PeerClosed),
        Err(TlsError::Io(error)) => Err(close_for(&error)),
        Err(_) => Err(ConnClose::TransportError),
    }
}

pub(crate) enum TlsWriteOutcome {
    Accepted(usize),
    Blocked,
    Interrupted,
    Closed(ConnClose),
}

pub(crate) fn tls_write_once(stream: &mut TcpStream, chunk: &[u8]) -> TlsWriteOutcome {
    use std::io::Write as _;

    if chunk.is_empty() {
        return TlsWriteOutcome::Accepted(0);
    }

    match stream.write(chunk) {
        Ok(accepted) => TlsWriteOutcome::Accepted(accepted),
        Err(error) => {
            let kind = error.kind();

            if kind == std::io::ErrorKind::WouldBlock {
                return TlsWriteOutcome::Blocked;
            }

            if kind == std::io::ErrorKind::Interrupted {
                return TlsWriteOutcome::Interrupted;
            }

            if kind == std::io::ErrorKind::ConnectionReset {
                return TlsWriteOutcome::Closed(ConnClose::Reset);
            }

            if kind == std::io::ErrorKind::BrokenPipe {
                return TlsWriteOutcome::Closed(ConnClose::PeerClosed);
            }

            TlsWriteOutcome::Closed(ConnClose::TransportError)
        }
    }
}

fn close_for(error: &std::io::Error) -> ConnClose {
    match error.kind() {
        std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted => {
            ConnClose::Reset
        }
        std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::UnexpectedEof => ConnClose::PeerClosed,
        _ => ConnClose::TransportError,
    }
}

pub(crate) fn tls_turn_active(
    conn: &mut ConnState,
    byte_budget: u64,
    progress: &mut PumpProgress,
) -> Result<(), ConnClose> {
    let mut remaining = byte_budget;

    let flushed = tls_flush_pending(conn, remaining)?;

    progress.bytes_written += flushed;
    remaining = remaining.saturating_sub(flushed);

    tls_generate_flight(conn)?;

    let staged = tls_flush_pending(conn, remaining)?;

    progress.bytes_written += staged;
    remaining = remaining.saturating_sub(staged);

    let draining_body = matches!(conn.phase, ConnPhase::DrainingBody { .. });
    let want_read = (conn.phase == ConnPhase::ReadingHead || draining_body) && conn.len < INPUT_CAP;

    if want_read {
        let (wire, _decrypted) = tls_read_available(conn, remaining)?;

        progress.bytes_read += wire;
        remaining = remaining.saturating_sub(wire);
    }

    tls_generate_flight(conn)?;

    let staged = tls_flush_pending(conn, remaining)?;

    progress.bytes_written += staged;
    remaining = remaining.saturating_sub(staged);

    let handshaking = conn.tls.as_ref().is_some_and(|tls| tls.is_handshaking());

    if !handshaking {
        tls_encrypt_output(conn)?;

        let staged = tls_flush_pending(conn, remaining)?;

        progress.bytes_written += staged;
    }

    Ok(())
}
