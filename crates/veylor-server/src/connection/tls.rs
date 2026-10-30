use veylor_runtime::TcpStream;
use veylor_tls::{TlsConn, alpn, drain_plaintext, feed_ciphertext};

use super::{ConnClose, ConnPhase, ConnState, INPUT_CAP, OUTPUT_CAP, PumpProgress};

// NOTE: TLS scratch floors, not bounds: encrypted records may exceed OUTPUT_CAP, so the
// pending buffer pre-reserves 16 KiB while the plaintext side matches INPUT_CAP. Floors,
// not caps — growth beyond them is normal and unbounded by these values.
pub const TLS_PENDING_MIN_BYTES: usize = 16384;

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

    ConnState::materialize_input(&mut conn.input);

    let spare = INPUT_CAP - conn.len;
    let want = (spare.min(4096) as u64).min(budget) as usize;

    if want == 0 {
        return Ok((0, 0));
    }

    let mut raw = [0u8; 4096];
    let mut wire = 0usize;
    let mut attempts = 0;

    let got = loop {
        if want <= wire {
            break wire;
        }

        let limit = (want - wire).min(4096 - wire);

        let outcome = tls_read_once(&mut conn.stream, &mut raw[wire..wire + limit]);

        match outcome {
            TlsReadOutcome::Progress(n) => {
                wire += n;

                break wire;
            }
            TlsReadOutcome::Blocked => {
                if wire > 0 {
                    break wire;
                }

                conn.read_ready = false;

                return Ok((0, 0));
            }
            TlsReadOutcome::Interrupted => {
                attempts += 1;

                if attempts >= 2 {
                    break wire;
                }
            }
            TlsReadOutcome::Eof => {
                return Err(ConnClose::PeerClosed);
            }
            TlsReadOutcome::Closed(reason) => {
                return Err(reason);
            }
        }
    };

    if got == 0 {
        return Ok((0, 0));
    }

    let Some(tls) = conn.tls.as_mut() else {
        return Err(ConnClose::TransportError);
    };

    conn.tls_plain.clear();

    let consumed = feed_ciphertext(tls, &raw[..got], &mut conn.tls_plain)
        .map_err(|_| ConnClose::TransportError)?;

    if consumed != got {
        return Err(ConnClose::TransportError);
    }

    let decrypted = conn.tls_plain.len();
    let end = conn.len + decrypted;

    if end > INPUT_CAP {
        return Err(ConnClose::TransportError);
    }

    if decrypted > 0 {
        if let Some(input) = conn.input.as_mut() {
            input[conn.len..end].copy_from_slice(&conn.tls_plain[..]);
        }

        conn.tls_plain.clear();
        conn.len = end;
    }

    conn.read_ready = true;

    Ok((got as u64, decrypted as u64))
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
    scratch: &mut Vec<u8>,
) -> Result<(u64, usize, bool), ConnClose> {
    if budget == 0 || spare.is_empty() {
        return Ok((0, 0, false));
    }
    let limit = spare.len().min(4096).min(budget as usize);
    if limit == 0 {
        return Ok((0, 0, false));
    }
    let mut raw = [0u8; 4096];
    let mut wire = 0usize;
    let mut attempts = 0;
    let blocked = loop {
        match tls_read_once(stream, &mut raw[wire..limit]) {
            TlsReadOutcome::Progress(n) => {
                wire += n;
                break false;
            }
            TlsReadOutcome::Blocked => {
                if wire > 0 {
                    break false;
                }
                break true;
            }
            TlsReadOutcome::Interrupted => {
                attempts += 1;
                if attempts >= 2 {
                    break false;
                }
            }
            TlsReadOutcome::Eof => return Err(ConnClose::PeerClosed),
            TlsReadOutcome::Closed(reason) => return Err(reason),
        }
    };
    if wire == 0 {
        return Ok((0, 0, blocked));
    }
    scratch.clear();
    let consumed =
        feed_ciphertext(tls, &raw[..wire], scratch).map_err(|_| ConnClose::TransportError)?;
    if consumed != wire {
        return Err(ConnClose::TransportError);
    }
    if scratch.len() > spare.len() {
        return Err(ConnClose::TransportError);
    }
    spare[..scratch.len()].copy_from_slice(scratch);
    let plain = scratch.len();
    scratch.clear();
    Ok((wire as u64, plain, blocked))
}

pub(crate) enum TlsWriteOutcome {
    Accepted(usize),
    Blocked,
    Interrupted,
    Closed(ConnClose),
}

pub(crate) enum TlsReadOutcome {
    Progress(usize),
    Blocked,
    Interrupted,
    Eof,
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

pub(crate) fn tls_read_once(stream: &mut TcpStream, scratch: &mut [u8]) -> TlsReadOutcome {
    use std::io::Read as _;

    if scratch.is_empty() {
        return TlsReadOutcome::Progress(0);
    }

    match stream.read(scratch) {
        Ok(0) => TlsReadOutcome::Eof,
        Ok(n) => TlsReadOutcome::Progress(n),
        Err(error) => {
            let kind = error.kind();

            if kind == std::io::ErrorKind::WouldBlock {
                return TlsReadOutcome::Blocked;
            }

            if kind == std::io::ErrorKind::Interrupted {
                return TlsReadOutcome::Interrupted;
            }

            if kind == std::io::ErrorKind::ConnectionReset {
                return TlsReadOutcome::Closed(ConnClose::Reset);
            }

            TlsReadOutcome::Closed(ConnClose::TransportError)
        }
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
