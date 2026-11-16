use std::io::{BufRead as _, Read, Write as _};

use crate::{TlsConn, TlsError};

// NOTE: one transport read outcome for the plaintext side. `Progress` may carry wire bytes
// without plaintext (a partial record); `WouldBlock` means the transport had nothing.
pub enum WireIo {
    Progress { wire: usize, plain: usize },
    WouldBlock,
    Eof,
}

pub fn decrypt_from<R: Read>(
    conn: &mut TlsConn,
    reader: &mut R,
    plain_out: &mut [u8],
) -> Result<WireIo, TlsError> {
    let mut filled = 0;

    exchange(conn, reader, |chunk| {
        let take = chunk.len().min(plain_out.len() - filled);

        plain_out[filled..filled + take].copy_from_slice(&chunk[..take]);

        filled += take;

        take
    })
}

pub fn decrypt_from_vec<R: Read>(
    conn: &mut TlsConn,
    reader: &mut R,
    plain_out: &mut Vec<u8>,
) -> Result<WireIo, TlsError> {
    exchange(conn, reader, |chunk| {
        plain_out.extend_from_slice(chunk);

        chunk.len()
    })
}

// NOTE: ciphertext already read off the socket: feed it in order and copy the decrypted
// chunks into the caller's fixed slice. Returns the ciphertext consumed and the plaintext
// written; whatever did not fit stays queued in the provider.
pub fn feed_ciphertext_into(
    conn: &mut TlsConn,
    input: &[u8],
    plain_out: &mut [u8],
) -> Result<(usize, usize), TlsError> {
    let mut filled = 0;

    drain_into(conn, plain_out, &mut filled)?;

    let mut cursor: &[u8] = input;

    while !cursor.is_empty() {
        let read = conn.inner.read_tls(&mut cursor)?;

        if read == 0 {
            break;
        }

        conn.inner.process_new_packets()?;

        drain_into(conn, plain_out, &mut filled)?;
    }

    Ok((input.len() - cursor.len(), filled))
}

// NOTE: true while the provider still holds decrypted bytes a smaller sink could not take;
// a caller with no new ciphertext still owes the reader one more pull.
pub fn plaintext_queued(conn: &mut TlsConn) -> bool {
    match conn.inner.reader().fill_buf() {
        Ok(chunk) => !chunk.is_empty(),
        Err(error) => error.kind() != std::io::ErrorKind::WouldBlock,
    }
}

pub fn feed_ciphertext(
    conn: &mut TlsConn,
    input: &[u8],
    plain_out: &mut Vec<u8>,
) -> Result<usize, TlsError> {
    let mut take = |chunk: &[u8]| {
        plain_out.extend_from_slice(chunk);

        chunk.len()
    };

    pump_plain(conn, &mut take)?;

    let mut cursor: &[u8] = input;

    while !cursor.is_empty() {
        let read = conn.inner.read_tls(&mut cursor)?;

        if read == 0 {
            break;
        }

        conn.inner.process_new_packets()?;

        pump_plain(conn, &mut take)?;
    }

    Ok(input.len() - cursor.len())
}

pub fn queue_close_notify(conn: &mut TlsConn, out: &mut Vec<u8>) -> Result<(), TlsError> {
    conn.inner.send_close_notify();

    while conn.inner.wants_write() {
        let wrote = conn.inner.write_tls(out)?;

        if wrote == 0 {
            break;
        }
    }

    Ok(())
}

pub fn drain_plaintext(
    conn: &mut TlsConn,
    plain: &[u8],
    out: &mut Vec<u8>,
) -> Result<(), TlsError> {
    conn.inner.writer().write_all(plain)?;

    let base = out.len();

    while conn.inner.wants_write() {
        let wrote = conn.inner.write_tls(out)?;

        if wrote == 0 {
            break;
        }
    }

    conn.bytes_encrypted += (out.len() - base) as u64;

    Ok(())
}

pub fn flush_ciphertext(conn: &mut TlsConn, out: &mut Vec<u8>) -> Result<(), TlsError> {
    let base = out.len();

    while conn.inner.wants_write() {
        let wrote = conn.inner.write_tls(out)?;

        if wrote == 0 {
            break;
        }
    }

    conn.bytes_encrypted += (out.len() - base) as u64;

    Ok(())
}

// NOTE: pull pending plaintext first so a caller that passed a small slice keeps the rest
// queued in the provider rather than reading past what it can deliver this turn.
fn exchange<R: Read>(
    conn: &mut TlsConn,
    reader: &mut R,
    mut put: impl FnMut(&[u8]) -> usize,
) -> Result<WireIo, TlsError> {
    let pending = pump_plain(conn, &mut put)?;

    if pending > 0 {
        return Ok(WireIo::Progress {
            wire: 0,
            plain: pending,
        });
    }

    let wire = match read_wire(conn, reader) {
        Ok(0) => return Ok(WireIo::Eof),
        Ok(wire) => wire,
        Err(error)
            if error.kind() == std::io::ErrorKind::WouldBlock
                || error.kind() == std::io::ErrorKind::Interrupted =>
        {
            return Ok(WireIo::WouldBlock);
        }
        Err(error) => return Err(TlsError::Io(error)),
    };

    conn.inner.process_new_packets()?;

    let plain = pump_plain(conn, &mut put)?;

    Ok(WireIo::Progress { wire, plain })
}

fn read_wire<R: Read>(conn: &mut TlsConn, reader: &mut R) -> std::io::Result<usize> {
    let mut attempts = 0;

    loop {
        match conn.inner.read_tls(reader) {
            Ok(read) => return Ok(read),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted && attempts < 2 => {
                attempts += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

// NOTE: the provider hands out decrypted bytes as a borrowed chunk; copying that chunk
// straight to the destination keeps one plaintext copy per record instead of three.
fn drain_into(
    conn: &mut TlsConn,
    plain_out: &mut [u8],
    filled: &mut usize,
) -> Result<(), TlsError> {
    pump_plain(conn, &mut |chunk| {
        let take = chunk.len().min(plain_out.len() - *filled);

        plain_out[*filled..*filled + take].copy_from_slice(&chunk[..take]);

        *filled += take;

        take
    })?;

    Ok(())
}

fn pump_plain(conn: &mut TlsConn, put: &mut impl FnMut(&[u8]) -> usize) -> Result<usize, TlsError> {
    let mut taken = 0;

    loop {
        let used = {
            let mut reader = conn.inner.reader();

            let chunk = match reader.fill_buf() {
                Ok(chunk) => chunk,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(error) => return Err(TlsError::Io(error)),
            };

            if chunk.is_empty() {
                conn.peer_closed = true;

                break;
            }

            put(chunk)
        };

        if used == 0 {
            break;
        }

        conn.inner.reader().consume(used);

        conn.bytes_decrypted += used as u64;
        taken += used;
    }

    Ok(taken)
}
