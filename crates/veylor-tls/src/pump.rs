use std::io::{Read as _, Write as _};

use crate::{TlsConn, TlsError};

// NOTE: plaintext drain quantum per pump turn: bounded so one connection cannot
// monopolize the worker on a large TLS record run; the remainder waits for the next turn.
const DRAIN_CHUNK: usize = 4096;

pub fn feed_ciphertext(
    conn: &mut TlsConn,
    input: &[u8],
    plain_out: &mut Vec<u8>,
) -> Result<usize, TlsError> {
    drain_reader(conn, plain_out)?;

    let mut cursor: &[u8] = input;

    while !cursor.is_empty() {
        let read = conn.inner.read_tls(&mut cursor)?;

        if read == 0 {
            break;
        }

        conn.inner.process_new_packets()?;

        drain_reader(conn, plain_out)?;
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

fn drain_reader(conn: &mut TlsConn, plain_out: &mut Vec<u8>) -> Result<(), TlsError> {
    let mut chunk = [0u8; DRAIN_CHUNK];

    loop {
        match conn.inner.reader().read(&mut chunk) {
            Ok(0) => return Ok(()),

            Ok(n) => {
                plain_out.extend_from_slice(&chunk[..n]);

                conn.bytes_decrypted += n as u64;
            }

            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),

            Err(e) => return Err(TlsError::Io(e)),
        }
    }
}
