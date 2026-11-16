use std::io::{self, Read, Write};

use crate::{
    TlsConn, TlsError, WireIo, decrypt_from_vec, drain_plaintext, flush_ciphertext,
    queue_close_notify,
};

const WRITE_CHUNK: usize = 16 * 1024;

// NOTE: blocking adapter for tests, tools and small clients; the servers use the pump functions
// directly. Handshake progress happens lazily inside read/write, like a std stream wrapper.
pub struct TlsStream<S> {
    io: S,
    conn: TlsConn,
    plain: Vec<u8>,
    plain_pos: usize,
    wire: Vec<u8>,
}

impl<S: Read + Write> TlsStream<S> {
    pub fn new(io: S, conn: TlsConn) -> Self {
        Self {
            io,
            conn,
            plain: Vec::new(),
            plain_pos: 0,
            wire: Vec::new(),
        }
    }

    pub fn handshake(&mut self) -> io::Result<()> {
        loop {
            self.flush_wire()?;

            if !self.conn.is_handshaking() {
                return Ok(());
            }

            self.pull()?;
        }
    }

    pub fn close_notify(&mut self) -> io::Result<()> {
        queue_close_notify(&mut self.conn, &mut self.wire).map_err(into_io)?;

        self.flush_wire()
    }

    pub fn alpn(&self) -> Option<&[u8]> {
        crate::alpn(&self.conn)
    }

    pub fn conn(&self) -> &TlsConn {
        &self.conn
    }

    pub fn get_ref(&self) -> &S {
        &self.io
    }

    pub fn get_mut(&mut self) -> &mut S {
        &mut self.io
    }

    pub fn into_inner(self) -> S {
        self.io
    }

    fn flush_wire(&mut self) -> io::Result<()> {
        flush_ciphertext(&mut self.conn, &mut self.wire).map_err(into_io)?;

        if self.wire.is_empty() {
            return Ok(());
        }

        let result = self.io.write_all(&self.wire);

        self.wire.clear();

        result
    }

    fn pull(&mut self) -> io::Result<()> {
        if self.plain_pos == self.plain.len() {
            self.plain.clear();
            self.plain_pos = 0;
        }

        match decrypt_from_vec(&mut self.conn, &mut self.io, &mut self.plain) {
            Ok(WireIo::Progress { .. }) => Ok(()),
            Ok(WireIo::WouldBlock) => Err(io::ErrorKind::WouldBlock.into()),
            Ok(WireIo::Eof) => Err(io::ErrorKind::UnexpectedEof.into()),
            Err(error) => {
                let _ = self.flush_wire();

                Err(into_io(error))
            }
        }
    }
}

impl<S: Read + Write> Read for TlsStream<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        loop {
            if self.plain_pos < self.plain.len() {
                let available = &self.plain[self.plain_pos..];
                let count = available.len().min(buf.len());

                buf[..count].copy_from_slice(&available[..count]);

                self.plain_pos += count;

                return Ok(count);
            }

            if self.conn.peer_closed() {
                return Ok(0);
            }

            self.flush_wire()?;
            self.pull()?;
        }
    }
}

impl<S: Read + Write> Write for TlsStream<S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        if self.conn.is_handshaking() {
            self.handshake()?;
        }

        for chunk in buf.chunks(WRITE_CHUNK) {
            drain_plaintext(&mut self.conn, chunk, &mut self.wire).map_err(into_io)?;

            self.flush_wire()?;
        }

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_wire()?;

        self.io.flush()
    }
}

fn into_io(error: TlsError) -> io::Error {
    match error {
        TlsError::Io(error) => error,
        other => io::Error::other(other),
    }
}
