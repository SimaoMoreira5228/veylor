use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use veylor_tls::{
    TlsConn, TlsConnector, TlsError, alpn, drain_plaintext, feed_ciphertext, flush_ciphertext,
};

use crate::budget::Budget;
use crate::error::{Error, TimeoutKind};
use crate::url::Origin;

const CIPHER_CHUNK: usize = 16 * 1024 + 512;
const WRITE_CHUNK: usize = 16 * 1024;

pub(crate) struct Wire {
    tcp: TcpStream,
    tls: Option<Box<TlsConn>>,
    cipher_in: Vec<u8>,
    cipher_out: Vec<u8>,
    alpn: Option<Vec<u8>>,
}

impl Wire {
    pub(crate) fn connect(
        origin: &Origin,
        connector: Option<&TlsConnector>,
        connect_timeout: Option<Duration>,
        budget: &Budget,
    ) -> Result<Self, Error> {
        let tcp = open_tcp(origin, connect_timeout, budget)?;
        let mut wire = Self {
            tcp,
            tls: None,
            cipher_in: Vec::new(),
            cipher_out: Vec::new(),
            alpn: None,
        };

        if origin.tls {
            wire.start_tls(origin, connector, connect_timeout, budget)?;
        }

        Ok(wire)
    }

    pub(crate) fn start_tls(
        &mut self,
        origin: &Origin,
        connector: Option<&TlsConnector>,
        connect_timeout: Option<Duration>,
        budget: &Budget,
    ) -> Result<(), Error> {
        let Some(connector) = connector else {
            return Err(Error::Tls("no tls connector".to_owned()));
        };
        let conn = connector
            .connect(&origin.host)
            .map_err(|error| Error::Tls(error.to_string()))?;

        self.tls = Some(Box::new(conn));
        self.handshake(connect_timeout, budget)
    }

    fn handshake(&mut self, limit: Option<Duration>, budget: &Budget) -> Result<(), Error> {
        let mut plain = Vec::new();

        loop {
            self.arm_timeouts(budget.clamp(limit), budget.clamp(limit))
                .map_err(|error| Error::from_io(error, TimeoutKind::Connect, budget.expired()))?;
            self.flush_cipher()
                .map_err(|error| Error::from_io(error, TimeoutKind::Connect, budget.expired()))?;

            let Some(tls) = self.tls.as_deref_mut() else {
                return Ok(());
            };

            if !tls.is_handshaking() {
                self.alpn = alpn(tls).map(<[u8]>::to_vec);

                return Ok(());
            }

            self.cipher_in.resize(CIPHER_CHUNK, 0);

            let read = self
                .tcp
                .read(&mut self.cipher_in)
                .map_err(|error| Error::from_io(error, TimeoutKind::Connect, budget.expired()))?;

            if read == 0 {
                return Err(Error::Tls("peer closed during the handshake".to_owned()));
            }

            feed_all(tls, &self.cipher_in[..read], &mut plain).map_err(tls_error)?;

            if !plain.is_empty() {
                return Err(Error::Protocol(
                    "data before the handshake finished".to_owned(),
                ));
            }
        }
    }

    pub(crate) fn alpn(&self) -> Option<&[u8]> {
        self.alpn.as_deref()
    }

    pub(crate) fn arm_timeouts(
        &self,
        read: io::Result<Option<Duration>>,
        write: io::Result<Option<Duration>>,
    ) -> io::Result<()> {
        self.tcp.set_read_timeout(read?)?;
        self.tcp.set_write_timeout(write?)
    }

    pub(crate) fn arm_read(&self, budget: &Budget) -> io::Result<()> {
        self.tcp.set_read_timeout(budget.read_timeout()?)
    }

    pub(crate) fn arm_write(&self, budget: &Budget) -> io::Result<()> {
        self.tcp.set_write_timeout(budget.write_timeout()?)
    }

    pub(crate) fn reader(&self) -> io::Result<TcpStream> {
        self.tcp.try_clone()
    }

    pub(crate) fn shutdown(&self) {
        let _ = self.tcp.shutdown(std::net::Shutdown::Both);
    }

    pub(crate) fn absorb(&mut self, cipher: &[u8], out: &mut Vec<u8>) -> io::Result<()> {
        let Some(tls) = self.tls.as_deref_mut() else {
            out.extend_from_slice(cipher);

            return Ok(());
        };

        feed_all(tls, cipher, out).map_err(into_io)?;
        self.cipher_out.clear();
        flush_ciphertext(tls, &mut self.cipher_out).map_err(into_io)?;

        if self.cipher_out.is_empty() {
            return Ok(());
        }

        self.tcp.write_all(&self.cipher_out)
    }

    pub(crate) fn read_plain(&mut self, out: &mut Vec<u8>, chunk: usize) -> io::Result<usize> {
        let Some(tls) = self.tls.as_deref_mut() else {
            let base = out.len();

            out.resize(base + chunk, 0);

            let result = loop {
                match self.tcp.read(&mut out[base..]) {
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    other => break other,
                }
            };

            out.truncate(base + *result.as_ref().unwrap_or(&0));

            return result;
        };

        let base = out.len();

        loop {
            if tls.peer_closed() {
                return Ok(0);
            }

            self.cipher_in.resize(CIPHER_CHUNK, 0);

            let read = match self.tcp.read(&mut self.cipher_in) {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                other => other?,
            };

            if read == 0 {
                return Ok(0);
            }

            feed_all(tls, &self.cipher_in[..read], out).map_err(into_io)?;
            self.cipher_out.clear();
            flush_ciphertext(tls, &mut self.cipher_out).map_err(into_io)?;

            if !self.cipher_out.is_empty() {
                self.tcp.write_all(&self.cipher_out)?;
            }

            if out.len() > base {
                return Ok(out.len() - base);
            }
        }
    }

    pub(crate) fn write_plain(&mut self, data: &[u8]) -> io::Result<()> {
        let Some(tls) = self.tls.as_deref_mut() else {
            return self.tcp.write_all(data);
        };

        for chunk in data.chunks(WRITE_CHUNK) {
            self.cipher_out.clear();
            drain_plaintext(tls, chunk, &mut self.cipher_out).map_err(into_io)?;
            self.tcp.write_all(&self.cipher_out)?;
        }

        Ok(())
    }

    fn flush_cipher(&mut self) -> io::Result<()> {
        let Some(tls) = self.tls.as_deref_mut() else {
            return Ok(());
        };

        self.cipher_out.clear();
        flush_ciphertext(tls, &mut self.cipher_out).map_err(into_io)?;

        if self.cipher_out.is_empty() {
            return Ok(());
        }

        self.tcp.write_all(&self.cipher_out)
    }

    pub(crate) fn is_stale(&self) -> bool {
        peek(&self.tcp) != Peek::Quiet
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Peek {
    Quiet,
    Data,
    Gone,
}

pub(crate) fn peek(tcp: &TcpStream) -> Peek {
    if tcp.set_nonblocking(true).is_err() {
        return Peek::Gone;
    }

    let mut byte = [0u8; 1];
    let state = match tcp.peek(&mut byte) {
        Ok(0) => Peek::Gone,
        Ok(_) => Peek::Data,
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => Peek::Quiet,
        Err(_) => Peek::Gone,
    };

    if tcp.set_nonblocking(false).is_err() {
        return Peek::Gone;
    }

    state
}

fn open_tcp(
    origin: &Origin,
    connect_timeout: Option<Duration>,
    budget: &Budget,
) -> Result<TcpStream, Error> {
    let overall = |error: io::Error| Error::from_io(error, TimeoutKind::Connect, budget.expired());
    let addresses = (origin.host.as_ref(), origin.port)
        .to_socket_addrs()
        .map_err(Error::Connect)?;
    let mut last = None;

    for address in addresses {
        let attempt = match budget.clamp(connect_timeout).map_err(overall)? {
            Some(limit) => TcpStream::connect_timeout(&address, limit),
            None => TcpStream::connect(address),
        };

        match attempt {
            Ok(tcp) => {
                let _ = tcp.set_nodelay(true);

                return Ok(tcp);
            }
            Err(error) => last = Some(error),
        }
    }

    match last {
        Some(error) => Err(overall(error)),
        None => Err(Error::Connect(io::Error::new(
            io::ErrorKind::NotFound,
            "host resolved to no addresses",
        ))),
    }
}

fn feed_all(tls: &mut TlsConn, input: &[u8], plain: &mut Vec<u8>) -> Result<(), TlsError> {
    let mut offset = 0;

    while offset < input.len() {
        let used = feed_ciphertext(tls, &input[offset..], plain)?;

        if used == 0 {
            return Err(TlsError::Handshake);
        }

        offset += used;
    }

    Ok(())
}

fn tls_error(error: TlsError) -> Error {
    match error {
        TlsError::Io(error) => Error::from_io(error, TimeoutKind::Connect, false),
        other => Error::Tls(other.to_string()),
    }
}

fn into_io(error: TlsError) -> io::Error {
    match error {
        TlsError::Io(error) => error,
        other => io::Error::new(io::ErrorKind::InvalidData, other.to_string()),
    }
}
