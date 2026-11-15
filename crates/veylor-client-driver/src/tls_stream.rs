use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use futures_lite::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use veylor_tls::{
    TlsConn, alpn, drain_plaintext, feed_ciphertext, flush_ciphertext, queue_close_notify,
};

use crate::h1::{protocol, tls_error};

const READ_CHUNK: usize = 16 * 1024 + 512;
const WRITE_CHUNK: usize = 16 * 1024;

pub struct TlsStream<S> {
    socket: S,
    tls: Box<TlsConn>,
    cipher_in: Vec<u8>,
    plain: Vec<u8>,
    plain_pos: usize,
    out: Vec<u8>,
    out_pos: usize,
    eof: bool,
    closing: bool,
}

impl<S: AsyncRead + AsyncWrite + Unpin> TlsStream<S> {
    pub fn new(socket: S, tls: TlsConn) -> Self {
        Self {
            socket,
            tls: Box::new(tls),
            cipher_in: vec![0; READ_CHUNK],
            plain: Vec::new(),
            plain_pos: 0,
            out: Vec::new(),
            out_pos: 0,
            eof: false,
            closing: false,
        }
    }

    pub async fn handshake(&mut self) -> io::Result<()> {
        flush_ciphertext(&mut self.tls, &mut self.out).map_err(tls_error)?;
        self.write_out().await?;

        while self.tls.is_handshaking() {
            let read = self.socket.read(&mut self.cipher_in).await?;

            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "peer closed during tls handshake",
                ));
            }

            self.feed(read)?;
            self.write_out().await?;
        }

        Ok(())
    }

    pub fn alpn(&self) -> Option<&[u8]> {
        alpn(&self.tls)
    }

    pub fn into_parts(self) -> (S, Box<TlsConn>) {
        (self.socket, self.tls)
    }

    async fn write_out(&mut self) -> io::Result<()> {
        self.socket.write_all(&self.out[self.out_pos..]).await?;
        self.out.clear();
        self.out_pos = 0;

        Ok(())
    }

    fn feed(&mut self, len: usize) -> io::Result<()> {
        if self.plain_pos == self.plain.len() {
            self.plain.clear();
            self.plain_pos = 0;
        }

        let mut offset = 0usize;

        while offset < len {
            let used =
                feed_ciphertext(&mut self.tls, &self.cipher_in[offset..len], &mut self.plain)
                    .map_err(tls_error)?;

            if used == 0 {
                return Err(protocol("tls stalled on input"));
            }

            offset += used;
        }

        flush_ciphertext(&mut self.tls, &mut self.out).map_err(tls_error)
    }

    fn poll_flush_out(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while self.out_pos < self.out.len() {
            let written =
                ready!(Pin::new(&mut self.socket).poll_write(cx, &self.out[self.out_pos..]))?;

            if written == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }

            self.out_pos += written;
        }

        self.out.clear();
        self.out_pos = 0;

        Poll::Ready(Ok(()))
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for TlsStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();

        loop {
            if this.plain_pos < this.plain.len() {
                let take = (this.plain.len() - this.plain_pos).min(buf.len());

                buf[..take].copy_from_slice(&this.plain[this.plain_pos..this.plain_pos + take]);
                this.plain_pos += take;

                return Poll::Ready(Ok(take));
            }

            if this.eof {
                return Poll::Ready(Ok(0));
            }

            let read = ready!(Pin::new(&mut this.socket).poll_read(cx, &mut this.cipher_in))?;

            if read == 0 {
                this.eof = true;

                continue;
            }

            this.feed(read)?;

            if let Poll::Ready(Err(error)) = this.poll_flush_out(cx) {
                return Poll::Ready(Err(error));
            }
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for TlsStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();

        ready!(this.poll_flush_out(cx))?;

        let take = data.len().min(WRITE_CHUNK);

        drain_plaintext(&mut this.tls, &data[..take], &mut this.out).map_err(tls_error)?;

        if let Poll::Ready(Err(error)) = this.poll_flush_out(cx) {
            return Poll::Ready(Err(error));
        }

        Poll::Ready(Ok(take))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        ready!(this.poll_flush_out(cx))?;

        Pin::new(&mut this.socket).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        if !this.closing {
            this.closing = true;
            queue_close_notify(&mut this.tls, &mut this.out).map_err(tls_error)?;
        }

        ready!(this.poll_flush_out(cx))?;

        Pin::new(&mut this.socket).poll_close(cx)
    }
}
