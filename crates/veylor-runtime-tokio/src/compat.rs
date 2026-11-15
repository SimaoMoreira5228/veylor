use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use futures_lite::io::{AsyncRead, AsyncWrite};
use tokio::io::{AsyncRead as TokioRead, AsyncWrite as TokioWrite, ReadBuf};

pub struct Compat<T>(pub T);

impl<T: TokioRead + Unpin> AsyncRead for Compat<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut read = ReadBuf::new(buf);

        ready!(Pin::new(&mut self.0).poll_read(cx, &mut read))?;

        Poll::Ready(Ok(read.filled().len()))
    }
}

impl<T: TokioWrite + Unpin> AsyncWrite for Compat<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}
