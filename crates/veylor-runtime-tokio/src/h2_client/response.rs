use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, ReadBuf};

use super::error::H2Error;
use super::headers::HeaderList;
use super::shared::StreamShared;

pub struct H2Response {
    pub status: u16,
    pub headers: HeaderList,
    pub body: H2Body,
}

pub struct H2Body {
    shared: Arc<StreamShared>,
}

impl H2Body {
    pub(crate) fn new(shared: Arc<StreamShared>) -> Self {
        Self { shared }
    }

    pub(crate) async fn head(&mut self) -> Result<(u16, HeaderList), H2Error> {
        std::future::poll_fn(|context| self.shared.poll_head(context)).await
    }

    pub fn poll_read_bytes(
        &mut self,
        context: &mut Context<'_>,
        output: &mut [u8],
    ) -> Poll<Result<usize, H2Error>> {
        self.shared.poll_read(context, output)
    }

    pub async fn read(&mut self, output: &mut [u8]) -> Result<usize, H2Error> {
        std::future::poll_fn(|context| self.shared.poll_read(context, output)).await
    }

    pub async fn read_to_end(&mut self, limit: usize) -> Result<Vec<u8>, H2Error> {
        let mut collected = Vec::new();
        let mut chunk = [0u8; 8192];
        loop {
            let count = self.read(&mut chunk).await?;
            if count == 0 {
                return Ok(collected);
            }
            if collected.len() + count > limit {
                return Err(H2Error::BodyOverflow);
            }
            collected.extend_from_slice(&chunk[..count]);
        }
    }

    pub fn trailers(&mut self) -> Option<HeaderList> {
        self.shared.take_trailers()
    }
}

impl AsyncRead for H2Body {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let output = buffer.initialize_unfilled();
        match self.shared.poll_read(context, output) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(count)) => {
                buffer.advance(count);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error.into())),
        }
    }
}

impl Drop for H2Body {
    fn drop(&mut self) {
        self.shared.discard_body();
        if !self.shared.is_finished() {
            self.shared
                .cancelled
                .store(true, std::sync::atomic::Ordering::Release);
        }
        self.shared.mark_dirty();
    }
}
