use std::io;
use std::task::{Context, Poll, Waker};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use veylor_client::{BodyFraming, Field};
use veylor_client_driver::{
    Deadlines, Event, H1Conn, H1Exchange, Next, ResponseHead, Step, StreamingBody, TlsConn,
    protocol, timed_out,
};
use veylor_tls::queue_close_notify;

pub struct Connection {
    socket: TcpStream,
    exchange: H1Exchange,
    deadlines: Deadlines,
}

impl Connection {
    pub fn new(
        socket: TcpStream,
        tls: Option<Box<TlsConn>>,
        alpn: Option<Vec<u8>>,
        deadlines: Deadlines,
    ) -> Self {
        Self {
            socket,
            exchange: H1Exchange::new(tls, alpn, deadlines.max_head),
            deadlines,
        }
    }

    pub async fn send_request(
        &mut self,
        method: &[u8],
        target: &[u8],
        headers: &[Field<'_>],
        body: &[u8],
    ) -> io::Result<()> {
        self.exchange.begin(method)?;

        let framing = match body.len() {
            0 => BodyFraming::None,
            len => BodyFraming::Fixed(len as u64),
        };
        let head = self
            .exchange
            .encode_head(method, target, headers, framing)?;

        self.write_pending(head).await?;

        if body.is_empty() {
            return Ok(());
        }

        let mut offset = 0usize;

        while offset < body.len() {
            let written = self.exchange.encode_body(&body[offset..])?;

            self.write_pending(written).await?;

            offset += written;
        }

        let tail = self.exchange.encode_tail(None)?;

        if tail > 0 {
            self.write_pending(tail).await?;
        }

        Ok(())
    }

    pub async fn send_streaming(
        &mut self,
        method: &[u8],
        target: &[u8],
        headers: &[Field<'_>],
        body: &StreamingBody,
    ) -> io::Result<()> {
        self.exchange.begin(method)?;

        let head = self
            .exchange
            .encode_head(method, target, headers, BodyFraming::Chunked)?;

        self.write_pending(head).await?;

        loop {
            let pull = body.pull(&mut self.exchange)?;

            if pull.written > 0 {
                self.write_pending(pull.written).await?;
            }

            match pull.next {
                Next::More => {}
                Next::Wait(listener) => match timeout(self.deadlines.write, listener).await {
                    Ok(()) => {}
                    Err(_) => return Err(timed_out("request body producer")),
                },
                Next::Tail(trailers) => {
                    let tail = self.exchange.encode_tail(trailers.as_ref())?;

                    return self.write_pending(tail).await;
                }
            }
        }
    }

    pub async fn read_head(&mut self) -> io::Result<ResponseHead> {
        let deadline = self.deadlines.head;

        match timeout(deadline, self.next_event(true)).await {
            Ok(Ok(Event::Head(head))) => Ok(head),
            Ok(Ok(_)) => Err(protocol("response ended before its head")),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(timed_out("response head")),
        }
    }

    async fn next_event(&mut self, want_head: bool) -> io::Result<Event> {
        loop {
            match self.exchange.next_step(want_head)? {
                Step::Event(event) => return Ok(event),
                Step::NeedRead => self.fill().await?,
            }
        }
    }

    async fn fill(&mut self) -> io::Result<()> {
        let slot = self.exchange.read_slot()?;
        let read = self.socket.read(slot).await?;

        if self.exchange.filled(read)? {
            self.write_cipher().await?;
        }

        Ok(())
    }

    async fn write_pending(&mut self, len: usize) -> io::Result<()> {
        let limit = self.deadlines.write;
        let wire = self.exchange.wire_out(len)?;

        match timeout(limit, self.socket.write_all(wire)).await {
            Ok(done) => done?,
            Err(_) => return Err(timed_out("request write")),
        }

        self.exchange.wrote(len)
    }

    async fn write_cipher(&mut self) -> io::Result<()> {
        let limit = self.deadlines.write;

        match timeout(limit, self.socket.write_all(self.exchange.cipher_out())).await {
            Ok(done) => done,
            Err(_) => Err(timed_out("tls write")),
        }
    }
}

impl H1Conn for Connection {
    fn exchange(&self) -> &H1Exchange {
        &self.exchange
    }

    fn exchange_mut(&mut self) -> &mut H1Exchange {
        &mut self.exchange
    }

    fn is_alive(&mut self) -> bool {
        if !self.exchange.is_idle_reusable() {
            return false;
        }

        let mut cx = Context::from_waker(Waker::noop());
        let mut probe = [0u8; 1024];

        loop {
            match self.socket.poll_read_ready(&mut cx) {
                Poll::Pending => return true,
                Poll::Ready(Err(_)) => return false,
                Poll::Ready(Ok(())) => {}
            }

            match self.socket.try_read(&mut probe) {
                Ok(0) => return false,
                Ok(read) => {
                    if !self.exchange.probe_fed(&probe[..read]) {
                        return false;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return true,
                Err(_) => return false,
            }
        }
    }

    async fn next_chunk(&mut self) -> io::Result<Option<&[u8]>> {
        let deadline = self.deadlines.body;

        let event = match timeout(deadline, self.next_event(false)).await {
            Ok(event) => event?,
            Err(_) => return Err(timed_out("response body")),
        };

        match event {
            Event::Body(range) => Ok(Some(self.exchange.chunk(range))),
            Event::Done => Ok(None),
            Event::Head(_) => Err(protocol("unexpected second response head")),
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        let Some(tls) = self.exchange.tls_mut() else {
            return;
        };
        let mut out = Vec::new();

        if queue_close_notify(tls, &mut out).is_ok() {
            let _ = self.socket.try_write(&out);
        }
    }
}
