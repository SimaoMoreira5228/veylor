use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use veylor_runtime::ConnectionToken;
use veylor_server::{HostHandle, RespondOutcome};

use crate::request::State;

const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const WRITE_POLL: Duration = Duration::from_millis(1);

const STREAM_HEADERS: [(&[u8], &[u8]); 3] = [
    (b"content-type", b"text/event-stream"),
    (b"cache-control", b"no-cache"),
    (b"x-accel-buffering", b"no"),
];

pub struct SseEvent {
    data: Vec<u8>,
    event: Option<Vec<u8>>,
    id: Option<Vec<u8>>,
    retry: Option<u64>,
}

impl SseEvent {
    pub fn data(data: impl Into<Vec<u8>>) -> Self {
        Self {
            data: data.into(),
            event: None,
            id: None,
            retry: None,
        }
    }

    pub fn event(mut self, name: impl Into<Vec<u8>>) -> Self {
        self.event = Some(name.into());
        self
    }

    pub fn id(mut self, id: impl Into<Vec<u8>>) -> Self {
        self.id = Some(id.into());
        self
    }

    pub fn retry(mut self, millis: u64) -> Self {
        self.retry = Some(millis);
        self
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.data.len() + 64);
        if let Some(event) = self.event.as_deref() {
            write_field(&mut out, b"event", event);
        }
        if let Some(id) = self.id.as_deref() {
            write_field(&mut out, b"id", id);
        }
        if let Some(retry) = self.retry {
            write_field(&mut out, b"retry", retry.to_string().as_bytes());
        }
        for line in self.data.split(|byte| *byte == b'\n') {
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            write_field(&mut out, b"data", line);
        }
        out.push(b'\n');
        out
    }
}

fn write_field(out: &mut Vec<u8>, name: &[u8], value: &[u8]) {
    out.extend_from_slice(name);
    out.extend_from_slice(b": ");
    out.extend(
        value
            .iter()
            .copied()
            .filter(|byte| !matches!(byte, b'\r' | b'\n')),
    );
    out.push(b'\n');
}

pub struct SseSender {
    host: HostHandle,
    token: ConnectionToken,
    closed: AtomicBool,
    ended: AtomicBool,
}

impl SseSender {
    pub(crate) fn begin(host: HostHandle, token: ConnectionToken) -> Option<Self> {
        let outcome = host.begin_stream_with_headers(token, 200, b"OK", &STREAM_HEADERS);
        (outcome == RespondOutcome::Ok).then(|| Self {
            host,
            token,
            closed: AtomicBool::new(false),
            ended: AtomicBool::new(false),
        })
    }

    pub fn send(&self, event: SseEvent) -> bool {
        self.send_raw(&event.encode())
    }

    pub fn send_raw(&self, bytes: &[u8]) -> bool {
        if self.is_closed() {
            return false;
        }
        let deadline = Instant::now() + WRITE_TIMEOUT;
        while Instant::now() < deadline {
            if !self.host.is_writable(self.token) {
                std::thread::sleep(WRITE_POLL);
                continue;
            }
            match self.host.write_stream(self.token, bytes) {
                RespondOutcome::Ok => return true,
                RespondOutcome::NotWritable | RespondOutcome::WouldBlock => {
                    std::thread::sleep(WRITE_POLL);
                }
                _ => break,
            }
        }
        self.closed.store(true, Ordering::Release);
        false
    }

    pub fn comment(&self, text: &str) -> bool {
        let mut frame = Vec::with_capacity(text.len() + 4);
        for line in text.split('\n') {
            frame.push(b':');
            frame.push(b' ');
            frame.extend_from_slice(line.strip_suffix('\r').unwrap_or(line).as_bytes());
            frame.push(b'\n');
        }
        frame.push(b'\n');
        self.send_raw(&frame)
    }

    pub fn sleep(&self, duration: Duration) -> bool {
        if self.is_closed() {
            return false;
        }
        std::thread::sleep(duration);
        !self.is_closed()
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    pub fn close(self) {}

    fn end(&self) {
        if self.ended.swap(true, Ordering::AcqRel) {
            return;
        }
        let deadline = Instant::now() + WRITE_TIMEOUT;
        loop {
            match self.host.end_stream(self.token) {
                RespondOutcome::Ok | RespondOutcome::Stale | RespondOutcome::Gone => return,
                _ => {}
            }
            if Instant::now() >= deadline {
                return;
            }
            std::thread::sleep(WRITE_POLL);
        }
    }
}

impl Drop for SseSender {
    fn drop(&mut self) {
        self.end();
    }
}

pub trait SseHandler<T, S: 'static>: Send + Sync + 'static {
    fn call(&self, state: &'static S, sender: SseSender);
}

pub struct SseArg;
pub struct SseStateArg;

impl<S: 'static, F> SseHandler<SseArg, S> for F
where
    F: Fn(SseSender) + Send + Sync + 'static,
{
    fn call(&self, _state: &'static S, sender: SseSender) {
        (self)(sender);
    }
}

impl<S: 'static, F> SseHandler<SseStateArg, S> for F
where
    F: Fn(State<S>, SseSender) + Send + Sync + 'static,
{
    fn call(&self, state: &'static S, sender: SseSender) {
        (self)(State(state), sender);
    }
}

pub(crate) type SseRoute<S> = std::sync::Arc<dyn Fn(&'static S, SseSender) + Send + Sync>;

pub(crate) fn erase_sse<T, S: 'static, H>(handler: H) -> SseRoute<S>
where
    H: SseHandler<T, S>,
{
    std::sync::Arc::new(move |state, sender| handler.call(state, sender))
}
