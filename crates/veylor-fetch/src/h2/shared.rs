use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::time::Instant;

use parking_lot::{Condvar, Mutex, MutexGuard};

use super::{Command, Refused};
use crate::budget::Budget;
use crate::headers::Headers;

pub(super) const STATE_OPEN: u8 = 0;
pub(super) const STATE_DRAINING: u8 = 1;
pub(super) const STATE_CLOSED: u8 = 2;

const MAX_QUEUED: usize = 1_024;
const CREDIT_REPORT: usize = 16 * 1024;

#[derive(Clone, Debug)]
pub(super) struct Failure {
    pub(super) kind: io::ErrorKind,
    pub(super) message: String,
    pub(super) unprocessed: bool,
}

impl Failure {
    pub(super) fn new(kind: io::ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            unprocessed: false,
        }
    }

    pub(super) fn unprocessed(mut self) -> Self {
        self.unprocessed = true;

        self
    }

    pub(super) fn to_io(&self) -> io::Error {
        io::Error::new(self.kind, self.message.clone())
    }
}

pub(super) struct Pending {
    pub(super) stream: Arc<StreamShared>,
    pub(super) command: Command,
    pub(super) deadline: Option<Instant>,
}

#[derive(Default)]
pub(super) struct Core {
    pub(super) queue: VecDeque<Pending>,
    pub(super) dirty: Vec<Arc<StreamShared>>,
    pub(super) inbound: Vec<u8>,
    pub(super) eof: Option<Failure>,
    pub(super) closing: bool,
}

pub(super) struct DriverShared {
    pub(super) core: Mutex<Core>,
    pub(super) wake: Condvar,
    pub(super) room: Condvar,
    pub(super) state: AtomicU8,
    pub(super) inflight: AtomicUsize,
    pub(super) stream_limit: AtomicUsize,
}

impl DriverShared {
    pub(super) fn new(stream_limit: usize) -> Arc<Self> {
        Arc::new(Self {
            core: Mutex::new(Core::default()),
            wake: Condvar::new(),
            room: Condvar::new(),
            state: AtomicU8::new(STATE_OPEN),
            inflight: AtomicUsize::new(0),
            stream_limit: AtomicUsize::new(stream_limit),
        })
    }

    pub(super) fn is_open(&self) -> bool {
        self.state.load(Ordering::Acquire) == STATE_OPEN
    }

    pub(super) fn reserve(&self, force: bool) -> bool {
        if !self.is_open() {
            return false;
        }

        let limit = self.stream_limit.load(Ordering::Acquire);
        let mut current = self.inflight.load(Ordering::Acquire);

        loop {
            if !force && current >= limit {
                return false;
            }

            match self.inflight.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    pub(super) fn release(&self) {
        self.inflight.fetch_sub(1, Ordering::AcqRel);
    }

    pub(super) fn submit(
        self: &Arc<Self>,
        command: Command,
        deadline: Option<Instant>,
    ) -> Result<Arc<StreamShared>, Refused> {
        let stream = Arc::new(StreamShared::new(Arc::clone(self)));
        let mut core = self.core.lock();

        if !self.is_open() || core.closing {
            let failure = Failure::new(io::ErrorKind::ConnectionAborted, "h2 connection closed");

            return Err(refused(failure.unprocessed()));
        }

        if core.queue.len() >= MAX_QUEUED {
            return Err(Refused {
                error: io::Error::new(io::ErrorKind::WouldBlock, "h2 request queue is full"),
                retryable: false,
                unprocessed: true,
            });
        }

        core.queue.push_back(Pending {
            stream: Arc::clone(&stream),
            command,
            deadline,
        });
        drop(core);
        self.wake.notify_one();

        Ok(stream)
    }
}

pub(super) fn refused(failure: Failure) -> Refused {
    Refused {
        error: failure.to_io(),
        retryable: true,
        unprocessed: failure.unprocessed,
    }
}

enum End {
    Open,
    Complete,
    Failed(Failure),
}

struct StreamInner {
    head: Option<(u16, Headers)>,
    body: Vec<u8>,
    start: usize,
    unreported: usize,
    end: End,
}

impl StreamInner {
    fn held(&self) -> usize {
        self.body.len() - self.start
    }
}

pub(super) struct StreamShared {
    inner: Mutex<StreamInner>,
    ready: Condvar,
    cancelled: AtomicBool,
    dirty: AtomicBool,
    released: AtomicBool,
    driver: Arc<DriverShared>,
}

impl StreamShared {
    fn new(driver: Arc<DriverShared>) -> Self {
        Self {
            inner: Mutex::new(StreamInner {
                head: None,
                body: Vec::new(),
                start: 0,
                unreported: 0,
                end: End::Open,
            }),
            ready: Condvar::new(),
            cancelled: AtomicBool::new(false),
            dirty: AtomicBool::new(false),
            released: AtomicBool::new(false),
            driver,
        }
    }

    fn mark_dirty(self: &Arc<Self>) {
        if !self.dirty.swap(true, Ordering::AcqRel) {
            self.driver.core.lock().dirty.push(Arc::clone(self));
        }

        self.driver.wake.notify_one();
    }

    pub(super) fn clear_dirty(&self) {
        self.dirty.store(false, Ordering::Release);
    }

    pub(super) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub(super) fn cancel(self: &Arc<Self>) {
        if self.cancelled.swap(true, Ordering::AcqRel) {
            return;
        }

        self.mark_dirty();
    }

    pub(super) fn held(&self) -> usize {
        self.inner.lock().held()
    }

    fn release_slot(&self) {
        if !self.released.swap(true, Ordering::AcqRel) {
            self.driver.release();
        }
    }

    pub(super) fn set_head(&self, status: u16, headers: Headers) {
        self.inner.lock().head = Some((status, headers));
        self.ready.notify_all();
    }

    pub(super) fn push_body(&self, data: &[u8], cap: usize) -> bool {
        let mut inner = self.inner.lock();

        if inner.held() + data.len() > cap {
            return false;
        }

        inner.body.extend_from_slice(data);
        drop(inner);
        self.ready.notify_all();

        true
    }

    pub(super) fn complete(&self) {
        let mut inner = self.inner.lock();

        if matches!(inner.end, End::Open) {
            inner.end = End::Complete;
        }

        drop(inner);
        self.ready.notify_all();
        self.release_slot();
    }

    pub(super) fn fail(&self, failure: Failure) {
        let mut inner = self.inner.lock();

        if matches!(inner.end, End::Open) {
            inner.end = End::Failed(failure);
        }

        drop(inner);
        self.ready.notify_all();
        self.release_slot();
    }

    pub(super) fn discard_body(&self) {
        let mut inner = self.inner.lock();

        inner.body = Vec::new();
        inner.start = 0;
    }

    pub(super) fn is_terminal(&self) -> bool {
        !matches!(self.inner.lock().end, End::Open)
    }

    fn wait(&self, guard: &mut MutexGuard<'_, StreamInner>, limit: Option<Instant>) -> bool {
        let Some(limit) = limit else {
            self.ready.wait(guard);

            return true;
        };

        if Instant::now() >= limit {
            return false;
        }

        !self.ready.wait_until(guard, limit).timed_out()
    }

    pub(super) fn wait_head(self: &Arc<Self>, budget: &Budget) -> Result<(u16, Headers), Refused> {
        let limit = wait_limit(budget);
        let mut inner = self.inner.lock();

        loop {
            if let Some(head) = inner.head.take() {
                return Ok(head);
            }

            match &inner.end {
                End::Failed(failure) => return Err(refused(failure.clone())),
                End::Complete => {
                    return Err(refused(Failure::new(
                        io::ErrorKind::UnexpectedEof,
                        "h2 stream ended without a response",
                    )));
                }
                End::Open => {}
            }

            if !self.wait(&mut inner, limit) {
                drop(inner);
                self.cancel();

                return Err(Refused {
                    error: io::ErrorKind::TimedOut.into(),
                    retryable: false,
                    unprocessed: false,
                });
            }
        }
    }

    pub(super) fn read(self: &Arc<Self>, buf: &mut [u8], budget: &Budget) -> io::Result<usize> {
        let limit = wait_limit(budget);
        let mut inner = self.inner.lock();

        loop {
            if inner.held() > 0 {
                let count = inner.held().min(buf.len());
                let start = inner.start;

                buf[..count].copy_from_slice(&inner.body[start..start + count]);
                inner.start += count;
                inner.unreported += count;

                let drained = inner.start == inner.body.len();

                if drained {
                    inner.body.clear();
                    inner.start = 0;
                }

                let report = drained || inner.unreported >= CREDIT_REPORT;

                if report {
                    inner.unreported = 0;
                }

                drop(inner);

                if report {
                    self.mark_dirty();
                }

                return Ok(count);
            }

            match &inner.end {
                End::Complete => return Ok(0),
                End::Failed(failure) => return Err(failure.to_io()),
                End::Open => {}
            }

            if !self.wait(&mut inner, limit) {
                drop(inner);
                self.cancel();

                return Err(io::ErrorKind::TimedOut.into());
            }
        }
    }
}

impl Drop for StreamShared {
    // NOTE: a stream can be dropped without ever reaching the driver, when its waiter gave up
    // while the request was still queued. Releasing here keeps the arrival counter honest.
    fn drop(&mut self) {
        self.release_slot();
    }
}

fn wait_limit(budget: &Budget) -> Option<Instant> {
    let idle = budget.read.map(|read| Instant::now() + read);

    match (idle, budget.overall) {
        (Some(idle), Some(overall)) => Some(idle.min(overall)),
        (Some(limit), None) | (None, Some(limit)) => Some(limit),
        (None, None) => None,
    }
}
