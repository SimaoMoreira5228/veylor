use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use event_listener::Event;
use parking_lot::Mutex;

use super::error::H2Error;
use super::headers::HeaderList;
use super::request::H2Request;

pub(crate) const STATE_OPEN: u8 = 0;
pub(crate) const STATE_DRAINING: u8 = 1;
pub(crate) const STATE_CLOSED: u8 = 2;

pub(crate) struct Pending {
    pub(crate) stream: Arc<StreamShared>,
    pub(crate) request: H2Request,
    pub(crate) deadline: Option<Instant>,
}

pub(crate) struct DriverShared {
    pub(crate) notify: Event,
    pub(crate) exited: Event,
    pub(crate) queue: Mutex<VecDeque<Pending>>,
    pub(crate) dirty: Mutex<Vec<Arc<StreamShared>>>,
    pub(crate) held_total: AtomicUsize,
    pub(crate) state: AtomicU8,
    pub(crate) handles: AtomicUsize,
    pub(crate) stream_limit: AtomicUsize,
    pub(crate) close_requested: AtomicBool,
    queue_cap: usize,
    default_timeout: Option<Duration>,
}

impl DriverShared {
    pub(crate) fn new(
        queue_cap: usize,
        default_timeout: Option<Duration>,
        stream_limit: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            notify: Event::new(),
            exited: Event::new(),
            queue: Mutex::new(VecDeque::with_capacity(queue_cap)),
            dirty: Mutex::new(Vec::new()),
            held_total: AtomicUsize::new(0),
            state: AtomicU8::new(STATE_OPEN),
            handles: AtomicUsize::new(1),
            stream_limit: AtomicUsize::new(stream_limit),
            close_requested: AtomicBool::new(false),
            queue_cap,
            default_timeout,
        })
    }

    pub(crate) fn is_reusable(&self) -> bool {
        self.state.load(Ordering::Acquire) == STATE_OPEN
    }

    pub(crate) fn submit(
        self: &Arc<Self>,
        request: H2Request,
    ) -> Result<Arc<StreamShared>, H2Error> {
        request.validate()?;
        let stream = Arc::new(StreamShared::new(self.clone()));
        let timeout = request.timeout.unwrap_or(self.default_timeout);
        let deadline = timeout.map(|timeout| Instant::now() + timeout);
        if let super::request::RequestBody::Stream(upload) = &request.body {
            upload.attach(self);
        }

        let mut queue = self.queue.lock();
        if self.state.load(Ordering::Acquire) != STATE_OPEN {
            return Err(H2Error::Closed);
        }
        if queue.len() >= self.queue_cap {
            return Err(H2Error::QueueFull);
        }
        queue.push_back(Pending {
            stream: stream.clone(),
            request,
            deadline,
        });
        drop(queue);
        self.notify.notify(1);
        Ok(stream)
    }
}

enum End {
    Open,
    Complete,
    Failed(H2Error),
}

struct StreamInner {
    head: Option<(u16, HeaderList)>,
    body: Vec<u8>,
    start: usize,
    trailers: Option<HeaderList>,
    end: End,
    waker: Option<Waker>,
}

impl StreamInner {
    fn held(&self) -> usize {
        self.body.len() - self.start
    }
}

pub(crate) struct StreamShared {
    inner: Mutex<StreamInner>,
    pub(crate) cancelled: AtomicBool,
    dirty: AtomicBool,
    pub(crate) driver: Arc<DriverShared>,
}

impl StreamShared {
    fn new(driver: Arc<DriverShared>) -> Self {
        Self {
            inner: Mutex::new(StreamInner {
                head: None,
                body: Vec::new(),
                start: 0,
                trailers: None,
                end: End::Open,
                waker: None,
            }),
            cancelled: AtomicBool::new(false),
            dirty: AtomicBool::new(false),
            driver,
        }
    }

    fn wake(waker: Option<Waker>) {
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    pub(crate) fn mark_dirty(self: &Arc<Self>) {
        if !self.dirty.swap(true, Ordering::AcqRel) {
            self.driver.dirty.lock().push(self.clone());
        }
        self.driver.notify.notify(1);
    }

    pub(crate) fn clear_dirty(&self) {
        self.dirty.store(false, Ordering::Release);
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub(crate) fn held(&self) -> usize {
        self.inner.lock().held()
    }

    pub(crate) fn set_head(&self, status: u16, headers: HeaderList) {
        let waker = {
            let mut inner = self.inner.lock();
            inner.head = Some((status, headers));
            inner.waker.take()
        };
        Self::wake(waker);
    }

    pub(crate) fn set_trailers(&self, trailers: HeaderList) {
        self.inner.lock().trailers = Some(trailers);
    }

    pub(crate) fn push_body(&self, data: &[u8], cap: usize) -> bool {
        let waker = {
            let mut inner = self.inner.lock();
            if inner.held() + data.len() > cap {
                return false;
            }
            inner.body.extend_from_slice(data);
            inner.waker.take()
        };
        self.driver
            .held_total
            .fetch_add(data.len(), Ordering::AcqRel);
        Self::wake(waker);
        true
    }

    pub(crate) fn complete(&self) {
        let waker = {
            let mut inner = self.inner.lock();
            if matches!(inner.end, End::Open) {
                inner.end = End::Complete;
            }
            inner.waker.take()
        };
        Self::wake(waker);
    }

    pub(crate) fn fail(&self, error: H2Error) {
        let waker = {
            let mut inner = self.inner.lock();
            if matches!(inner.end, End::Open) {
                inner.end = End::Failed(error);
            }
            inner.waker.take()
        };
        Self::wake(waker);
    }

    pub(crate) fn discard_body(&self) {
        let held = {
            let mut inner = self.inner.lock();
            let held = inner.held();
            inner.body = Vec::new();
            inner.start = 0;
            held
        };
        self.driver.held_total.fetch_sub(held, Ordering::AcqRel);
    }

    pub(crate) fn is_finished(&self) -> bool {
        !matches!(self.inner.lock().end, End::Open)
    }

    pub(crate) fn poll_head(
        &self,
        context: &mut Context<'_>,
    ) -> Poll<Result<(u16, HeaderList), H2Error>> {
        let mut inner = self.inner.lock();
        if let Some(head) = inner.head.take() {
            return Poll::Ready(Ok(head));
        }
        match inner.end {
            End::Failed(error) => Poll::Ready(Err(error)),
            End::Complete => Poll::Ready(Err(H2Error::Closed)),
            End::Open => {
                inner.waker = Some(context.waker().clone());
                Poll::Pending
            }
        }
    }

    pub(crate) fn poll_read(
        self: &Arc<Self>,
        context: &mut Context<'_>,
        output: &mut [u8],
    ) -> Poll<Result<usize, H2Error>> {
        let mut inner = self.inner.lock();
        if inner.held() > 0 {
            let count = inner.held().min(output.len());
            let start = inner.start;
            output[..count].copy_from_slice(&inner.body[start..start + count]);
            inner.start += count;
            if inner.start == inner.body.len() {
                inner.body.clear();
                inner.start = 0;
            }
            drop(inner);
            self.driver.held_total.fetch_sub(count, Ordering::AcqRel);
            self.mark_dirty();
            return Poll::Ready(Ok(count));
        }
        match inner.end {
            End::Complete => Poll::Ready(Ok(0)),
            End::Failed(error) => Poll::Ready(Err(error)),
            End::Open => {
                inner.waker = Some(context.waker().clone());
                Poll::Pending
            }
        }
    }

    pub(crate) fn take_trailers(&self) -> Option<HeaderList> {
        self.inner.lock().trailers.take()
    }
}
