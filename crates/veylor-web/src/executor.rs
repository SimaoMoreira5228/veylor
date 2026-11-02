use std::any::Any;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use pin_project_lite::pin_project;

pub trait Executor: Send + Sync + 'static {
    fn spawn(&self, task: Pin<Box<dyn Future<Output = ()> + Send + 'static>>);

    fn spawn_blocking(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        let _ = std::thread::Builder::new()
            .name("veylor-web-blocking".to_owned())
            .spawn(task);
    }
}

pub(crate) struct InlineExecutor;

impl Executor for InlineExecutor {
    fn spawn(&self, task: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) {
        block_on(task);
    }
}

pub struct ThreadExecutor;

impl Executor for ThreadExecutor {
    fn spawn(&self, task: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) {
        let _ = std::thread::Builder::new()
            .name("veylor-web-task".to_owned())
            .spawn(move || block_on(task));
    }
}

pin_project! {
    pub(crate) struct CatchUnwind<F> {
        #[pin]
        future: F,
    }
}

impl<F> CatchUnwind<F> {
    pub(crate) fn new(future: F) -> Self {
        Self { future }
    }
}

impl<F: Future> Future for CatchUnwind<F> {
    type Output = Result<F::Output, Box<dyn Any + Send>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match catch_unwind(AssertUnwindSafe(|| this.future.poll(cx))) {
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => Poll::Ready(Err(payload)),
        }
    }
}

pub(crate) struct Inflight {
    count: Mutex<usize>,
    done: Condvar,
}

impl Inflight {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            count: Mutex::new(0),
            done: Condvar::new(),
        })
    }

    pub(crate) fn add(self: &Arc<Self>) -> InflightGuard {
        if let Ok(mut count) = self.count.lock() {
            *count += 1;
        }
        InflightGuard(Arc::clone(self))
    }

    fn remove(&self) {
        let Ok(mut count) = self.count.lock() else {
            return;
        };
        *count = count.saturating_sub(1);
        if *count == 0 {
            self.done.notify_all();
        }
    }

    pub(crate) fn wait(&self) {
        let Ok(mut count) = self.count.lock() else {
            return;
        };
        while *count > 0 {
            count = match self.done.wait(count) {
                Ok(count) => count,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
    }
}

pub(crate) struct InflightGuard(Arc<Inflight>);

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.0.remove();
    }
}

struct Unpark(std::thread::Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

// NOTE: dispatcher-thread executor for handlers that complete without external IO. A
// future that parks on a timer or socket needs a real runtime; this one only wakes on
// `wake()` and on a short timeout so a lost wakeup cannot wedge the dispatcher.
pub(crate) fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);

    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::park_timeout(std::time::Duration::from_millis(1)),
        }
    }
}
