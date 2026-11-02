use std::future::Future;
use std::pin::Pin;

use tokio::runtime::Handle;
use veylor_web::Executor;

pub struct TokioExecutor {
    handle: Handle,
}

impl TokioExecutor {
    pub fn new(handle: Handle) -> Self {
        Self { handle }
    }

    pub fn current() -> Self {
        Self {
            handle: Handle::current(),
        }
    }
}

impl Executor for TokioExecutor {
    fn spawn(&self, task: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) {
        self.handle.spawn(task);
    }

    fn spawn_blocking(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        self.handle.spawn_blocking(task);
    }
}
