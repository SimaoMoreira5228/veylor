use std::future::Future;
use std::pin::Pin;

use veylor_web::Executor;

pub struct SmolExecutor;

impl Executor for SmolExecutor {
    fn spawn(&self, task: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) {
        smol::spawn(task).detach();
    }

    fn spawn_blocking(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        smol::unblock(task).detach();
    }
}
