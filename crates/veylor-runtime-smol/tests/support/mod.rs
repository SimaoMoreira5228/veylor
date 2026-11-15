#![allow(dead_code)]

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use smol::{Task, Timer};

#[derive(Debug)]
pub struct Elapsed;

#[derive(Debug)]
pub struct JoinError;

pub struct Join<T>(RefCell<Option<Task<T>>>);

impl<T> Join<T> {
    pub async fn abort(&self) {
        let task = self.0.borrow_mut().take();

        if let Some(task) = task {
            task.cancel().await;
        }
    }

    pub fn is_finished(&self) -> bool {
        self.0.borrow().as_ref().is_none_or(Task::is_finished)
    }
}

impl<T> Future for Join<T> {
    type Output = Result<T, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(task) = self.0.get_mut().as_mut() else {
            return Poll::Ready(Err(JoinError));
        };

        Pin::new(task).poll(cx).map(Ok)
    }
}

impl<T> Drop for Join<T> {
    fn drop(&mut self) {
        if let Some(task) = self.0.get_mut().take() {
            task.detach();
        }
    }
}

pub fn spawn<F>(future: F) -> Join<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    Join(RefCell::new(Some(smol::spawn(future))))
}

pub async fn sleep(duration: Duration) {
    Timer::after(duration).await;
}

pub async fn timeout<F: Future>(limit: Duration, future: F) -> Result<F::Output, Elapsed> {
    smol::future::or(async { Ok(future.await) }, async {
        Timer::after(limit).await;

        Err(Elapsed)
    })
    .await
}
