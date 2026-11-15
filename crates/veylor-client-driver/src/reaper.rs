use std::future::Future;
use std::sync::Weak;
use std::time::{Duration, Instant};

use futures_lite::future::or;

use crate::h1::H1Conn;
use crate::pool::Pool;

const MIN_SLEEP: Duration = Duration::from_millis(10);

pub async fn run_reaper<C, S, F>(pool: Weak<Pool<C>>, sleep: S)
where
    C: H1Conn,
    S: Fn(Duration) -> F,
    F: Future<Output = ()>,
{
    loop {
        let Some(strong) = pool.upgrade() else {
            return;
        };

        let wake = strong.listen_wake();
        let next = strong.reap(Instant::now());

        drop(strong);

        match next {
            Some(at) => {
                let wait = at.saturating_duration_since(Instant::now()).max(MIN_SLEEP);

                or(wake, sleep(wait)).await;
            }
            None => wake.await,
        }
    }
}
