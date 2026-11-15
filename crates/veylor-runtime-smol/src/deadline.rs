use std::future::Future;
use std::io;
use std::time::Duration;

use smol::Timer;
use smol::future::or;
use veylor_client_driver::timed_out;

pub async fn within<T>(
    limit: Duration,
    what: &str,
    work: impl Future<Output = io::Result<T>>,
) -> io::Result<T> {
    or(work, async {
        Timer::after(limit).await;

        Err(timed_out(what))
    })
    .await
}
