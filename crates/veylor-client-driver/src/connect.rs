use std::future::{Future, poll_fn};
use std::io;
use std::net::SocketAddr;
use std::task::Poll;
use std::time::Duration;

use futures_lite::future::or;

use crate::h1::timed_out;

#[derive(Clone, Copy)]
pub struct ConnectTimeouts {
    pub connect: Duration,
    pub attempt: Duration,
    pub stagger: Duration,
    pub handshake: Duration,
}

async fn attempt<T>(
    connecting: impl Future<Output = io::Result<T>>,
    limit: impl Future<Output = ()>,
) -> io::Result<T> {
    or(connecting, async {
        limit.await;

        Err(timed_out("connect"))
    })
    .await
}

pub async fn race_connect<T, C, CF, S, SF>(
    addrs: Vec<SocketAddr>,
    timeouts: ConnectTimeouts,
    connect: C,
    sleep: S,
) -> io::Result<T>
where
    C: Fn(SocketAddr) -> CF,
    CF: Future<Output = io::Result<T>>,
    S: Fn(Duration) -> SF,
    SF: Future<Output = ()>,
{
    let mut queue = addrs.into_iter();
    let mut running = Vec::new();
    let mut stagger = None;
    let mut last = Some(io::Error::new(
        io::ErrorKind::NotFound,
        "host resolved to no addresses",
    ));
    let mut overall = Box::pin(sleep(timeouts.connect));
    let mut launch = true;

    poll_fn(|cx| {
        if overall.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(timed_out("connect")));
        }

        loop {
            if launch || running.is_empty() {
                launch = false;

                if let Some(addr) = queue.next() {
                    running.push(Box::pin(attempt(connect(addr), sleep(timeouts.attempt))));
                    stagger = (queue.len() > 0).then(|| Box::pin(sleep(timeouts.stagger)));
                }
            }

            let mut index = 0;

            while index < running.len() {
                match running[index].as_mut().poll(cx) {
                    Poll::Pending => index += 1,
                    Poll::Ready(Ok(socket)) => return Poll::Ready(Ok(socket)),
                    Poll::Ready(Err(error)) => {
                        last = Some(error);
                        drop(running.remove(index));
                    }
                }
            }

            if running.is_empty() {
                if queue.len() == 0 {
                    let error = last.take().expect("an error was recorded");

                    return Poll::Ready(Err(error));
                }

                continue;
            }

            let fired = match stagger.as_mut() {
                Some(timer) => timer.as_mut().poll(cx).is_ready(),
                None => false,
            };

            if fired {
                launch = true;
                stagger = None;

                continue;
            }

            return Poll::Pending;
        }
    })
    .await
}

pub fn interleave(addrs: Vec<SocketAddr>) -> Vec<SocketAddr> {
    let Some(first) = addrs.first().map(SocketAddr::is_ipv6) else {
        return addrs;
    };

    let (same, other): (Vec<_>, Vec<_>) = addrs.into_iter().partition(|a| a.is_ipv6() == first);
    let mut same = same.into_iter();
    let mut other = other.into_iter();
    let mut out = Vec::new();

    loop {
        match (same.next(), other.next()) {
            (None, None) => return out,
            (a, b) => out.extend(a.into_iter().chain(b)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::interleave;

    #[test]
    fn families_alternate_starting_with_the_first() {
        let list: Vec<SocketAddr> = ["[::1]:1", "[::2]:1", "1.1.1.1:1", "[::3]:1", "2.2.2.2:1"]
            .iter()
            .map(|a| a.parse().expect("addr"))
            .collect();
        let order: Vec<String> = interleave(list)
            .iter()
            .map(|a| a.ip().to_string())
            .collect();

        assert_eq!(order, ["::1", "1.1.1.1", "::2", "2.2.2.2", "::3"]);
    }
}
