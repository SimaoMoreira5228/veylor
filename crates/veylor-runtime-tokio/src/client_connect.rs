use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};

use tokio::net::{TcpStream, lookup_host};
use tokio::time::{Instant, sleep, timeout, timeout_at};
use veylor_client_driver::{
    ConnectTimeouts, Deadlines, H2Client, H2Config, Origin, TlsConnector, TlsStream, interleave,
    race_connect, timed_out,
};

use crate::client_conn::Connection;
use crate::compat::Compat;

pub enum Connected {
    Http1(Box<Connection>),
    Http2(H2Client),
}

pub struct ConnectPlan<'a> {
    pub tls: &'a TlsConnector,
    pub timeouts: ConnectTimeouts,
    pub deadlines: Deadlines,
    pub h2: &'a H2Config,
    pub prior_knowledge_h2: bool,
}

pub fn spawn_h2<S>(io: S, config: H2Config) -> H2Client
where
    S: futures_lite::io::AsyncRead + futures_lite::io::AsyncWrite + Unpin + Send + 'static,
{
    let (client, start) = H2Client::new(config);

    tokio::spawn(start.run(io, sleep));

    client
}

pub async fn connect(origin: &Origin, plan: &ConnectPlan<'_>) -> io::Result<Connected> {
    let socket = open_socket(origin, plan.timeouts).await?;

    socket.set_nodelay(true)?;

    if !origin.tls {
        if plan.prior_knowledge_h2 {
            return Ok(Connected::Http2(spawn_h2(Compat(socket), plan.h2.clone())));
        }

        return Ok(Connected::Http1(Box::new(Connection::new(
            socket,
            None,
            None,
            plan.deadlines,
        ))));
    }

    let state = plan
        .tls
        .connect(&origin.host)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let mut stream = TlsStream::new(Compat(socket), state);

    match timeout(plan.timeouts.handshake, stream.handshake()).await {
        Ok(done) => done?,
        Err(_) => return Err(timed_out("tls handshake")),
    }

    let alpn = stream.alpn().map(<[u8]>::to_vec);

    if alpn.as_deref() == Some(b"h2".as_slice()) {
        return Ok(Connected::Http2(spawn_h2(stream, plan.h2.clone())));
    }

    let (socket, tls) = stream.into_parts();

    Ok(Connected::Http1(Box::new(Connection::new(
        socket.0,
        Some(tls),
        alpn,
        plan.deadlines,
    ))))
}

pub async fn connect_h2(addr: SocketAddr, config: H2Config) -> io::Result<H2Client> {
    let socket = TcpStream::connect(addr).await?;

    socket.set_nodelay(true)?;

    Ok(spawn_h2(Compat(socket), config))
}

async fn open_socket(origin: &Origin, timeouts: ConnectTimeouts) -> io::Result<TcpStream> {
    let deadline = Instant::now() + timeouts.connect;

    let addrs = match origin.host.parse::<IpAddr>() {
        Ok(ip) => vec![SocketAddr::new(ip, origin.port)],
        Err(_) => {
            let found = timeout_at(deadline, lookup_host((origin.host.as_str(), origin.port)))
                .await
                .map_err(|_| timed_out("dns lookup"))??;

            interleave(found.collect())
        }
    };

    race_addrs(addrs, timeouts, deadline, TcpStream::connect).await
}

async fn race_addrs<F, Fut>(
    addrs: Vec<SocketAddr>,
    timeouts: ConnectTimeouts,
    deadline: Instant,
    connect: F,
) -> io::Result<TcpStream>
where
    F: Fn(SocketAddr) -> Fut,
    Fut: Future<Output = io::Result<TcpStream>>,
{
    let remaining = deadline.saturating_duration_since(Instant::now());

    if remaining.is_zero() {
        return Err(timed_out("connect"));
    }

    let timeouts = ConnectTimeouts {
        connect: remaining,
        ..timeouts
    };

    race_connect(addrs, timeouts, connect, sleep).await
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::net::SocketAddr;
    use std::time::Duration;

    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::{Instant, sleep};
    use veylor_client_driver::ConnectTimeouts;

    use super::race_addrs;

    const STAGGER: Duration = Duration::from_millis(50);

    fn timeouts(connect: Duration) -> ConnectTimeouts {
        ConnectTimeouts {
            connect,
            attempt: Duration::from_secs(5),
            stagger: STAGGER,
            handshake: Duration::from_secs(5),
        }
    }

    async fn listener() -> (TcpListener, SocketAddr) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let addr = listener.local_addr().expect("addr");

        (listener, addr)
    }

    async fn race(
        addrs: Vec<SocketAddr>,
        connect: Duration,
        delays: [(u16, Duration); 2],
    ) -> io::Result<TcpStream> {
        let deadline = Instant::now() + connect;

        race_addrs(addrs, timeouts(connect), deadline, move |addr| async move {
            for (port, delay) in delays {
                if addr.port() == port {
                    sleep(delay).await;
                }
            }

            TcpStream::connect(addr).await
        })
        .await
    }

    #[tokio::test]
    async fn a_blackholed_first_address_does_not_block_the_second() {
        let (_one, first) = listener().await;
        let (_two, second) = listener().await;
        let started = Instant::now();
        let delays = [
            (first.port(), Duration::from_secs(3600)),
            (0, Duration::ZERO),
        ];
        let socket = race(vec![first, second], Duration::from_secs(5), delays)
            .await
            .expect("connected");

        assert_eq!(socket.peer_addr().expect("peer"), second);
        assert!(started.elapsed() >= STAGGER);
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn an_earlier_attempt_keeps_running_and_can_still_win() {
        let (_one, first) = listener().await;
        let (_two, second) = listener().await;
        let delays = [
            (first.port(), Duration::from_millis(120)),
            (second.port(), Duration::from_millis(600)),
        ];
        let socket = race(vec![first, second], Duration::from_secs(5), delays)
            .await
            .expect("connected");

        assert_eq!(socket.peer_addr().expect("peer"), first);
    }

    #[tokio::test]
    async fn a_failed_attempt_starts_the_next_one_immediately() {
        let (dead, refused) = listener().await;
        let (_live, second) = listener().await;

        drop(dead);

        let started = Instant::now();
        let delays = [(0, Duration::ZERO), (0, Duration::ZERO)];
        let socket = race(vec![refused, second], Duration::from_secs(5), delays)
            .await
            .expect("connected");

        assert_eq!(socket.peer_addr().expect("peer"), second);
        assert!(started.elapsed() < STAGGER);
    }

    #[tokio::test]
    async fn the_overall_deadline_bounds_a_hopeless_race() {
        let (_one, first) = listener().await;
        let (_two, second) = listener().await;
        let hour = Duration::from_secs(3600);
        let delays = [(first.port(), hour), (second.port(), hour)];
        let error = race(vec![first, second], Duration::from_millis(300), delays)
            .await
            .expect_err("must fail");

        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }
}
