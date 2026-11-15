use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::time::Instant;

use smol::{Async, Timer};
use veylor_client_driver::{
    ConnectTimeouts, Deadlines, H2Client, H2Config, Origin, TlsConnector, TlsStream, interleave,
    race_connect, timed_out,
};

use crate::client_conn::Connection;
use crate::deadline::within;

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
    S: smol::io::AsyncRead + smol::io::AsyncWrite + Unpin + Send + 'static,
{
    let (client, start) = H2Client::new(config);

    smol::spawn(start.run(io, |delay| async move {
        Timer::after(delay).await;
    }))
    .detach();

    client
}

pub async fn connect(origin: &Origin, plan: &ConnectPlan<'_>) -> io::Result<Connected> {
    let socket = open_socket(origin, plan.timeouts).await?;

    socket.get_ref().set_nodelay(true)?;

    if !origin.tls {
        if plan.prior_knowledge_h2 {
            return Ok(Connected::Http2(spawn_h2(socket, plan.h2.clone())));
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
    let mut stream = TlsStream::new(socket, state);

    within(plan.timeouts.handshake, "tls handshake", stream.handshake()).await?;

    let alpn = stream.alpn().map(<[u8]>::to_vec);

    if alpn.as_deref() == Some(b"h2".as_slice()) {
        return Ok(Connected::Http2(spawn_h2(stream, plan.h2.clone())));
    }

    let (socket, tls) = stream.into_parts();

    Ok(Connected::Http1(Box::new(Connection::new(
        socket,
        Some(tls),
        alpn,
        plan.deadlines,
    ))))
}

pub async fn connect_h2(addr: SocketAddr, config: H2Config) -> io::Result<H2Client> {
    let socket = Async::<TcpStream>::connect(addr).await?;

    socket.get_ref().set_nodelay(true)?;

    Ok(spawn_h2(socket, config))
}

async fn open_socket(origin: &Origin, timeouts: ConnectTimeouts) -> io::Result<Async<TcpStream>> {
    let deadline = Instant::now() + timeouts.connect;

    let addrs = match origin.host.parse::<IpAddr>() {
        Ok(ip) => vec![SocketAddr::new(ip, origin.port)],
        Err(_) => {
            let lookup = async {
                smol::net::resolve((origin.host.as_str(), origin.port))
                    .await
                    .map(interleave)
            };

            within(timeouts.connect, "dns lookup", lookup).await?
        }
    };

    race_addrs(addrs, timeouts, deadline, Async::<TcpStream>::connect).await
}

async fn race_addrs<F, Fut>(
    addrs: Vec<SocketAddr>,
    timeouts: ConnectTimeouts,
    deadline: Instant,
    connect: F,
) -> io::Result<Async<TcpStream>>
where
    F: Fn(SocketAddr) -> Fut,
    Fut: Future<Output = io::Result<Async<TcpStream>>>,
{
    let remaining = deadline.saturating_duration_since(Instant::now());

    if remaining.is_zero() {
        return Err(timed_out("connect"));
    }

    let timeouts = ConnectTimeouts {
        connect: remaining,
        ..timeouts
    };

    race_connect(addrs, timeouts, connect, |delay| async move {
        Timer::after(delay).await;
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::time::{Duration, Instant};

    use smol::{Async, Timer};
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

    fn listener() -> (TcpListener, SocketAddr) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind");
        let addr = listener.local_addr().expect("addr");

        (listener, addr)
    }

    async fn race(
        addrs: Vec<SocketAddr>,
        connect: Duration,
        delays: [(u16, Duration); 2],
    ) -> io::Result<Async<TcpStream>> {
        let deadline = Instant::now() + connect;

        race_addrs(addrs, timeouts(connect), deadline, move |addr| async move {
            for (port, delay) in delays {
                if addr.port() == port {
                    Timer::after(delay).await;
                }
            }

            Async::<TcpStream>::connect(addr).await
        })
        .await
    }

    fn peer(socket: &Async<TcpStream>) -> SocketAddr {
        socket.get_ref().peer_addr().expect("peer")
    }

    #[test]
    fn a_blackholed_first_address_does_not_block_the_second() {
        smol::block_on(async {
            let (_one, first) = listener();
            let (_two, second) = listener();
            let started = Instant::now();
            let delays = [
                (first.port(), Duration::from_secs(3600)),
                (0, Duration::ZERO),
            ];
            let socket = race(vec![first, second], Duration::from_secs(5), delays)
                .await
                .expect("connected");

            assert_eq!(peer(&socket), second);
            assert!(started.elapsed() >= STAGGER);
            assert!(started.elapsed() < Duration::from_secs(2));
        });
    }

    #[test]
    fn an_earlier_attempt_keeps_running_and_can_still_win() {
        smol::block_on(async {
            let (_one, first) = listener();
            let (_two, second) = listener();
            let delays = [
                (first.port(), Duration::from_millis(120)),
                (second.port(), Duration::from_millis(600)),
            ];
            let socket = race(vec![first, second], Duration::from_secs(5), delays)
                .await
                .expect("connected");

            assert_eq!(peer(&socket), first);
        });
    }

    #[test]
    fn a_failed_attempt_starts_the_next_one_immediately() {
        smol::block_on(async {
            let (dead, refused) = listener();
            let (_live, second) = listener();

            drop(dead);

            let started = Instant::now();
            let delays = [(0, Duration::ZERO), (0, Duration::ZERO)];
            let socket = race(vec![refused, second], Duration::from_secs(5), delays)
                .await
                .expect("connected");

            assert_eq!(peer(&socket), second);
            assert!(started.elapsed() < STAGGER);
        });
    }

    #[test]
    fn the_overall_deadline_bounds_a_hopeless_race() {
        smol::block_on(async {
            let (_one, first) = listener();
            let (_two, second) = listener();
            let hour = Duration::from_secs(3600);
            let delays = [(first.port(), hour), (second.port(), hour)];
            let error = race(vec![first, second], Duration::from_millis(300), delays)
                .await
                .expect_err("must fail");

            assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        });
    }
}
