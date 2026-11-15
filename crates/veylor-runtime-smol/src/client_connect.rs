use std::io;
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use smol::{Async, Timer};
use veylor_client_driver::{
    ConnectTimeouts, Deadlines, H2Client, H2Config, Origin, TlsConnector, TlsStream, interleave,
    timed_out,
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

    let mut last = io::Error::new(io::ErrorKind::NotFound, "host resolved to no addresses");

    for addr in addrs {
        let attempt: Duration = timeouts
            .attempt
            .min(deadline.saturating_duration_since(Instant::now()));

        if attempt.is_zero() {
            return Err(timed_out("connect"));
        }

        match within(attempt, "connect", Async::<TcpStream>::connect(addr)).await {
            Ok(socket) => return Ok(socket),
            Err(error) => last = error,
        }
    }

    Err(last)
}
