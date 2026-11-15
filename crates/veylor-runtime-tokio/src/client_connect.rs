use std::io;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use tokio::net::{TcpStream, lookup_host};
use tokio::time::{Instant, sleep, timeout, timeout_at};
use veylor_client_driver::{
    ConnectTimeouts, Deadlines, H2Client, H2Config, Origin, TlsConnector, TlsStream, interleave,
    timed_out,
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

    let mut last = io::Error::new(io::ErrorKind::NotFound, "host resolved to no addresses");

    for addr in addrs {
        let attempt: Duration = timeouts
            .attempt
            .min(deadline.saturating_duration_since(Instant::now()));

        if attempt.is_zero() {
            return Err(timed_out("connect"));
        }

        match timeout(attempt, TcpStream::connect(addr)).await {
            Ok(Ok(socket)) => return Ok(socket),
            Ok(Err(error)) => last = error,
            Err(_) => last = timed_out("connect"),
        }
    }

    Err(last)
}
