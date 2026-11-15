use std::io;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use tokio::net::{TcpStream, lookup_host};
use tokio::time::{Instant, timeout, timeout_at};
use veylor_tls::TlsConnector;

use crate::client_conn::{Connection, Deadlines, timed_out};
use crate::client_url::Origin;
use crate::h2_client::{H2Client, H2Config};
use crate::tls_stream::TlsStream;

#[derive(Clone, Copy)]
pub struct ConnectTimeouts {
    pub connect: Duration,
    pub attempt: Duration,
    pub handshake: Duration,
}

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

pub async fn connect(origin: &Origin, plan: &ConnectPlan<'_>) -> io::Result<Connected> {
    let socket = open_socket(origin, plan.timeouts).await?;

    socket.set_nodelay(true)?;

    if !origin.tls {
        if plan.prior_knowledge_h2 {
            return Ok(Connected::Http2(H2Client::with_io(socket, plan.h2.clone())));
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

    match timeout(plan.timeouts.handshake, stream.handshake()).await {
        Ok(done) => done?,
        Err(_) => return Err(timed_out("tls handshake")),
    }

    let alpn = stream.alpn().map(<[u8]>::to_vec);

    if alpn.as_deref() == Some(b"h2".as_slice()) {
        return Ok(Connected::Http2(H2Client::with_io(stream, plan.h2.clone())));
    }

    let (socket, tls) = stream.into_parts();

    Ok(Connected::Http1(Box::new(Connection::new(
        socket,
        Some(tls),
        alpn,
        plan.deadlines,
    ))))
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
        let attempt = timeouts
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

fn interleave(addrs: Vec<SocketAddr>) -> Vec<SocketAddr> {
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
