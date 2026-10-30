use std::io;
use std::net::SocketAddr;

use super::TcpListener;

#[cfg(unix)]
pub fn bind_reuse_port(addr: SocketAddr) -> io::Result<TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};
    use std::os::fd::AsFd;
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    rustix::net::sockopt::set_socket_reuseport(socket.as_fd(), true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    Ok(TcpListener::from_std(socket.into()))
}

#[cfg(not(unix))]
pub fn bind_reuse_port(_addr: SocketAddr) -> io::Result<TcpListener> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "reuse-port listener requires a Unix target; use worker_count = 1",
    ))
}
