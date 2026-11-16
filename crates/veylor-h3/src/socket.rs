use std::io;
use std::net::{SocketAddr, UdpSocket};

const RECEIVE_BUFFER: usize = 4 * 1024 * 1024;
const SEND_BUFFER: usize = 1024 * 1024;

// NOTE: the kernel default is 208 KiB per direction. On loopback at 1.5M requests/s
// the server socket held a 30-76 KiB receive queue and dropped 0.9% of its datagrams,
// and each datagram carries about ten requests.
pub(crate) fn bind_udp(address: SocketAddr) -> io::Result<UdpSocket> {
    let socket = UdpSocket::bind(address)?;
    socket.set_nonblocking(true)?;
    let settings = socket2::SockRef::from(&socket);
    settings.set_recv_buffer_size(RECEIVE_BUFFER)?;
    settings.set_send_buffer_size(SEND_BUFFER)?;
    Ok(socket)
}
