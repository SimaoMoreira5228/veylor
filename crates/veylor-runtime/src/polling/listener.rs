use std::io;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::sync::Arc;

use mio::net::{TcpListener as MioTcpListener, TcpStream};
use mio::{Interest, Token};

use super::{AcceptGate, PollSource};

// NOTE: the managed server hands the same listener to the poller and to the accept
// path, and readiness only exists on the poller side; carrying the gate on the
// listener is what lets one readiness event buy exactly one accept syscall.
pub struct TcpListener {
    inner: MioTcpListener,
    gate: Arc<AcceptGate>,
}

impl TcpListener {
    pub fn bind(address: SocketAddr) -> io::Result<Self> {
        Ok(Self::from_mio(MioTcpListener::bind(address)?))
    }

    pub fn from_std(listener: StdTcpListener) -> Self {
        Self::from_mio(MioTcpListener::from_std(listener))
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    pub fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        self.inner.accept()
    }

    pub(crate) fn gate(&self) -> &AcceptGate {
        &self.gate
    }

    fn from_mio(inner: MioTcpListener) -> Self {
        Self {
            inner,
            gate: Arc::new(AcceptGate::new()),
        }
    }
}

impl PollSource for TcpListener {
    fn register_mio(
        &mut self,
        registry: &mio::Registry,
        token: Token,
        interest: Interest,
    ) -> io::Result<()> {
        registry.register(&mut self.inner, token, interest)
    }

    fn reregister_mio(
        &mut self,
        registry: &mio::Registry,
        token: Token,
        interest: Interest,
    ) -> io::Result<()> {
        registry.reregister(&mut self.inner, token, interest)
    }

    fn deregister_mio(&mut self, registry: &mio::Registry) -> io::Result<()> {
        registry.deregister(&mut self.inner)
    }

    fn accept_gate(&self) -> Option<Arc<AcceptGate>> {
        Some(Arc::clone(&self.gate))
    }

    #[cfg(target_os = "linux")]
    fn raw_fd(&self) -> std::os::fd::RawFd {
        use std::os::fd::AsRawFd;
        self.inner.as_raw_fd()
    }
}
