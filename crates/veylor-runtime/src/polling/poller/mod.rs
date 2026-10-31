use std::io;
use std::time::Duration;

use mio::{Events, Interest, Poll as MioPoll, Token};

use crate::timers::TimerQueue;

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
mod uring_backend;

pub const WAKE_TOKEN: usize = usize::MAX;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PollBackend {
    #[default]
    Mio,
    #[cfg(target_os = "linux")]
    IoUring,
}

#[derive(Clone)]
pub enum WakeHandle {
    Mio(std::sync::Arc<mio::Waker>),
    #[cfg(target_os = "linux")]
    Uring(uring_backend::UringWake),
}

impl WakeHandle {
    pub fn wake(&self) -> io::Result<()> {
        match self {
            Self::Mio(waker) => waker.wake(),
            #[cfg(target_os = "linux")]
            Self::Uring(wake) => wake.wake(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PollInterest {
    Readable,
    Writable,
    ReadWrite,
}

impl PollInterest {
    pub(crate) fn mio(self) -> Interest {
        match self {
            Self::Readable => Interest::READABLE,
            Self::Writable => Interest::WRITABLE,
            Self::ReadWrite => Interest::READABLE | Interest::WRITABLE,
        }
    }

    #[cfg(target_os = "linux")]
    fn poll_mask(self) -> i16 {
        match self {
            Self::Readable => libc::POLLIN,
            Self::Writable => libc::POLLOUT,
            Self::ReadWrite => libc::POLLIN | libc::POLLOUT,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Readiness {
    pub token: usize,
    pub readable: bool,
    pub writable: bool,
    pub read_closed_hint: bool,
    pub write_closed_hint: bool,
    pub error_hint: bool,
}

pub trait PollSource {
    fn register_mio(
        &mut self,
        registry: &mio::Registry,
        token: Token,
        interest: Interest,
    ) -> io::Result<()>;

    fn reregister_mio(
        &mut self,
        registry: &mio::Registry,
        token: Token,
        interest: Interest,
    ) -> io::Result<()>;

    fn deregister_mio(&mut self, registry: &mio::Registry) -> io::Result<()>;

    #[cfg(target_os = "linux")]
    fn raw_fd(&self) -> std::os::fd::RawFd;
}

macro_rules! impl_poll_source {
    ($type:ty) => {
        impl PollSource for $type {
            fn register_mio(
                &mut self,
                registry: &mio::Registry,
                token: Token,
                interest: Interest,
            ) -> io::Result<()> {
                registry.register(self, token, interest)
            }

            fn reregister_mio(
                &mut self,
                registry: &mio::Registry,
                token: Token,
                interest: Interest,
            ) -> io::Result<()> {
                registry.reregister(self, token, interest)
            }

            fn deregister_mio(&mut self, registry: &mio::Registry) -> io::Result<()> {
                registry.deregister(self)
            }

            #[cfg(target_os = "linux")]
            fn raw_fd(&self) -> std::os::fd::RawFd {
                use std::os::fd::AsRawFd;
                self.as_raw_fd()
            }
        }
    };
}

impl_poll_source!(mio::net::TcpStream);
impl_poll_source!(mio::net::TcpListener);

pub struct Poller {
    backend: Backend,
    ready: Vec<Readiness>,
}

enum Backend {
    Mio(MioBackend),
    #[cfg(target_os = "linux")]
    Uring(Box<uring_backend::UringBackend>),
}

struct MioBackend {
    poller: MioPoll,
    events: Events,
    waker: Option<std::sync::Arc<mio::Waker>>,
}

impl Poller {
    pub fn new(event_capacity: usize) -> io::Result<Self> {
        Self::new_with_backend(event_capacity, PollBackend::Mio)
    }

    pub fn new_with_backend(event_capacity: usize, backend: PollBackend) -> io::Result<Self> {
        if event_capacity == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "event capacity must be nonzero",
            ));
        }

        let backend = match backend {
            PollBackend::Mio => {
                let poller = MioPoll::new()?;
                Backend::Mio(MioBackend {
                    poller,
                    events: Events::with_capacity(event_capacity),
                    waker: None,
                })
            }
            #[cfg(target_os = "linux")]
            PollBackend::IoUring => {
                Backend::Uring(Box::new(uring_backend::UringBackend::new(event_capacity)?))
            }
        };

        Ok(Self {
            backend,
            ready: Vec::with_capacity(event_capacity),
        })
    }

    pub fn register<S: PollSource + ?Sized>(
        &mut self,
        source: &mut S,
        token: usize,
        interest: PollInterest,
    ) -> io::Result<()> {
        validate_token(token)?;
        match &mut self.backend {
            Backend::Mio(backend) => {
                source.register_mio(backend.poller.registry(), Token(token), interest.mio())
            }
            #[cfg(target_os = "linux")]
            Backend::Uring(backend) => backend.register(source.raw_fd(), token, interest),
        }
    }

    pub fn reregister<S: PollSource + ?Sized>(
        &mut self,
        source: &mut S,
        token: usize,
        interest: PollInterest,
    ) -> io::Result<()> {
        validate_token(token)?;
        match &mut self.backend {
            Backend::Mio(backend) => {
                source.reregister_mio(backend.poller.registry(), Token(token), interest.mio())
            }
            #[cfg(target_os = "linux")]
            Backend::Uring(backend) => backend.reregister(source.raw_fd(), token, interest),
        }
    }

    pub fn deregister<S: PollSource + ?Sized>(&mut self, source: &mut S) -> io::Result<()> {
        match &mut self.backend {
            Backend::Mio(backend) => source.deregister_mio(backend.poller.registry()),
            #[cfg(target_os = "linux")]
            Backend::Uring(backend) => backend.deregister(source.raw_fd()),
        }
    }

    pub fn poll(
        &mut self,
        timeout: Option<Duration>,
    ) -> io::Result<impl Iterator<Item = Readiness> + '_> {
        self.ready.clear();
        match &mut self.backend {
            Backend::Mio(backend) => {
                backend.poller.poll(&mut backend.events, timeout)?;
                self.ready
                    .extend(backend.events.iter().map(|event| Readiness {
                        token: event.token().0,
                        readable: event.is_readable(),
                        writable: event.is_writable(),
                        read_closed_hint: event.is_read_closed(),
                        write_closed_hint: event.is_write_closed(),
                        error_hint: event.is_error(),
                    }));
            }
            #[cfg(target_os = "linux")]
            Backend::Uring(backend) => backend.poll(timeout, &mut self.ready)?,
        }
        Ok(self.ready.iter().copied())
    }

    pub fn poll_with_timers(
        &mut self,
        timers: &TimerQueue<'_, '_>,
        now_ns: u64,
        max_wait_ms: i32,
    ) -> io::Result<impl Iterator<Item = Readiness> + '_> {
        let wait_ms = timers.wait_timeout_ms(now_ns, max_wait_ms);
        let timeout = if wait_ms < 0 {
            None
        } else {
            Some(Duration::from_millis(wait_ms as u64))
        };

        self.poll(timeout)
    }

    pub fn event_capacity(&self) -> usize {
        self.ready.capacity()
    }

    pub fn create_waker(&mut self) -> io::Result<WakeHandle> {
        match &mut self.backend {
            Backend::Mio(backend) => {
                if backend.waker.is_some() {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "poller already has a waker",
                    ));
                }
                let waker = std::sync::Arc::new(mio::Waker::new(
                    backend.poller.registry(),
                    Token(WAKE_TOKEN),
                )?);
                backend.waker = Some(std::sync::Arc::clone(&waker));
                Ok(WakeHandle::Mio(waker))
            }
            #[cfg(target_os = "linux")]
            Backend::Uring(backend) => backend.create_waker(),
        }
    }
}

fn validate_token(token: usize) -> io::Result<()> {
    if token == WAKE_TOKEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "token is reserved for poller wake events",
        ));
    }
    Ok(())
}
