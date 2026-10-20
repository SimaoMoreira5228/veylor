use std::io;
#[cfg(not(any(target_os = "wasi", target_os = "horizon")))]
use std::sync::Arc;
use std::time::Duration;

#[cfg(not(any(target_os = "wasi", target_os = "horizon")))]
use mio::Waker;
use mio::event::Source;
use mio::{Events, Interest, Poll as MioPoll, Token};

use crate::timers::TimerQueue;

pub const WAKE_TOKEN: usize = usize::MAX;

#[cfg(not(any(target_os = "wasi", target_os = "horizon")))]
#[derive(Clone)]
pub struct WakeHandle {
    waker: Arc<Waker>,
}

#[cfg(not(any(target_os = "wasi", target_os = "horizon")))]
impl WakeHandle {
    pub fn wake(&self) -> io::Result<()> {
        self.waker.wake()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PollInterest {
    Readable,
    Writable,
    ReadWrite,
}

impl PollInterest {
    fn mio(self) -> Interest {
        match self {
            Self::Readable => Interest::READABLE,
            Self::Writable => Interest::WRITABLE,
            Self::ReadWrite => Interest::READABLE | Interest::WRITABLE,
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

pub struct Poller {
    poller: MioPoll,
    events: Events,
    #[cfg(not(any(target_os = "wasi", target_os = "horizon")))]
    waker: Option<Arc<Waker>>,
}

impl Poller {
    pub fn new(event_capacity: usize) -> io::Result<Self> {
        if event_capacity == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "event capacity must be nonzero",
            ));
        }

        let poller = MioPoll::new()?;
        Ok(Self {
            poller,
            events: Events::with_capacity(event_capacity),
            #[cfg(not(any(target_os = "wasi", target_os = "horizon")))]
            waker: None,
        })
    }

    pub fn register<S: Source + ?Sized>(
        &self,
        source: &mut S,
        token: usize,
        interest: PollInterest,
    ) -> io::Result<()> {
        validate_token(token)?;
        self.poller
            .registry()
            .register(source, Token(token), interest.mio())
    }

    pub fn reregister<S: Source + ?Sized>(
        &self,
        source: &mut S,
        token: usize,
        interest: PollInterest,
    ) -> io::Result<()> {
        validate_token(token)?;
        self.poller
            .registry()
            .reregister(source, Token(token), interest.mio())
    }

    pub fn deregister<S: Source + ?Sized>(&self, source: &mut S) -> io::Result<()> {
        self.poller.registry().deregister(source)
    }

    pub fn poll(
        &mut self,
        timeout: Option<Duration>,
    ) -> io::Result<impl Iterator<Item = Readiness> + '_> {
        self.poller.poll(&mut self.events, timeout)?;
        Ok(self.events.iter().map(|event| Readiness {
            token: event.token().0,
            readable: event.is_readable(),
            writable: event.is_writable(),
            read_closed_hint: event.is_read_closed(),
            write_closed_hint: event.is_write_closed(),
            error_hint: event.is_error(),
        }))
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
        self.events.capacity()
    }

    #[cfg(not(any(target_os = "wasi", target_os = "horizon")))]
    pub fn create_waker(&mut self) -> io::Result<WakeHandle> {
        if self.waker.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "poller already has a waker",
            ));
        }
        let waker = Arc::new(Waker::new(self.poller.registry(), Token(WAKE_TOKEN))?);
        self.waker = Some(Arc::clone(&waker));
        Ok(WakeHandle { waker })
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
