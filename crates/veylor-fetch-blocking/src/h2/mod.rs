mod driver;
mod fields;
mod inbound;

use std::io;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

use crate::budget::Budget;
use crate::error::Error;
use crate::headers::Headers;
use crate::outgoing::Outgoing;
use crate::wire::{Peek, Wire, peek};

pub(crate) const BODY_BUFFER_CAP: usize = 128 * 1024;

#[derive(Clone, Copy)]
pub(crate) struct H2Settings {
    pub(crate) max_head: usize,
    pub(crate) idle_timeout: Duration,
    pub(crate) handshake_timeout: Duration,
}

pub(super) struct Command {
    pub(super) method: String,
    pub(super) authority: String,
    pub(super) path: String,
    pub(super) headers: Vec<(String, String)>,
    pub(super) body: Vec<u8>,
    pub(super) expects_body: bool,
    pub(super) budget: Budget,
}

#[derive(Clone, Debug)]
pub(super) struct Failure {
    pub(super) kind: io::ErrorKind,
    pub(super) message: String,
}

impl Failure {
    pub(super) fn new(kind: io::ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub(super) fn to_io(&self) -> io::Error {
        io::Error::new(self.kind, self.message.clone())
    }
}

#[derive(Default)]
pub(super) struct State {
    pub(super) command: Option<Command>,
    pub(super) head: Option<(u16, Headers)>,
    pub(super) head_seen: bool,
    pub(super) body: Vec<u8>,
    pub(super) body_pos: usize,
    pub(super) ended: bool,
    pub(super) failure: Option<Failure>,
    pub(super) shutdown: bool,
    pub(super) dead: bool,
    pub(super) idle: bool,
}

pub(super) struct Shared {
    pub(super) state: Mutex<State>,
    pub(super) wake: Condvar,
}

pub(crate) struct H2Handle {
    shared: Arc<Shared>,
    probe: TcpStream,
    busy: AtomicBool,
}

pub(crate) struct Refused {
    pub(crate) error: io::Error,
    pub(crate) retryable: bool,
}

impl H2Handle {
    pub(crate) fn spawn(wire: Wire, settings: H2Settings) -> Result<Arc<Self>, Error> {
        let probe = wire.probe().map_err(Error::Io)?;
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                idle: true,
                ..State::default()
            }),
            wake: Condvar::new(),
        });
        let worker = Arc::clone(&shared);

        std::thread::Builder::new()
            .name("veylor-fetch-h2".to_owned())
            .spawn(move || driver::run(wire, worker, settings))
            .map_err(Error::Io)?;

        Ok(Arc::new(Self {
            shared,
            probe,
            busy: AtomicBool::new(true),
        }))
    }

    pub(crate) fn try_acquire(&self) -> bool {
        if self
            .busy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }

        if self.is_usable() {
            return true;
        }

        self.busy.store(false, Ordering::Release);

        false
    }

    pub(crate) fn release(&self) {
        self.busy.store(false, Ordering::Release);
    }

    pub(crate) fn is_dead(&self) -> bool {
        let state = self.shared.state.lock();

        state.dead || state.shutdown
    }

    fn is_usable(&self) -> bool {
        let idle = {
            let state = self.shared.state.lock();

            if state.dead || state.shutdown {
                return false;
            }

            state.idle
        };

        !idle || peek(&self.probe) != Peek::Gone
    }

    pub(crate) fn request(
        &self,
        request: &Outgoing<'_>,
        authority: &str,
        budget: &Budget,
    ) -> Result<(u16, Headers), Refused> {
        let command = Command {
            method: request.method.to_owned(),
            authority: authority.to_owned(),
            path: request.target.to_owned(),
            headers: fields::lowered(request.headers),
            body: request.body.to_vec(),
            expects_body: request.expects_body(),
            budget: *budget,
        };

        {
            let mut state = self.shared.state.lock();

            if state.dead || state.shutdown {
                return Err(Refused {
                    error: io::Error::new(io::ErrorKind::ConnectionAborted, "h2 connection closed"),
                    retryable: true,
                });
            }

            state.command = Some(command);
            state.head = None;
            state.head_seen = false;
            state.body.clear();
            state.body_pos = 0;
            state.ended = false;
            state.failure = None;
            state.idle = false;
        }

        self.shared.wake.notify_all();

        let mut state = self.shared.state.lock();

        loop {
            if let Some(head) = state.head.take() {
                return Ok(head);
            }

            if let Some(failure) = &state.failure {
                return Err(Refused {
                    error: failure.to_io(),
                    retryable: !state.head_seen,
                });
            }

            if self.wait(&mut state, budget).is_err() {
                state.shutdown = true;
                self.shared.wake.notify_all();

                return Err(Refused {
                    error: io::ErrorKind::TimedOut.into(),
                    retryable: false,
                });
            }
        }
    }

    pub(crate) fn read(&self, buf: &mut [u8], budget: &Budget) -> io::Result<usize> {
        let mut state = self.shared.state.lock();

        loop {
            if state.body_pos < state.body.len() {
                let available = &state.body[state.body_pos..];
                let count = available.len().min(buf.len());

                buf[..count].copy_from_slice(&available[..count]);
                state.body_pos += count;

                if state.body_pos == state.body.len() {
                    state.body.clear();
                    state.body_pos = 0;
                }

                self.shared.wake.notify_all();

                return Ok(count);
            }

            if state.ended {
                return Ok(0);
            }

            if let Some(failure) = &state.failure {
                return Err(failure.to_io());
            }

            if self.wait(&mut state, budget).is_err() {
                state.shutdown = true;
                self.shared.wake.notify_all();

                return Err(io::ErrorKind::TimedOut.into());
            }
        }
    }

    pub(crate) fn is_finished(&self) -> bool {
        let state = self.shared.state.lock();

        state.ended && state.body_pos >= state.body.len()
    }

    pub(crate) fn abandon(&self) {
        self.shared.state.lock().shutdown = true;
        self.shared.wake.notify_all();
    }

    fn wait(
        &self,
        state: &mut parking_lot::MutexGuard<'_, State>,
        budget: &Budget,
    ) -> Result<(), ()> {
        let Some(deadline) = budget.overall else {
            self.shared.wake.wait(state);

            return Ok(());
        };

        if Instant::now() >= deadline {
            return Err(());
        }

        self.shared.wake.wait_until(state, deadline);

        Ok(())
    }
}

impl Drop for H2Handle {
    fn drop(&mut self) {
        self.shared.state.lock().shutdown = true;
        self.shared.wake.notify_all();
    }
}
