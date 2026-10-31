use std::collections::HashMap;
use std::collections::VecDeque;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::Arc;
use std::time::Duration;

use io_uring::squeue::Entry;
use io_uring::types::{Fd, SubmitArgs, Timespec};
use io_uring::{IoUring, opcode};

use super::{PollInterest, Readiness, WAKE_TOKEN, WakeHandle};

const WAKE_REQUEST: u64 = u64::MAX;

struct Registration {
    fd: RawFd,
    interest: PollInterest,
    request: u64,
}

pub(super) struct UringBackend {
    ring: IoUring,
    registrations: HashMap<usize, Registration>,
    requests: HashMap<u64, usize>,
    pending: VecDeque<Entry>,
    completed: Vec<(u64, i32)>,
    next_request: u64,
    wake: Option<Arc<OwnedFd>>,
}

#[derive(Clone)]
pub struct UringWake {
    fd: Arc<OwnedFd>,
}

impl UringWake {
    pub(super) fn wake(&self) -> io::Result<()> {
        let value = 1u64.to_ne_bytes();
        let written =
            unsafe { libc::write(self.fd.as_raw_fd(), value.as_ptr().cast(), value.len()) };
        if written < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

impl UringBackend {
    pub(super) fn new(event_capacity: usize) -> io::Result<Self> {
        let entries = (event_capacity.saturating_mul(4)).max(64);
        let ring = IoUring::new(entries as u32)?;
        Ok(Self {
            ring,
            registrations: HashMap::new(),
            requests: HashMap::new(),
            pending: VecDeque::new(),
            completed: Vec::with_capacity(event_capacity),
            next_request: 1,
            wake: None,
        })
    }

    pub(super) fn register(
        &mut self,
        fd: RawFd,
        token: usize,
        interest: PollInterest,
    ) -> io::Result<()> {
        self.cancel_token(token);
        let request = self.allocate_request(token);
        self.registrations.insert(
            token,
            Registration {
                fd,
                interest,
                request,
            },
        );
        self.rearm(token, request, fd, interest);
        Ok(())
    }

    pub(super) fn reregister(
        &mut self,
        fd: RawFd,
        token: usize,
        interest: PollInterest,
    ) -> io::Result<()> {
        let same = self
            .registrations
            .get(&token)
            .is_some_and(|registration| registration.fd == fd);
        if !same {
            return self.register(fd, token, interest);
        }
        self.cancel_token(token);
        let request = self.allocate_request(token);
        self.registrations.insert(
            token,
            Registration {
                fd,
                interest,
                request,
            },
        );
        self.rearm(token, request, fd, interest);
        Ok(())
    }

    pub(super) fn deregister(&mut self, fd: RawFd) -> io::Result<()> {
        let token = self
            .registrations
            .iter()
            .find_map(|(token, registration)| (registration.fd == fd).then_some(*token));
        if let Some(token) = token {
            self.cancel_token(token);
        }
        Ok(())
    }

    pub(super) fn create_waker(&mut self) -> io::Result<WakeHandle> {
        if self.wake.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "poller already has a waker",
            ));
        }
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let wake = Arc::new(unsafe { OwnedFd::from_raw_fd(fd) });
        self.wake = Some(Arc::clone(&wake));
        self.pending.push_back(
            opcode::PollAdd::new(Fd(fd), libc::POLLIN as u32)
                .build()
                .user_data(WAKE_REQUEST),
        );
        Ok(WakeHandle::Uring(UringWake { fd: wake }))
    }

    pub(super) fn poll(
        &mut self,
        timeout: Option<Duration>,
        ready: &mut Vec<Readiness>,
    ) -> io::Result<()> {
        self.flush()?;
        self.wait(timeout)?;
        self.drain(ready);
        self.flush()
    }

    fn wait(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        match timeout {
            Some(timeout) if timeout.is_zero() => {
                self.ring.submitter().submit()?;
                Ok(())
            }
            Some(timeout) => {
                let timespec = Timespec::from(timeout);
                let args = SubmitArgs::new().timespec(&timespec);
                match self.ring.submitter().submit_with_args(1, &args) {
                    Ok(_) => Ok(()),
                    Err(error) if is_wait_timeout(&error) => Ok(()),
                    Err(error) => Err(error),
                }
            }
            None => {
                self.ring.submitter().submit_and_wait(1)?;
                Ok(())
            }
        }
    }

    fn drain(&mut self, ready: &mut Vec<Readiness>) {
        self.completed.clear();
        {
            let Self {
                ring, completed, ..
            } = self;
            let mut completion = ring.completion();
            completion.sync();
            for entry in &mut completion {
                completed.push((entry.user_data(), entry.result()));
            }
        }
        let completed = std::mem::take(&mut self.completed);
        for (request, result) in &completed {
            if *request == WAKE_REQUEST {
                self.drain_wake();
                self.rearm_wake();
                if ready.len() < ready.capacity() {
                    ready.push(Readiness {
                        token: WAKE_TOKEN,
                        readable: true,
                        writable: false,
                        read_closed_hint: false,
                        write_closed_hint: false,
                        error_hint: false,
                    });
                }
                continue;
            }
            let Some(token) = self.requests.remove(request) else {
                continue;
            };
            let Some(registration) = self.registrations.get(&token) else {
                continue;
            };
            if registration.request != *request {
                continue;
            }
            let fd = registration.fd;
            let interest = registration.interest;
            if *result < 0 {
                let error = io::Error::from_raw_os_error(-*result);
                if ready.len() < ready.capacity() {
                    ready.push(Readiness {
                        token,
                        readable: false,
                        writable: false,
                        read_closed_hint: false,
                        write_closed_hint: false,
                        error_hint: true,
                    });
                }
                if matches!(error.raw_os_error(), Some(libc::EBADF) | Some(libc::ENOENT)) {
                    self.registrations.remove(&token);
                }
                let rerequest = self.allocate_request(token);
                if let Some(registration) = self.registrations.get_mut(&token) {
                    registration.request = rerequest;
                    self.rearm(token, rerequest, fd, interest);
                }
                continue;
            }
            let mask = *result;
            if ready.len() < ready.capacity() {
                ready.push(readiness(token, mask));
            }
            let rerequest = self.allocate_request(token);
            if let Some(registration) = self.registrations.get_mut(&token) {
                registration.request = rerequest;
            }
            self.rearm(token, rerequest, fd, interest);
        }
        self.completed = completed;
    }

    fn rearm(&mut self, token: usize, request: u64, fd: RawFd, interest: PollInterest) {
        self.requests.insert(request, token);
        self.pending.push_back(
            opcode::PollAdd::new(Fd(fd), interest.poll_mask() as u32)
                .build()
                .user_data(request),
        );
    }

    fn rearm_wake(&mut self) {
        let Some(wake) = &self.wake else {
            return;
        };
        self.pending.push_back(
            opcode::PollAdd::new(Fd(wake.as_raw_fd()), libc::POLLIN as u32)
                .build()
                .user_data(WAKE_REQUEST),
        );
    }

    fn drain_wake(&self) {
        let Some(wake) = &self.wake else {
            return;
        };
        let mut value = 0u64;
        loop {
            let read = unsafe {
                libc::read(
                    wake.as_raw_fd(),
                    (&mut value as *mut u64).cast(),
                    std::mem::size_of::<u64>(),
                )
            };
            if read <= 0 {
                return;
            }
        }
    }

    fn cancel_token(&mut self, token: usize) {
        let Some(registration) = self.registrations.remove(&token) else {
            return;
        };
        self.requests.remove(&registration.request);
        self.pending.push_back(
            opcode::PollRemove::new(registration.request)
                .build()
                .user_data(registration.request),
        );
    }

    fn allocate_request(&mut self, token: usize) -> u64 {
        let request = self.next_request;
        self.next_request = self.next_request.wrapping_add(1).max(1);
        self.requests.insert(request, token);
        request
    }

    fn flush(&mut self) -> io::Result<()> {
        loop {
            {
                let mut queue = self.ring.submission();
                while let Some(entry) = self.pending.front() {
                    if unsafe { queue.push(entry) }.is_err() {
                        break;
                    }
                    self.pending.pop_front();
                }
                queue.sync();
            }
            if self.pending.is_empty() {
                return Ok(());
            }
            self.ring.submitter().submit()?;
        }
    }
}

fn readiness(token: usize, mask: i32) -> Readiness {
    Readiness {
        token,
        readable: mask & i32::from(libc::POLLIN) != 0,
        writable: mask & i32::from(libc::POLLOUT) != 0,
        read_closed_hint: mask & i32::from(libc::POLLHUP | libc::POLLRDHUP) != 0,
        write_closed_hint: mask & i32::from(libc::POLLHUP) != 0,
        error_hint: mask & i32::from(libc::POLLERR) != 0,
    }
}

fn is_wait_timeout(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::ETIME) | Some(libc::EINTR) | Some(libc::EAGAIN)
    )
}
