use std::sync::atomic::{AtomicU8, Ordering};

const UNATTACHED: u8 = 0;
const ARMED: u8 = 1;
const SPENT: u8 = 2;

// NOTE: readiness is edge-triggered, so a listener the poller never armed has no
// queued connection behind it; consuming the arm keeps the accept path at one
// syscall per readiness event instead of one per turn. A listener that is not
// registered on a poller stays unattached and every accept attempt goes through,
// because nothing else can vouch for an empty backlog there.
#[derive(Debug, Default)]
pub struct AcceptGate {
    state: AtomicU8,
}

// NOTE: only the worker thread that owns the listener touches the gate, so the
// atomics need atomicity and no ordering.
impl AcceptGate {
    pub const fn new() -> Self {
        Self {
            state: AtomicU8::new(UNATTACHED),
        }
    }

    // NOTE: attaching buys one attempt: a connection can land between bind and the
    // first poll, and an edge-triggered registration does not promise to report what
    // was already queued at registration time.
    pub(crate) fn attach(&self) {
        self.state.store(ARMED, Ordering::Relaxed);
    }

    pub(crate) fn detach(&self) {
        self.state.store(UNATTACHED, Ordering::Relaxed);
    }

    pub(crate) fn arm(&self) {
        self.state.store(ARMED, Ordering::Relaxed);
    }

    // NOTE: a batch that stopped short of EAGAIN re-arms the gate because the
    // backlog is still non-empty and no further readiness edge will arrive.
    pub(crate) fn consume(&self) -> bool {
        match self.state.load(Ordering::Relaxed) {
            UNATTACHED => true,
            ARMED => {
                self.state.store(SPENT, Ordering::Relaxed);
                true
            }
            _ => false,
        }
    }

    #[cfg(test)]
    pub(crate) fn armed(&self) -> bool {
        self.state.load(Ordering::Relaxed) == ARMED
    }
}
