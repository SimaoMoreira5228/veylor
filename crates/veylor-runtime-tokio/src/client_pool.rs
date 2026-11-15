use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use parking_lot::{Mutex, MutexGuard};
use tokio::sync::Notify;
use tokio::time::timeout_at;

use crate::client_conn::{Connection, timed_out};
use crate::client_url::Origin;
use crate::h2_client::H2Client;

const PRUNE_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Protocol {
    Http1,
    Http2,
}

#[derive(Clone, Debug)]
pub struct PoolConfig {
    pub max_idle_per_origin: usize,
    pub max_idle_total: usize,
    pub max_per_origin: usize,
    pub max_h2_per_origin: usize,
    pub max_waiters_per_origin: usize,
    pub idle_timeout: Duration,
    pub acquire_timeout: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_idle_per_origin: 4,
            max_idle_total: 64,
            max_per_origin: 16,
            max_h2_per_origin: 2,
            max_waiters_per_origin: 1024,
            idle_timeout: Duration::from_secs(30),
            acquire_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PoolStats {
    pub origins: usize,
    pub open: usize,
    pub idle: usize,
    pub h2: usize,
}

struct Idle {
    conn: Box<Connection>,
    since: Instant,
}

struct H2Load {
    inflight: AtomicUsize,
    last_ms: AtomicU64,
    epoch: Instant,
}

struct H2Slot {
    client: H2Client,
    load: Arc<H2Load>,
}

struct Entry {
    idle: Vec<Idle>,
    open: usize,
    h2: Vec<H2Slot>,
    gated: bool,
    known: Option<Protocol>,
    waiters: Arc<AtomicUsize>,
    notify: Arc<Notify>,
}

impl Entry {
    fn new() -> Self {
        Self {
            idle: Vec::new(),
            open: 0,
            h2: Vec::new(),
            gated: false,
            known: None,
            waiters: Arc::new(AtomicUsize::new(0)),
            notify: Arc::new(Notify::new()),
        }
    }

    fn is_empty(&self) -> bool {
        self.open == 0
            && self.idle.is_empty()
            && self.h2.is_empty()
            && !self.gated
            && self.waiters.load(Ordering::Acquire) == 0
    }
}

struct State {
    origins: HashMap<Origin, Entry>,
    idle_total: usize,
    last_prune: Instant,
}

pub struct Pool {
    config: PoolConfig,
    epoch: Instant,
    state: Mutex<State>,
}

pub struct Lease {
    pool: Arc<Pool>,
    origin: Origin,
    conn: Option<Box<Connection>>,
}

pub struct H2Ticket {
    client: H2Client,
    load: Arc<H2Load>,
}

pub struct Dial {
    pool: Arc<Pool>,
    origin: Origin,
    gated: bool,
    counted: bool,
    done: bool,
}

pub enum Grant {
    Idle(Lease),
    H2(H2Ticket),
    Dial(Dial),
}

struct WaitGuard(Arc<AtomicUsize>);

impl Drop for WaitGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl H2Load {
    fn touch(&self) {
        let ms = self.epoch.elapsed().as_millis() as u64;

        self.last_ms.store(ms, Ordering::Release);
    }

    fn idle_for(&self, now: Instant) -> Option<Duration> {
        if self.inflight.load(Ordering::Acquire) > 0 {
            return None;
        }

        let used = Duration::from_millis(self.last_ms.load(Ordering::Acquire));

        Some(now.duration_since(self.epoch).saturating_sub(used))
    }
}

impl Pool {
    pub fn new(mut config: PoolConfig) -> Arc<Self> {
        config.max_per_origin = config.max_per_origin.max(1);
        config.max_h2_per_origin = config.max_h2_per_origin.max(1);

        Arc::new(Self {
            config,
            epoch: Instant::now(),
            state: Mutex::new(State {
                origins: HashMap::new(),
                idle_total: 0,
                last_prune: Instant::now(),
            }),
        })
    }

    pub fn stats(&self) -> PoolStats {
        let state = self.lock();

        PoolStats {
            origins: state.origins.len(),
            open: state.origins.values().map(|e| e.open).sum(),
            idle: state.idle_total,
            h2: state.origins.values().map(|e| e.h2.len()).sum(),
        }
    }

    pub fn close_idle(&self) {
        let mut state = self.lock();
        let before = state.idle_total;

        for entry in state.origins.values_mut() {
            entry.open -= entry.idle.len();
            entry.idle.clear();
            entry
                .h2
                .retain(|slot| slot.load.inflight.load(Ordering::Acquire) > 0);
            entry.notify.notify_waiters();
        }

        state.idle_total -= before;
        state.origins.retain(|_, entry| !entry.is_empty());
    }

    pub async fn acquire(
        self: &Arc<Self>,
        origin: &Origin,
        hint: Option<Protocol>,
    ) -> io::Result<Grant> {
        let deadline = tokio::time::Instant::now() + self.config.acquire_timeout;
        let mut guard: Option<WaitGuard> = None;

        loop {
            let notified = {
                let mut state = self.lock();

                match self.try_acquire(&mut state, origin, hint, guard.is_none())? {
                    Step::Ready(grant) => return Ok(grant),
                    Step::Wait(notify, waiters) => {
                        if guard.is_none() {
                            guard = Some(WaitGuard(waiters));
                        }

                        notify.notified_owned()
                    }
                }
            };

            if timeout_at(deadline, notified).await.is_err() {
                return Err(timed_out("waiting for a pooled connection"));
            }
        }
    }

    fn try_acquire(
        self: &Arc<Self>,
        state: &mut State,
        origin: &Origin,
        hint: Option<Protocol>,
        first_wait: bool,
    ) -> io::Result<Step> {
        let now = Instant::now();

        self.prune(state, now);

        if !state.origins.contains_key(origin) {
            state.origins.insert(origin.clone(), Entry::new());
        }

        let entry = state.origins.get_mut(origin).expect("entry exists");

        entry.h2.retain(|slot| slot.client.is_reusable());

        let mut wait = false;
        let best = entry
            .h2
            .iter()
            .min_by_key(|slot| slot.load.inflight.load(Ordering::Acquire));

        if let Some(slot) = best {
            let load = slot.load.inflight.load(Ordering::Acquire);
            let room = load < slot.client.stream_limit();

            if room || entry.h2.len() >= self.config.max_h2_per_origin {
                return Ok(Step::Ready(Grant::H2(ticket(slot))));
            }

            wait = entry.gated;
        }

        if !wait {
            let mut dropped = 0usize;
            let mut revived = None;

            while let Some(mut idle) = entry.idle.pop() {
                dropped += 1;

                let fresh = now.duration_since(idle.since) < self.config.idle_timeout;

                if fresh && idle.conn.is_alive() {
                    revived = Some(idle.conn);
                    break;
                }

                entry.open -= 1;
            }

            state.idle_total -= dropped;

            let entry = state.origins.get_mut(origin).expect("entry exists");

            if let Some(conn) = revived {
                return Ok(Step::Ready(Grant::Idle(self.lease(origin, conn))));
            }

            let kind = hint.or(entry.known);

            if kind == Some(Protocol::Http1) {
                if entry.open < self.config.max_per_origin {
                    entry.open += 1;

                    return Ok(Step::Ready(Grant::Dial(self.dial(origin, false, true))));
                }
            } else if !entry.gated {
                entry.gated = true;

                return Ok(Step::Ready(Grant::Dial(self.dial(origin, true, false))));
            }
        }

        let entry = state.origins.get_mut(origin).expect("entry exists");

        if first_wait && entry.waiters.load(Ordering::Acquire) >= self.config.max_waiters_per_origin
        {
            if entry.is_empty() {
                state.origins.remove(origin);
            }

            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "too many callers waiting for a connection",
            ));
        }

        if first_wait {
            entry.waiters.fetch_add(1, Ordering::AcqRel);
        }

        Ok(Step::Wait(
            Arc::clone(&entry.notify),
            Arc::clone(&entry.waiters),
        ))
    }

    fn lease(self: &Arc<Self>, origin: &Origin, conn: Box<Connection>) -> Lease {
        Lease {
            pool: Arc::clone(self),
            origin: origin.clone(),
            conn: Some(conn),
        }
    }

    fn dial(self: &Arc<Self>, origin: &Origin, gated: bool, counted: bool) -> Dial {
        Dial {
            pool: Arc::clone(self),
            origin: origin.clone(),
            gated,
            counted,
            done: false,
        }
    }

    fn release(&self, origin: &Origin, conn: Option<Box<Connection>>) {
        let mut state = self.lock();
        let now = Instant::now();
        let keep = conn.filter(|c| c.is_idle_reusable()).filter(|_| {
            self.config.max_idle_per_origin > 0 && state.idle_total < self.config.max_idle_total
        });

        let Some(entry) = state.origins.get_mut(origin) else {
            return;
        };

        match keep {
            Some(mut conn) if entry.idle.len() < self.config.max_idle_per_origin => {
                conn.trim();
                entry.idle.push(Idle { conn, since: now });
                entry.notify.notify_waiters();
                state.idle_total += 1;
            }
            _ => {
                entry.open -= 1;
                entry.notify.notify_waiters();

                if entry.is_empty() {
                    state.origins.remove(origin);
                }
            }
        }

        self.prune(&mut state, now);
    }

    fn prune(&self, state: &mut State, now: Instant) {
        let interval = (self.config.idle_timeout / 4).min(PRUNE_INTERVAL);

        if now.duration_since(state.last_prune) < interval {
            return;
        }

        state.last_prune = now;

        let idle_timeout = self.config.idle_timeout;
        let mut removed = 0usize;

        for entry in state.origins.values_mut() {
            let before = entry.idle.len();

            entry
                .idle
                .retain(|idle| now.duration_since(idle.since) < idle_timeout);

            let gone = before - entry.idle.len();

            entry.open -= gone;
            removed += gone;

            entry.h2.retain(|slot| {
                slot.client.is_reusable()
                    && slot
                        .load
                        .idle_for(now)
                        .is_none_or(|idle| idle < idle_timeout)
            });

            if gone > 0 {
                entry.notify.notify_waiters();
            }
        }

        state.idle_total -= removed;
        state.origins.retain(|_, entry| !entry.is_empty());
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock()
    }
}

enum Step {
    Ready(Grant),
    Wait(Arc<Notify>, Arc<AtomicUsize>),
}

fn ticket(slot: &H2Slot) -> H2Ticket {
    slot.load.inflight.fetch_add(1, Ordering::AcqRel);
    slot.load.touch();

    H2Ticket {
        client: slot.client.clone(),
        load: Arc::clone(&slot.load),
    }
}

impl Lease {
    pub fn conn_ref(&self) -> &Connection {
        self.conn.as_ref().expect("lease holds a connection")
    }

    pub fn conn(&mut self) -> &mut Connection {
        self.conn.as_mut().expect("lease holds a connection")
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.pool.release(&self.origin, self.conn.take());
    }
}

impl H2Ticket {
    pub fn client(&self) -> &H2Client {
        &self.client
    }
}

impl Drop for H2Ticket {
    fn drop(&mut self) {
        self.load.touch();
        self.load.inflight.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Dial {
    pub fn finish_h1(mut self, conn: Box<Connection>) -> Lease {
        self.done = true;

        let mut state = self.pool.lock();

        if let Some(entry) = state.origins.get_mut(&self.origin) {
            if !self.counted {
                entry.open += 1;
            }

            entry.gated = false;
            entry.known = Some(Protocol::Http1);
            entry.notify.notify_waiters();
        }

        drop(state);

        self.pool.lease(&self.origin, conn)
    }

    pub fn finish_h2(mut self, client: H2Client) -> H2Ticket {
        self.done = true;

        let load = Arc::new(H2Load {
            inflight: AtomicUsize::new(1),
            last_ms: AtomicU64::new(0),
            epoch: self.pool.epoch,
        });

        load.touch();

        let mut state = self.pool.lock();

        if let Some(entry) = state.origins.get_mut(&self.origin) {
            if self.counted {
                entry.open -= 1;
            }

            entry.gated = false;
            entry.known = Some(Protocol::Http2);
            entry.h2.push(H2Slot {
                client: client.clone(),
                load: Arc::clone(&load),
            });
            entry.notify.notify_waiters();
        }

        H2Ticket { client, load }
    }
}

impl Drop for Dial {
    fn drop(&mut self) {
        if self.done {
            return;
        }

        let mut state = self.pool.lock();

        let Some(entry) = state.origins.get_mut(&self.origin) else {
            return;
        };

        if self.counted {
            entry.open -= 1;
        }

        if self.gated {
            entry.gated = false;
        }

        entry.notify.notify_waiters();

        if entry.is_empty() {
            state.origins.remove(&self.origin);
        }
    }
}
