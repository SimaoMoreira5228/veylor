use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, MutexGuard};
use tokio::sync::Notify;
use tokio::time::timeout_at;

use crate::client_conn::{Connection, timed_out};
use crate::client_url::Origin;

const PRUNE_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone, Debug)]
pub struct PoolConfig {
    pub max_idle_per_origin: usize,
    pub max_idle_total: usize,
    pub max_per_origin: usize,
    pub idle_timeout: Duration,
    pub acquire_timeout: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_idle_per_origin: 4,
            max_idle_total: 64,
            max_per_origin: 16,
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
}

struct Idle {
    conn: Connection,
    since: Instant,
}

struct Entry {
    idle: Vec<Idle>,
    open: usize,
    notify: Arc<Notify>,
}

struct State {
    origins: HashMap<Origin, Entry>,
    idle_total: usize,
    last_prune: Instant,
}

pub struct Pool {
    config: PoolConfig,
    state: Mutex<State>,
}

pub struct Lease {
    pool: Arc<Pool>,
    origin: Origin,
    conn: Option<Connection>,
}

impl Pool {
    pub fn new(mut config: PoolConfig) -> Arc<Self> {
        config.max_per_origin = config.max_per_origin.max(1);

        Arc::new(Self {
            config,
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
        }
    }

    pub fn close_idle(&self) {
        let mut state = self.lock();
        let before = state.idle_total;

        for entry in state.origins.values_mut() {
            entry.open -= entry.idle.len();
            entry.idle.clear();
            entry.notify.notify_one();
        }

        state.idle_total -= before;
        state.origins.retain(|_, entry| entry.open > 0);
    }

    pub async fn acquire(self: &Arc<Self>, origin: &Origin) -> io::Result<Lease> {
        let deadline = tokio::time::Instant::now() + self.config.acquire_timeout;

        loop {
            let notified = {
                let mut state = self.lock();
                let now = Instant::now();

                self.prune(&mut state, now);

                let entry = state
                    .origins
                    .entry(origin.clone())
                    .or_insert_with(|| Entry {
                        idle: Vec::new(),
                        open: 0,
                        notify: Arc::new(Notify::new()),
                    });
                let mut revived = None;
                let mut dropped = 0usize;

                while let Some(mut idle) = entry.idle.pop() {
                    dropped += 1;

                    let fresh = now.duration_since(idle.since) < self.config.idle_timeout;

                    if fresh && idle.conn.is_alive() {
                        revived = Some(idle.conn);
                        break;
                    }

                    entry.open -= 1;
                }

                let notify = Arc::clone(&entry.notify);

                if let Some(conn) = revived {
                    state.idle_total -= dropped;

                    return Ok(self.lease(origin, Some(conn)));
                }

                let entry = state.origins.get_mut(origin).expect("entry exists");

                if entry.open < self.config.max_per_origin {
                    entry.open += 1;
                    state.idle_total -= dropped;

                    return Ok(self.lease(origin, None));
                }

                state.idle_total -= dropped;

                notify.notified_owned()
            };

            if timeout_at(deadline, notified).await.is_err() {
                return Err(timed_out("waiting for a pooled connection"));
            }
        }
    }

    fn lease(self: &Arc<Self>, origin: &Origin, conn: Option<Connection>) -> Lease {
        Lease {
            pool: Arc::clone(self),
            origin: origin.clone(),
            conn,
        }
    }

    fn release(&self, origin: &Origin, conn: Option<Connection>) {
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
                entry.notify.notify_one();
                state.idle_total += 1;
            }
            _ => {
                entry.open -= 1;
                entry.notify.notify_one();
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

            if gone > 0 {
                entry.notify.notify_one();
            }
        }

        state.idle_total -= removed;
        state
            .origins
            .retain(|_, entry| entry.open > 0 || !entry.idle.is_empty());
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock()
    }
}

impl Lease {
    pub fn set(&mut self, conn: Connection) -> &mut Connection {
        self.conn.insert(conn)
    }

    pub fn conn(&mut self) -> &mut Connection {
        self.conn.as_mut().expect("lease holds a connection")
    }

    pub fn has_conn(&self) -> bool {
        self.conn.is_some()
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.pool.release(&self.origin, self.conn.take());
    }
}
