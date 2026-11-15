use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::h1::H1Conn;
use crate::h2::H2Handle;
use crate::url::Origin;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PoolStats {
    pub idle_h1: usize,
    pub h2: usize,
}

struct Idle {
    conn: Box<H1Conn>,
    since: Instant,
}

#[derive(Default)]
struct Inner {
    h1: HashMap<Origin, Vec<Idle>>,
    h2: HashMap<Origin, Vec<Arc<H2Handle>>>,
    idle_total: usize,
}

pub(crate) struct Pool {
    inner: Mutex<Inner>,
    idle_timeout: Duration,
    max_per_host: usize,
    max_total: usize,
}

impl Pool {
    pub(crate) fn new(idle_timeout: Duration, max_per_host: usize, max_total: usize) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            idle_timeout,
            max_per_host,
            max_total,
        }
    }

    pub(crate) fn stats(&self) -> PoolStats {
        let inner = self.inner.lock();

        PoolStats {
            idle_h1: inner.idle_total,
            h2: inner
                .h2
                .values()
                .flatten()
                .filter(|handle| !handle.is_dead())
                .count(),
        }
    }

    pub(crate) fn take_h1(&self, origin: &Origin) -> Option<Box<H1Conn>> {
        loop {
            let candidate = {
                let mut inner = self.inner.lock();
                let list = inner.h1.get_mut(origin)?;
                let candidate = list.pop();

                if list.is_empty() {
                    inner.h1.remove(origin);
                }

                if candidate.is_some() {
                    inner.idle_total -= 1;
                }

                candidate?
            };

            if candidate.since.elapsed() > self.idle_timeout || candidate.conn.is_stale() {
                continue;
            }

            return Some(candidate.conn);
        }
    }

    pub(crate) fn put_h1(&self, origin: &Origin, mut conn: Box<H1Conn>) {
        if !conn.is_reusable() {
            return;
        }

        conn.trim();

        let mut guard = self.inner.lock();
        let inner = &mut *guard;

        if inner.idle_total >= self.max_total || self.max_per_host == 0 {
            return;
        }

        let list = inner.h1.entry(origin.clone()).or_default();
        let before = list.len();

        list.retain(|idle| idle.since.elapsed() <= self.idle_timeout);

        if list.len() >= self.max_per_host {
            list.remove(0);
        }

        list.push(Idle {
            conn,
            since: Instant::now(),
        });

        inner.idle_total = inner.idle_total - before + list.len();
    }

    pub(crate) fn take_h2(&self, origin: &Origin) -> Option<Arc<H2Handle>> {
        let mut inner = self.inner.lock();
        let list = inner.h2.get_mut(origin)?;

        list.retain(|handle| !handle.is_dead());

        let found = list
            .iter()
            .rev()
            .find(|handle| handle.try_acquire())
            .cloned();

        if list.is_empty() {
            inner.h2.remove(origin);
        }

        found
    }

    pub(crate) fn add_h2(&self, origin: &Origin, handle: &Arc<H2Handle>) {
        let mut inner = self.inner.lock();
        let list = inner.h2.entry(origin.clone()).or_default();

        list.retain(|existing| !existing.is_dead());

        if list.len() < self.max_per_host {
            list.push(Arc::clone(handle));
        }
    }
}
