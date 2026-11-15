use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

use crate::h1::H1Conn;
use crate::h2::H2Handle;
use crate::url::Origin;

const MAX_H2_PER_ORIGIN: usize = 2;
const MAX_REMEMBERED_H1: usize = 256;

// NOTE: routes are the pool key and name the connect endpoint, never the request authority: one
// forward-proxy connection serves many targets while a tunnel is bound to one (proxy, target) pair.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum Route {
    Direct(Origin),
    Forward(Origin),
    Tunnel { proxy: Origin, target: Origin },
}

impl Route {
    pub(crate) fn uses_tls(&self) -> bool {
        match self {
            Self::Direct(origin) => origin.tls,
            Self::Forward(_) => false,
            Self::Tunnel { .. } => true,
        }
    }
}

#[derive(Default)]
struct Gate {
    dialing: HashSet<Route>,
    h1_only: HashSet<Route>,
}

pub(crate) struct DialGuard<'a> {
    pool: &'a Pool,
    route: Route,
}

impl Drop for DialGuard<'_> {
    fn drop(&mut self) {
        self.pool.gate.lock().dialing.remove(&self.route);
        self.pool.dialed.notify_all();
    }
}

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
    h1: HashMap<Route, Vec<Idle>>,
    h2: HashMap<Route, Vec<Arc<H2Handle>>>,
    idle_total: usize,
}

pub(crate) struct Pool {
    inner: Mutex<Inner>,
    gate: Mutex<Gate>,
    dialed: Condvar,
    idle_timeout: Duration,
    max_per_host: usize,
    max_total: usize,
}

impl Pool {
    pub(crate) fn new(idle_timeout: Duration, max_per_host: usize, max_total: usize) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            gate: Mutex::new(Gate::default()),
            dialed: Condvar::new(),
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

    pub(crate) fn take_h1(&self, route: &Route) -> Option<Box<H1Conn>> {
        loop {
            let candidate = {
                let mut inner = self.inner.lock();
                let list = inner.h1.get_mut(route)?;
                let candidate = list.pop();

                if list.is_empty() {
                    inner.h1.remove(route);
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

    pub(crate) fn put_h1(&self, route: &Route, mut conn: Box<H1Conn>) {
        if !conn.is_reusable() {
            return;
        }

        conn.trim();

        let mut guard = self.inner.lock();
        let inner = &mut *guard;

        if inner.idle_total >= self.max_total || self.max_per_host == 0 {
            return;
        }

        let list = inner.h1.entry(route.clone()).or_default();
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

    pub(crate) fn take_h2(&self, route: &Route) -> Option<Arc<H2Handle>> {
        let mut inner = self.inner.lock();
        let list = inner.h2.get_mut(route)?;

        list.retain(|handle| !handle.is_dead());

        let found = list.iter().min_by_key(|handle| handle.load()).cloned();
        let full = list.len() >= MAX_H2_PER_ORIGIN;

        if list.is_empty() {
            inner.h2.remove(route);
        }

        let handle = found?;

        if handle.reserve(false) || (full && handle.reserve(true)) {
            return Some(handle);
        }

        None
    }

    pub(crate) fn add_h2(&self, route: &Route, handle: &Arc<H2Handle>) {
        let mut inner = self.inner.lock();
        let list = inner.h2.entry(route.clone()).or_default();

        list.retain(|existing| !existing.is_dead());

        if list.len() < MAX_H2_PER_ORIGIN {
            list.push(Arc::clone(handle));
        }
    }

    pub(crate) fn is_h1_only(&self, route: &Route) -> bool {
        self.gate.lock().h1_only.contains(route)
    }

    pub(crate) fn mark_h1_only(&self, route: &Route) {
        let mut gate = self.gate.lock();

        if gate.h1_only.len() < MAX_REMEMBERED_H1 {
            gate.h1_only.insert(route.clone());
        }
    }

    // NOTE: dials to one origin are serialized so a burst of threads shares the first h2
    // connection instead of each opening its own. Waiters re-check the pool once they get in.
    pub(crate) fn dial(&self, route: &Route, deadline: Option<Instant>) -> Option<DialGuard<'_>> {
        let mut gate = self.gate.lock();

        while gate.dialing.contains(route) {
            let Some(deadline) = deadline else {
                self.dialed.wait(&mut gate);

                continue;
            };

            if self.dialed.wait_until(&mut gate, deadline).timed_out() {
                return None;
            }
        }

        gate.dialing.insert(route.clone());

        Some(DialGuard {
            pool: self,
            route: route.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use super::Route;
    use crate::url::Url;

    fn origin(url: &str) -> crate::url::Origin {
        Url::parse(url).expect("url").origin().clone()
    }

    #[test]
    fn routes_share_the_origin_host_allocation() {
        let origin = origin("http://example.com/x");
        let direct = Route::Direct(origin.clone());
        let forward = Route::Forward(origin.clone());

        let Route::Direct(direct) = &direct else {
            panic!("not a direct route");
        };
        let Route::Forward(forward) = &forward else {
            panic!("not a forward route");
        };

        assert!(Arc::ptr_eq(&origin.host, &direct.host));
        assert!(Arc::ptr_eq(&origin.host, &forward.host));
        assert_eq!(direct.host.as_ref(), "example.com");
    }

    #[test]
    fn equal_origins_make_equal_pool_keys() {
        let a = origin("http://Example.COM:80/x");
        let b = origin("http://example.com/y");
        let mut keys = HashSet::new();

        assert_eq!(a, b);
        assert!(keys.insert(Route::Direct(a)));

        assert!(keys.contains(&Route::Direct(b)));
        assert!(!keys.contains(&Route::Forward(origin("http://example.com/"))));
        assert!(!keys.contains(&Route::Direct(origin("http://example.com:81/"))));
    }
}
