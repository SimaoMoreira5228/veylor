#![allow(unsafe_code)]

use veylor_http1::Header;

use super::state::ConnState;
use super::{HEADER_SLOTS, INPUT_CAP, OUTPUT_CAP};

// NOTE: an idle connection gives its slabs back only when both gates pass: two worker
// sweeps that moved no bytes (so back-to-back and pipelined keep-alive never re-take one)
// and this much silence (so a connection polled every few milliseconds does not churn).
// The reclaim pass in the turn loop applies the silence gate alone, because a worker that
// went quiet stops sweeping and therefore stops counting turns.
pub(crate) const SLAB_HOLD_NS: u64 = 50_000_000;
pub(crate) const SLAB_QUIET_TURNS: u8 = 2;

// NOTE: freed slabs are kept per worker only up to this many of each kind; the rest go back
// to the allocator, so a wave of idle connections cannot ratchet the process.
pub(crate) const SLAB_POOL_CAP: usize = 32;

// NOTE: a pass that gave back fewer connections than this cannot have freed enough pages for
// the trim to pay for its walk, so it is skipped.
pub(crate) const TRIM_MIN_RELEASED: usize = 64;

// NOTE: bounded free-list for one slab class. The slab itself is boxed and moves between the
// pool and its owner without copying, so every class pays one pointer move per adopt/release.
pub(crate) struct BoundedPool<T> {
    free: Vec<Box<T>>,
    cap: usize,
}

impl<T> BoundedPool<T> {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            free: Vec::new(),
            cap,
        }
    }

    pub(crate) fn take(&mut self, make: impl FnOnce() -> Box<T>) -> Box<T> {
        self.free.pop().unwrap_or_else(make)
    }

    pub(crate) fn give(&mut self, slab: Box<T>) {
        if self.free.len() >= self.cap {
            return;
        }
        self.free.push(slab);
    }

    // NOTE: a scaled owner lowers the cap between passes; the excess leaves the pool here,
    // so the free list can never stay above a cap it has already shrunk past. Dropping the
    // box is what returns the slab to the allocator; the count is the wave the caller uses
    // to decide whether handing the pages back to the system is worth the walk.
    pub(crate) fn trim_to(&mut self, cap: usize) -> usize {
        self.cap = cap;
        if self.free.len() <= cap {
            return 0;
        }
        let dropped = self.free.len() - cap;
        self.free.truncate(cap);
        dropped
    }

    // NOTE: test-only probes: the pool tests assert a class stopped at a cap and that the
    // owner can raise it again.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.free.len()
    }

    #[cfg(test)]
    pub(crate) fn cap(&self) -> usize {
        self.cap
    }
}

pub(crate) struct SlabPool {
    input: BoundedPool<[u8; INPUT_CAP]>,
    output: BoundedPool<[u8; OUTPUT_CAP]>,
    headers: BoundedPool<[Header; HEADER_SLOTS]>,
}

impl SlabPool {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            input: BoundedPool::new(cap),
            output: BoundedPool::new(cap),
            headers: BoundedPool::new(cap),
        }
    }

    // NOTE: one inspection per transport wakeup, not per request: a connection that still
    // holds its slabs pays three pointer checks. A recycled header slab may carry ranges
    // from its previous owner, but the parser fills `[..count]` and every reader is bounded
    // by that count, exactly as within one connection.
    pub(crate) fn adopt(&mut self, conn: &mut ConnState, now_ns: u64) -> bool {
        let mut installed = false;
        if conn.input.is_none() {
            conn.input = Some(self.input.take(|| Box::new([0; INPUT_CAP])));
            installed = true;
        }
        if conn.output.is_none() {
            conn.output = Some(self.output.take(|| Box::new([0; OUTPUT_CAP])));
            installed = true;
        }
        if conn.headers.is_none() {
            conn.headers = Some(
                self.headers
                    .take(|| Box::new([Header::default(); HEADER_SLOTS])),
            );
            installed = true;
        }
        if !installed {
            return false;
        }
        conn.reserve_tls_scratch();
        conn.note_slab_adopt(now_ns);
        true
    }

    pub(crate) fn release(&mut self, conn: &mut ConnState) -> bool {
        let mut released = false;
        if let Some(input) = conn.input.take() {
            self.input.give(input);
            released = true;
        }
        if let Some(output) = conn.output.take() {
            self.output.give(output);
            released = true;
        }
        if let Some(headers) = conn.headers.take() {
            self.headers.give(headers);
            released = true;
        }
        if !released {
            return false;
        }
        conn.drop_tls_scratch();
        conn.note_slab_release();
        true
    }
}

// NOTE: only moves the due moment from "nothing to reclaim" to one hold window out; an armed
// value is always the earliest candidate, so a later arming needs no minimum.
pub(crate) fn arm_slab_due(slab_due_ns: &mut u64, now_ns: u64) {
    if *slab_due_ns != 0 {
        return;
    }
    *slab_due_ns = now_ns.saturating_add(SLAB_HOLD_NS);
}

// NOTE: returns the next moment an idle slab holder can be released, or zero when the worker
// has none, so a worker whose connections all have work never pays for a pass, plus whether
// this pass released anything at all. A connection that is busy when the pass runs cannot be a
// candidate; the sweep arms the due moment again when its turn ends with nothing left to do.
pub(crate) fn reclaim_idle_slabs(
    conns: &mut [Option<ConnState>],
    slabs: &mut SlabPool,
    horizon: usize,
    now_ns: u64,
) -> (u64, bool) {
    let mut due = 0u64;
    let mut released = 0usize;
    let end = horizon.min(conns.len());
    for entry in conns[..end].iter_mut() {
        let Some(conn) = entry.as_mut() else {
            continue;
        };
        if !conn.holds_slabs() {
            continue;
        }
        if conn.slab_release_ready(now_ns) {
            slabs.release(conn);
            released += 1;
            continue;
        }
        if !conn.slab_idle() {
            continue;
        }
        let candidate = conn.slab_release_at();
        due = if due == 0 {
            candidate
        } else {
            due.min(candidate)
        };
    }
    trim_released_pages(released, TRIM_MIN_RELEASED);
    (due, released > 0)
}

pub(crate) fn trim_released_pages(released: usize, min_released: usize) {
    if released < min_released {
        return;
    }
    trim_heap();
}

// NOTE: the tail of a release wave that the sweep gave back between passes. It is only worth
// the walk when the worker is about to sleep, which is where the turn loop calls this.
pub(crate) fn trim_settled_heap() {
    trim_heap();
}

// NOTE: glibc keeps freed heap pages mapped, so a wave of returned slabs would otherwise hold
// the process at its peak RSS for the rest of its life. Callers decide when a wave is worth
// the walk; allocators without the call (mimalloc, jemalloc) purge on their own schedule and
// other platforms compile it out.
fn trim_heap() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    // SAFETY: malloc_trim takes the glibc arena locks, needs no live pointer from this frame,
    // and only madvises free pages inside glibc's own heap.
    unsafe {
        unsafe extern "C" {
            fn malloc_trim(pad: usize) -> i32;
        }
        malloc_trim(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use veylor_runtime::{ConnectionToken, TcpStream};

    fn test_conn() -> ConnState {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind rig");
        let addr = listener.local_addr().expect("rig addr");
        let stream = TcpStream::connect(addr).expect("rig connect");
        ConnState::new(test_token(), stream)
    }

    fn test_token() -> ConnectionToken {
        ConnectionToken {
            worker_id: 0,
            slot: 0,
            request_seq: 0,
            stream_id: 0,
            worker_generation: 1,
            generation: 1,
        }
    }

    #[test]
    fn adopt_installs_every_slab_and_release_drains_them() {
        let mut pool = SlabPool::new(SLAB_POOL_CAP);
        let mut conn = test_conn();
        assert!(pool.adopt(&mut conn, 0));
        assert!(conn.input.is_some());
        assert!(conn.output.is_some());
        assert!(conn.headers.is_some());
        assert!(!pool.adopt(&mut conn, 0));

        assert!(pool.release(&mut conn));
        assert!(conn.input.is_none());
        assert!(conn.output.is_none());
        assert!(conn.headers.is_none());
        assert!(!pool.release(&mut conn));

        let mut reused = test_conn();
        assert!(pool.adopt(&mut reused, 0));
        assert!(reused.input.is_some());
    }

    #[test]
    fn released_slabs_round_trip_through_the_pool() {
        let mut pool = SlabPool::new(SLAB_POOL_CAP);
        let mut conn = test_conn();
        pool.adopt(&mut conn, 0);
        let input = conn.input.as_ref().map(|slab| slab.as_ptr());
        let output = conn.output.as_ref().map(|slab| slab.as_ptr());
        let headers = conn.headers.as_ref().map(|slab| slab.as_ptr());
        pool.release(&mut conn);

        let mut next = test_conn();
        pool.adopt(&mut next, 0);
        assert_eq!(next.input.as_ref().map(|slab| slab.as_ptr()), input);
        assert_eq!(next.output.as_ref().map(|slab| slab.as_ptr()), output);
        assert_eq!(next.headers.as_ref().map(|slab| slab.as_ptr()), headers);
    }

    #[test]
    fn pool_stops_at_its_cap_and_drops_the_excess() {
        let cap = 4;
        let mut pool = SlabPool::new(cap);
        let mut conns: Vec<ConnState> = (0..cap * 3).map(|_| test_conn()).collect();
        for conn in conns.iter_mut() {
            pool.adopt(conn, 0);
        }
        for conn in conns.iter_mut() {
            pool.release(conn);
        }
        assert_eq!(pool.input.len(), cap);
        assert_eq!(pool.output.len(), cap);
        assert_eq!(pool.headers.len(), cap);
        for conn in conns.iter() {
            assert!(conn.input.is_none());
        }
    }

    #[test]
    fn a_bounded_pool_trims_to_a_lower_cap_and_refills_to_a_higher_one() {
        let mut pool = BoundedPool::<[u8; 8]>::new(4);
        for _ in 0..6 {
            pool.give(Box::new([0; 8]));
        }
        assert_eq!(pool.len(), 4);

        assert_eq!(pool.trim_to(2), 2);
        assert_eq!(pool.len(), 2);
        pool.give(Box::new([0; 8]));
        assert_eq!(pool.len(), 2);
        assert_eq!(pool.trim_to(2), 0);

        pool.trim_to(4);
        pool.give(Box::new([0; 8]));
        assert_eq!(pool.len(), 3);

        assert_eq!(pool.trim_to(0), 3);
        assert_eq!(pool.len(), 0);
        assert_eq!(pool.cap(), 0);
    }

    #[test]
    fn adopt_marks_the_connection_active() {
        let mut pool = SlabPool::new(SLAB_POOL_CAP);
        let mut conn = test_conn();
        pool.adopt(&mut conn, 42);
        assert_eq!(conn.last_active_ns, 42);
        assert_eq!(conn.quiet_turns, 0);
    }
}
