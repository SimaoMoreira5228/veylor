#![allow(unsafe_code)]

use veylor_http2::{H2Role, HpackEntrySlot, HpackHeader, HpackTable, StreamSlot, StreamTable};
use veylor_runtime::{AcceptedConnection, ConnectionTimeouts, Readiness, TcpStream};
use veylor_tls::TlsConn;

use crate::connection::tls::TLS_PENDING_MIN_BYTES;
use crate::connection::{
    BoundedPool, INPUT_CAP, OUTPUT_CAP, SLAB_HOLD_NS, SLAB_QUIET_TURNS, trim_released_pages,
};

use super::{
    ACK_SLOTS, DECODE_BYTES, DECODE_ENTRIES, ENCODE_BYTES, ENCODE_ENTRIES, H2Conn,
    HEADER_BLOCK_CAP, HEADER_SLOTS, INPUT_STAGING_CAP, MAX_H2_CONNS, STREAM_SLOTS, TUNNEL_STEP,
};

// NOTE: H1 tokens occupy 1..=MAX_CONNECTIONS (`slot + 1`), so the H2 token range starts
// above the whole H1 table instead of at MAX_CONNECTIONS.
pub(crate) const H2_STREAM_TOKEN_BASE: usize = crate::static_server::MAX_CONNECTIONS + 1;
pub(crate) const H2_ACCEPT_STAGING: usize = 8;
pub(crate) const H2_PENDING_CAP: usize = 65_536 + 512;

// NOTE: freed H2 slabs are kept per worker only up to this many of each kind; the rest go back
// to the allocator, so a wave of idle connections cannot ratchet the process. Eight covers a
// worker's whole wakeup edge without holding a measurable share of the 64-connection ceiling.
pub(crate) const H2_SLAB_POOL_CAP: usize = 8;

// NOTE: one released H2 connection frees more than a hundred times the memory of one released
// H1 connection, so any wave that is not a single trickle pays for the trim walk.
pub(crate) const H2_TRIM_MIN_RELEASED: usize = 4;

pub(crate) struct H2Buffers {
    pub(crate) slots: Box<[StreamSlot; STREAM_SLOTS]>,
    pub(crate) block: Box<[u8; HEADER_BLOCK_CAP]>,
    pub(crate) dec_entries: Box<[HpackEntrySlot; DECODE_ENTRIES]>,
    pub(crate) dec_scratch: Box<[u8; DECODE_BYTES]>,
    pub(crate) enc_entries: Box<[HpackEntrySlot; ENCODE_ENTRIES]>,
    pub(crate) enc_scratch: Box<[u8; ENCODE_BYTES]>,
    pub(crate) ack: Box<[u32; ACK_SLOTS]>,
    pub(crate) headers: Box<[HpackHeader; HEADER_SLOTS]>,
}

impl H2Buffers {
    pub(crate) fn new() -> Self {
        Self {
            slots: Box::new([StreamSlot::EMPTY; STREAM_SLOTS]),
            block: Box::new([0; HEADER_BLOCK_CAP]),
            dec_entries: Box::new([HpackEntrySlot::EMPTY; DECODE_ENTRIES]),
            dec_scratch: Box::new([0; DECODE_BYTES]),
            enc_entries: Box::new([HpackEntrySlot::EMPTY; ENCODE_ENTRIES]),
            enc_scratch: Box::new([0; ENCODE_BYTES]),
            ack: Box::new([0; ACK_SLOTS]),
            headers: Box::new([HpackHeader::EMPTY; HEADER_SLOTS]),
        }
    }
}

// NOTE: the worker-local pool for the four slabs an idle H2 connection does not need: input
// staging, the pending output buffer, and the file and tunnel chunk scratches. The engine
// tables (stream slots, header block, HPACK entries) stay with the connection because their
// dynamic state is connection-scoped.
pub(crate) struct H2Slabs {
    staging: BoundedPool<[u8; INPUT_STAGING_CAP]>,
    pending: BoundedPool<[u8; H2_PENDING_CAP]>,
    file: BoundedPool<[u8; OUTPUT_CAP]>,
    tunnel: BoundedPool<[u8; TUNNEL_STEP]>,
}

impl H2Slabs {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            staging: BoundedPool::new(cap),
            pending: BoundedPool::new(cap),
            file: BoundedPool::new(cap),
            tunnel: BoundedPool::new(cap),
        }
    }

    // NOTE: installed together on the readiness edge that follows a release, so a step never
    // runs with one of the four missing; a pool hit is a pointer move, a miss the only case
    // that allocates.
    pub(crate) fn adopt(
        &mut self,
        meta: &mut H2Meta,
        conn: &mut Option<H2Conn<'_>>,
        now_ns: u64,
    ) -> bool {
        let Some(conn) = conn.as_mut() else {
            return false;
        };
        let mut installed = false;
        if meta.pending.is_none() {
            meta.pending = Some(self.pending.take(|| Box::new([0; H2_PENDING_CAP])));
            installed = true;
        }
        if conn.staging.is_none() {
            conn.staging = Some(self.staging.take(|| Box::new([0; INPUT_STAGING_CAP])));
            installed = true;
        }
        if conn.file_scratch.is_none() {
            conn.file_scratch = Some(self.file.take(|| Box::new([0; OUTPUT_CAP])));
            installed = true;
        }
        if conn.tunnel_scratch.is_none() {
            conn.tunnel_scratch = Some(self.tunnel.take(|| Box::new([0; TUNNEL_STEP])));
            installed = true;
        }
        if !installed {
            return false;
        }
        meta.reserve_tls_scratch();
        meta.note_slab_adopt(now_ns);
        true
    }

    pub(crate) fn release(&mut self, meta: &mut H2Meta, conn: &mut Option<H2Conn<'_>>) -> bool {
        if !self.give_back(meta, conn) {
            return false;
        }
        meta.note_slab_release();
        true
    }

    // NOTE: a closing connection returns whatever it still holds; staged input keeps its
    // staging slab private to the drop instead of handing live bytes to the next owner.
    pub(crate) fn reclaim(&mut self, meta: &mut H2Meta, conn: &mut Option<H2Conn<'_>>) {
        self.give_back(meta, conn);
    }

    fn give_back(&mut self, meta: &mut H2Meta, conn: &mut Option<H2Conn<'_>>) -> bool {
        let mut released = false;
        if let Some(pending) = meta.pending.take() {
            self.pending.give(pending);
            released = true;
        }
        if let Some(live) = conn.as_mut() {
            if let Some(staging) = live.take_staging() {
                self.staging.give(staging);
                released = true;
            }
            if let Some(scratch) = live.file_scratch.take() {
                self.file.give(scratch);
                released = true;
            }
            if let Some(scratch) = live.tunnel_scratch.take() {
                self.tunnel.give(scratch);
                released = true;
            }
        }
        if !released {
            return false;
        }
        meta.pending_len = 0;
        meta.pending_sent = 0;
        meta.drop_tls_scratch();
        true
    }
}

// NOTE: scalar half of a slot: borrow-free, mutates freely beside driver borrows.
#[derive(Default)]
pub(crate) struct H2Meta {
    pub(crate) stream: Option<TcpStream>,
    pub(crate) tls: Option<Box<TlsConn>>,
    pub(crate) tls_pending: Vec<u8>,
    pub(crate) tls_pending_sent: usize,
    pub(crate) tls_plain: Vec<u8>,
    pub(crate) occupied: bool,
    pub(crate) readable: bool,
    pub(crate) claimed: bool,
    pub(crate) write_blocked: bool,
    pub(crate) watching_write: bool,
    pub(crate) defer_write: bool,
    pub(crate) write_deferred: bool,
    pub(crate) batched_read: usize,
    pub(crate) eof: bool,
    pub(crate) error: bool,
    pub(crate) staged: bool,
    pub(crate) serving: bool,
    pub(crate) timed_out: bool,
    pub(crate) token: usize,
    pub(crate) pending: Option<Box<[u8; H2_PENDING_CAP]>>,
    pub(crate) pending_len: usize,
    pub(crate) pending_sent: usize,
    pub(crate) deadline_ns: u64,
    pub(crate) last_progress_ns: u64,
    pub(crate) flood_window_start_ns: u64,
    pub(crate) flood_rst_base: u32,
    pub(crate) flood_settings_base: u32,
    pub(crate) tunnel_cursor: u8,
    // NOTE: worker sweeps since the last byte moved; paired with the silence window so a
    // connection polled every few milliseconds keeps its slabs instead of churning them.
    pub(crate) quiet_turns: u8,
}

impl H2Meta {
    pub(crate) fn pending_unflushed(&self) -> bool {
        self.pending_len > self.pending_sent
    }

    pub(crate) fn tls_unflushed(&self) -> bool {
        self.tls_pending.len() > self.tls_pending_sent
    }

    pub(crate) fn wants_pump(&self) -> bool {
        self.readable
            || self.staged
            || ((self.pending_unflushed() || self.tls_unflushed()) && !self.write_blocked)
    }

    // NOTE: waiting with slabs and nothing to do: no armed read claim, no staged input, no
    // stream or parked response open on the wire, no file or tunnel transfer to resume.
    pub(crate) fn slab_idle(&self, conn: Option<&H2Conn<'_>>) -> bool {
        if !self.occupied || self.readable || self.claimed || self.staged {
            return false;
        }
        if self.timed_out || self.error {
            return false;
        }
        if self.pending_unflushed() || self.tls_unflushed() {
            return false;
        }
        let Some(live) = conn else {
            return false;
        };
        if self.pending.is_none() && !live.holds_slabs() {
            return false;
        }
        live.staged_len() == 0
            && !live.has_pending_responses()
            && !live.has_file_transfers()
            && live.active_streams() == 0
    }

    pub(crate) fn slab_release_due(&self, conn: Option<&H2Conn<'_>>, now_ns: u64) -> bool {
        self.quiet_turns >= SLAB_QUIET_TURNS
            && now_ns.saturating_sub(self.last_progress_ns) >= SLAB_HOLD_NS
            && self.slab_idle(conn)
    }

    pub(crate) fn note_slab_adopt(&mut self, now_ns: u64) {
        self.last_progress_ns = now_ns;
        self.quiet_turns = 0;
    }

    pub(crate) fn note_slab_release(&mut self) {
        self.quiet_turns = 0;
    }

    pub(crate) fn note_progress(&mut self, now_ns: u64) {
        self.last_progress_ns = now_ns;
        self.quiet_turns = 0;
    }

    pub(crate) fn note_quiet(&mut self) {
        self.quiet_turns = self.quiet_turns.saturating_add(1);
    }

    pub(crate) fn reserve_tls_scratch(&mut self) {
        if self.tls.is_none() {
            return;
        }
        self.tls_pending
            .reserve(TLS_PENDING_MIN_BYTES.saturating_sub(self.tls_pending.capacity()));
        self.tls_plain
            .reserve(INPUT_CAP.saturating_sub(self.tls_plain.capacity()));
    }

    pub(crate) fn drop_tls_scratch(&mut self) {
        if self.tls.is_none() {
            return;
        }
        self.tls_pending = Vec::new();
        self.tls_pending_sent = 0;
        self.tls_plain = Vec::new();
    }
}

struct H2Tables {
    streams: StreamTable<'static>,
    decoder: HpackTable<'static>,
    encoder: HpackTable<'static>,
}

impl H2Tables {
    // SAFETY: `bufs` must be a heap allocation that outlives this value and is never moved.
    unsafe fn new(bufs: &'static mut H2Buffers) -> Self {
        Self {
            streams: StreamTable::new(
                &mut bufs.slots[..],
                H2Role::Server,
                &mut bufs.block[..],
                HEADER_BLOCK_CAP as u32,
            ),
            decoder: HpackTable::new(&mut bufs.dec_entries[..], &mut bufs.dec_scratch[..], 4_096),
            encoder: HpackTable::new(&mut bufs.enc_entries[..], &mut bufs.enc_scratch[..], 4_096),
        }
    }
}

// NOTE: the server slot is a controlled self-reference: `bufs` is a stable Box, the
// table struct lives at a fixed address inside the slot, and `conn` borrows the tables.
// The slot itself is always held through Box<H2Slot>, so none of those addresses move,
// and the field order drops conn before tables before bufs.
pub(crate) struct H2Slot {
    pub(crate) conn: Option<H2Conn<'static>>,
    tables: Option<H2Tables>,
    pub(crate) bufs: Box<H2Buffers>,
    pub(crate) meta: H2Meta,
}

impl H2Slot {
    fn boxed() -> Box<Self> {
        Box::new(Self {
            conn: None,
            tables: None,
            bufs: Box::new(H2Buffers::new()),
            meta: H2Meta::default(),
        })
    }

    pub(crate) fn open(&mut self) -> bool {
        self.conn = None;
        self.tables = None;
        let bufs: *mut H2Buffers = &mut *self.bufs;
        // SAFETY: `bufs` is owned by this boxed slot and never moves; the table struct is
        // stored at a fixed address in the same slot before the conn borrows it.
        let tables = unsafe {
            let bufs = std::mem::transmute::<&mut H2Buffers, &'static mut H2Buffers>(&mut *bufs);
            H2Tables::new(bufs)
        };
        self.tables = Some(tables);
        let Some(tables) = self.tables.as_mut() else {
            return false;
        };
        let tables = tables as *mut H2Tables;
        // SAFETY: both referents live in this boxed slot; the conn is dropped before
        // either field and no other reference reaches them while it is alive.
        let conn = unsafe {
            let tables = &mut *tables;
            let bufs = &mut *bufs;
            H2Conn::new(
                &mut tables.streams,
                &mut tables.decoder,
                &mut tables.encoder,
                &mut bufs.ack[..],
                &mut bufs.headers[..],
            )
        };
        self.conn = Some(unsafe { std::mem::transmute::<H2Conn<'_>, H2Conn<'static>>(conn) });
        true
    }

    pub(crate) fn close(&mut self) {
        self.conn = None;
        self.tables = None;
        self.meta = H2Meta::default();
    }
}

pub(crate) struct H2Slots {
    pub(crate) slots: Vec<Option<Box<H2Slot>>>,
    pub(crate) slabs: H2Slabs,
    batches: super::batch::Batches,
}

impl H2Slots {
    pub(crate) fn new() -> Self {
        Self {
            slots: (0..MAX_H2_CONNS).map(|_| None).collect(),
            slabs: H2Slabs::new(H2_SLAB_POOL_CAP),
            batches: super::batch::Batches::new(),
        }
    }

    pub(crate) fn recv_ready(&mut self, poller: &mut veylor_runtime::Poller, budget: u64) {
        self.batches.recv_ready(&mut self.slots, poller, budget);
    }

    pub(crate) fn send_deferred(
        &mut self,
        poller: &mut veylor_runtime::Poller,
        budget: u64,
    ) -> bool {
        self.batches.send_deferred(&mut self.slots, poller, budget)
    }

    pub(crate) fn batch_available(&mut self) -> bool {
        self.batches.ensure()
    }

    pub(crate) fn capacity(&self) -> usize {
        self.slots.len()
    }

    pub(crate) fn occupied(&self) -> usize {
        self.slots
            .iter()
            .flatten()
            .filter(|slot| slot.meta.occupied)
            .count()
    }

    pub(crate) fn wants_pump(&self) -> bool {
        self.slots
            .iter()
            .flatten()
            .any(|slot| slot.meta.occupied && slot.meta.wants_pump())
    }

    pub(crate) fn free_slot(&self) -> Option<usize> {
        self.slots.iter().position(|slot| slot.is_none())
    }

    pub(crate) fn ensure(&mut self, index: usize) -> Option<&mut H2Slot> {
        let entry = self.slots.get_mut(index)?;
        Some(entry.get_or_insert_with(H2Slot::boxed))
    }

    pub(crate) fn take_staging(&mut self) -> Box<[u8; INPUT_STAGING_CAP]> {
        self.slabs.staging.take(|| Box::new([0; INPUT_STAGING_CAP]))
    }

    // NOTE: the readiness edge is the only place slabs come back: the recv batch and the step
    // both write through them, so a slot that wants work must hold them first.
    pub(crate) fn adopt_ready(&mut self, now_ns: u64) {
        let H2Slots { slots, slabs, .. } = self;
        for entry in slots.iter_mut() {
            let Some(slot) = entry.as_deref_mut() else {
                continue;
            };
            if !slot.meta.occupied {
                continue;
            }
            let wants = slot.meta.wants_pump() || slot.meta.staged || slot.meta.timed_out;
            let missing = slot.meta.pending.is_none()
                || slot.conn.as_ref().is_some_and(|live| !live.holds_slabs());
            if wants && missing {
                let H2Slot { meta, conn, .. } = slot;
                slabs.adopt(meta, conn, now_ns);
            }
        }
    }

    pub(crate) fn adopt_at(&mut self, index: usize, now_ns: u64) -> bool {
        let H2Slots { slots, slabs, .. } = self;
        let Some(slot) = slots.get_mut(index).and_then(|entry| entry.as_deref_mut()) else {
            return false;
        };
        let H2Slot { meta, conn, .. } = slot;
        slabs.adopt(meta, conn, now_ns)
    }

    // NOTE: one pass per turn over at most MAX_H2_CONNS slots. A slot that never went quiet
    // pays one counter compare; the trim only runs on a wave whose worker has nothing left.
    pub(crate) fn reclaim_idle(&mut self, now_ns: u64) -> usize {
        let H2Slots { slots, slabs, .. } = self;
        let mut released = 0usize;
        for entry in slots.iter_mut() {
            let Some(slot) = entry.as_deref_mut() else {
                continue;
            };
            if !slot.meta.occupied || slot.meta.quiet_turns < SLAB_QUIET_TURNS {
                continue;
            }
            let H2Slot { meta, conn, .. } = slot;
            if !meta.slab_release_due(conn.as_ref(), now_ns) {
                continue;
            }
            if slabs.release(meta, conn) {
                released += 1;
            }
        }
        let pending = slots
            .iter()
            .flatten()
            .any(|slot| slot.meta.occupied && slot.meta.wants_pump());
        if released > 0 && !pending {
            trim_released_pages(released, H2_TRIM_MIN_RELEASED);
        }
        released
    }

    pub(crate) fn slot_mut(&mut self, index: usize) -> Option<&mut H2Slot> {
        self.slots
            .get_mut(index)
            .and_then(|slot| slot.as_deref_mut())
    }

    pub(crate) fn meta_mut(&mut self, index: usize) -> Option<&mut H2Meta> {
        self.slot_mut(index).map(|slot| &mut slot.meta)
    }

    pub(crate) fn close(&mut self, index: usize) {
        let H2Slots { slots, slabs, .. } = self;
        let Some(entry) = slots.get_mut(index) else {
            return;
        };
        if let Some(slot) = entry.as_deref_mut() {
            let H2Slot { meta, conn, .. } = slot;
            slabs.reclaim(meta, conn);
            slot.close();
        }
        *entry = None;
    }

    pub(crate) fn note_readiness(&mut self, event: &Readiness) -> bool {
        if event.token < H2_STREAM_TOKEN_BASE
            || event.token >= H2_STREAM_TOKEN_BASE + self.slots.len()
        {
            return false;
        }
        let index = event.token - H2_STREAM_TOKEN_BASE;
        if let Some(meta) = self.meta_mut(index)
            && meta.occupied
        {
            if event.readable || event.read_closed_hint {
                meta.readable = true;
                // NOTE: only a fresh edge proves bytes are already waiting, which is what
                // lets a batched recv complete instead of arming io_uring's own poll.
                meta.claimed = true;
            }
            if event.writable {
                meta.write_blocked = false;
            }
            if event.error_hint {
                meta.error = true;
            }
        }
        true
    }

    // NOTE: h2c tracks timeouts in slot scalars (no table tokens): head budget covers the
    // handshake, then last-progress drives body-progress activity and the idle bound.
    pub(crate) fn sweep_deadlines(&mut self, now_ns: u64, timeouts: ConnectionTimeouts) {
        for slot in self.slots.iter_mut().flatten() {
            let meta = &mut slot.meta;
            if !meta.occupied || meta.timed_out {
                continue;
            }
            if !meta.serving {
                if now_ns > meta.deadline_ns {
                    meta.timed_out = true;
                }
            } else if now_ns.saturating_sub(meta.last_progress_ns) > timeouts.body_progress_ns
                || now_ns > meta.deadline_ns
            {
                meta.timed_out = true;
            }
        }
    }
}

pub(crate) fn accept_staging() -> [Option<AcceptedConnection>; H2_ACCEPT_STAGING] {
    std::array::from_fn(|_| None)
}

pub(crate) fn idle_for_drain(meta: &H2Meta, conn: Option<&H2Conn<'_>>) -> bool {
    conn.is_some_and(|active| {
        active.handshake_complete() && active.staged_len() == 0 && !active.has_pending_responses()
    }) && !meta.pending_unflushed()
        && !meta.tls_unflushed()
}

pub(crate) fn pending_spare(meta: &mut H2Meta) -> &mut [u8] {
    let len = meta.pending_len.min(H2_PENDING_CAP);
    match meta.pending.as_deref_mut() {
        Some(staged) => &mut staged[len..],
        None => &mut [],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opened(conns: usize) -> H2Slots {
        let mut slots = H2Slots::new();
        for index in 0..conns {
            let slot = slots.ensure(index).expect("slot");
            assert!(slot.open());
            slot.meta.occupied = true;
            assert!(slots.adopt_at(index, 0));
        }
        slots
    }

    fn pointer(slots: &mut H2Slots, index: usize) -> (usize, usize) {
        let slot = slots.slot_mut(index).expect("slot");
        let pending = slot.meta.pending.as_ref().expect("pending").as_ptr() as usize;
        let staging = slot
            .conn
            .as_ref()
            .and_then(|conn| conn.staging.as_ref())
            .expect("staging")
            .as_ptr() as usize;
        (pending, staging)
    }

    #[test]
    fn a_quiet_connection_gives_its_slabs_back_and_the_next_edge_takes_them() {
        let mut slots = opened(1);
        let (pending, staging) = pointer(&mut slots, 0);
        slots.slot_mut(0).expect("slot").meta.quiet_turns = SLAB_QUIET_TURNS;
        assert_eq!(slots.reclaim_idle(SLAB_HOLD_NS), 1);
        let slot = slots.slot_mut(0).expect("slot");
        assert!(slot.meta.pending.is_none());
        assert!(slot.conn.as_ref().is_some_and(|conn| !conn.holds_slabs()));

        assert!(slots.adopt_at(0, SLAB_HOLD_NS));
        assert_eq!(pointer(&mut slots, 0), (pending, staging));
    }

    #[test]
    fn silence_alone_is_not_enough_for_a_connection_the_worker_never_saw_quiet() {
        let mut slots = opened(1);
        slots.slot_mut(0).expect("slot").meta.quiet_turns = SLAB_QUIET_TURNS - 1;
        assert_eq!(slots.reclaim_idle(SLAB_HOLD_NS), 0);
    }

    #[test]
    fn the_hold_window_alone_is_not_enough() {
        let mut slots = opened(1);
        slots.slot_mut(0).expect("slot").meta.quiet_turns = SLAB_QUIET_TURNS;
        assert_eq!(slots.reclaim_idle(SLAB_HOLD_NS - 1), 0);
    }

    #[test]
    fn an_armed_read_claim_keeps_its_slabs() {
        let mut slots = opened(1);
        let slot = slots.slot_mut(0).expect("slot");
        slot.meta.quiet_turns = SLAB_QUIET_TURNS;
        slot.meta.readable = true;
        slot.meta.claimed = true;
        assert_eq!(slots.reclaim_idle(SLAB_HOLD_NS), 0);
    }

    #[test]
    fn pending_output_keeps_its_slabs() {
        let mut slots = opened(1);
        let slot = slots.slot_mut(0).expect("slot");
        slot.meta.quiet_turns = SLAB_QUIET_TURNS;
        slot.meta.pending_len = 9;
        assert_eq!(slots.reclaim_idle(SLAB_HOLD_NS), 0);
    }

    #[test]
    fn the_pool_stops_at_its_cap_and_drops_the_excess() {
        let cap = 3;
        let mut slots = H2Slots::new();
        slots.slabs = H2Slabs::new(cap);
        for index in 0..cap * 4 {
            let slot = slots.ensure(index).expect("slot");
            assert!(slot.open());
            slot.meta.occupied = true;
            assert!(slots.adopt_at(index, 0));
            slots.slot_mut(index).expect("slot").meta.quiet_turns = SLAB_QUIET_TURNS;
        }
        assert_eq!(slots.reclaim_idle(SLAB_HOLD_NS), cap * 4);
        assert_eq!(slots.slabs.pending.len(), cap);
        assert_eq!(slots.slabs.staging.len(), cap);
        assert_eq!(slots.slabs.file.len(), cap);
        assert_eq!(slots.slabs.tunnel.len(), cap);
    }
}
