#![allow(unsafe_code)]

use veylor_runtime::{AcceptedConnection, ConnectionTimeouts, Readiness, TcpStream};
use veylor_tls::TlsConn;

use crate::connection::tls::TLS_PENDING_MIN_BYTES;
use crate::connection::{
    BoundedPool, OUTPUT_CAP, SLAB_HOLD_NS, SLAB_QUIET_TURNS, trim_released_pages,
};

use super::file::FileTable;
pub(crate) use super::tables::H2Slot;
use super::tables::{H2Buffers, H2DecoderSlab, H2EngineSlab};
use super::{H2Conn, INPUT_STAGING_CAP, MAX_H2_CONNS, TUNNEL_STEP};

// NOTE: H1 tokens occupy 1..=MAX_CONNECTIONS (`slot + 1`), so the H2 token range starts
// above the whole H1 table instead of at MAX_CONNECTIONS.
pub(crate) const H2_STREAM_TOKEN_BASE: usize = crate::static_server::MAX_CONNECTIONS + 1;
pub(crate) const H2_ACCEPT_STAGING: usize = 8;
pub(crate) const H2_PENDING_CAP: usize = 65_536 + 512;

// NOTE: the ciphertext read quantum: one full TLS record (16 KiB payload plus its AEAD tag
// and 5-byte header), so a batch read usually carries whole records and a partial one only
// when the record and the read quantum disagree.
pub(crate) const TLS_CIPHER_CAP: usize = 16_640;

// NOTE: freed H2 slabs are kept per worker only up to this many of each kind; the rest go back
// to the allocator, so a wave of idle connections cannot ratchet the process. Eight stays the
// ceiling and it is reached at half the worker's connection limit: one set per four live
// connections, at least two. At the 256-connection mark a worker carries sixteen slots and
// keeps four sets instead of eight, while the 512 mark and above keep today's cap and pay no
// extra allocation churn on the ramp.
pub(crate) const H2_SLAB_POOL_CAP: usize = 8;
pub(crate) const H2_SLAB_POOL_MIN: usize = 2;
pub(crate) const H2_CONNS_PER_SLAB_SET: usize = 4;

// NOTE: one released H2 connection frees more than a hundred times the memory of one released
// H1 connection, so any wave that is not a single trickle pays for the trim walk. A pool shrink
// counts its dropped slabs in the same wave, for the same reason: they are the pass where the
// pages actually leave the process.
pub(crate) const H2_TRIM_MIN_RELEASED: usize = 4;

// NOTE: the worker-local pool for everything an idle H2 connection does not need: the engine
// tables' arrays, the decoder slab on close, the file-transfer table once a transfer ends, the
// input staging, pending output, and file/tunnel chunk scratches, and the TLS ciphertext read
// buffer. The encoder's own counters stay with the connection; its storage travels and is
// reset on adopt.
pub(crate) struct H2Slabs {
    pub(crate) engine: BoundedPool<H2EngineSlab>,
    pub(crate) decoder: BoundedPool<H2DecoderSlab>,
    pub(crate) files: BoundedPool<FileTable>,
    staging: BoundedPool<[u8; INPUT_STAGING_CAP]>,
    pending: BoundedPool<[u8; H2_PENDING_CAP]>,
    file: BoundedPool<[u8; OUTPUT_CAP]>,
    tunnel: BoundedPool<[u8; TUNNEL_STEP]>,
    cipher: BoundedPool<[u8; TLS_CIPHER_CAP]>,
}

impl H2Slabs {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            engine: BoundedPool::new(cap),
            decoder: BoundedPool::new(cap),
            files: BoundedPool::new(cap),
            staging: BoundedPool::new(cap),
            pending: BoundedPool::new(cap),
            file: BoundedPool::new(cap),
            tunnel: BoundedPool::new(cap),
            cipher: BoundedPool::new(cap),
        }
    }

    // NOTE: the pool follows the worker's live connection count between passes: one set per
    // four connections, at least two and at most the ceiling, so the cap stops moving at half
    // the worker's connection limit. The floor holds even at zero connections, because that is
    // the moment the next accept storm needs it: dropping the pool there turns every slab of
    // the next wave into an allocation. Raising the cap needs no allocation here; the next
    // release fills it. The return counts the slabs a shrink handed back, which the caller
    // folds into the wave that decides the heap trim.
    pub(crate) fn scale_to(&mut self, live: usize) -> usize {
        let cap = (live / H2_CONNS_PER_SLAB_SET).clamp(H2_SLAB_POOL_MIN, H2_SLAB_POOL_CAP);
        self.engine.trim_to(cap)
            + self.decoder.trim_to(cap)
            + self.files.trim_to(cap)
            + self.staging.trim_to(cap)
            + self.pending.trim_to(cap)
            + self.file.trim_to(cap)
            + self.tunnel.trim_to(cap)
            + self.cipher.trim_to(cap)
    }

    // NOTE: installed together on the readiness edge that follows a release, so a step never
    // runs with one of the slabs missing; a pool hit is a pointer move, a miss the only case
    // that allocates. The engine slab is re-attached here and its encoder reset before the
    // next header block can be encoded. The ciphertext slab exists only for TLS, which is why
    // it is the one class an h2c connection never takes.
    pub(crate) fn adopt(
        &mut self,
        meta: &mut H2Meta,
        conn: &mut Option<H2Conn<'_>>,
        bufs: &mut H2Buffers,
        now_ns: u64,
    ) -> bool {
        let Some(conn) = conn.as_mut() else {
            return false;
        };
        if bufs.engine.is_none() {
            bufs.engine = Some(self.engine.take(H2EngineSlab::new));
        }
        if !conn.tables_attached() {
            let Some(engine) = bufs.engine.as_deref_mut() else {
                return false;
            };
            if !conn.attach_engine(engine) {
                return false;
            }
        }
        if meta.pending.is_none() {
            meta.pending = Some(self.pending.take(|| Box::new([0; H2_PENDING_CAP])));
        }
        if conn.staging.is_none() {
            conn.staging = Some(self.staging.take(|| Box::new([0; INPUT_STAGING_CAP])));
        }
        if conn.file_scratch.is_none() {
            conn.file_scratch = Some(self.file.take(|| Box::new([0; OUTPUT_CAP])));
        }
        if conn.tunnel_scratch.is_none() {
            conn.tunnel_scratch = Some(self.tunnel.take(|| Box::new([0; TUNNEL_STEP])));
        }
        if meta.tls.is_some() && meta.tls_cipher.is_none() {
            meta.tls_cipher = Some(self.cipher.take(|| Box::new([0; TLS_CIPHER_CAP])));
        }
        meta.reserve_tls_scratch();
        meta.note_slab_adopt(now_ns);
        true
    }

    pub(crate) fn release(
        &mut self,
        meta: &mut H2Meta,
        conn: &mut Option<H2Conn<'_>>,
        bufs: &mut H2Buffers,
    ) -> bool {
        let transports = self.give_back(meta, conn);
        let mut engine = None;
        if let Some(live) = conn.as_mut()
            && live.detach_engine()
        {
            engine = bufs.engine.take();
        }
        let returned = engine.is_some();
        if let Some(engine) = engine {
            self.engine.give(engine);
        }
        if !transports && !returned {
            return false;
        }
        meta.note_slab_release();
        true
    }

    // NOTE: a closing connection returns whatever it still holds; staged input keeps its
    // staging slab private to the drop instead of handing live bytes to the next owner. The
    // engine and decoder slabs come back through `H2Slot::retire`, after the tables drop.
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
            if let Some(files) = live.take_file_table() {
                self.files.give(files);
                released = true;
            }
        }
        if let Some(cipher) = meta.tls_cipher.take() {
            self.cipher.give(cipher);
            released = true;
        }
        if !released {
            return false;
        }
        meta.pending_len = 0;
        meta.pending_sent = 0;
        meta.tls_cipher_read = 0;
        meta.tls_cipher_fed = 0;
        meta.tls_plain_queued = false;
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
    pub(crate) tls_cipher: Option<Box<[u8; TLS_CIPHER_CAP]>>,
    pub(crate) tls_cipher_read: usize,
    pub(crate) tls_cipher_fed: usize,
    pub(crate) tls_plain_queued: bool,
    pub(crate) tls_wire_eof: bool,
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

    pub(crate) fn tls_cipher_pending(&self) -> bool {
        self.tls_cipher_read > self.tls_cipher_fed
    }

    pub(crate) fn wants_pump(&self) -> bool {
        self.readable
            || self.staged
            || self.tls_cipher_pending()
            || self.tls_plain_queued
            || self.tls_wire_eof
            || ((self.pending_unflushed() || self.tls_unflushed()) && !self.write_blocked)
    }

    // NOTE: waiting with slabs and nothing to do: no armed read claim, no staged input, no
    // ciphertext or queued plaintext to decrypt, no stream or parked response open on the
    // wire, no file or tunnel transfer to resume.
    pub(crate) fn slab_idle(&self, conn: Option<&H2Conn<'_>>) -> bool {
        if !self.occupied || self.readable || self.claimed || self.staged {
            return false;
        }
        if self.timed_out || self.error {
            return false;
        }
        if self.tls_cipher_pending() || self.tls_plain_queued || self.tls_wire_eof {
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
    }

    pub(crate) fn drop_tls_scratch(&mut self) {
        if self.tls.is_none() {
            return;
        }
        self.tls_pending = Vec::new();
        self.tls_pending_sent = 0;
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

    #[cfg(test)]
    pub(crate) fn ensure(&mut self, index: usize) -> Option<&mut H2Slot> {
        let entry = self.slots.get_mut(index)?;
        Some(entry.get_or_insert_with(H2Slot::boxed))
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
                || (slot.meta.tls.is_some() && slot.meta.tls_cipher.is_none())
                || slot.conn.as_ref().is_some_and(|live| live.needs_slabs());
            if wants && missing {
                let H2Slot {
                    meta, conn, bufs, ..
                } = slot;
                slabs.adopt(meta, conn, bufs, now_ns);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn adopt_at(&mut self, index: usize, now_ns: u64) -> bool {
        let H2Slots { slots, slabs, .. } = self;
        let Some(slot) = slots.get_mut(index).and_then(|entry| entry.as_deref_mut()) else {
            return false;
        };
        slot.prepare(slabs, now_ns)
    }

    // NOTE: one pass per turn over at most MAX_H2_CONNS slots. A slot that never went quiet
    // pays one counter compare; the trim only runs on a wave whose worker has nothing left.
    // The live count is read before the release wave so this pass already gives the excess
    // away instead of filling a cap the worker no longer justifies.
    pub(crate) fn reclaim_idle(&mut self, now_ns: u64) -> usize {
        let live = self.occupied();
        let H2Slots { slots, slabs, .. } = self;
        let dropped = slabs.scale_to(live);
        let mut released = 0usize;
        for entry in slots.iter_mut() {
            let Some(slot) = entry.as_deref_mut() else {
                continue;
            };
            if !slot.meta.occupied || slot.meta.quiet_turns < SLAB_QUIET_TURNS {
                continue;
            }
            let H2Slot {
                meta, conn, bufs, ..
            } = slot;
            if !meta.slab_release_due(conn.as_ref(), now_ns) {
                continue;
            }
            if slabs.release(meta, conn, bufs) {
                released += 1;
            }
        }
        let pending = slots
            .iter()
            .flatten()
            .any(|slot| slot.meta.occupied && slot.meta.wants_pump());
        if (released > 0 || dropped > 0) && !pending {
            trim_released_pages(released + dropped, H2_TRIM_MIN_RELEASED);
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
            let (engine, decoder) = slot.retire();
            if let Some(engine) = engine {
                slabs.engine.give(engine);
            }
            if let Some(decoder) = decoder {
                slabs.decoder.give(decoder);
            }
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
    use veylor_tls::TlsAcceptor;

    const CERT_DER: &[u8] = include_bytes!("../../../veylor-tls/tests/testdata/server_cert.der");
    const KEY_DER: &[u8] = include_bytes!("../../../veylor-tls/tests/testdata/server_key.der");

    fn opened(conns: usize) -> H2Slots {
        let mut slots = H2Slots::new();
        for index in 0..conns {
            let slot = slots.ensure(index).expect("slot");
            slot.meta.occupied = true;
            assert!(slots.adopt_at(index, 0));
        }
        slots
    }

    fn pointers(slots: &mut H2Slots, index: usize) -> (usize, usize, usize) {
        let slot = slots.slot_mut(index).expect("slot");
        let pending = slot.meta.pending.as_ref().expect("pending").as_ptr() as usize;
        let staging = slot
            .conn
            .as_ref()
            .and_then(|conn| conn.staging.as_ref())
            .expect("staging")
            .as_ptr() as usize;
        let slots = slot.bufs.engine.as_ref().expect("engine").slots_pointer();
        (pending, staging, slots)
    }

    #[test]
    fn a_quiet_connection_gives_its_slabs_back_and_the_next_edge_takes_them() {
        let mut slots = opened(1);
        let first = pointers(&mut slots, 0);
        slots.slot_mut(0).expect("slot").meta.quiet_turns = SLAB_QUIET_TURNS;
        assert_eq!(slots.reclaim_idle(SLAB_HOLD_NS), 1);
        let slot = slots.slot_mut(0).expect("slot");
        assert!(slot.meta.pending.is_none());
        assert!(slot.bufs.engine.is_none());
        assert!(slot.conn.as_ref().is_some_and(|conn| !conn.holds_slabs()));

        assert!(slots.adopt_at(0, SLAB_HOLD_NS));
        assert_eq!(pointers(&mut slots, 0), first);
    }

    #[test]
    fn a_connection_that_never_served_a_file_holds_no_transfer_table() {
        let mut slots = opened(1);
        assert!(slots.slot_mut(0).expect("slot").bufs.engine.is_some());
        assert!(
            !slots
                .slot_mut(0)
                .expect("slot")
                .conn
                .as_ref()
                .expect("conn")
                .file_table_allocated()
        );
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
    fn the_pool_cap_tracks_the_live_connection_count() {
        let mut slabs = H2Slabs::new(H2_SLAB_POOL_CAP);
        slabs.scale_to(0);
        assert_eq!(slabs.pending.cap(), H2_SLAB_POOL_MIN);
        assert_eq!(slabs.engine.cap(), H2_SLAB_POOL_MIN);
        slabs.scale_to(1);
        assert_eq!(slabs.pending.cap(), H2_SLAB_POOL_MIN);
        slabs.scale_to(H2_CONNS_PER_SLAB_SET * 4);
        assert_eq!(slabs.pending.cap(), 4);
        assert_eq!(slabs.decoder.cap(), 4);
        assert_eq!(slabs.cipher.cap(), 4);
        slabs.scale_to(H2_CONNS_PER_SLAB_SET * 2);
        assert_eq!(slabs.pending.cap(), H2_SLAB_POOL_MIN);
        slabs.scale_to(MAX_H2_CONNS);
        assert_eq!(slabs.pending.cap(), H2_SLAB_POOL_CAP);
        slabs.scale_to(MAX_H2_CONNS * 2);
        assert_eq!(slabs.pending.cap(), H2_SLAB_POOL_CAP);
    }

    #[test]
    fn a_worker_below_the_ceiling_keeps_a_scaled_pool_and_reuses_it() {
        let live = H2_CONNS_PER_SLAB_SET * 3;
        let scaled = live / H2_CONNS_PER_SLAB_SET;
        let mut slots = opened(live);
        let released: Vec<(usize, usize, usize)> =
            (0..live).map(|index| pointers(&mut slots, index)).collect();
        for index in 0..live {
            slots.slot_mut(index).expect("slot").meta.quiet_turns = SLAB_QUIET_TURNS;
        }
        assert_eq!(slots.reclaim_idle(SLAB_HOLD_NS), live);
        assert_eq!(slots.slabs.pending.len(), scaled);
        assert_eq!(slots.slabs.staging.len(), scaled);
        assert_eq!(slots.slabs.file.len(), scaled);
        assert_eq!(slots.slabs.tunnel.len(), scaled);
        assert_eq!(slots.slabs.engine.len(), scaled);
        assert_eq!(slots.slabs.cipher.len(), 0);

        assert!(slots.adopt_at(live - 1, SLAB_HOLD_NS));
        assert!(released.contains(&pointers(&mut slots, live - 1)));
    }

    #[test]
    fn an_empty_worker_keeps_the_warm_floor_and_drops_the_rest() {
        let live = H2_CONNS_PER_SLAB_SET + 4;
        let mut slots = opened(live);
        for index in 0..live {
            slots.slot_mut(index).expect("slot").meta.quiet_turns = SLAB_QUIET_TURNS;
        }
        assert_eq!(slots.reclaim_idle(SLAB_HOLD_NS), live);
        for index in 0..live {
            slots.close(index);
        }
        slots.reclaim_idle(SLAB_HOLD_NS);
        assert_eq!(slots.slabs.pending.len(), H2_SLAB_POOL_MIN);
        assert_eq!(slots.slabs.engine.len(), H2_SLAB_POOL_MIN);
        assert_eq!(slots.slabs.decoder.len(), H2_SLAB_POOL_MIN);
        assert_eq!(slots.slabs.pending.cap(), H2_SLAB_POOL_MIN);

        let slot = slots.ensure(0).expect("slot");
        slot.meta.occupied = true;
        assert!(slots.adopt_at(0, 0));
    }

    #[test]
    fn a_tls_connection_takes_and_returns_the_ciphertext_slab() {
        let mut acceptor = TlsAcceptor::new(CERT_DER, KEY_DER, 1).expect("acceptor builds");
        let mut slots = opened(1);
        let slot = slots.slot_mut(0).expect("slot");
        slot.meta.tls = Some(Box::new(acceptor.admit().expect("slot available")));
        assert!(slots.adopt_at(0, 0));

        let taken = slots
            .slot_mut(0)
            .expect("slot")
            .meta
            .tls_cipher
            .as_ref()
            .expect("cipher slab")
            .as_ptr() as usize;

        slots.slot_mut(0).expect("slot").meta.quiet_turns = SLAB_QUIET_TURNS;
        assert_eq!(slots.reclaim_idle(SLAB_HOLD_NS), 1);
        assert!(slots.slot_mut(0).expect("slot").meta.tls_cipher.is_none());
        assert_eq!(slots.slabs.cipher.len(), 1);

        assert!(slots.adopt_at(0, SLAB_HOLD_NS));
        let retaken = slots
            .slot_mut(0)
            .expect("slot")
            .meta
            .tls_cipher
            .as_ref()
            .expect("cipher slab")
            .as_ptr() as usize;
        assert_eq!(retaken, taken);
    }

    #[test]
    fn closing_a_slot_returns_its_decoder_slab_and_the_next_accept_takes_it() {
        let mut slots = opened(1);
        let decoder = slots
            .slot_mut(0)
            .expect("slot")
            .bufs
            .decoder
            .as_ref()
            .expect("decoder")
            .scratch_pointer();
        slots.close(0);
        assert_eq!(slots.slots.len(), MAX_H2_CONNS);
        assert!(slots.slot_mut(0).is_none());
        assert_eq!(slots.slabs.decoder.len(), 1);
        assert_eq!(slots.slabs.engine.len(), 1);

        let slot = slots.ensure(0).expect("slot");
        slot.meta.occupied = true;
        assert!(slots.adopt_at(0, 0));
        assert_eq!(
            slots
                .slot_mut(0)
                .expect("slot")
                .bufs
                .decoder
                .as_ref()
                .expect("decoder")
                .scratch_pointer(),
            decoder
        );
    }
}
