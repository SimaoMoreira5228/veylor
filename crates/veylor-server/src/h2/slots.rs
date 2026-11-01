#![allow(unsafe_code)]

use veylor_http2::{H2Role, HpackEntrySlot, HpackHeader, HpackTable, StreamSlot, StreamTable};
use veylor_runtime::{AcceptedConnection, ConnectionTimeouts, Readiness, TcpStream};
use veylor_tls::TlsConn;

use super::{
    ACK_SLOTS, DECODE_BYTES, DECODE_ENTRIES, ENCODE_BYTES, ENCODE_ENTRIES, H2Conn,
    HEADER_BLOCK_CAP, HEADER_SLOTS, INPUT_STAGING_CAP, MAX_H2_CONNS, STREAM_SLOTS,
};

// NOTE: H1 tokens occupy 1..=MAX_CONNECTIONS (`slot + 1`), so the H2 token range starts
// above the whole H1 table instead of at MAX_CONNECTIONS.
pub(crate) const H2_STREAM_TOKEN_BASE: usize = crate::static_server::MAX_CONNECTIONS + 1;
pub(crate) const H2_ACCEPT_STAGING: usize = 8;
pub(crate) const H2_PENDING_CAP: usize = 16_384 + 512;

// NOTE: per-conn backing stores, boxed so idle slots cost two words (~57 KiB live per
// active conn; 17 KiB pending rides the meta). Sizes mirror the step-driver harness.
pub(crate) struct H2Buffers {
    pub(crate) staging: Box<[u8; INPUT_STAGING_CAP]>,
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
            staging: Box::new([0; INPUT_STAGING_CAP]),
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

impl Default for H2Buffers {
    fn default() -> Self {
        Self::new()
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
    pub(crate) write_blocked: bool,
    pub(crate) watching_write: bool,
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
}

struct H2Tables {
    streams: StreamTable<'static>,
    decoder: HpackTable<'static>,
    encoder: HpackTable<'static>,
}

impl H2Tables {
    /// # Safety
    ///
    /// `bufs` must be a heap allocation that outlives this value and is never moved.
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
                &mut bufs.staging[..],
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
    slots: Vec<Option<Box<H2Slot>>>,
}

impl H2Slots {
    pub(crate) fn new() -> Self {
        Self {
            slots: (0..MAX_H2_CONNS).map(|_| None).collect(),
        }
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

    pub(crate) fn slot_mut(&mut self, index: usize) -> Option<&mut H2Slot> {
        self.slots
            .get_mut(index)
            .and_then(|slot| slot.as_deref_mut())
    }

    pub(crate) fn meta_mut(&mut self, index: usize) -> Option<&mut H2Meta> {
        self.slot_mut(index).map(|slot| &mut slot.meta)
    }

    pub(crate) fn close(&mut self, index: usize) {
        if let Some(slot) = self.slots.get_mut(index).and_then(|slot| slot.as_mut()) {
            slot.close();
        }
        if let Some(slot) = self.slots.get_mut(index) {
            *slot = None;
        }
    }

    pub(crate) fn note_readiness(&mut self, event: &Readiness) -> bool {
        if event.token < H2_STREAM_TOKEN_BASE
            || event.token >= H2_STREAM_TOKEN_BASE + self.slots.len()
        {
            return false;
        }
        let index = event.token - H2_STREAM_TOKEN_BASE;
        if let Some(meta) = self.meta_mut(index) {
            if meta.occupied {
                if event.readable || event.read_closed_hint {
                    meta.readable = true;
                }
                if event.writable {
                    meta.write_blocked = false;
                }
                if event.error_hint {
                    meta.error = true;
                }
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
