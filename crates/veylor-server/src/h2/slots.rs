use veylor_http2::{HpackEntrySlot, HpackHeader, StreamSlot};
use veylor_runtime::{AcceptedConnection, ConnectionTimeouts, TcpStream};
use veylor_tls::TlsConn;

use super::{
    ACK_SLOTS, DECODE_BYTES, DECODE_ENTRIES, ENCODE_BYTES, ENCODE_ENTRIES, H2Conn,
    HEADER_BLOCK_CAP, HEADER_SLOTS, INPUT_STAGING_CAP, MAX_H2_CONNS, STREAM_SLOTS,
};

pub(crate) const H2_STREAM_TOKEN_BASE: usize = 1024;
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
    pub(crate) tls: Option<TlsConn>,
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

#[derive(Default)]
pub(crate) struct H2BufSlots {
    pub(crate) b0: H2Buffers,
    pub(crate) b1: H2Buffers,
    pub(crate) b2: H2Buffers,
    pub(crate) b3: H2Buffers,
    pub(crate) b4: H2Buffers,
    pub(crate) b5: H2Buffers,
    pub(crate) b6: H2Buffers,
    pub(crate) b7: H2Buffers,
}

#[derive(Default)]
pub(crate) struct H2MetaSlots {
    pub(crate) m0: H2Meta,
    pub(crate) m1: H2Meta,
    pub(crate) m2: H2Meta,
    pub(crate) m3: H2Meta,
    pub(crate) m4: H2Meta,
    pub(crate) m5: H2Meta,
    pub(crate) m6: H2Meta,
    pub(crate) m7: H2Meta,
}

pub(crate) fn accept_staging() -> [Option<AcceptedConnection>; H2_ACCEPT_STAGING] {
    std::array::from_fn(|_| None)
}
pub(crate) fn occupied(metas: &H2MetaSlots) -> usize {
    (0..MAX_H2_CONNS)
        .filter(|&index| meta_ref(metas, index).occupied)
        .count()
}

pub(crate) fn free_slot(metas: &H2MetaSlots) -> Option<usize> {
    (0..MAX_H2_CONNS).find(|&index| !meta_ref(metas, index).occupied)
}

pub(crate) fn wants_pump(metas: &H2MetaSlots) -> bool {
    (0..MAX_H2_CONNS)
        .any(|index| meta_ref(metas, index).occupied && meta_ref(metas, index).wants_pump())
}

pub(crate) fn meta_ref(metas: &H2MetaSlots, index: usize) -> &H2Meta {
    match index {
        0 => &metas.m0,
        1 => &metas.m1,
        2 => &metas.m2,
        3 => &metas.m3,
        4 => &metas.m4,
        5 => &metas.m5,
        6 => &metas.m6,
        _ => &metas.m7,
    }
}

pub(crate) fn meta_at(metas: &mut H2MetaSlots, index: usize) -> &mut H2Meta {
    match index {
        0 => &mut metas.m0,
        1 => &mut metas.m1,
        2 => &mut metas.m2,
        3 => &mut metas.m3,
        4 => &mut metas.m4,
        5 => &mut metas.m5,
        6 => &mut metas.m6,
        _ => &mut metas.m7,
    }
}

// NOTE: h2c tracks timeouts in slot scalars (no table tokens): head budget covers the
// handshake, then last-progress drives body-progress activity and the idle bound.
pub(crate) fn sweep_deadlines(metas: &mut H2MetaSlots, now_ns: u64, timeouts: ConnectionTimeouts) {
    for index in 0..MAX_H2_CONNS {
        let meta = meta_at(metas, index);
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

pub(crate) fn idle_for_drain(meta: &H2Meta, conn: Option<&H2Conn<'_>>) -> bool {
    conn.is_some_and(|active| active.handshake_complete() && active.staged_len() == 0)
        && !meta.pending_unflushed()
        && !meta.tls_unflushed()
}

pub(crate) fn pending_spare(meta: &mut H2Meta) -> &mut [u8] {
    let len = meta.pending_len.min(H2_PENDING_CAP);
    match meta.pending.as_deref_mut() {
        Some(staged) => &mut staged[len..],
        None => &mut [],
    }
}
