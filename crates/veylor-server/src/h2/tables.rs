#![allow(unsafe_code)]

use veylor_http2::{H2Role, HpackEntrySlot, HpackHeader, HpackTable, StreamSlot, StreamTable};

use super::slots::{H2Meta, H2Slabs};
use super::{
    ACK_SLOTS, DECODE_BYTES, DECODE_ENTRIES, ENCODE_BYTES, ENCODE_ENTRIES, H2Conn,
    HEADER_BLOCK_CAP, HEADER_SLOTS, STREAM_SLOTS,
};

// NOTE: the storage behind the stream table's slot array and header-block assembler, plus the
// encoder's entry and scratch arrays. The tables keep their peer-visible counters and settings
// when a connection gives this slab back; only the arrays travel, and the encoder's next block
// opens with a table size update to zero so the peer drops what the arrays used to hold.
pub(crate) struct H2EngineSlab {
    slots: Box<[StreamSlot; STREAM_SLOTS]>,
    block: Box<[u8; HEADER_BLOCK_CAP]>,
    headers: Box<[HpackHeader; HEADER_SLOTS]>,
    enc_entries: Box<[HpackEntrySlot; ENCODE_ENTRIES]>,
    enc_scratch: Box<[u8; ENCODE_BYTES]>,
}

pub(crate) struct H2EngineView {
    pub(crate) slots: &'static mut [StreamSlot],
    pub(crate) block: &'static mut [u8],
    pub(crate) headers: &'static mut [HpackHeader],
    pub(crate) enc_entries: &'static mut [HpackEntrySlot],
    pub(crate) enc_scratch: &'static mut [u8],
}

impl H2EngineSlab {
    pub(crate) fn new() -> Box<Self> {
        Box::new(Self {
            slots: Box::new([StreamSlot::EMPTY; STREAM_SLOTS]),
            block: Box::new([0; HEADER_BLOCK_CAP]),
            headers: Box::new([HpackHeader::EMPTY; HEADER_SLOTS]),
            enc_entries: Box::new([HpackEntrySlot::EMPTY; ENCODE_ENTRIES]),
            enc_scratch: Box::new([0; ENCODE_BYTES]),
        })
    }

    // SAFETY: the slab is Box-owned by the slot's `bufs` and is neither moved nor taken while
    // the returned views live; the caller drops the borrowing tables first.
    pub(crate) unsafe fn view(&mut self) -> H2EngineView {
        let Self {
            slots,
            block,
            headers,
            enc_entries,
            enc_scratch,
        } = self;
        H2EngineView {
            slots: unsafe { extend(&mut slots[..]) },
            block: unsafe { extend(&mut block[..]) },
            headers: unsafe { extend(&mut headers[..]) },
            enc_entries: unsafe { extend(&mut enc_entries[..]) },
            enc_scratch: unsafe { extend(&mut enc_scratch[..]) },
        }
    }

    #[cfg(test)]
    pub(crate) fn slots_pointer(&self) -> usize {
        self.slots.as_ptr() as usize
    }
}

// NOTE: the decoder table is peer-visible state, so it stays with the connection for its whole
// life; only a closed connection returns this slab, and the next owner starts from a fresh
// table at the same place a new connection would.
pub(crate) struct H2DecoderSlab {
    entries: Box<[HpackEntrySlot; DECODE_ENTRIES]>,
    scratch: Box<[u8; DECODE_BYTES]>,
    ack: Box<[u32; ACK_SLOTS]>,
}

impl H2DecoderSlab {
    pub(crate) fn new() -> Box<Self> {
        Box::new(Self {
            entries: Box::new([HpackEntrySlot::EMPTY; DECODE_ENTRIES]),
            scratch: Box::new([0; DECODE_BYTES]),
            ack: Box::new([0; ACK_SLOTS]),
        })
    }

    // SAFETY: as `H2EngineSlab::view`.
    pub(crate) unsafe fn view(
        &mut self,
    ) -> (
        &'static mut [HpackEntrySlot],
        &'static mut [u8],
        &'static mut [u32],
    ) {
        let Self {
            entries,
            scratch,
            ack,
        } = self;
        unsafe {
            (
                extend(&mut entries[..]),
                extend(&mut scratch[..]),
                extend(&mut ack[..]),
            )
        }
    }

    #[cfg(test)]
    pub(crate) fn scratch_pointer(&self) -> usize {
        self.scratch.as_ptr() as usize
    }
}

pub(crate) struct H2Buffers {
    pub(crate) engine: Option<Box<H2EngineSlab>>,
    pub(crate) decoder: Option<Box<H2DecoderSlab>>,
}

impl H2Buffers {
    pub(crate) fn new() -> Self {
        Self {
            engine: None,
            decoder: None,
        }
    }
}

struct H2Tables {
    streams: StreamTable<'static>,
    decoder: HpackTable<'static>,
    encoder: HpackTable<'static>,
}

// NOTE: the server slot is a controlled self-reference: `bufs` is a stable Box and the slabs
// inside it never move while attached, the table struct lives at a fixed address inside the
// slot, and `conn` borrows the tables. The slot is always held through Box<H2Slot>, so none of
// those addresses move, and the field order drops conn before tables before bufs.
pub(crate) struct H2Slot {
    pub(crate) conn: Option<H2Conn<'static>>,
    tables: Option<H2Tables>,
    pub(crate) bufs: Box<H2Buffers>,
    pub(crate) meta: H2Meta,
}

impl H2Slot {
    pub(crate) fn boxed() -> Box<Self> {
        Box::new(Self {
            conn: None,
            tables: None,
            bufs: Box::new(H2Buffers::new()),
            meta: H2Meta::default(),
        })
    }

    // NOTE: the accept path and every re-adopt after a release come through here: the two
    // engine slabs are installed first, the tables are built on the first pass, and the
    // transport slabs land last so a step never sees half a buffer set.
    pub(crate) fn prepare(&mut self, slabs: &mut H2Slabs, now_ns: u64) -> bool {
        let H2Slot {
            conn,
            tables,
            bufs,
            meta,
        } = self;
        if bufs.engine.is_none() {
            bufs.engine = Some(slabs.engine.take(H2EngineSlab::new));
        }
        if bufs.decoder.is_none() {
            bufs.decoder = Some(slabs.decoder.take(H2DecoderSlab::new));
        }
        if conn.is_none() && !open_tables(conn, tables, bufs) {
            return false;
        }
        slabs.adopt(meta, conn, bufs, now_ns)
    }

    // NOTE: a closing slot hands both slabs back before it drops, so the next accept starts
    // warm; the tables and the conn borrow the slabs and must be gone first.
    pub(crate) fn retire(&mut self) -> (Option<Box<H2EngineSlab>>, Option<Box<H2DecoderSlab>>) {
        self.conn = None;
        self.tables = None;
        self.meta = H2Meta::default();
        (self.bufs.engine.take(), self.bufs.decoder.take())
    }
}

fn open_tables(
    conn: &mut Option<H2Conn<'static>>,
    tables: &mut Option<H2Tables>,
    bufs: &mut H2Buffers,
) -> bool {
    *conn = None;
    *tables = None;
    let H2Buffers {
        engine, decoder, ..
    } = bufs;
    let (Some(engine), Some(decoder)) = (engine.as_deref_mut(), decoder.as_deref_mut()) else {
        return false;
    };
    // SAFETY: both slabs are Box-owned fields of the slot's `bufs`, which outlives the conn.
    let (entries, scratch, ack) = unsafe { decoder.view() };
    let engine = unsafe { engine.view() };
    let H2EngineView {
        slots,
        block,
        headers,
        enc_entries,
        enc_scratch,
    } = engine;
    let built = H2Tables {
        streams: StreamTable::new(slots, H2Role::Server, block, HEADER_BLOCK_CAP as u32),
        decoder: HpackTable::new(entries, scratch, 4_096),
        encoder: HpackTable::new(enc_entries, enc_scratch, 4_096),
    };
    *tables = Some(built);
    let Some(stored) = tables.as_mut() else {
        return false;
    };
    let stored = stored as *mut H2Tables;
    // SAFETY: the tables live in this boxed slot and are dropped before `bufs`; the conn
    // borrows them and nothing else reaches them while it is alive.
    let live = unsafe {
        let tables = &mut *stored;
        H2Conn::new(
            &mut tables.streams,
            &mut tables.decoder,
            &mut tables.encoder,
            ack,
            headers,
        )
    };
    *conn = Some(unsafe { std::mem::transmute::<H2Conn<'_>, H2Conn<'static>>(live) });
    true
}

// SAFETY: the caller guarantees the referent outlives every borrow handed out from here.
unsafe fn extend<T: ?Sized>(value: &mut T) -> &'static mut T {
    unsafe { &mut *(value as *mut T) }
}
