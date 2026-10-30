use std::io::{self, Read, Write};

use veylor_http2::{HpackEntrySlot, HpackHeader, StreamSlot};
use veylor_runtime::{
    AcceptedConnection, ConnectionTimeouts, ConnectionToken, PollInterest, Poller, Readiness,
    TcpListener, TcpStream,
};
use veylor_tls::{TlsAcceptor, TlsConn};

use super::{
    ACK_SLOTS, DECODE_BYTES, DECODE_ENTRIES, ENCODE_BYTES, ENCODE_ENTRIES, FrozenNativeRoute,
    FrozenRoute, FrozenRoutesView, H2Conn, HEADER_BLOCK_CAP, HEADER_SLOTS, INPUT_STAGING_CAP,
    MAX_H2_CONNS, STREAM_SLOTS,
};
use crate::connection::ConnClose;
use crate::connection::tls::{
    TlsWriteOutcome, tls_encrypt_bytes, tls_read_decrypted, tls_write_once,
};
use crate::host::{HostShared, StagedWork};
use crate::static_routes::MAX_RESPONSE_BODY;
use crate::static_server::turn::apply_h2_staged;
use crate::static_server::{CloseCounts, H2StaticRoute, MAX_CONNECTIONS};

// NOTE: spanning driver borrows persist across turns: only same-site overwrites end
// old regions, so workers pre-allocate buffer sets and borrow FIELDS at one inline
// build site per slot; closes visibly clear the conn. 8x32 streams out-multiplex H1.
pub(crate) const H2C_LISTENER_TOKEN: usize = usize::MAX - 1;
pub(crate) const H2_STREAM_TOKEN_BASE: usize = 1024;
pub(crate) const H2_ACCEPT_STAGING: usize = 8;
pub(crate) const H2_PENDING_CAP: usize = 16_384 + 512;
pub(crate) const H2_FLUSH_THRESHOLD: usize = 1024;
pub(crate) const MAX_H2_STEPS_PER_TURN: usize = 4;

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
    stream: Option<TcpStream>,
    pub(crate) tls: Option<TlsConn>,
    tls_pending: Vec<u8>,
    tls_pending_sent: usize,
    tls_plain: Vec<u8>,
    pub(crate) occupied: bool,
    readable: bool,
    write_blocked: bool,
    watching_write: bool,
    error: bool,
    staged: bool,
    serving: bool,
    timed_out: bool,
    pub(crate) token: usize,
    pending: Option<Box<[u8; H2_PENDING_CAP]>>,
    pub(crate) pending_len: usize,
    pending_sent: usize,
    deadline_ns: u64,
    last_progress_ns: u64,
}

impl H2Meta {
    fn pending_unflushed(&self) -> bool {
        self.pending_len > self.pending_sent
    }

    fn tls_unflushed(&self) -> bool {
        self.tls_pending.len() > self.tls_pending_sent
    }

    fn wants_pump(&self) -> bool {
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

pub(crate) struct H2Turn<'a> {
    pub(crate) routes: &'a FrozenRoutesView<'a>,
    pub(crate) host: &'a HostShared,
    pub(crate) worker_id: u32,
    pub(crate) staged: &'a mut Vec<StagedWork>,
    pub(crate) budget: u64,
    pub(crate) now_ns: u64,
    pub(crate) timeouts: ConnectionTimeouts,
    pub(crate) draining: bool,
    pub(crate) linger_due: bool,
    pub(crate) forcing: bool,
    pub(crate) counts: &'a mut CloseCounts,
    pub(crate) tls_acceptor: Option<&'a mut TlsAcceptor>,
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

fn idle_for_drain(meta: &H2Meta, conn: Option<&H2Conn<'_>>) -> bool {
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

enum H2Flush {
    Done,
    Blocked,
    Retry,
    Closed(ConnClose),
}

enum H2Write {
    Accepted(usize),
    Blocked,
    Interrupted,
    Closed(ConnClose),
}

enum H2Read {
    Progress(usize),
    Blocked,
    Eof,
    Closed(ConnClose),
}

// NOTE: runtime-dispatch contract minus tokens (EINTR once, WouldBlock backpressures,
// RST/EPIPE map like H1); double-INTR reports Interrupted so the turn retries promptly.
fn write_once(stream: &mut TcpStream, bytes: &[u8]) -> H2Write {
    if bytes.is_empty() {
        return H2Write::Accepted(0);
    }
    for _ in 0..2 {
        match stream.write(bytes) {
            Ok(accepted) => return H2Write::Accepted(accepted),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return H2Write::Blocked,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == io::ErrorKind::ConnectionReset => {
                return H2Write::Closed(ConnClose::Reset);
            }
            Err(error) if error.kind() == io::ErrorKind::BrokenPipe => {
                return H2Write::Closed(ConnClose::PeerClosed);
            }
            Err(_) => return H2Write::Closed(ConnClose::TransportError),
        }
    }
    H2Write::Interrupted
}

fn read_once(stream: &mut TcpStream, spare: &mut [u8]) -> H2Read {
    if spare.is_empty() {
        return H2Read::Blocked;
    }
    for _ in 0..2 {
        match stream.read(spare) {
            Ok(0) => return H2Read::Eof,
            Ok(read) => return H2Read::Progress(read),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return H2Read::Blocked,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == io::ErrorKind::ConnectionReset => {
                return H2Read::Closed(ConnClose::Reset);
            }
            Err(_) => return H2Read::Closed(ConnClose::TransportError),
        }
    }
    H2Read::Blocked
}

// NOTE: h2c upgrades to ReadWrite while blocked so writability wakes the flush, then
// downgrades. No turn ever waits inside h2c: every path ends Done/Blocked/Retry/Closed.
fn flush_pending(meta: &mut H2Meta, stream_poller: &mut Poller, budget: &mut u64) -> H2Flush {
    if !meta.pending_unflushed() {
        return H2Flush::Done;
    }
    let Some(stream) = meta.stream.as_mut() else {
        return H2Flush::Closed(ConnClose::TransportError);
    };
    let Some(staged) = meta.pending.as_deref() else {
        return H2Flush::Closed(ConnClose::TransportError);
    };
    let mut sent = meta.pending_sent;
    let mut allowance = *budget;
    let mut outcome = H2Flush::Done;
    while sent < meta.pending_len {
        if allowance == 0 {
            outcome = H2Flush::Blocked;
            break;
        }
        let end = (meta.pending_len - sent).min(allowance as usize) + sent;
        match write_once(stream, &staged[sent..end]) {
            H2Write::Accepted(accepted) => {
                sent += accepted;
                allowance -= accepted as u64;
                if accepted == 0 {
                    outcome = H2Flush::Blocked;
                    break;
                }
            }
            H2Write::Blocked => {
                outcome = H2Flush::Blocked;
                break;
            }
            H2Write::Interrupted => {
                outcome = H2Flush::Retry;
                break;
            }
            H2Write::Closed(reason) => {
                outcome = H2Flush::Closed(reason);
                break;
            }
        }
    }
    *budget = allowance;
    meta.pending_sent = sent;
    match outcome {
        H2Flush::Done => {
            meta.pending_len = 0;
            meta.pending_sent = 0;
            meta.write_blocked = false;
            if meta.watching_write {
                meta.watching_write = false;
                if meta.token != 0 {
                    let _ = stream_poller.reregister(stream, meta.token, PollInterest::Readable);
                }
            }
            H2Flush::Done
        }
        H2Flush::Blocked => {
            meta.write_blocked = true;
            if !meta.watching_write {
                meta.watching_write = true;
                if meta.token != 0 {
                    let _ = stream_poller.reregister(stream, meta.token, PollInterest::ReadWrite);
                }
            }
            H2Flush::Blocked
        }
        H2Flush::Retry | H2Flush::Closed(_) => outcome,
    }
}

fn flush_h2(meta: &mut H2Meta, stream_poller: &mut Poller, budget: &mut u64) -> H2Flush {
    if meta.tls.is_some() {
        tls_flush_h2(meta, stream_poller, budget)
    } else {
        flush_pending(meta, stream_poller, budget)
    }
}

fn tls_flush_h2(meta: &mut H2Meta, stream_poller: &mut Poller, budget: &mut u64) -> H2Flush {
    if meta.tls_pending.len() <= meta.tls_pending_sent && meta.pending_len > meta.pending_sent {
        let Some(staged) = meta.pending.as_deref() else {
            return H2Flush::Closed(ConnClose::TransportError);
        };
        let plain_end = meta.pending_len.min(staged.len());
        let plain_start = meta.pending_sent.min(plain_end);
        meta.tls_pending.clear();
        meta.tls_pending_sent = 0;
        let Some(tls) = meta.tls.as_mut() else {
            return H2Flush::Closed(ConnClose::TransportError);
        };
        if tls_encrypt_bytes(tls, &staged[plain_start..plain_end], &mut meta.tls_pending).is_err() {
            return H2Flush::Closed(ConnClose::TransportError);
        }
        meta.pending_sent = meta.pending_len;
    }
    let Some(stream) = meta.stream.as_mut() else {
        return H2Flush::Closed(ConnClose::TransportError);
    };
    let staged_end = meta.tls_pending.len();
    let mut sent = meta.tls_pending_sent;
    let mut allowance = *budget;
    let mut outcome = H2Flush::Done;
    let mut attempts = 0;
    while sent < staged_end {
        if allowance == 0 {
            outcome = H2Flush::Blocked;
            break;
        }
        let end = (staged_end - sent).min(allowance as usize) + sent;
        let width = end - sent;
        match tls_write_once(stream, &meta.tls_pending[sent..end]) {
            TlsWriteOutcome::Accepted(accepted) => {
                sent += accepted;
                allowance -= accepted as u64;
                if accepted == 0 {
                    outcome = H2Flush::Blocked;
                    break;
                }
                if accepted < width {
                    outcome = H2Flush::Blocked;
                    break;
                }
            }
            TlsWriteOutcome::Blocked => {
                outcome = H2Flush::Blocked;
                break;
            }
            TlsWriteOutcome::Interrupted => {
                attempts += 1;
                if attempts >= 2 {
                    break;
                }
            }
            TlsWriteOutcome::Closed(reason) => {
                outcome = H2Flush::Closed(reason);
                break;
            }
        }
    }
    *budget = allowance;
    meta.tls_pending_sent = sent;
    match outcome {
        H2Flush::Done => {
            meta.tls_pending.clear();
            meta.tls_pending_sent = 0;
            meta.write_blocked = false;
            if meta.watching_write {
                meta.watching_write = false;
                if meta.token != 0 {
                    let _ = stream_poller.reregister(stream, meta.token, PollInterest::Readable);
                }
            }
            H2Flush::Done
        }
        H2Flush::Blocked => {
            meta.write_blocked = true;
            if !meta.watching_write {
                meta.watching_write = true;
                if meta.token != 0 {
                    let _ = stream_poller.reregister(stream, meta.token, PollInterest::ReadWrite);
                }
            }
            H2Flush::Blocked
        }
        H2Flush::Retry | H2Flush::Closed(_) => outcome,
    }
}

pub(crate) fn open_slot(
    meta: &mut H2Meta,
    stream: TcpStream,
    index: usize,
    now_ns: u64,
    head_budget_ns: u64,
    stream_poller: &mut Poller,
    counts: &mut CloseCounts,
) -> bool {
    // NOTE: transient only: registers the stream and arms scalars (driver built at call site).
    let token = H2_STREAM_TOKEN_BASE + index;
    let mut owned = stream;
    if stream_poller
        .register(&mut owned, token, PollInterest::Readable)
        .is_err()
    {
        counts.add(ConnClose::TransportError);
        return false;
    }
    *meta = H2Meta {
        stream: Some(owned),
        occupied: true,
        readable: true,
        token,
        deadline_ns: now_ns.saturating_add(head_budget_ns),
        last_progress_ns: now_ns,
        pending: Some(Box::new([0; H2_PENDING_CAP])),
        ..Default::default()
    };
    true
}

pub(crate) fn abort_slot(meta: &mut H2Meta, stream_poller: &mut Poller, counts: &mut CloseCounts) {
    if let Some(mut stream) = meta.stream.take() {
        let _ = stream_poller.deregister(&mut stream);
    }
    meta.occupied = false;
    meta.readable = false;
    counts.add(ConnClose::TransportError);
}

// NOTE: driver-chain construction expands inline once per slot over buffer FIELDS;
// Option temporaries and helper params are rejected; transient helpers stay plain fns.
macro_rules! build_h2_conn {
    ($bufs:ident, $b:ident, $ts:ident, $dec:ident, $enc:ident, $c:ident) => {{
        // NOTE: killing the conn first ends the previous generation's regions; without
        // this the fallthrough would merge stale and fresh borrows.
        $c = None;
        $ts = Some(veylor_http2::StreamTable::new(
            &mut $bufs.$b.slots[..],
            veylor_http2::H2Role::Server,
            &mut $bufs.$b.block[..],
            $crate::h2::HEADER_BLOCK_CAP as u32,
        ));
        $dec = Some(veylor_http2::HpackTable::new(
            &mut $bufs.$b.dec_entries[..],
            &mut $bufs.$b.dec_scratch[..],
            4_096,
        ));
        $enc = Some(veylor_http2::HpackTable::new(
            &mut $bufs.$b.enc_entries[..],
            &mut $bufs.$b.enc_scratch[..],
            4_096,
        ));
        match ($ts.as_mut(), $dec.as_mut(), $enc.as_mut()) {
            (Some(live_streams), Some(live_decoder), Some(live_encoder)) => {
                $c = Some($crate::h2::H2Conn::new(
                    live_streams,
                    live_decoder,
                    live_encoder,
                    &mut $bufs.$b.staging[..],
                    &mut $bufs.$b.ack[..],
                    &mut $bufs.$b.headers[..],
                ));
                true
            }
            _ => false,
        }
    }};
}

pub(crate) use build_h2_conn;

// NOTE: teardown unparks the conn's full stream-keyed range (empty slots no-op).
fn unpark_conn(host: &HostShared, worker: u32, meta: &H2Meta) {
    let index = meta.token.saturating_sub(H2_STREAM_TOKEN_BASE);
    for sub in 0..STREAM_SLOTS {
        host.unpark_closed(
            worker,
            (MAX_CONNECTIONS + index * STREAM_SLOTS + sub) as u32,
        );
    }
}

pub(crate) fn abort_tls_slot(
    meta: &mut H2Meta,
    stream_poller: &mut Poller,
    counts: &mut CloseCounts,
    tls_acceptor: &mut Option<TlsAcceptor>,
) {
    if meta.tls.take().is_some() {
        if let Some(acceptor) = tls_acceptor.as_mut() {
            acceptor.release();
        }
    }
    abort_slot(meta, stream_poller, counts);
}

fn shut_slot(meta: &mut H2Meta, poller: &mut Poller, reason: ConnClose, turn: &mut H2Turn<'_>) {
    unpark_conn(turn.host, turn.worker_id, meta);
    if let Some(mut stream) = meta.stream.take() {
        let _ = poller.deregister(&mut stream);
    }
    if meta.tls.take().is_some() {
        if let Some(acceptor) = turn.tls_acceptor.as_mut() {
            acceptor.release();
        }
    }
    // NOTE: the caller clears the conn visibly; stale tables keep dead borrows only.
    meta.occupied = false;
    meta.readable = false;
    turn.counts.add(reason);
}

// NOTE: polite close (timeouts, drain-idle): stage GOAWAY, flush once, then close.
fn goaway_close(
    meta: &mut H2Meta,
    conn: &mut Option<H2Conn<'_>>,
    stream_poller: &mut Poller,
    reason: ConnClose,
    turn: &mut H2Turn<'_>,
) -> Option<ConnClose> {
    if let Some(active) = conn.as_mut().filter(|active| !active.is_closed()) {
        let staged = active.drain(pending_spare(meta));
        meta.pending_len += staged.written;
    }
    let fatal = match flush_h2(meta, stream_poller, &mut turn.budget) {
        H2Flush::Closed(fatal) => fatal,
        H2Flush::Done | H2Flush::Blocked | H2Flush::Retry => reason,
    };
    shut_slot(meta, stream_poller, fatal, turn);
    Some(fatal)
}

pub(crate) fn pump_slot(
    meta: &mut H2Meta,
    conn: &mut Option<H2Conn<'_>>,
    turn: &mut H2Turn<'_>,
    stream_poller: &mut Poller,
    index: usize,
) -> Option<ConnClose> {
    if !meta.occupied {
        return None;
    }
    if conn.is_none() {
        shut_slot(meta, stream_poller, ConnClose::TransportError, turn);
        return Some(ConnClose::TransportError);
    }
    if turn.forcing {
        shut_slot(meta, stream_poller, ConnClose::Shutdown, turn);
        return Some(ConnClose::Shutdown);
    }
    if meta.timed_out {
        return goaway_close(meta, conn, stream_poller, ConnClose::Timeout, turn);
    }
    if turn.draining && turn.linger_due && idle_for_drain(meta, conn.as_ref()) {
        return goaway_close(meta, conn, stream_poller, ConnClose::Shutdown, turn);
    }
    if meta.error {
        shut_slot(meta, stream_poller, ConnClose::TransportError, turn);
        return Some(ConnClose::TransportError);
    }
    let mut close_reason: Option<ConnClose> = None;
    let mut eof = false;
    let mut progressed = apply_h2_staged(meta, conn, turn);
    progressed |= super::apply_h2_streams(meta, conn, turn);
    if let H2Flush::Closed(reason) = flush_h2(meta, stream_poller, &mut turn.budget) {
        close_reason = Some(reason);
    }
    // NOTE: reads pause while output is pending: bytes behind a saturated socket only grow work.
    let wants_read = meta.readable && close_reason.is_none() && !meta.pending_unflushed();
    if wants_read {
        if meta.tls.is_some() {
            let Some(live) = conn.as_mut() else {
                shut_slot(meta, stream_poller, ConnClose::TransportError, turn);
                return Some(ConnClose::TransportError);
            };
            let room = live.spare_mut().len().min(turn.budget as usize);
            if room > 0 {
                let spare = &mut live.spare_mut()[..room];
                let (Some(stream), Some(tls)) = (meta.stream.as_mut(), meta.tls.as_mut()) else {
                    shut_slot(meta, stream_poller, ConnClose::TransportError, turn);
                    return Some(ConnClose::TransportError);
                };
                match tls_read_decrypted(stream, tls, spare, turn.budget, &mut meta.tls_plain) {
                    Ok((wire, plain, blocked)) => {
                        live.commit_staged(plain);
                        turn.budget = turn.budget.saturating_sub(wire);
                        if blocked {
                            meta.readable = false;
                        }
                        if wire > 0 || plain > 0 {
                            progressed = true;
                        }
                    }
                    Err(reason) => close_reason = Some(reason),
                }
            }
        } else {
            let (Some(live), Some(stream)) = (conn.as_mut(), meta.stream.as_mut()) else {
                shut_slot(meta, stream_poller, ConnClose::TransportError, turn);
                return Some(ConnClose::TransportError);
            };
            let room = live.spare_mut().len().min(turn.budget as usize);
            if room > 0 {
                match read_once(stream, &mut live.spare_mut()[..room]) {
                    H2Read::Progress(read) => {
                        live.commit_staged(read);
                        turn.budget -= read as u64;
                        progressed = true;
                    }
                    H2Read::Blocked => meta.readable = false,
                    H2Read::Eof => eof = true,
                    H2Read::Closed(reason) => close_reason = Some(reason),
                }
            }
        }
    }
    let base = ConnectionToken {
        worker_id: turn.worker_id,
        slot: (MAX_CONNECTIONS + index * STREAM_SLOTS) as u32,
        worker_generation: 1,
        generation: 1,
        ..Default::default()
    };
    let mut steps = 0;
    while close_reason.is_none() && !eof && steps < MAX_H2_STEPS_PER_TURN && turn.budget > 0 {
        if pending_spare(meta).len() < H2_FLUSH_THRESHOLD && meta.pending_unflushed() {
            if let H2Flush::Closed(reason) = flush_h2(meta, stream_poller, &mut turn.budget) {
                close_reason = Some(reason);
                break;
            }
            if pending_spare(meta).len() < H2_FLUSH_THRESHOLD {
                break;
            }
        }
        let Some(live) = conn.as_mut() else {
            close_reason = Some(ConnClose::TransportError);
            break;
        };
        let staged_before = live.staged_len();
        let ctx = super::H2HostCtx {
            host: turn.host,
            base,
        };
        let outcome = live.step_host(pending_spare(meta), turn.routes, ctx);
        turn.counts.h2_served += u64::from(outcome.served);
        turn.counts.h2_refused += u64::from(outcome.refused);
        meta.pending_len += outcome.written;
        let step_progressed = outcome.written > 0
            || outcome.served > 0
            || outcome.refused > 0
            || live.staged_len() != staged_before;
        progressed |= step_progressed;
        if outcome.closed {
            // NOTE: closed+goaway comes only from fail paths; bare close with output/refusal
            // is the post-refusal policy close, bare close with neither is peer going away.
            if outcome.goaway {
                close_reason = Some(ConnClose::ProtocolError);
            } else if outcome.refused > 0 || outcome.written > 0 {
                close_reason = Some(ConnClose::Completed);
            } else {
                close_reason = Some(ConnClose::PeerClosed);
            }
            break;
        }
        steps += 1;
        if !outcome.pending || !step_progressed {
            break;
        }
    }
    // NOTE: closed means flush-then-close: staged bytes go best-effort before the FIN.
    // Flush failure reports transport truth; a blocked remainder drops with the slot.
    if meta.pending_unflushed() || meta.tls_unflushed() {
        if let H2Flush::Closed(fatal) = flush_h2(meta, stream_poller, &mut turn.budget) {
            close_reason = Some(fatal);
        }
    }
    if eof && close_reason.is_none() {
        close_reason = Some(ConnClose::PeerClosed);
    }
    if progressed {
        meta.last_progress_ns = turn.now_ns;
        meta.deadline_ns = turn.now_ns.saturating_add(turn.timeouts.keep_alive_idle_ns);
    }
    if !meta.serving {
        meta.serving = conn.as_ref().is_some_and(|live| live.handshake_complete());
    }
    meta.staged = conn.as_ref().is_some_and(|live| live.staged_len() > 0);
    if let Some(reason) = close_reason {
        shut_slot(meta, stream_poller, reason, turn);
        return Some(reason);
    }
    // NOTE: terminal without a reason is unreachable; fail closed, never wedge the slot.
    if conn.as_ref().is_some_and(|live| live.is_closed()) {
        shut_slot(meta, stream_poller, ConnClose::Completed, turn);
        return Some(ConnClose::Completed);
    }
    None
}

// NOTE: fatal-path abandon (worker exiting): streams drop, slots count Shutdown.
pub(crate) fn abandon_slots(
    metas: &mut H2MetaSlots,
    host: &HostShared,
    worker: u32,
    counts: &mut CloseCounts,
    tls_acceptor: &mut Option<TlsAcceptor>,
) {
    for index in 0..MAX_H2_CONNS {
        let meta = meta_at(metas, index);
        if meta.occupied {
            unpark_conn(host, worker, meta);
            meta.occupied = false;
            meta.stream.take();
            if meta.tls.take().is_some() {
                if let Some(acceptor) = tls_acceptor.as_mut() {
                    acceptor.release();
                }
            }
            counts.add(ConnClose::Shutdown);
        }
    }
}

pub(crate) fn restart_h2_drain(
    drain_start_ns: &mut u64,
    h2_force_ns: &mut u64,
    now_ns: u64,
    shutdown_grace_ns: u64,
) {
    // NOTE: the shutdown counter tracks H1 slots only: restart the linger clock here so
    // h2c conns still get their GOAWAY drain (bounded by the same grace).
    *drain_start_ns = now_ns;
    *h2_force_ns = now_ns.saturating_add(shutdown_grace_ns);
}

pub(crate) fn note_readiness(metas: &mut H2MetaSlots, event: &Readiness) -> bool {
    if event.token < H2_STREAM_TOKEN_BASE || event.token >= H2_STREAM_TOKEN_BASE + MAX_H2_CONNS {
        return false;
    }
    let meta = meta_at(metas, event.token - H2_STREAM_TOKEN_BASE);
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
    true
}

pub(crate) fn register_listener(
    worker_poller: &mut Poller,
    h2c_listener: &mut Option<TcpListener>,
) -> Result<(), &'static str> {
    // NOTE: h2c readiness rides the worker poller under its own token, so h2c accepts
    // share the turn's event budget; Worker::turn ignores the token.
    if let Some(bound) = h2c_listener.as_mut() {
        worker_poller
            .register(bound, H2C_LISTENER_TOKEN, PollInterest::Readable)
            .map_err(|_| "h2c listener register failed")?;
    }
    Ok(())
}

pub(crate) fn project_routes<'a>(
    h2_routes: &'a [H2StaticRoute],
) -> (Vec<FrozenRoute<'a>>, Vec<FrozenNativeRoute<'a>>) {
    // NOTE: the h2c route projection is built once per worker from the owned mirror;
    // only status eligibility (200..=599 except 204) filters here. Natives skip the
    // status/body filter (no stored response) and project alongside, never inside.
    let flat = h2_routes
        .iter()
        .filter(|route| {
            route.native.is_none()
                && route.status != 204
                && (200..=599).contains(&route.status)
                && route.body.len() <= MAX_RESPONSE_BODY
        })
        .map(|route| FrozenRoute {
            method: &route.method,
            path: &route.path,
            status: route.status,
            body: &route.body,
            gzip_body: route.gzip_body.as_deref(),
            managed: route.managed,
            file_root: route.file_root.as_deref(),
            trailers: &route.trailers,
        })
        .collect();
    let natives = h2_routes
        .iter()
        .filter_map(|route| {
            route
                .native
                .map(|(handler, user_data, gzip)| FrozenNativeRoute {
                    method: &route.method,
                    path: &route.path,
                    handler,
                    user_data,
                    gzip,
                })
        })
        .collect();
    (flat, natives)
}
// NOTE: single accept-arm definition, invoked once per slot index below.
macro_rules! accept_h2_slot {
    ($slot:literal, $metas:ident, $meta:ident, $bufs:ident, $buf:ident, $ts:ident, $dec:ident, $enc:ident, $conn:ident, $accepted:ident, $now:ident, $config:ident, $poller:ident, $result:ident) => {
        if $crate::h2::turn::open_slot(
            &mut $metas.$meta,
            $accepted.stream,
            $slot,
            $now,
            $config.timeouts.request_head_ns,
            &mut $poller,
            &mut $result.counts,
        ) && !$crate::h2::turn::build_h2_conn!($bufs, $buf, $ts, $dec, $enc, $conn)
        {
            $crate::h2::turn::abort_slot(&mut $metas.$meta, &mut $poller, &mut $result.counts);
        }
    };
}

pub(crate) use accept_h2_slot;

macro_rules! move_h2_tls_slot {
    ($slot:literal, $metas:ident, $meta:ident, $bufs:ident, $buf:ident, $ts:ident, $dec:ident, $enc:ident, $conn:ident, $stream:ident, $tls:ident, $prefix:ident, $now:ident, $config:ident, $poller:ident, $result:ident, $tlsacc:ident) => {{
        let mut moved = false;
        if $crate::h2::turn::open_slot(
            &mut $metas.$meta,
            $stream,
            $slot,
            $now,
            $config.timeouts.request_head_ns,
            &mut $poller,
            &mut $result.counts,
        ) {
            if $crate::h2::turn::build_h2_conn!($bufs, $buf, $ts, $dec, $enc, $conn) {
                $crate::h2::turn::meta_at(&mut $metas, $slot).tls = Some($tls);
                moved = $conn
                    .as_mut()
                    .is_some_and(|live| live.push_input(&$prefix).is_ok());
                if !moved {
                    $crate::h2::turn::abort_tls_slot(
                        &mut $metas.$meta,
                        &mut $poller,
                        &mut $result.counts,
                        &mut $tlsacc,
                    );
                }
            } else {
                $crate::h2::turn::abort_slot(&mut $metas.$meta, &mut $poller, &mut $result.counts);
                if let Some(acceptor) = $tlsacc.as_mut() {
                    acceptor.release();
                }
            }
        } else if let Some(acceptor) = $tlsacc.as_mut() {
            acceptor.release();
        }
        moved
    }};
}

pub(crate) use move_h2_tls_slot;

// NOTE: per-turn h2c accept dispatch in `worker_main`. h2c shares the H1 accept budget;
// on an H1-error turn the budget is unknown, so h2c skips (fail closed).
macro_rules! drive_h2_accepts {
    ($listener:ident, $enabled:ident, $draining:ident, $budget:ident, $now:ident, $staging:ident, $metas:ident, $bufs:ident, $poller:ident, $config:ident, $result:ident, $ts0:ident, $ts1:ident, $ts2:ident, $ts3:ident, $ts4:ident, $ts5:ident, $ts6:ident, $ts7:ident, $dec0:ident, $dec1:ident, $dec2:ident, $dec3:ident, $dec4:ident, $dec5:ident, $dec6:ident, $dec7:ident, $enc0:ident, $enc1:ident, $enc2:ident, $enc3:ident, $enc4:ident, $enc5:ident, $enc6:ident, $enc7:ident, $c0:ident, $c1:ident, $c2:ident, $c3:ident, $c4:ident, $c5:ident, $c6:ident, $c7:ident) => {
        if $enabled && !$draining && $budget > 0 {
            if let Some(bound) = $listener.as_mut() {
                // NOTE: one nonblocking accept per turn; WouldBlock short-circuits empty.
                if veylor_runtime::accept_ready(bound, &mut $staging, $budget).is_ok() {
                    for entry in $staging.iter_mut() {
                        let Some(accepted) = entry.take() else {
                            continue;
                        };
                        match $crate::h2::turn::free_slot(&$metas) {
                            Some(0) => $crate::h2::turn::accept_h2_slot!(
                                0, $metas, m0, $bufs, b0, $ts0, $dec0, $enc0, $c0, accepted, $now,
                                $config, $poller, $result
                            ),
                            Some(1) => $crate::h2::turn::accept_h2_slot!(
                                1, $metas, m1, $bufs, b1, $ts1, $dec1, $enc1, $c1, accepted, $now,
                                $config, $poller, $result
                            ),
                            Some(2) => $crate::h2::turn::accept_h2_slot!(
                                2, $metas, m2, $bufs, b2, $ts2, $dec2, $enc2, $c2, accepted, $now,
                                $config, $poller, $result
                            ),
                            Some(3) => $crate::h2::turn::accept_h2_slot!(
                                3, $metas, m3, $bufs, b3, $ts3, $dec3, $enc3, $c3, accepted, $now,
                                $config, $poller, $result
                            ),
                            Some(4) => $crate::h2::turn::accept_h2_slot!(
                                4, $metas, m4, $bufs, b4, $ts4, $dec4, $enc4, $c4, accepted, $now,
                                $config, $poller, $result
                            ),
                            Some(5) => $crate::h2::turn::accept_h2_slot!(
                                5, $metas, m5, $bufs, b5, $ts5, $dec5, $enc5, $c5, accepted, $now,
                                $config, $poller, $result
                            ),
                            Some(6) => $crate::h2::turn::accept_h2_slot!(
                                6, $metas, m6, $bufs, b6, $ts6, $dec6, $enc6, $c6, accepted, $now,
                                $config, $poller, $result
                            ),
                            Some(7) => $crate::h2::turn::accept_h2_slot!(
                                7, $metas, m7, $bufs, b7, $ts7, $dec7, $enc7, $c7, accepted, $now,
                                $config, $poller, $result
                            ),
                            None | Some(_) => $result
                                .counts
                                .add($crate::connection::ConnClose::TransportError),
                        }
                    }
                }
            }
        }
    };
}

pub(crate) use drive_h2_accepts;

// NOTE: single pump-arm definition, invoked once per slot index below.
macro_rules! pump_h2_one {
    ($slot:literal, $metas:ident, $meta:ident, $conn:ident, $turn:ident, $poller:ident) => {{
        let closed = $crate::h2::turn::pump_slot(
            &mut $metas.$meta,
            &mut $conn,
            &mut $turn,
            &mut $poller,
            $slot,
        );
        if closed.is_some() {
            $conn = None;
        }
    }};
}

pub(crate) use pump_h2_one;

// NOTE: per-turn h2c pump dispatch in `worker_main`; arms invoke pump_h2_one!.
macro_rules! drive_h2_pumps {
    ($enabled:ident, $view:ident, $draining:ident, $budget:ident, $now:ident, $metas:ident, $poller:ident, $config:ident, $result:ident, $worker:ident, $force:ident, $h2force:ident, $drainstart:ident, $ts0:ident, $ts1:ident, $ts2:ident, $ts3:ident, $ts4:ident, $ts5:ident, $ts6:ident, $ts7:ident, $dec0:ident, $dec1:ident, $dec2:ident, $dec3:ident, $dec4:ident, $dec5:ident, $dec6:ident, $dec7:ident, $enc0:ident, $enc1:ident, $enc2:ident, $enc3:ident, $enc4:ident, $enc5:ident, $enc6:ident, $enc7:ident, $c0:ident, $c1:ident, $c2:ident, $c3:ident, $c4:ident, $c5:ident, $c6:ident, $c7:ident, $host:ident, $wid:ident, $h2staged:ident, $tlsacc:ident) => {
        if $enabled {
            $crate::h2::turn::sweep_deadlines(&mut $metas, $now, $config.timeouts);
            if let Some(view) = $view.as_ref() {
                let mut turn = $crate::h2::turn::H2Turn {
                    routes: view,
                    host: &$host,
                    worker_id: $wid,
                    staged: &mut $h2staged,
                    budget: 0,
                    now_ns: $now,
                    timeouts: $config.timeouts,
                    draining: $draining,
                    linger_due: $draining
                        && $drainstart > 0
                        && $now.saturating_sub($drainstart) >= $config.drain_idle_linger_ns,
                    forcing: $worker.shutdown_phase() == veylor_runtime::ShutdownPhase::Forcing
                        || $force.load(std::sync::atomic::Ordering::Acquire)
                        || ($h2force > 0 && $now >= $h2force),
                    counts: &mut $result.counts,
                    tls_acceptor: $tlsacc.as_mut(),
                };
                for index in 0..$crate::h2::MAX_H2_CONNS {
                    turn.budget = $budget;
                    match index {
                        0 => $crate::h2::turn::pump_h2_one!(0, $metas, m0, $c0, turn, $poller),
                        1 => $crate::h2::turn::pump_h2_one!(1, $metas, m1, $c1, turn, $poller),
                        2 => $crate::h2::turn::pump_h2_one!(2, $metas, m2, $c2, turn, $poller),
                        3 => $crate::h2::turn::pump_h2_one!(3, $metas, m3, $c3, turn, $poller),
                        4 => $crate::h2::turn::pump_h2_one!(4, $metas, m4, $c4, turn, $poller),
                        5 => $crate::h2::turn::pump_h2_one!(5, $metas, m5, $c5, turn, $poller),
                        6 => $crate::h2::turn::pump_h2_one!(6, $metas, m6, $c6, turn, $poller),
                        7 => $crate::h2::turn::pump_h2_one!(7, $metas, m7, $c7, turn, $poller),
                        _ => {}
                    }
                }
            }
        }
    };
}

pub(crate) use drive_h2_pumps;
