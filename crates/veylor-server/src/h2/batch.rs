#![allow(unsafe_code)]

#[cfg(target_os = "linux")]
use std::io;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;

use veylor_runtime::Poller;
#[cfg(target_os = "linux")]
use veylor_runtime::{BatchFailure, BatchRing, PollInterest, ReadOp, SendOp};
#[cfg(target_os = "linux")]
use veylor_tls::plaintext_queued;

#[cfg(target_os = "linux")]
use super::MAX_H2_CONNS;
#[cfg(target_os = "linux")]
use super::slots::H2Meta;
use super::slots::H2Slot;

// NOTE: the ring is per worker table, not per connection: capacity is the connection
// ceiling, so every eligible slot fits in one submission and no op is ever dropped. It
// is built on first use so H1-only workers never pay an io_uring setup.
pub(crate) struct Batches {
    #[cfg(target_os = "linux")]
    ring: Option<BatchRing>,
    #[cfg(target_os = "linux")]
    tried: bool,
    #[cfg(target_os = "linux")]
    reads: Vec<ReadOp>,
    #[cfg(target_os = "linux")]
    writes: Vec<SendOp>,
    #[cfg(target_os = "linux")]
    widths: Vec<usize>,
}

impl Batches {
    pub(crate) fn new() -> Self {
        #[cfg(target_os = "linux")]
        let batches = Self {
            ring: None,
            tried: false,
            reads: Vec::with_capacity(MAX_H2_CONNS),
            writes: Vec::with_capacity(MAX_H2_CONNS),
            widths: Vec::with_capacity(MAX_H2_CONNS),
        };
        #[cfg(not(target_os = "linux"))]
        let batches = Self {};
        batches
    }

    pub(crate) fn ensure(&mut self) -> bool {
        #[cfg(target_os = "linux")]
        {
            if !self.tried {
                self.tried = true;
                self.ring = BatchRing::new(MAX_H2_CONNS as u32).ok();
            }
            self.ring.is_some()
        }
        #[cfg(not(target_os = "linux"))]
        false
    }

    pub(crate) fn recv_ready(
        &mut self,
        table: &mut [Option<Box<H2Slot>>],
        poller: &mut Poller,
        budget: u64,
    ) {
        #[cfg(target_os = "linux")]
        submit_reads(self, table, poller, budget);
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (table, poller, budget);
        }
    }

    pub(crate) fn send_deferred(
        &mut self,
        table: &mut [Option<Box<H2Slot>>],
        poller: &mut Poller,
        budget: u64,
    ) -> bool {
        #[cfg(target_os = "linux")]
        {
            submit_writes(self, table, poller, budget)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (table, poller, budget);
            false
        }
    }
}

// NOTE: one recv per eligible slot per turn. io_uring arms internal polling when a
// submitted recv would block, which would stall the whole worker on an empty socket,
// so the claim is dropped after every attempt and only a full read re-arms the edge
// through the poller (mio is edge-triggered; a short read means the socket drained).
// TLS reads ciphertext into its own slab; the pump decrypts it into the staging spare.
#[cfg(target_os = "linux")]
fn submit_reads(
    batches: &mut Batches,
    table: &mut [Option<Box<H2Slot>>],
    poller: &mut Poller,
    budget: u64,
) {
    if budget == 0 {
        return;
    }
    let Some(capacity) = batches.ring.as_ref().map(BatchRing::capacity) else {
        return;
    };
    batches.reads.clear();
    batches.widths.clear();
    for (index, entry) in table.iter_mut().enumerate() {
        if batches.reads.len() == capacity {
            break;
        }
        let Some(slot) = entry.as_mut() else {
            continue;
        };
        let meta = &slot.meta;
        if !meta.occupied
            || !meta.readable
            || !meta.claimed
            || meta.error
            || meta.timed_out
            || meta.tls_cipher_pending()
        {
            continue;
        }
        let Some(fd) = meta.stream.as_ref().map(AsRawFd::as_raw_fd) else {
            continue;
        };
        let tls = meta.tls.is_some();
        if slot.conn.as_ref().is_none_or(|conn| conn.is_closed()) {
            continue;
        }
        let room = slot
            .conn
            .as_mut()
            .map_or(0, |conn| conn.spare_mut().len())
            .min(budget as usize);
        if room == 0 {
            continue;
        }
        if tls {
            // NOTE: plaintext still queued in the provider must reach the staging window
            // before the socket is read again, or it would arrive behind newer records.
            if slot
                .meta
                .tls
                .as_mut()
                .is_some_and(|tls| plaintext_queued(tls))
            {
                continue;
            }
            let Some(cipher) = slot.meta.tls_cipher.as_deref_mut() else {
                continue;
            };
            let width = room.min(cipher.len());
            // SAFETY: `cipher` points into the slot's retained ciphertext slab, which
            // outlives the `recv_many` call below; the pump does not touch it until every
            // completion has been drained.
            batches
                .reads
                .push(unsafe { ReadOp::new(fd, index as u32, cipher.as_mut_ptr(), width) });
            batches.widths.push(width);
            continue;
        }
        let Some(conn) = slot.conn.as_mut() else {
            continue;
        };
        let spare = conn.spare_mut();
        let width = spare.len().min(budget as usize);
        // SAFETY: `spare` points into the slot's retained staging slab, which outlives
        // the `recv_many` call below; no other reference touches it until every
        // completion has been drained.
        batches
            .reads
            .push(unsafe { ReadOp::new(fd, index as u32, spare.as_mut_ptr(), width) });
        batches.widths.push(width);
    }
    if batches.reads.is_empty() {
        return;
    }
    let outcome = match batches.ring.take() {
        Some(ring) => ring.recv_many(&mut batches.reads),
        None => return,
    };
    match outcome {
        Ok(ring) => batches.ring = Some(ring),
        Err(BatchFailure::NotSubmitted) => return,
        Err(BatchFailure::Lost(_)) => {
            // NOTE: a batch that failed after submission leaves the peer's byte position
            // unknowable, so every contributing slot closes instead of being retried.
            for op in &batches.reads {
                if let Some(slot) = table
                    .get_mut(op.slot as usize)
                    .and_then(|entry| entry.as_mut())
                {
                    slot.meta.readable = false;
                    slot.meta.claimed = false;
                    slot.meta.error = true;
                }
            }
            return;
        }
    }
    settle_reads(batches, table, poller);
}

#[cfg(target_os = "linux")]
fn settle_reads(batches: &Batches, table: &mut [Option<Box<H2Slot>>], poller: &mut Poller) {
    for (op, &width) in batches.reads.iter().zip(batches.widths.iter()) {
        let Some(slot) = table
            .get_mut(op.slot as usize)
            .and_then(|entry| entry.as_mut())
        else {
            continue;
        };
        let meta = &mut slot.meta;
        if op.result > 0 {
            let read = op.result as usize;
            if meta.tls.is_some() {
                // NOTE: TLS delivered ciphertext, not plaintext; the pump decrypts it into
                // the staging window, so the staged count moves there and not here.
                meta.tls_cipher_read = read;
                meta.tls_cipher_fed = 0;
            } else if let Some(conn) = slot.conn.as_mut() {
                conn.commit_staged(read);
            }
            meta.claimed = false;
            meta.batched_read += read;
            meta.readable = false;
            if read == width {
                reregister(meta, poller, PollInterest::Readable);
            }
            continue;
        }
        if op.result == 0 {
            meta.claimed = false;
            meta.readable = false;
            // NOTE: TLS closes only after the provider's queued plaintext has been fed and
            // stepped, so a record that filled the spare is not dropped by the FIN.
            if meta.tls.is_some() {
                meta.tls_wire_eof = true;
            } else {
                meta.eof = true;
            }
            continue;
        }
        // NOTE: the ring cancels a recv that would have armed internal polling, so a
        // stale claim ends here instead of stalling the worker on an empty socket.
        if op.canceled() {
            meta.claimed = false;
            meta.readable = false;
            continue;
        }
        match io::Error::from_raw_os_error(-op.result).kind() {
            // NOTE: a signal keeps both the claim and the readiness, so the next turn
            // retries a socket that still holds bytes instead of waiting for a new edge.
            io::ErrorKind::Interrupted => {}
            io::ErrorKind::WouldBlock => {
                meta.claimed = false;
                meta.readable = false;
            }
            _ => {
                meta.claimed = false;
                meta.error = true;
            }
        }
    }
}

// NOTE: the pump marks `write_deferred` instead of calling write(2); this pass sends
// every queued range in one submission and applies the completions afterwards, so a
// partial or blocked socket is re-armed exactly as the direct flush would.
#[cfg(target_os = "linux")]
fn submit_writes(
    batches: &mut Batches,
    table: &mut [Option<Box<H2Slot>>],
    poller: &mut Poller,
    budget: u64,
) -> bool {
    batches.writes.clear();
    batches.widths.clear();
    for (index, entry) in table.iter_mut().enumerate() {
        let Some(slot) = entry.as_mut() else {
            continue;
        };
        let meta = &mut slot.meta;
        if !meta.write_deferred
            || meta.write_blocked
            || meta.tls.is_some()
            || !meta.pending_unflushed()
        {
            continue;
        }
        let Some(staged) = meta.pending.as_deref() else {
            meta.write_deferred = false;
            meta.error = true;
            continue;
        };
        let start = meta.pending_sent.min(meta.pending_len);
        let end = meta.pending_len.min(staged.len());
        if start >= end {
            meta.write_deferred = false;
            continue;
        }
        let Some(fd) = meta.stream.as_ref().map(AsRawFd::as_raw_fd) else {
            meta.write_deferred = false;
            meta.error = true;
            continue;
        };
        let width = (end - start).min(budget as usize);
        if width == 0 {
            continue;
        }
        let src = &staged[start..start + width];
        // SAFETY: `src` points into the slot's retained pending slab, which outlives the
        // `send_many` call below; no other reference mutates it until every completion
        // has been drained.
        batches
            .writes
            .push(unsafe { SendOp::new(fd, index as u32, src.as_ptr(), width) });
        batches.widths.push(width);
    }
    if batches.writes.is_empty() {
        return false;
    }
    let outcome = match batches.ring.take() {
        Some(ring) => ring.send_many(&mut batches.writes),
        None => return false,
    };
    match outcome {
        Ok(ring) => batches.ring = Some(ring),
        Err(BatchFailure::NotSubmitted) => return false,
        Err(BatchFailure::Lost(_)) => {
            // NOTE: a submitted send that failed leaves the peer's byte position
            // unknowable, so the contributing connections close instead of retrying.
            for op in &batches.writes {
                if let Some(slot) = table
                    .get_mut(op.slot as usize)
                    .and_then(|entry| entry.as_mut())
                {
                    slot.meta.write_deferred = false;
                    slot.meta.error = true;
                }
            }
            return true;
        }
    }
    settle_writes(batches, table, poller)
}

#[cfg(target_os = "linux")]
fn settle_writes(
    batches: &Batches,
    table: &mut [Option<Box<H2Slot>>],
    poller: &mut Poller,
) -> bool {
    let mut failed = false;
    for (op, &width) in batches.writes.iter().zip(batches.widths.iter()) {
        let Some(slot) = table
            .get_mut(op.slot as usize)
            .and_then(|entry| entry.as_mut())
        else {
            continue;
        };
        let meta = &mut slot.meta;
        meta.write_deferred = false;
        if op.result > 0 {
            let accepted = (op.result as usize).min(meta.pending_len - meta.pending_sent);
            meta.pending_sent += accepted;
            if meta.pending_sent >= meta.pending_len {
                meta.pending_len = 0;
                meta.pending_sent = 0;
                disarm_write(meta, poller);
            } else if accepted < width {
                arm_write(meta, poller);
            }
            continue;
        }
        if op.result == 0 {
            meta.error = true;
            failed = true;
            continue;
        }
        // NOTE: the ring cancels a send that would have armed internal polling; no byte
        // moved, so the range waits for writability exactly like a short write.
        if op.canceled() {
            arm_write(meta, poller);
            continue;
        }
        match io::Error::from_raw_os_error(-op.result).kind() {
            io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted => arm_write(meta, poller),
            _ => {
                meta.error = true;
                failed = true;
            }
        }
    }
    failed
}

// NOTE: interest flips mirror the direct flush exactly: a socket that accepted part of
// the range waits for writability, and a drained range drops back to read-only.
#[cfg(target_os = "linux")]
fn arm_write(meta: &mut H2Meta, poller: &mut Poller) {
    meta.write_blocked = true;
    if meta.watching_write {
        return;
    }
    meta.watching_write = true;
    reregister(meta, poller, PollInterest::ReadWrite);
}

#[cfg(target_os = "linux")]
fn disarm_write(meta: &mut H2Meta, poller: &mut Poller) {
    meta.write_blocked = false;
    if !meta.watching_write {
        return;
    }
    meta.watching_write = false;
    reregister(meta, poller, PollInterest::Readable);
}

#[cfg(target_os = "linux")]
fn reregister(meta: &mut H2Meta, poller: &mut Poller, interest: PollInterest) {
    if meta.token == 0 {
        return;
    }
    if let Some(stream) = meta.stream.as_mut() {
        let _ = poller.reregister(stream, meta.token, interest);
    }
}
