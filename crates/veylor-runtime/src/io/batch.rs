#![allow(unsafe_code)]
#![allow(clippy::missing_safety_doc)]

use std::io;
use std::os::fd::RawFd;

use io_uring::types::Fd;
use io_uring::{IoUring, opcode};

// NOTE: cancel completions share the ring with the batch ops; the top bit keeps their tag
// out of the op index space, which the ring capacity bounds.
const CANCEL_TAG: u64 = 1 << 63;
const WORD_BITS: usize = u64::BITS as usize;

pub struct ReadOp {
    pub fd: RawFd,
    pub slot: u32,
    pub result: i32,
    buffer: *mut u8,
    length: usize,
}

impl ReadOp {
    // SAFETY: `buffer` must point to `length` writable bytes that stay valid and are not
    // accessed through another reference until the enclosing `recv_many` returns.
    pub unsafe fn new(fd: RawFd, slot: u32, buffer: *mut u8, length: usize) -> Self {
        Self {
            fd,
            slot,
            result: 0,
            buffer,
            length,
        }
    }

    // NOTE: the ring cancels a recv that would have armed internal polling; the peer sent
    // nothing to this reader, so the op is a block, not a transport failure.
    pub fn canceled(&self) -> bool {
        self.result == -libc::ECANCELED
    }
}

pub struct SendOp {
    pub fd: RawFd,
    pub slot: u32,
    pub result: i32,
    buffer: *const u8,
    length: usize,
}

impl SendOp {
    // SAFETY: `buffer` must point to `length` readable bytes that stay valid and are not
    // mutated through another reference until the enclosing `send_many` returns.
    pub unsafe fn new(fd: RawFd, slot: u32, buffer: *const u8, length: usize) -> Self {
        Self {
            fd,
            slot,
            result: 0,
            buffer,
            length,
        }
    }

    // NOTE: the ring cancels a send that would have armed internal polling; no byte
    // moved, so the caller treats it as a blocked write and retries on writability.
    pub fn canceled(&self) -> bool {
        self.result == -libc::ECANCELED
    }
}

pub struct BatchRing {
    ring: IoUring,
    capacity: usize,
    settled: Vec<u64>,
}

// NOTE: every batch consumes the ring so a failure cannot be retried on state the kernel
// may already own; the ring comes back only when every op settled. `NotSubmitted` means
// no SQE was pushed and nothing reached the kernel: the connections keep their byte
// positions and want the direct read/write path. `Lost` means an SQE may have run: every
// contributing connection must close, because the peer's offset past the batch is
// unknowable.
#[must_use]
pub enum BatchFailure {
    NotSubmitted,
    Lost(io::Error),
}

impl BatchRing {
    pub fn new(entries: u32) -> io::Result<Self> {
        // NOTE: one worker owns this ring, so single-issuer taskrun removes the
        // cross-CPU task-work IPIs on every completion. Older kernels reject the
        // flags; fall back to a plain ring so batching still works there.
        let ring = IoUring::builder()
            .setup_single_issuer()
            .setup_coop_taskrun()
            .setup_defer_taskrun()
            .build(entries)
            .or_else(|_| IoUring::new(entries))?;
        let capacity = usize::try_from(entries).unwrap_or(0);
        Ok(Self {
            ring,
            capacity,
            settled: vec![0; capacity.div_ceil(WORD_BITS)],
        })
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[cfg(test)]
    pub(crate) fn ring_fd(&self) -> RawFd {
        use std::os::fd::AsRawFd;
        self.ring.as_raw_fd()
    }

    pub fn send_many(mut self, ops: &mut [SendOp]) -> Result<BatchRing, BatchFailure> {
        if ops.is_empty() {
            return Ok(self);
        }
        if ops.len() > self.capacity || !self.push_sends(ops) {
            return Err(BatchFailure::NotSubmitted);
        }
        let count = ops.len();
        let driven = self.drive(count, |index, result| {
            if let Some(op) = ops.get_mut(index) {
                op.result = result;
            }
        });
        match driven {
            Ok(()) => Ok(self),
            Err(error) => Err(BatchFailure::Lost(error)),
        }
    }

    pub fn recv_many(mut self, ops: &mut [ReadOp]) -> Result<BatchRing, BatchFailure> {
        if ops.is_empty() {
            return Ok(self);
        }
        if ops.len() > self.capacity || !self.push_recvs(ops) {
            return Err(BatchFailure::NotSubmitted);
        }
        let count = ops.len();
        let driven = self.drive(count, |index, result| {
            if let Some(op) = ops.get_mut(index) {
                op.result = result;
            }
        });
        match driven {
            Ok(()) => Ok(self),
            Err(error) => Err(BatchFailure::Lost(error)),
        }
    }

    fn push_sends(&mut self, ops: &[SendOp]) -> bool {
        let mut queue = self.ring.submission();
        for (index, op) in ops.iter().enumerate() {
            let entry = opcode::Send::new(
                Fd(op.fd),
                op.buffer,
                u32::try_from(op.length).unwrap_or(u32::MAX),
            )
            .build()
            .user_data(index as u64);
            // SAFETY: the entry copies the staged pointer; the caller keeps those bytes
            // valid until `drive` has settled every op of this batch.
            if unsafe { queue.push(&entry) }.is_err() {
                return false;
            }
        }
        true
    }

    fn push_recvs(&mut self, ops: &[ReadOp]) -> bool {
        let mut queue = self.ring.submission();
        for (index, op) in ops.iter().enumerate() {
            let entry = opcode::Recv::new(
                Fd(op.fd),
                op.buffer,
                u32::try_from(op.length).unwrap_or(u32::MAX),
            )
            .build()
            .user_data(index as u64);
            // SAFETY: the entry copies the staged pointer; the caller keeps those bytes
            // writable and unreferenced until `drive` has settled every op of this batch.
            if unsafe { queue.push(&entry) }.is_err() {
                return false;
            }
        }
        true
    }

    // NOTE: io_uring arms internal polling for a send or recv that would block, so its
    // completion arrives only once the peer moves bytes; waiting for every op parks the
    // whole worker on one full socket buffer. The batch therefore waits for nothing,
    // settles whatever completed inline, and cancels the remainder, which reports
    // -ECANCELED without having touched the buffer or the peer's stream position.
    fn drive<F: FnMut(usize, i32)>(&mut self, count: usize, mut settle: F) -> io::Result<()> {
        self.settled.fill(0);
        self.enter(0)?;
        let mut done = self.collect(count, &mut settle);
        if done == count {
            return Ok(());
        }
        self.push_cancels(count)?;
        while done < count {
            self.enter(1)?;
            done += self.collect(count, &mut settle);
        }
        Ok(())
    }

    fn enter(&mut self, want: u32) -> io::Result<()> {
        loop {
            match self.ring.submitter().submit_and_wait(want as usize) {
                Ok(_) => return Ok(()),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
    }

    fn collect<F: FnMut(usize, i32)>(&mut self, count: usize, settle: &mut F) -> usize {
        let mut done = 0;
        let mut queue = self.ring.completion();
        queue.sync();
        for completion in &mut queue {
            let tag = completion.user_data();
            if tag & CANCEL_TAG != 0 {
                continue;
            }
            let index = usize::try_from(tag).unwrap_or(usize::MAX);
            if index >= count || !mark(&mut self.settled, index) {
                continue;
            }
            settle(index, completion.result());
            done += 1;
        }
        done
    }

    fn push_cancels(&mut self, count: usize) -> io::Result<()> {
        let mut queue = self.ring.submission();
        for index in 0..count {
            if marked(&self.settled, index) {
                continue;
            }
            let entry = opcode::AsyncCancel::new(index as u64)
                .build()
                .user_data(CANCEL_TAG | index as u64);
            // SAFETY: a cancel entry carries no buffer and no length.
            if unsafe { queue.push(&entry) }.is_err() {
                return Err(io::Error::other("cancel queue full"));
            }
        }
        Ok(())
    }
}

fn marked(bits: &[u64], index: usize) -> bool {
    let Some(word) = bits.get(index / WORD_BITS) else {
        return false;
    };
    word & (1u64 << (index % WORD_BITS)) != 0
}

fn mark(bits: &mut [u64], index: usize) -> bool {
    let Some(word) = bits.get_mut(index / WORD_BITS) else {
        return false;
    };
    let bit = 1u64 << (index % WORD_BITS);
    if *word & bit != 0 {
        return false;
    }
    *word |= bit;
    true
}
