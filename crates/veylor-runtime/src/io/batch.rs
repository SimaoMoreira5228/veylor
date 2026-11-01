//! Batched socket I/O over one io_uring submission.
//!
//! Readiness still comes from the poller; this module only amortises the
//! syscall that moves bytes. A turn's ready connections submit their recv
//! SQEs together, one `io_uring_enter` drives them, and completions land back
//! in the caller's order. The caller owns the buffers and must keep every
//! submitted operation alive (and unmoved) until `recv_many` returns.
#![allow(unsafe_code)]

use std::io;
use std::os::fd::RawFd;

use io_uring::types::Fd;
use io_uring::{IoUring, opcode};

pub struct ReadOp {
    pub fd: RawFd,
    pub slot: u32,
    pub result: i32,
    buffer: *mut u8,
    length: usize,
}

impl ReadOp {
    /// # Safety
    ///
    /// `buffer` must point to `length` writable bytes that stay valid and are not
    /// accessed through another reference until the enclosing `recv_many` returns.
    pub unsafe fn new(fd: RawFd, slot: u32, buffer: *mut u8, length: usize) -> Self {
        Self {
            fd,
            slot,
            result: 0,
            buffer,
            length,
        }
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
    /// # Safety
    ///
    /// `buffer` must point to `length` readable bytes that stay valid and are not
    /// mutated through another reference until the enclosing `send_many` returns.
    pub unsafe fn new(fd: RawFd, slot: u32, buffer: *const u8, length: usize) -> Self {
        Self {
            fd,
            slot,
            result: 0,
            buffer,
            length,
        }
    }
}

pub struct BatchRing {
    ring: IoUring,
    capacity: usize,
}

impl BatchRing {
    pub fn new(entries: u32) -> io::Result<Self> {
        let ring = IoUring::new(entries)?;
        let capacity = usize::try_from(entries).unwrap_or(0);
        Ok(Self { ring, capacity })
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn send_many(&mut self, ops: &mut [SendOp]) -> io::Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        if ops.len() > self.capacity {
            return Err(io::Error::other("batch exceeds ring capacity"));
        }
        {
            let mut queue = self.ring.submission();
            for (index, op) in ops.iter().enumerate() {
                let entry = opcode::Send::new(
                    Fd(op.fd),
                    op.buffer,
                    u32::try_from(op.length).unwrap_or(u32::MAX),
                )
                .build()
                .user_data(index as u64);
                // SAFETY: same contract as recv_many; the caller owns the staged response
                // bytes and the submit-and-wait below drains each completion first.
                unsafe {
                    queue
                        .push(&entry)
                        .map_err(|_| io::Error::other("submission queue full"))?;
                }
            }
        }
        self.ring.submit_and_wait(ops.len())?;
        let mut queue = self.ring.completion();
        queue.sync();
        for completion in &mut queue {
            let index = usize::try_from(completion.user_data()).unwrap_or(usize::MAX);
            if let Some(op) = ops.get_mut(index) {
                op.result = completion.result();
            }
        }
        Ok(())
    }

    pub fn recv_many(&mut self, ops: &mut [ReadOp]) -> io::Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        if ops.len() > self.capacity {
            return Err(io::Error::other("batch exceeds ring capacity"));
        }
        {
            let mut queue = self.ring.submission();
            for (index, op) in ops.iter().enumerate() {
                let entry = opcode::Recv::new(
                    Fd(op.fd),
                    op.buffer,
                    u32::try_from(op.length).unwrap_or(u32::MAX),
                )
                .build()
                .user_data(index as u64);
                // SAFETY: the entry is copied into the shared submission ring, which the
                // kernel owns until submission; the caller keeps every buffer valid and the
                // submit-and-wait below drains each completion before returning.
                unsafe {
                    queue
                        .push(&entry)
                        .map_err(|_| io::Error::other("submission queue full"))?;
                }
            }
        }
        self.ring.submit_and_wait(ops.len())?;
        let mut queue = self.ring.completion();
        queue.sync();
        for completion in &mut queue {
            let index = usize::try_from(completion.user_data()).unwrap_or(usize::MAX);
            if let Some(op) = ops.get_mut(index) {
                op.result = completion.result();
            }
        }
        Ok(())
    }
}
