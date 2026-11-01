//! Turn-level recv batching: the poller already named the readable sockets,
//! so one io_uring submission can move every ready connection's bytes with a
//! single enter instead of one recv syscall per socket.
#![allow(unsafe_code)]

use std::os::fd::AsRawFd;

use veylor_runtime::{BatchRing, ReadOp};

use crate::connection::{ConnPhase, ConnState, INPUT_CAP};

pub(crate) fn recv_ready(
    ring: &mut BatchRing,
    conns: &mut [Option<ConnState>],
    slots: &[usize],
    budget: u64,
) {
    if slots.is_empty() || budget == 0 {
        return;
    }
    let mut ops: Vec<ReadOp> = Vec::with_capacity(slots.len().min(ring.capacity()));
    for &slot in slots {
        if ops.len() == ring.capacity() {
            break;
        }
        let Some(conn) = conns.get_mut(slot).and_then(|entry| entry.as_mut()) else {
            continue;
        };
        if !conn.read_ready
            || conn.has_tls()
            || conn.phase != ConnPhase::ReadingHead
            || conn.len >= INPUT_CAP
        {
            continue;
        }
        let fd = conn.stream.as_raw_fd();
        let len = conn.len;
        let input = ConnState::materialize_input(&mut conn.input);
        let room = &mut input[len..];
        let width = room.len().min(budget as usize).max(1);
        // SAFETY: `room` points into the connection's retained input slab, which lives
        // across the `recv_many` call below; no other reference touches it until the
        // call has drained every completion.
        ops.push(unsafe { ReadOp::new(fd, slot as u32, room.as_mut_ptr(), width) });
    }
    if ops.is_empty() || ring.recv_many(&mut ops).is_err() {
        return;
    }
    for op in &ops {
        let Some(conn) = conns
            .get_mut(op.slot as usize)
            .and_then(|entry| entry.as_mut())
        else {
            continue;
        };
        if op.result > 0 {
            let count = op.result as usize;
            conn.len += count;
            conn.batched_read += count;
            conn.read_ready = false;
        }
    }
}
