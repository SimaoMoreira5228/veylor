//! Turn-level send batching: plain H1 responses stage in the connection output
//! slab, so one io_uring submission can push every deferred response at once.
#![allow(unsafe_code)]

use std::io;
use std::os::fd::AsRawFd;

use veylor_runtime::{BatchRing, SendOp};

use crate::connection::{ConnPhase, ConnState};

pub(crate) fn send_pending(
    ring: &mut BatchRing,
    conns: &mut [Option<ConnState>],
    slots: &[usize],
    budget: u64,
) {
    if slots.is_empty() || budget == 0 {
        return;
    }
    let mut ops: Vec<SendOp> = Vec::with_capacity(slots.len().min(ring.capacity()));
    for &slot in slots {
        if ops.len() == ring.capacity() {
            break;
        }
        let Some(conn) = conns.get_mut(slot).and_then(|entry| entry.as_mut()) else {
            continue;
        };
        if !conn.write_deferred
            || conn.write_blocked
            || conn.has_tls()
            || conn.phase != ConnPhase::SendingResponse
            || conn.file.is_some()
            || conn.out_sent >= conn.out_len
        {
            continue;
        }
        let fd = conn.stream.as_raw_fd();
        let start = conn.out_sent;
        let end = conn.out_len;
        let Some(output) = conn.output.as_ref() else {
            continue;
        };
        let src = &output[start..end];
        let width = src.len().min(budget as usize).max(1);
        // SAFETY: `src` points into the connection's retained output slab, which lives
        // across the `send_many` call below; no other reference writes it until every
        // completion has been drained.
        ops.push(unsafe { SendOp::new(fd, slot as u32, src.as_ptr(), width) });
    }
    if ops.is_empty() || ring.send_many(&mut ops).is_err() {
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
            conn.out_sent += op.result as usize;
            conn.deferred_sent = op.result as usize;
            conn.write_blocked = false;
        } else if op.result == 0 {
            conn.error_hint = true;
        } else {
            match io::Error::from_raw_os_error(-op.result).kind() {
                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted => {
                    conn.write_blocked = true;
                }
                _ => conn.error_hint = true,
            }
        }
    }
}
