#![allow(unsafe_code)]

use std::io;
use std::os::fd::AsRawFd;

use veylor_runtime::{BatchFailure, BatchRing, PollInterest, Poller, SendOp, Worker};

use crate::connection::{ConnClose, ConnPhase, ConnState, PumpProgress, reuse_keep_alive};
use crate::host::HostShared;

use super::CloseCounts;
use super::conn_lifecycle::{apply_pump_hooks, close_conn};
use super::turn::conn_poll_token;

// NOTE: `Some(slots)` means the ring died after an SQE may have reached the kernel, so
// the batch is not retried and every contributing connection must close.
pub(crate) fn send_pending(
    ring: &mut Option<BatchRing>,
    conns: &mut [Option<ConnState>],
    slots: &[usize],
    budget: u64,
) -> Option<Vec<usize>> {
    if slots.is_empty() || budget == 0 {
        return None;
    }
    let batch_ring = ring.take()?;
    let capacity = batch_ring.capacity();
    let mut ops: Vec<SendOp> = Vec::with_capacity(slots.len().min(capacity));
    for &slot in slots {
        if ops.len() == capacity {
            break;
        }
        let Some(conn) = conns.get_mut(slot).and_then(|entry| entry.as_mut()) else {
            continue;
        };
        if !conn.write_deferred
            || conn.write_blocked
            || conn.has_tls()
            || conn.out_sent >= conn.out_len
        {
            continue;
        }
        let batched = match conn.phase {
            ConnPhase::SendingResponse => conn.file.is_none(),
            ConnPhase::Tunnel => conn.tunnel_echo.is_some(),
            _ => false,
        };
        if !batched {
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
    if ops.is_empty() {
        *ring = Some(batch_ring);
        return None;
    }
    settle_batch(batch_ring.send_many(&mut ops), ring, conns, &ops)
}

fn settle_batch(
    outcome: Result<BatchRing, BatchFailure>,
    ring: &mut Option<BatchRing>,
    conns: &mut [Option<ConnState>],
    ops: &[SendOp],
) -> Option<Vec<usize>> {
    match outcome {
        Ok(batch_ring) => {
            *ring = Some(batch_ring);
            settle(conns, ops);
            None
        }
        Err(BatchFailure::NotSubmitted) => None,
        Err(BatchFailure::Lost(_)) => Some(ops.iter().map(|op| op.slot as usize).collect()),
    }
}

fn settle(conns: &mut [Option<ConnState>], ops: &[SendOp]) {
    for op in ops {
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
        } else if op.canceled() {
            conn.write_blocked = true;
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

#[allow(clippy::too_many_arguments)]
pub(super) fn flush_deferred_sends(
    slots: &[usize],
    conns: &mut [Option<ConnState>],
    worker: &mut Worker<'_>,
    stream_poller: &mut Poller,
    host: &HostShared,
    counts: &mut CloseCounts,
    tls_acceptor: &mut Option<veylor_tls::TlsAcceptor>,
    now_ns: u64,
) {
    for &slot in slots {
        let (token, sent, error, done, closing, reason) = {
            let Some(conn) = conns[slot].as_mut() else {
                continue;
            };
            conn.write_deferred = false;
            conn.defer_write = false;
            let sent = conn.deferred_sent;
            conn.deferred_sent = 0;
            (
                conn.token(),
                sent,
                conn.error_hint,
                conn.phase == ConnPhase::SendingResponse && !conn.has_pending_output(),
                conn.closing,
                conn.close_reason,
            )
        };
        if error {
            close_conn(
                slot,
                ConnClose::TransportError,
                host,
                conns,
                stream_poller,
                worker,
                counts,
                tls_acceptor,
            );
            continue;
        }
        if done {
            if closing {
                close_conn(
                    slot,
                    reason,
                    host,
                    conns,
                    stream_poller,
                    worker,
                    counts,
                    tls_acceptor,
                );
                continue;
            }
            if let Some(conn) = conns[slot].as_mut() {
                conn.file = None;
                reuse_keep_alive(conn);
            }
            let progress = PumpProgress {
                response_flushed: true,
                ..PumpProgress::default()
            };
            if let Some(reason) = apply_pump_hooks(worker, token, progress, now_ns) {
                close_conn(
                    slot,
                    reason,
                    host,
                    conns,
                    stream_poller,
                    worker,
                    counts,
                    tls_acceptor,
                );
            }
            continue;
        }
        let progress = PumpProgress {
            bytes_written: sent as u64,
            ..PumpProgress::default()
        };
        if let Some(reason) = apply_pump_hooks(worker, token, progress, now_ns) {
            close_conn(
                slot,
                reason,
                host,
                conns,
                stream_poller,
                worker,
                counts,
                tls_acceptor,
            );
            continue;
        }
        if let Some(conn) = conns[slot].as_mut() {
            let want = conn.write_blocked;
            if want != conn.write_interest {
                conn.write_interest = want;
                let interest = if want {
                    PollInterest::ReadWrite
                } else {
                    PollInterest::Readable
                };
                let _ = stream_poller.reregister(&mut conn.stream, conn_poll_token(slot), interest);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use veylor_runtime::{BatchFailure, SendOp};

    use super::settle_batch;

    fn ops(buffer: &[u8]) -> [SendOp; 2] {
        // SAFETY: nothing is submitted; `Lost` and `NotSubmitted` read no bytes through
        // these pointers, and `buffer` outlives the ops in the caller.
        unsafe {
            [
                SendOp::new(3, 11, buffer.as_ptr(), buffer.len()),
                SendOp::new(7, 42, buffer.as_ptr(), buffer.len()),
            ]
        }
    }

    #[test]
    fn lost_batch_names_every_contributor() {
        let buffer = [0u8; 8];
        let ops = ops(&buffer);
        let mut ring = None;
        let mut conns: [Option<super::ConnState>; 0] = [];
        let lost = settle_batch(
            Err(BatchFailure::Lost(io::Error::other("released ring"))),
            &mut ring,
            &mut conns,
            &ops,
        );
        assert_eq!(lost, Some(vec![11, 42]));
        assert!(ring.is_none());
        assert!(ops.iter().all(|op| op.result == 0));
    }

    #[test]
    fn not_submitted_batch_keeps_connections() {
        let buffer = [0u8; 8];
        let ops = ops(&buffer);
        let mut ring = None;
        let mut conns: [Option<super::ConnState>; 0] = [];
        let lost = settle_batch(Err(BatchFailure::NotSubmitted), &mut ring, &mut conns, &ops);
        assert!(lost.is_none());
        assert!(ring.is_none());
        assert!(ops.iter().all(|op| op.result == 0));
    }
}
