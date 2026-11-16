#![allow(unsafe_code)]

use std::os::fd::AsRawFd;

use veylor_runtime::{BatchFailure, BatchRing, PollInterest, Poller, ReadOp};

use crate::connection::{ConnPhase, ConnState, INPUT_CAP};

use super::turn::conn_poll_token;

// NOTE: one batch scratch per worker, sized to the event ceiling, so a turn with reads pays
// no allocation; the widths ride alongside the ops because a short read is the only proof
// that the socket drained.
pub(crate) struct ReadBatch {
    ops: Vec<ReadOp>,
    widths: Vec<usize>,
}

impl ReadBatch {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            ops: Vec::with_capacity(capacity),
            widths: Vec::with_capacity(capacity),
        }
    }

    // NOTE: `Some(slots)` means the ring died after an SQE may have reached the kernel, so
    // the batch is not retried and every contributing connection must close. That Vec is
    // allocated on that path only; the ring itself is already gone.
    pub(crate) fn recv_ready(
        &mut self,
        ring: &mut Option<BatchRing>,
        conns: &mut [Option<ConnState>],
        poller: &mut Poller,
        slots: &[usize],
        budget: u64,
    ) -> Option<Vec<usize>> {
        if slots.is_empty() || budget == 0 {
            return None;
        }
        let batch_ring = ring.take()?;
        let capacity = batch_ring.capacity();
        self.ops.clear();
        self.widths.clear();
        for &slot in slots {
            if self.ops.len() == capacity {
                break;
            }
            let Some(conn) = conns.get_mut(slot).and_then(|entry| entry.as_mut()) else {
                continue;
            };
            let tunneling = conn.phase == ConnPhase::Tunnel && conn.tunnel_echo.is_some();
            if !conn.read_ready
                || conn.has_tls()
                || !(conn.phase == ConnPhase::ReadingHead || tunneling)
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
            self.ops
                .push(unsafe { ReadOp::new(fd, slot as u32, room.as_mut_ptr(), width) });
            self.widths.push(width);
        }
        if self.ops.is_empty() {
            *ring = Some(batch_ring);
            return None;
        }
        settle_batch(
            batch_ring.recv_many(&mut self.ops),
            ring,
            conns,
            poller,
            &self.ops,
            &self.widths,
        )
    }
}

fn settle_batch(
    outcome: Result<BatchRing, BatchFailure>,
    ring: &mut Option<BatchRing>,
    conns: &mut [Option<ConnState>],
    poller: &mut Poller,
    ops: &[ReadOp],
    widths: &[usize],
) -> Option<Vec<usize>> {
    match outcome {
        Ok(batch_ring) => {
            *ring = Some(batch_ring);
            settle(conns, poller, ops, widths);
            None
        }
        Err(BatchFailure::NotSubmitted) => None,
        Err(BatchFailure::Lost(_)) => Some(ops.iter().map(|op| op.slot as usize).collect()),
    }
}

// NOTE: mio is edge-triggered, so a read that filled the whole width may have left bytes in
// the socket while the claim that brought it here is spent. Re-registering read interest
// re-arms the edge; a socket the read actually drained stays silent, so this cannot spin.
fn settle(conns: &mut [Option<ConnState>], poller: &mut Poller, ops: &[ReadOp], widths: &[usize]) {
    for (op, &width) in ops.iter().zip(widths) {
        let slot = op.slot as usize;
        let Some(conn) = conns.get_mut(slot).and_then(|entry| entry.as_mut()) else {
            continue;
        };
        if op.result <= 0 {
            continue;
        }
        let count = op.result as usize;
        conn.len += count;
        conn.batched_read += count;
        conn.read_ready = false;

        if count == width {
            rearm_readable(conn, poller, slot);
        }
    }
}

fn rearm_readable(conn: &mut ConnState, poller: &mut Poller, slot: usize) {
    let interest = if conn.write_interest {
        PollInterest::ReadWrite
    } else {
        PollInterest::Readable
    };

    let _ = poller.reregister(&mut conn.stream, conn_poll_token(slot), interest);
}

#[cfg(test)]
mod tests {
    use std::io::{self, Write};
    use std::net::{TcpListener, TcpStream as StdStream};
    use std::time::Duration;

    use veylor_runtime::{
        BatchFailure, BatchRing, ConnectionToken, PollInterest, Poller, ReadOp, Readiness,
        TcpStream,
    };

    use crate::connection::ConnState;
    use crate::static_server::turn::conn_poll_token;

    use super::{ReadBatch, settle_batch};

    fn ops(buffer: &mut [u8]) -> [ReadOp; 2] {
        // SAFETY: nothing is submitted; `Lost` and `NotSubmitted` read no bytes through
        // these pointers, and `buffer` outlives the ops in the caller.
        unsafe {
            [
                ReadOp::new(3, 11, buffer.as_mut_ptr(), buffer.len()),
                ReadOp::new(7, 42, buffer.as_mut_ptr(), buffer.len()),
            ]
        }
    }

    fn test_token() -> ConnectionToken {
        ConnectionToken {
            worker_id: 0,
            slot: 0,
            request_seq: 0,
            stream_id: 0,
            worker_generation: 1,
            generation: 1,
        }
    }

    fn rig(poller: &mut Poller) -> (ConnState, StdStream) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind rig");
        let addr = listener.local_addr().expect("rig addr");
        let client = StdStream::connect(addr).expect("rig connect");
        let (accepted, _) = listener.accept().expect("rig accept");
        accepted.set_nonblocking(true).expect("nonblocking");
        let mut conn = ConnState::new(test_token(), TcpStream::from_std(accepted));
        poller
            .register(&mut conn.stream, conn_poll_token(0), PollInterest::Readable)
            .expect("register rig");
        (conn, client)
    }

    fn read_once(conns: &mut [Option<ConnState>], poller: &mut Poller, budget: u64) {
        let mut ring = BatchRing::new(4).ok();
        let mut batch = ReadBatch::new(1);
        let lost = batch.recv_ready(&mut ring, conns, poller, &[0], budget);
        assert!(lost.is_none());
        assert!(ring.is_some(), "rig needs a working batch ring");
    }

    fn poll_once(poller: &mut Poller, timeout: Duration) -> Vec<Readiness> {
        loop {
            match poller.poll(Some(timeout)) {
                Ok(ready) => return ready.collect(),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => panic!("poll rig: {error:?}"),
            }
        }
    }

    #[test]
    fn exact_fill_rearms_readability_while_bytes_remain() {
        let mut poller = Poller::new(4).expect("poller");
        let (conn, mut client) = rig(&mut poller);
        let mut conns = [Some(conn)];
        client.write_all(b"abcdef").expect("write rig bytes");
        read_once(&mut conns, &mut poller, 4);
        let conn = conns[0].as_ref().expect("conn");
        assert_eq!(conn.len, 4);
        assert!(!conn.read_ready);

        let ready = poll_once(&mut poller, Duration::from_millis(200));
        assert_eq!(ready.len(), 1, "exact fill must re-arm the edge");
        assert_eq!(ready[0].token, conn_poll_token(0));
        assert!(ready[0].readable);
    }

    #[test]
    fn exact_fill_on_a_drained_socket_stays_quiet() {
        let mut poller = Poller::new(4).expect("poller");
        let (conn, mut client) = rig(&mut poller);
        let mut conns = [Some(conn)];
        client.write_all(b"abcd").expect("write rig bytes");
        read_once(&mut conns, &mut poller, 4);
        assert_eq!(conns[0].as_ref().expect("conn").len, 4);

        let ready = poll_once(&mut poller, Duration::from_millis(100));
        assert!(ready.is_empty(), "drained socket must not report ready");
    }

    #[test]
    fn lost_batch_names_every_contributor() {
        let mut buffer = [0u8; 8];
        let ops = ops(&mut buffer);
        let mut ring = None;
        let mut poller = Poller::new(4).expect("poller");
        let mut conns: [Option<super::ConnState>; 0] = [];
        let lost = settle_batch(
            Err(BatchFailure::Lost(io::Error::other("released ring"))),
            &mut ring,
            &mut conns,
            &mut poller,
            &ops,
            &[8, 8],
        );
        assert_eq!(lost, Some(vec![11, 42]));
        assert!(ring.is_none());
        assert!(ops.iter().all(|op| op.result == 0));
    }

    #[test]
    fn not_submitted_batch_keeps_connections() {
        let mut buffer = [0u8; 8];
        let ops = ops(&mut buffer);
        let mut ring = None;
        let mut poller = Poller::new(4).expect("poller");
        let mut conns: [Option<super::ConnState>; 0] = [];
        let lost = settle_batch(
            Err(BatchFailure::NotSubmitted),
            &mut ring,
            &mut conns,
            &mut poller,
            &ops,
            &[8, 8],
        );
        assert!(lost.is_none());
        assert!(ring.is_none());
        assert!(ops.iter().all(|op| op.result == 0));
    }
}
