#![allow(unsafe_code)]

use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpStream as StdTcpStream};
use std::os::fd::AsRawFd;
use std::time::{Duration, Instant};

use super::{BatchFailure, BatchRing, ReadOp, SendOp};
use crate::{TcpListener, TcpStream};

// NOTE: a stalled batch never returns on its own, because the peer never reads, so any
// bound well above scheduling noise separates "completed with a cancelled op" from
// "parked the worker".
const BOUND: Duration = Duration::from_secs(1);

fn pair() -> (TcpStream, StdTcpStream) {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("bind");
    let address = listener.local_addr().expect("local addr");
    let peer = StdTcpStream::connect(address).expect("connect");
    let (accepted, _) = listener.accept().expect("accept");
    (accepted, peer)
}

fn fill_send_buffer(stream: &TcpStream) -> usize {
    let block = vec![0u8; 1 << 20];
    let mut total = 0;
    loop {
        match (&*stream).write(&block) {
            Ok(written) => total += written,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return total,
            Err(error) => panic!("fill write: {error}"),
        }
    }
}

fn drain_until_blocked(peer: &mut StdTcpStream) {
    peer.set_nonblocking(true).expect("peer nonblocking");
    let mut scratch = vec![0u8; 1 << 20];
    loop {
        match peer.read(&mut scratch) {
            Ok(0) => return,
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(error) => panic!("peer drain: {error}"),
        }
    }
}

#[test]
fn accepted_sockets_are_nonblocking() {
    let (accepted, _peer) = pair();
    let flags = unsafe { libc::fcntl(accepted.as_raw_fd(), libc::F_GETFL) };
    assert!(flags >= 0);
    assert_ne!(
        flags & libc::O_NONBLOCK,
        0,
        "the accept path must hand the ring a nonblocking socket"
    );
}

#[test]
fn full_send_buffer_completes_the_batch_with_a_cancelled_op() {
    let (accepted, mut peer) = pair();
    let filled = fill_send_buffer(&accepted);
    assert!(filled > 0, "send buffer never filled");
    let ring = BatchRing::new(8).expect("ring");
    let payload = vec![7u8; 4_096];
    let mut ops =
        [unsafe { SendOp::new(accepted.as_raw_fd(), 0, payload.as_ptr(), payload.len()) }];
    let started = Instant::now();
    let outcome = ring.send_many(&mut ops);
    let elapsed = started.elapsed();
    assert!(
        elapsed < BOUND,
        "a full send buffer held the batch {elapsed:?}"
    );
    let mut ring = match outcome {
        Ok(ring) => ring,
        Err(_) => panic!("a full send buffer must cancel its op, not lose the ring"),
    };
    assert!(ops[0].canceled(), "result {}", ops[0].result);

    drain_until_blocked(&mut peer);
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let mut retry =
            [unsafe { SendOp::new(accepted.as_raw_fd(), 0, payload.as_ptr(), payload.len()) }];
        ring = match ring.send_many(&mut retry) {
            Ok(ring) => ring,
            Err(_) => panic!("retry lost the ring"),
        };
        if retry[0].result == payload.len() as i32 {
            break;
        }
        assert!(retry[0].canceled(), "retry result {}", retry[0].result);
        assert!(
            Instant::now() < deadline,
            "the peer drain never reopened the buffer"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn a_blocked_socket_does_not_hold_its_batch_peers() {
    let mut streams = Vec::new();
    let mut peers = Vec::new();
    for _ in 0..4 {
        let (accepted, peer) = pair();
        streams.push(accepted);
        peers.push(peer);
    }
    let filled = fill_send_buffer(&streams[3]);
    assert!(filled > 0, "send buffer never filled");
    let payloads: Vec<Vec<u8>> = (0..4).map(|index| vec![index as u8; 4_096]).collect();
    let mut ops: Vec<SendOp> = streams
        .iter()
        .zip(payloads.iter())
        .enumerate()
        .map(|(index, (stream, payload))| unsafe {
            SendOp::new(
                stream.as_raw_fd(),
                index as u32,
                payload.as_ptr(),
                payload.len(),
            )
        })
        .collect();
    let ring = BatchRing::new(16).expect("ring");
    let started = Instant::now();
    let outcome = ring.send_many(&mut ops);
    let elapsed = started.elapsed();
    assert!(
        elapsed < BOUND,
        "one full socket held the batch {elapsed:?}"
    );
    assert!(outcome.is_ok());
    for (index, op) in ops.iter().enumerate().take(3) {
        assert_eq!(op.result, 4_096, "peer {index} lost its send");
        assert!(!op.canceled());
    }
    assert!(ops[3].canceled(), "result {}", ops[3].result);
}

#[test]
fn empty_socket_read_is_cancelled_instead_of_waiting() {
    let (accepted, mut peer) = pair();
    let ring = BatchRing::new(8).expect("ring");
    let mut buffer = [0u8; 64];
    let mut ops = [unsafe { ReadOp::new(accepted.as_raw_fd(), 0, buffer.as_mut_ptr(), 64) }];
    let started = Instant::now();
    let outcome = ring.recv_many(&mut ops);
    let elapsed = started.elapsed();
    assert!(
        elapsed < BOUND,
        "an empty socket held the batch {elapsed:?}"
    );
    let ring = match outcome {
        Ok(ring) => ring,
        Err(_) => panic!("an empty socket must cancel its op, not lose the ring"),
    };
    assert!(ops[0].canceled(), "result {}", ops[0].result);

    peer.write_all(b"ping").expect("peer write");
    let mut retry =
        [unsafe { ReadOp::new(accepted.as_raw_fd(), 0, buffer.as_mut_ptr(), buffer.len()) }];
    let outcome = ring.recv_many(&mut retry);
    assert!(outcome.is_ok());
    assert_eq!(retry[0].result, 4);
    assert_eq!(&buffer[..4], b"ping");
}

#[test]
fn submit_failure_after_push_loses_the_ring() {
    let (accepted, _peer) = pair();
    let ring = BatchRing::new(8).expect("ring");
    replace_ring_fd(&ring);
    let payload = [9u8; 32];
    let mut ops =
        [unsafe { SendOp::new(accepted.as_raw_fd(), 0, payload.as_ptr(), payload.len()) }];
    let outcome = ring.send_many(&mut ops);
    match outcome {
        // NOTE: a released ring fd is rejected by io_uring_enter (EOPNOTSUPP here, EBADF
        // once the number is reused), so the test pins the classification, not the errno.
        Err(BatchFailure::Lost(error)) => assert!(error.raw_os_error().is_some()),
        _ => panic!("a submit on a released ring must be reported as lost"),
    }
    assert_eq!(ops[0].result, 0, "a lost batch reports no byte count");
}

#[test]
fn oversized_batch_is_not_submitted() {
    let ring = BatchRing::new(2).expect("ring");
    let payload = [1u8; 8];
    let mut ops: Vec<SendOp> = (0..3)
        .map(|index| unsafe { SendOp::new(index, index as u32, payload.as_ptr(), payload.len()) })
        .collect();
    let outcome = ring.send_many(&mut ops);
    assert!(matches!(outcome, Err(BatchFailure::NotSubmitted)));
    assert!(ops.iter().all(|op| op.result == 0));
}

// NOTE: `dup2` replaces the ring's descriptor in one step, so the kernel releases the
// io_uring instance and the crate's later close lands on the sink instead of a file
// another thread opened in the gap. The userspace ring maps stay valid, which is what
// lets the batch push SQEs before the submit fails.
fn replace_ring_fd(ring: &BatchRing) {
    let sink = std::fs::File::open("/dev/null").expect("open /dev/null");
    let replaced = unsafe { libc::dup2(sink.as_raw_fd(), ring.ring_fd()) };
    assert_eq!(replaced, ring.ring_fd());
}
