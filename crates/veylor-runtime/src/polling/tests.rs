use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream as StdTcpStream};
use std::time::{Duration, Instant};

use super::{
    AcceptBatchOutcome, AcceptStop, AcceptedConnection, PollInterest, Poller, TcpListener,
    WAKE_TOKEN, accept_ready,
};
use crate::{TimerExpiration, TimerQueue};

#[test]
fn rejects_zero_event_capacity() {
    assert!(Poller::new(0).is_err());
}

#[test]
fn polls_with_the_timer_queue_timeout() {
    let mut timer_storage = [TimerExpiration::default(); 1];
    let mut index_storage = [0; 3];
    let mut timers = TimerQueue::new(&mut timer_storage, &mut index_storage).unwrap();
    timers
        .schedule(TimerExpiration {
            timer_id: 1,
            deadline_ns: 10,
        })
        .unwrap();
    let mut poller = Poller::new(1).unwrap();

    let started = Instant::now();
    let events = poller.poll_with_timers(&timers, 10, 1_000).unwrap();

    assert_eq!(events.count(), 0);
    assert!(started.elapsed() < Duration::from_millis(200));
}

#[test]
fn registers_listener_and_stream_and_reports_read_write_readiness() {
    let address: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut listener = TcpListener::bind(address).unwrap();
    let address = listener.local_addr().unwrap();
    let mut poller = Poller::new(4).unwrap();
    assert_eq!(poller.event_capacity(), 4);
    poller
        .register(&mut listener, 10, PollInterest::Readable)
        .unwrap();

    let mut client = StdTcpStream::connect(address).unwrap();
    assert!(
        poller
            .poll(Some(Duration::from_secs(2)))
            .unwrap()
            .any(|event| event.token == 10 && event.readable)
    );

    let (mut stream, _) = listener.accept().unwrap();
    poller
        .register(&mut stream, 11, PollInterest::Readable)
        .unwrap();
    client.write_all(b"ping").unwrap();
    assert!(
        poller
            .poll(Some(Duration::from_secs(2)))
            .unwrap()
            .any(|event| event.token == 11 && event.readable)
    );

    let mut input = [0; 4];
    stream.read_exact(&mut input).unwrap();
    assert_eq!(&input, b"ping");

    poller
        .reregister(&mut stream, 12, PollInterest::Writable)
        .unwrap();
    assert!(
        poller
            .poll(Some(Duration::from_secs(2)))
            .unwrap()
            .any(|event| event.token == 12 && event.writable)
    );
    poller.deregister(&mut stream).unwrap();
    poller.deregister(&mut listener).unwrap();
}

#[test]
fn returns_one_event_at_a_time_when_the_event_buffer_is_full() {
    let address_a: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let address_b: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut listener_a = TcpListener::bind(address_a).unwrap();
    let mut listener_b = TcpListener::bind(address_b).unwrap();
    let address_a = listener_a.local_addr().unwrap();
    let address_b = listener_b.local_addr().unwrap();
    let mut poller = Poller::new(1).unwrap();
    poller
        .register(&mut listener_a, 20, PollInterest::Readable)
        .unwrap();
    poller
        .register(&mut listener_b, 21, PollInterest::Readable)
        .unwrap();
    let _client_a = StdTcpStream::connect(address_a).unwrap();
    let _client_b = StdTcpStream::connect(address_b).unwrap();

    let first_token = {
        let mut events = poller.poll(Some(Duration::from_secs(2))).unwrap();
        let token = events.next().unwrap().token;
        assert!(events.next().is_none());
        token
    };
    match first_token {
        20 => drop(listener_a.accept().unwrap()),
        21 => drop(listener_b.accept().unwrap()),
        _ => panic!("unexpected listener token"),
    }

    let mut events = poller.poll(Some(Duration::from_secs(2))).unwrap();
    let second_token = events.next().unwrap().token;
    assert!(events.next().is_none());
    assert_ne!(second_token, first_token);
    match second_token {
        20 => drop(listener_a.accept().unwrap()),
        21 => drop(listener_b.accept().unwrap()),
        _ => panic!("unexpected listener token"),
    }
}

#[test]
fn accepts_until_would_block_and_keeps_peer_addresses() {
    let address: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut listener = TcpListener::bind(address).unwrap();
    let address = listener.local_addr().unwrap();
    let _client = StdTcpStream::connect(address).unwrap();
    let mut storage: [Option<AcceptedConnection>; 2] = [None, None];

    let batch = accept_ready(&mut listener, &mut storage, 8).unwrap();
    assert_eq!(batch.accepted, 1);
    assert_eq!(batch.attempts, 2);
    assert!(matches!(
        batch.outcome,
        AcceptBatchOutcome::Stopped(AcceptStop::Drained)
    ));
    assert!(storage[0].as_ref().unwrap().peer.is_ipv4());
    assert!(storage[0].as_ref().unwrap().stream.nodelay().unwrap());
    assert!(storage[1].is_none());
}

#[test]
fn accept_batches_report_storage_and_attempt_budgets_and_resume() {
    let address: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut listener = TcpListener::bind(address).unwrap();
    let address = listener.local_addr().unwrap();
    let _client_a = StdTcpStream::connect(address).unwrap();
    let _client_b = StdTcpStream::connect(address).unwrap();

    let mut one_slot: [Option<AcceptedConnection>; 1] = [None];
    let first = accept_ready(&mut listener, &mut one_slot, 8).unwrap();
    assert_eq!(first.accepted, 1);
    assert!(matches!(
        first.outcome,
        AcceptBatchOutcome::Stopped(AcceptStop::StorageFull)
    ));
    drop(one_slot[0].take().unwrap());

    let mut two_slots: [Option<AcceptedConnection>; 2] = [None, None];
    let second = accept_ready(&mut listener, &mut two_slots, 1).unwrap();
    assert_eq!(second.accepted, 1);
    assert_eq!(second.attempts, 1);
    assert!(matches!(
        second.outcome,
        AcceptBatchOutcome::Stopped(AcceptStop::BudgetExhausted)
    ));
    drop(two_slots[0].take().unwrap());

    let resumed = accept_ready(&mut listener, &mut two_slots, 8).unwrap();
    assert_eq!(resumed.accepted, 0);
    assert_eq!(resumed.attempts, 1);
    assert!(matches!(
        resumed.outcome,
        AcceptBatchOutcome::Stopped(AcceptStop::Drained)
    ));
}

#[test]
fn empty_storage_and_invalid_batch_inputs_are_distinguished() {
    let address: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut listener = TcpListener::bind(address).unwrap();
    let mut no_slots: [Option<AcceptedConnection>; 0] = [];
    let batch = accept_ready(&mut listener, &mut no_slots, 1).unwrap();
    assert_eq!(batch.accepted, 0);
    assert_eq!(batch.attempts, 0);
    assert!(matches!(
        batch.outcome,
        AcceptBatchOutcome::Stopped(AcceptStop::StorageFull)
    ));
    assert_eq!(
        accept_ready(&mut listener, &mut no_slots, 0)
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::InvalidInput
    );

    let mut occupied: [Option<AcceptedConnection>; 1] = [None];
    let _client = StdTcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let accepted = accept_ready(&mut listener, &mut occupied, 1).unwrap();
    assert_eq!(accepted.accepted, 1);
    let invalid = accept_ready(&mut listener, &mut occupied, 1).unwrap();
    assert_eq!(invalid.accepted, 0);
    assert_eq!(invalid.attempts, 0);
    assert!(matches!(
        invalid.outcome,
        AcceptBatchOutcome::InvalidStorage
    ));
}

#[cfg(not(any(target_os = "wasi", target_os = "horizon")))]
#[test]
fn cross_thread_wake_uses_the_reserved_token() {
    let mut poller = Poller::new(2).unwrap();
    let waker = poller.create_waker().unwrap();
    assert!(poller.create_waker().is_err());
    let wake_thread = std::thread::spawn(move || waker.wake().unwrap());

    let mut events = poller.poll(Some(Duration::from_secs(2))).unwrap();
    let event = events.next().unwrap();
    assert_eq!(event.token, WAKE_TOKEN);
    assert!(event.readable);
    assert!(events.next().is_none());
    wake_thread.join().unwrap();
}
