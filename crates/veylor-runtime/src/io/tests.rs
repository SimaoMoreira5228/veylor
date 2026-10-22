use std::io::{Read as StdRead, Write as StdWrite};
use std::net::{TcpListener as StdListener, TcpStream as StdTcpStream};

use super::{CloseReason, IoBatch, IoOutcome, ReadAttempt, ReadStatus, WriteAttempt, WriteStatus};
use super::{dispatch, map_read_error, map_write_error, note_completion};
use super::{note_read_progress, note_write_progress};
use crate::{
    ConnectionSlot, ConnectionTable, ConnectionTimeouts, ConnectionToken, TimerExpiration,
    schedule_phase_deadline,
};
use crate::{TcpStream, TimerQueue};

fn token(slot: u32) -> ConnectionToken {
    ConnectionToken {
        worker_id: 1,
        slot,
        request_seq: 0,
        stream_id: 0,
        worker_generation: 1,
        generation: 1,
    }
}

fn feeder_pair() -> (TcpStream, StdTcpStream) {
    let listener = StdListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let feeder = StdTcpStream::connect(addr).unwrap();
    let (accepted, _) = listener.accept().unwrap();
    // NOTE: mio's from_std preserves the socket flags, so nonblocking mode must
    // be set explicitly; otherwise every dispatch read/write blocks.
    accepted.set_nonblocking(true).unwrap();
    (TcpStream::from_std(accepted), feeder)
}

fn feed(feeder: &mut StdTcpStream, bytes: usize) {
    feeder.write_all(&vec![7u8; bytes]).unwrap();
}

fn timeouts() -> ConnectionTimeouts {
    ConnectionTimeouts {
        request_head_ns: 1_000_000,
        body_progress_ns: 1_000_000,
        response_write_ns: 1_000_000,
        keep_alive_idle_ns: 60_000_000_000,
    }
}

#[derive(Clone, Copy, Debug)]
struct EntryReport {
    read: ReadStatus,
    write: WriteStatus,
    progressed: bool,
    closed: Option<CloseReason>,
}

fn run_once(
    stream: &mut TcpStream,
    scratch: &mut [u8],
    read_ready: bool,
    write_src: &[u8],
    write_ready: bool,
    budget: u64,
) -> (IoOutcome, EntryReport) {
    let mut batch = [IoBatch::new(
        token(0),
        stream,
        scratch,
        read_ready,
        write_src,
        write_ready,
    )];
    let outcome = dispatch(budget, &mut batch);
    let report = EntryReport {
        read: batch[0].read,
        write: batch[0].write,
        progressed: batch[0].progressed,
        closed: batch[0].closed,
    };
    (outcome, report)
}

#[test]
fn budget_splits_fairly_across_one_hot_and_two_idle_entries() {
    let (mut hot, mut hot_feeder) = feeder_pair();
    let (mut idle_a, _keep_a) = feeder_pair();
    let (mut idle_b, _keep_b) = feeder_pair();
    feed(&mut hot_feeder, 65_536);
    let mut hot_scratch = [0u8; 8_192];
    let mut idle_scratch_a = [0u8; 8_192];
    let mut idle_scratch_b = [0u8; 8_192];
    let mut batch = [
        IoBatch::new(token(0), &mut hot, &mut hot_scratch, true, &[], false),
        IoBatch::new(token(1), &mut idle_a, &mut idle_scratch_a, true, &[], false),
        IoBatch::new(token(2), &mut idle_b, &mut idle_scratch_b, true, &[], false),
    ];
    let outcome: IoOutcome = dispatch(3_000, &mut batch);
    assert_eq!(outcome.bytes_read, 1_000);
    assert_eq!(outcome.bytes_written, 0);
    assert_eq!(outcome.would_block, 2);
    assert_eq!(outcome.closed, 0);
    assert_eq!(batch[0].read, ReadStatus::Progress(1_000));
    assert_eq!(batch[1].read, ReadStatus::WouldBlock);
    assert_eq!(batch[2].read, ReadStatus::WouldBlock);
}

#[test]
fn two_hot_entries_share_the_turn_budget_evenly() {
    let (mut first, mut first_feeder) = feeder_pair();
    let (mut second, mut second_feeder) = feeder_pair();
    feed(&mut first_feeder, 65_536);
    feed(&mut second_feeder, 65_536);
    let mut first_scratch = [0u8; 8_192];
    let mut second_scratch = [0u8; 8_192];
    let mut batch = [
        IoBatch::new(token(0), &mut first, &mut first_scratch, true, &[], false),
        IoBatch::new(token(1), &mut second, &mut second_scratch, true, &[], false),
    ];
    let outcome = dispatch(2_000, &mut batch);
    assert_eq!(outcome.bytes_read, 2_000);
    assert_eq!(batch[0].read, ReadStatus::Progress(1_000));
    assert_eq!(batch[1].read, ReadStatus::Progress(1_000));
}

#[test]
fn eagain_yields_accepted_offset_and_resumes_from_it() {
    let (mut stream, mut feeder) = feeder_pair();
    let payload = vec![9u8; 24 << 20];
    let mut scratch = [0u8; 0];
    let mut offset = 0;
    for _ in 0..64 {
        let (_, report) = run_once(
            &mut stream,
            &mut scratch,
            false,
            &payload[offset..],
            true,
            1 << 20,
        );
        match report.write {
            WriteStatus::WouldBlock(accepted) => {
                offset += accepted;
                break;
            }
            WriteStatus::Progress(accepted) | WriteStatus::BudgetExhausted(accepted) => {
                offset += accepted;
            }
            ref status => panic!("expected progress or block while filling, got {status:?}"),
        }
        if offset == payload.len() {
            panic!("socket absorbed the whole payload without blocking");
        }
    }
    assert!(offset > 0);
    assert!(offset < payload.len());
    let drain = offset.min(65_536);
    feeder.read_exact(&mut vec![0u8; drain]).unwrap();
    let (outcome, report) = run_once(
        &mut stream,
        &mut scratch,
        false,
        &payload[offset..],
        true,
        1 << 20,
    );
    assert!(outcome.bytes_written > 0);
    assert!(matches!(
        report.write,
        WriteStatus::Progress(_) | WriteStatus::WouldBlock(_)
    ));
}

#[test]
fn eof_maps_to_peer_closed() {
    let (mut stream, feeder) = feeder_pair();
    drop(feeder);
    let mut scratch = [0u8; 8_192];
    let (outcome, report) = run_once(&mut stream, &mut scratch, true, &[], false, 8_192);
    assert_eq!(report.read, ReadStatus::Eof);
    assert_eq!(report.closed, Some(CloseReason::PeerClosed));
    assert!(!report.progressed);
    assert_eq!(outcome.closed, 1);
    assert_eq!(outcome.bytes_read, 0);
}

#[test]
fn reset_errors_map_to_reset_close_reason() {
    // NOTE: stable Rust cannot set SO_LINGER without a new dependency, and an
    // unread-data close still delivers FIN on this stack, so no user-space
    // sequence reliably produces ECONNRESET here. The mapping itself is pinned
    // with synthetic errors instead; the Odin reference covers the kernel path.
    assert_eq!(
        map_read_error(&std::io::Error::from(std::io::ErrorKind::ConnectionReset)),
        ReadAttempt::Closed(CloseReason::Reset)
    );
    assert_eq!(
        map_write_error(&std::io::Error::from(std::io::ErrorKind::ConnectionReset)),
        WriteAttempt::Closed(CloseReason::Reset)
    );
    assert_eq!(
        map_write_error(&std::io::Error::from(std::io::ErrorKind::BrokenPipe)),
        WriteAttempt::Closed(CloseReason::PeerClosed)
    );
    assert_eq!(
        map_read_error(&std::io::Error::from_raw_os_error(111)),
        ReadAttempt::Error(111)
    );
    assert_eq!(
        map_write_error(&std::io::Error::from_raw_os_error(111)),
        WriteAttempt::Error(111)
    );
}

#[test]
fn deadline_refreshes_only_on_byte_progress() {
    let mut slots = [ConnectionSlot::EMPTY; 2];
    let mut free = [0u32; 2];
    let mut timer_storage = [TimerExpiration::default(); 4];
    let mut timer_index = [0u32; 12];
    let mut table = ConnectionTable::new(1, 1, &mut slots, &mut free).unwrap();
    let mut timers = TimerQueue::new(&mut timer_storage, &mut timer_index).unwrap();
    let live = table.allocate(11).unwrap();
    schedule_phase_deadline(&mut table, &mut timers, live, 10_000_000, timeouts()).unwrap();
    assert!(!note_read_progress(&mut table, &mut timers, live, 0, 10_500_000, timeouts()).unwrap());
    assert!(
        !note_write_progress(&mut table, &mut timers, live, 0, 10_500_000, timeouts()).unwrap()
    );
    assert!(timers.pop_expired(10_500_000).unwrap().is_none());
    assert!(note_read_progress(&mut table, &mut timers, live, 5, 50_000_000, timeouts()).unwrap());
    assert!(timers.pop_expired(12_000_000).unwrap().is_none());
    assert!(note_write_progress(&mut table, &mut timers, live, 5, 60_000_000, timeouts()).unwrap());
    assert!(timers.pop_expired(51_000_000).unwrap().is_none());
    note_completion(&mut table, &mut timers, live).unwrap();
    assert!(table.deadline(live).unwrap().is_none());
    note_completion(&mut table, &mut timers, live).unwrap();
}

#[test]
fn zero_budget_turn_does_no_io_and_reports_cleanly() {
    let (mut stream, mut feeder) = feeder_pair();
    feed(&mut feeder, 1_024);
    let mut scratch = [0u8; 8_192];
    let (outcome, report) = run_once(&mut stream, &mut scratch, true, &[], false, 0);
    assert_eq!(outcome, IoOutcome::default());
    assert_eq!(report.read, ReadStatus::BudgetExhausted);
    let (outcome, report) = run_once(&mut stream, &mut scratch, true, &[], false, 8_192);
    assert_eq!(outcome.bytes_read, 1_024);
    assert_eq!(report.read, ReadStatus::Progress(1_024));
}

#[test]
fn reads_pause_while_output_is_still_pending() {
    let (mut stream, _feeder) = feeder_pair();
    let payload = vec![3u8; 8 << 20];
    let mut scratch = [0u8; 8_192];
    let mut offset = 0;
    for _ in 0..400 {
        let (_, report) = run_once(
            &mut stream,
            &mut [],
            false,
            &payload[offset..],
            true,
            65_536,
        );
        match report.write {
            WriteStatus::WouldBlock(accepted) => {
                offset += accepted;
                break;
            }
            WriteStatus::Progress(accepted) | WriteStatus::BudgetExhausted(accepted) => {
                offset += accepted;
            }
            ref status => panic!("expected progress or block while filling, got {status:?}"),
        }
        if offset == payload.len() {
            panic!("socket absorbed the whole fill payload without blocking");
        }
    }
    let (outcome, report) = run_once(
        &mut stream,
        &mut scratch,
        true,
        &payload[offset..],
        true,
        65_536,
    );
    assert_eq!(report.write, WriteStatus::WouldBlock(0));
    assert_eq!(report.read, ReadStatus::PausedForWrite);
    assert_eq!(outcome.bytes_read, 0);
    assert_eq!(outcome.bytes_written, 0);
}

#[test]
fn unarmed_output_does_not_pause_reads() {
    let (mut stream, mut feeder) = feeder_pair();
    feed(&mut feeder, 1_024);
    let mut scratch = [0u8; 8_192];
    let stale_src = [9u8; 16];
    let (_, report) = run_once(&mut stream, &mut scratch, true, &stale_src, false, 8_192);
    assert_eq!(report.write, WriteStatus::Idle);
    assert_eq!(report.read, ReadStatus::Progress(1_024));
}
