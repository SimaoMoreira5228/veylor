use std::net::{SocketAddr, TcpStream as StdTcpStream};

use super::{
    AdmittedConnection, TurnConfig, TurnInput, TurnOutcome, Worker, WorkerError, WorkerStorage,
};
use crate::{
    AcceptedConnection, ConnectionDeadline, ConnectionDeadlineKind, ConnectionPhase,
    ConnectionSlot, ConnectionTimeouts, ConnectionToken, PollInterest, Poller, Readiness,
    ShutdownPhase, ShutdownResult, ShutdownState, TcpListener, TimerExpiration,
};

const LISTENER_TOKEN: usize = 7;
const SHUTDOWN_TIMER_ID: u64 = 7;

struct Harness<const SLOTS: usize, const TIMERS: usize, const INDEX: usize> {
    slots: [ConnectionSlot; SLOTS],
    free: [u32; SLOTS],
    timer_storage: [TimerExpiration; TIMERS],
    timer_index: [u32; INDEX],
    staging: [Option<AcceptedConnection>; SLOTS],
    live: [Option<ConnectionToken>; SLOTS],
    shutdown: ShutdownState,
    poller: Option<Poller>,
}

impl<const SLOTS: usize, const TIMERS: usize, const INDEX: usize> Harness<SLOTS, TIMERS, INDEX> {
    fn new() -> Self {
        Self {
            slots: [ConnectionSlot::EMPTY; SLOTS],
            free: [0; SLOTS],
            timer_storage: [TimerExpiration::default(); TIMERS],
            timer_index: [0; INDEX],
            staging: [(); SLOTS].map(|()| None),
            live: [None; SLOTS],
            shutdown: ShutdownState::new(),
            poller: None,
        }
    }
}

fn timeouts() -> ConnectionTimeouts {
    ConnectionTimeouts {
        request_head_ns: 1_000_000,
        body_progress_ns: 1_000_000,
        response_write_ns: 1_000_000,
        keep_alive_idle_ns: 60_000_000_000,
    }
}

fn open_worker<const SLOTS: usize, const TIMERS: usize, const INDEX: usize>(
    harness: &mut Harness<SLOTS, TIMERS, INDEX>,
    config: TurnConfig,
) -> (Worker<'_>, TcpListener, SocketAddr) {
    let mut poller = Poller::new(8).unwrap();
    let address: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut listener = TcpListener::bind(address).unwrap();
    let address = listener.local_addr().unwrap();
    poller
        .register(&mut listener, LISTENER_TOKEN, PollInterest::Readable)
        .unwrap();
    harness.poller = Some(poller);
    let worker = Worker::new(
        1,
        1,
        config,
        timeouts(),
        WorkerStorage {
            slots: &mut harness.slots,
            free_stack: &mut harness.free,
            timer_storage: &mut harness.timer_storage,
            timer_index_storage: &mut harness.timer_index,
            staging: &mut harness.staging,
            live: &mut harness.live,
        },
        &mut harness.shutdown,
        harness.poller.as_mut().unwrap(),
    )
    .unwrap();

    (worker, listener, address)
}

fn drive(
    worker: &mut Worker<'_>,
    listener: &mut TcpListener,
    now_ns: u64,
    max_wait_ms: i32,
    events: &mut [Readiness],
    expired: &mut [ConnectionDeadline],
    admitted: &mut [Option<AdmittedConnection>],
) -> Result<TurnOutcome, WorkerError> {
    worker.turn(
        listener,
        TurnInput {
            listener_token: LISTENER_TOKEN,
            now_ns,
            max_wait_ms,
            events,
            expired,
            admitted,
        },
    )
}

fn event_buffer() -> [Readiness; 8] {
    [Readiness {
        token: 0,
        readable: false,
        writable: false,
        read_closed_hint: false,
        write_closed_hint: false,
        error_hint: false,
    }; 8]
}

fn deadline_placeholder() -> ConnectionDeadline {
    ConnectionDeadline {
        token: ConnectionToken {
            worker_id: 0,
            slot: 0,
            request_seq: 0,
            stream_id: 0,
            worker_generation: 0,
            generation: 0,
        },
        timer_id: 0,
        kind: ConnectionDeadlineKind::RequestHead,
    }
}

fn connect(address: SocketAddr) -> StdTcpStream {
    StdTcpStream::connect(address).unwrap()
}

fn take_token(admitted: &mut [Option<AdmittedConnection>], index: usize) -> ConnectionToken {
    admitted[index].take().unwrap().token
}

fn admit_one(
    worker: &mut Worker<'_>,
    listener: &mut TcpListener,
    now_ns: u64,
    events: &mut [Readiness],
    expired: &mut [ConnectionDeadline],
    admitted: &mut [Option<AdmittedConnection>],
) -> ConnectionToken {
    let outcome = drive(worker, listener, now_ns, 200, events, expired, admitted).unwrap();
    assert_eq!(outcome.accepted, 1);
    take_token(admitted, 0)
}

#[test]
fn accept_into_slots_pauses_when_table_fills_preserving_backlog() {
    let mut harness = Harness::<4, 8, 24>::new();
    let (mut worker, mut listener, address) = open_worker(&mut harness, TurnConfig::DEFAULT);
    let _clients = [
        connect(address),
        connect(address),
        connect(address),
        connect(address),
        connect(address),
    ];
    let mut events = event_buffer();
    let mut expired = [deadline_placeholder(); 4];
    let mut admitted = [(); 4].map(|()| None);

    let first = drive(
        &mut worker,
        &mut listener,
        1_000_000,
        200,
        &mut events,
        &mut expired,
        &mut admitted,
    )
    .unwrap();
    assert_eq!(first.accepted, 4);
    assert_eq!(worker.live_count(), 4);
    assert_eq!(worker.pending_accepts(), 0);

    let mut admitted_overflow = [(); 4].map(|()| None);
    let paused = drive(
        &mut worker,
        &mut listener,
        1_000_000,
        200,
        &mut events,
        &mut expired,
        &mut admitted_overflow,
    );
    assert!(matches!(paused, Err(WorkerError::SlotCapacity)));
    assert_eq!(worker.pending_accepts(), 1);
    assert_eq!(worker.live_count(), 4);

    let token = take_token(&mut admitted, 0);
    worker.close(token).unwrap();
    assert_eq!(worker.live_count(), 3);

    let resumed = drive(
        &mut worker,
        &mut listener,
        1_000_000,
        200,
        &mut events,
        &mut expired,
        &mut admitted_overflow,
    )
    .unwrap();
    assert_eq!(resumed.accepted, 1);
    assert_eq!(worker.pending_accepts(), 0);
    assert_eq!(worker.live_count(), 4);
}

#[test]
fn deadlines_bind_on_admit_expire_by_kind_and_cancel_on_close() {
    let mut harness = Harness::<4, 8, 24>::new();
    let (mut worker, mut listener, address) = open_worker(&mut harness, TurnConfig::DEFAULT);
    let _client_a = connect(address);
    let mut events = event_buffer();
    let mut expired = [deadline_placeholder(); 4];
    let mut admitted = [(); 4].map(|()| None);

    let start = 10_000_000;
    let first = drive(
        &mut worker,
        &mut listener,
        start,
        200,
        &mut events,
        &mut expired,
        &mut admitted,
    )
    .unwrap();
    assert_eq!(first.accepted, 1);
    assert_eq!(first.expired_deadlines, 0);
    let token_a = take_token(&mut admitted, 0);
    worker.ensure_head_deadline(token_a, start).unwrap();

    let due = drive(
        &mut worker,
        &mut listener,
        start + 2_000_000,
        0,
        &mut events,
        &mut expired,
        &mut admitted,
    )
    .unwrap();
    assert_eq!(due.expired_deadlines, 1);
    assert_eq!(expired[0].kind, ConnectionDeadlineKind::RequestHead);
    assert_eq!(worker.live_count(), 1);

    assert_eq!(expired[0].token, token_a);
    worker.close(token_a).unwrap();
    assert_eq!(worker.live_count(), 0);

    let _client_b = connect(address);
    let admitted_b = drive(
        &mut worker,
        &mut listener,
        start + 2_000_000,
        200,
        &mut events,
        &mut expired,
        &mut admitted,
    )
    .unwrap();
    assert_eq!(admitted_b.accepted, 1);
    let token_b = take_token(&mut admitted, 0);
    worker.close(token_b).unwrap();

    let cancelled = drive(
        &mut worker,
        &mut listener,
        start + 4_000_000,
        0,
        &mut events,
        &mut expired,
        &mut admitted,
    )
    .unwrap();
    assert_eq!(cancelled.expired_deadlines, 0);
    assert_eq!(worker.live_count(), 0);
}

#[test]
fn zero_event_budget_is_rejected_at_construction() {
    let mut harness = Harness::<2, 4, 12>::new();
    let mut poller = Poller::new(2).unwrap();
    let config = TurnConfig {
        max_events_per_turn: 0,
        ..TurnConfig::DEFAULT
    };
    let worker = Worker::new(
        1,
        1,
        config,
        timeouts(),
        WorkerStorage {
            slots: &mut harness.slots,
            free_stack: &mut harness.free,
            timer_storage: &mut harness.timer_storage,
            timer_index_storage: &mut harness.timer_index,
            staging: &mut harness.staging,
            live: &mut harness.live,
        },
        &mut harness.shutdown,
        &mut poller,
    );

    assert!(matches!(worker, Err(WorkerError::ZeroEventBudget)));
    assert!(TurnConfig::DEFAULT.validate().is_ok());
}

#[test]
fn shutdown_drain_stops_admission_and_force_closes_at_deadline() {
    let mut harness = Harness::<4, 8, 24>::new();
    let (mut worker, mut listener, address) = open_worker(&mut harness, TurnConfig::DEFAULT);
    let _client_a = connect(address);
    let mut events = event_buffer();
    let mut expired = [deadline_placeholder(); 4];
    let mut admitted = [(); 4].map(|()| None);

    let start = 20_000_000;
    let token_a = admit_one(
        &mut worker,
        &mut listener,
        start,
        &mut events,
        &mut expired,
        &mut admitted,
    );

    let _client_b = connect(address);
    let shutdown = worker.begin_shutdown(start, 10_000_000, SHUTDOWN_TIMER_ID);
    assert_eq!(shutdown, ShutdownResult::Draining);
    assert_eq!(worker.shutdown_phase(), ShutdownPhase::Draining);

    let draining = drive(
        &mut worker,
        &mut listener,
        start + 1_000,
        200,
        &mut events,
        &mut expired,
        &mut admitted,
    )
    .unwrap();
    assert_eq!(draining.accepted, 0);
    assert!(!draining.shutdown_complete);
    assert_eq!(worker.pending_accepts(), 0);
    assert_eq!(worker.live_count(), 1);

    let forced = drive(
        &mut worker,
        &mut listener,
        start + 10_000_001,
        0,
        &mut events,
        &mut expired,
        &mut admitted,
    )
    .unwrap();
    assert_eq!(forced.accepted, 0);
    assert!(forced.shutdown_complete);
    assert_eq!(worker.live_count(), 0);
    assert_eq!(worker.shutdown_phase(), ShutdownPhase::Complete);
    assert!(matches!(
        worker.close(token_a),
        Err(WorkerError::StaleToken)
    ));
    assert_eq!(worker.destroy(), ShutdownResult::Destroyed);
}

#[test]
fn repeated_start_stop_cycles_leave_no_live_slots() {
    for _ in 0..32 {
        let mut harness = Harness::<2, 4, 12>::new();
        let (mut worker, mut listener, address) = open_worker(&mut harness, TurnConfig::DEFAULT);
        let _client = connect(address);
        let mut events = event_buffer();
        let mut expired = [deadline_placeholder(); 2];
        let mut admitted = [(); 2].map(|()| None);

        let outcome = drive(
            &mut worker,
            &mut listener,
            1_000_000,
            200,
            &mut events,
            &mut expired,
            &mut admitted,
        )
        .unwrap();
        assert_eq!(outcome.accepted, 1);

        let token = take_token(&mut admitted, 0);
        worker.close(token).unwrap();
        assert_eq!(worker.live_count(), 0);
        assert_eq!(
            worker.begin_shutdown(1_000_000, 0, SHUTDOWN_TIMER_ID),
            ShutdownResult::Completed
        );
        assert_eq!(worker.destroy(), ShutdownResult::Destroyed);
        assert_eq!(worker.shutdown_phase(), ShutdownPhase::Destroyed);
    }
}

#[test]
fn expired_timer_ids_never_target_a_reused_slot_generation() {
    let mut harness = Harness::<2, 4, 12>::new();
    let (mut worker, mut listener, address) = open_worker(&mut harness, TurnConfig::DEFAULT);
    let _client_a = connect(address);
    let mut events = event_buffer();
    let mut expired = [deadline_placeholder(); 2];
    let mut admitted = [(); 2].map(|()| None);

    let start = 30_000_000;
    let token_a = admit_one(
        &mut worker,
        &mut listener,
        start,
        &mut events,
        &mut expired,
        &mut admitted,
    );
    worker.close(token_a).unwrap();

    let _client_b = connect(address);
    let token_b = admit_one(
        &mut worker,
        &mut listener,
        start + 500_000,
        &mut events,
        &mut expired,
        &mut admitted,
    );
    assert_eq!(token_b.slot, token_a.slot);
    assert_ne!(token_b.generation, token_a.generation);

    let stale_check = drive(
        &mut worker,
        &mut listener,
        start + 1_000_001,
        0,
        &mut events,
        &mut expired,
        &mut admitted,
    )
    .unwrap();
    assert_eq!(stale_check.expired_deadlines, 0);
    assert_eq!(worker.live_count(), 1);
    worker.close(token_b).unwrap();
    assert_eq!(worker.live_count(), 0);
}

#[test]
fn timer_exhaustion_fails_closed_without_losing_the_slot() {
    let mut harness = Harness::<2, 1, 3>::new();
    let (mut worker, mut listener, address) = open_worker(&mut harness, TurnConfig::DEFAULT);
    let _client_a = connect(address);
    let _client_b = connect(address);
    let mut events = event_buffer();
    let mut expired = [deadline_placeholder(); 2];
    let mut admitted = [(); 2].map(|()| None);

    let first = drive(
        &mut worker,
        &mut listener,
        1_000_000,
        200,
        &mut events,
        &mut expired,
        &mut admitted,
    )
    .unwrap();
    assert_eq!(first.accepted, 2);
    let token_a = take_token(&mut admitted, 0);
    let token_b = take_token(&mut admitted, 1);
    worker.ensure_head_deadline(token_a, 1_000_000).unwrap();
    assert!(matches!(
        worker.ensure_head_deadline(token_b, 1_000_000),
        Err(WorkerError::TimerCapacity)
    ));
    assert_eq!(worker.live_count(), 2);
    assert_eq!(worker.pending_accepts(), 0);
    worker.close(token_a).unwrap();
    worker.close(token_b).unwrap();
    assert_eq!(worker.live_count(), 0);

    let _client_c = connect(address);
    admit_one(
        &mut worker,
        &mut listener,
        1_000_000,
        &mut events,
        &mut expired,
        &mut admitted,
    );
    assert_eq!(worker.live_count(), 1);
}

#[test]
fn progress_refresh_moves_deadline_while_zero_bytes_leave_it() {
    let mut harness = Harness::<4, 8, 24>::new();
    let (mut worker, mut listener, address) = open_worker(&mut harness, TurnConfig::DEFAULT);
    let _client = connect(address);
    let mut events = event_buffer();
    let mut expired = [deadline_placeholder(); 4];
    let mut admitted = [(); 4].map(|()| None);

    let start = 40_000_000;
    let token = admit_one(
        &mut worker,
        &mut listener,
        start,
        &mut events,
        &mut expired,
        &mut admitted,
    );

    assert!(worker.note_progress(token, 100, start + 500_000).unwrap());
    let moved = drive(
        &mut worker,
        &mut listener,
        start + 1_000_001,
        0,
        &mut events,
        &mut expired,
        &mut admitted,
    )
    .unwrap();
    assert_eq!(moved.expired_deadlines, 0);

    assert!(!worker.note_progress(token, 0, start + 1_000_002).unwrap());
    let due = drive(
        &mut worker,
        &mut listener,
        start + 1_500_001,
        0,
        &mut events,
        &mut expired,
        &mut admitted,
    )
    .unwrap();
    assert_eq!(due.expired_deadlines, 1);
    assert_eq!(expired[0].kind, ConnectionDeadlineKind::RequestHead);
    worker.close(token).unwrap();
}

#[test]
fn phase_transitions_reschedule_per_phase_timeouts() {
    let mut harness = Harness::<4, 8, 24>::new();
    let (mut worker, mut listener, address) = open_worker(&mut harness, TurnConfig::DEFAULT);
    let _client = connect(address);
    let mut events = event_buffer();
    let mut expired = [deadline_placeholder(); 4];
    let mut admitted = [(); 4].map(|()| None);

    let start = 50_000_000;
    let token = admit_one(
        &mut worker,
        &mut listener,
        start,
        &mut events,
        &mut expired,
        &mut admitted,
    );

    worker
        .transition_phase(token, ConnectionPhase::ReadingRequestBody, start + 200_000)
        .unwrap();
    let quiet = drive(
        &mut worker,
        &mut listener,
        start + 1_000_001,
        0,
        &mut events,
        &mut expired,
        &mut admitted,
    )
    .unwrap();
    assert_eq!(quiet.expired_deadlines, 0);
    let body_due = drive(
        &mut worker,
        &mut listener,
        start + 1_200_001,
        0,
        &mut events,
        &mut expired,
        &mut admitted,
    )
    .unwrap();
    assert_eq!(body_due.expired_deadlines, 1);
    assert_eq!(expired[0].kind, ConnectionDeadlineKind::BodyProgress);

    worker
        .transition_phase(token, ConnectionPhase::WritingResponse, start + 1_200_002)
        .unwrap();
    let response_due = drive(
        &mut worker,
        &mut listener,
        start + 2_200_003,
        0,
        &mut events,
        &mut expired,
        &mut admitted,
    )
    .unwrap();
    assert_eq!(response_due.expired_deadlines, 1);
    assert_eq!(expired[0].kind, ConnectionDeadlineKind::ResponseWrite);
    worker.close(token).unwrap();
}

#[test]
fn invalid_phase_transition_fails_closed_into_closing() {
    let mut harness = Harness::<4, 8, 24>::new();
    let (mut worker, mut listener, address) = open_worker(&mut harness, TurnConfig::DEFAULT);
    let _client = connect(address);
    let mut events = event_buffer();
    let mut expired = [deadline_placeholder(); 4];
    let mut admitted = [(); 4].map(|()| None);

    let start = 60_000_000;
    let token = admit_one(
        &mut worker,
        &mut listener,
        start,
        &mut events,
        &mut expired,
        &mut admitted,
    );

    assert!(matches!(
        worker.transition_phase(token, ConnectionPhase::KeepAliveIdle, start + 100),
        Err(WorkerError::Invalid)
    ));
    assert!(matches!(
        worker.transition_phase(token, ConnectionPhase::ReadingRequestHead, start + 100),
        Err(WorkerError::Invalid)
    ));
    assert!(matches!(
        worker.note_progress(token, 10, start + 100),
        Err(WorkerError::Invalid)
    ));
    worker.close(token).unwrap();
}

#[test]
fn stale_tokens_fail_closed_for_all_deadline_hooks() {
    let mut harness = Harness::<4, 8, 24>::new();
    let (mut worker, mut listener, address) = open_worker(&mut harness, TurnConfig::DEFAULT);
    let _client_a = connect(address);
    let mut events = event_buffer();
    let mut expired = [deadline_placeholder(); 4];
    let mut admitted = [(); 4].map(|()| None);

    let start = 70_000_000;
    let stale = admit_one(
        &mut worker,
        &mut listener,
        start,
        &mut events,
        &mut expired,
        &mut admitted,
    );
    worker.close(stale).unwrap();

    let _client_b = connect(address);
    let live = admit_one(
        &mut worker,
        &mut listener,
        start + 100,
        &mut events,
        &mut expired,
        &mut admitted,
    );
    assert_ne!(live.generation, stale.generation);

    assert!(!worker.note_progress(stale, 0, start + 200).unwrap());
    assert!(matches!(
        worker.note_progress(stale, 10, start + 200),
        Err(WorkerError::StaleToken)
    ));
    assert!(matches!(
        worker.transition_phase(stale, ConnectionPhase::ReadingRequestBody, start + 200),
        Err(WorkerError::StaleToken)
    ));
    assert!(matches!(
        worker.schedule_idle(stale, start + 200),
        Err(WorkerError::StaleToken)
    ));
    assert!(matches!(
        worker.cancel_idle(stale),
        Err(WorkerError::StaleToken)
    ));
    assert!(matches!(
        worker.cancel_deadline(stale),
        Err(WorkerError::StaleToken)
    ));
    assert!(matches!(worker.phase(stale), Err(WorkerError::StaleToken)));
    assert!(matches!(
        worker.resume_idle(stale, start + 200),
        Err(WorkerError::StaleToken)
    ));
    worker.close(live).unwrap();
}

#[test]
fn idle_arm_cancel_resume_round_trip() {
    let mut harness = Harness::<4, 8, 24>::new();
    let (mut worker, mut listener, address) = open_worker(&mut harness, TurnConfig::DEFAULT);
    let _client = connect(address);
    let mut events = event_buffer();
    let mut expired = [deadline_placeholder(); 4];
    let mut admitted = [(); 4].map(|()| None);

    let start = 80_000_000;
    let token = admit_one(
        &mut worker,
        &mut listener,
        start,
        &mut events,
        &mut expired,
        &mut admitted,
    );

    worker
        .transition_phase(token, ConnectionPhase::WritingResponse, start + 100)
        .unwrap();
    worker.cancel_deadline(token).unwrap();
    worker.cancel_deadline(token).unwrap();
    worker.schedule_idle(token, start + 200).unwrap();
    assert!(matches!(
        worker.schedule_idle(token, start + 200),
        Err(WorkerError::Invalid)
    ));

    worker.cancel_idle(token).unwrap();
    worker.cancel_idle(token).unwrap();
    worker
        .transition_phase(token, ConnectionPhase::ReadingRequestHead, start + 300)
        .unwrap();
    assert!(matches!(
        worker.cancel_idle(token),
        Err(WorkerError::Invalid)
    ));

    let due = drive(
        &mut worker,
        &mut listener,
        start + 1_000_301,
        0,
        &mut events,
        &mut expired,
        &mut admitted,
    )
    .unwrap();
    assert_eq!(due.expired_deadlines, 1);
    assert_eq!(expired[0].kind, ConnectionDeadlineKind::RequestHead);
    worker.close(token).unwrap();
}

#[test]
fn phase_reports_current_connection_phase() {
    let mut harness = Harness::<4, 8, 24>::new();
    let (mut worker, mut listener, address) = open_worker(&mut harness, TurnConfig::DEFAULT);
    let _client = connect(address);
    let mut events = event_buffer();
    let mut expired = [deadline_placeholder(); 4];
    let mut admitted = [(); 4].map(|()| None);
    let start = 90_000_000;
    let token = admit_one(
        &mut worker,
        &mut listener,
        start,
        &mut events,
        &mut expired,
        &mut admitted,
    );
    assert_eq!(
        worker.phase(token).unwrap(),
        ConnectionPhase::ReadingRequestHead
    );
    worker
        .transition_phase(token, ConnectionPhase::ReadingRequestBody, start + 100)
        .unwrap();
    assert_eq!(
        worker.phase(token).unwrap(),
        ConnectionPhase::ReadingRequestBody
    );
    worker.close(token).unwrap();
}

#[test]
fn resume_idle_rearms_head_deadline_in_one_round_trip() {
    let mut harness = Harness::<4, 8, 24>::new();
    let (mut worker, mut listener, address) = open_worker(&mut harness, TurnConfig::DEFAULT);
    let _client = connect(address);
    let mut events = event_buffer();
    let mut expired = [deadline_placeholder(); 4];
    let mut admitted = [(); 4].map(|()| None);
    let start = 100_000_000;
    let token = admit_one(
        &mut worker,
        &mut listener,
        start,
        &mut events,
        &mut expired,
        &mut admitted,
    );
    worker
        .transition_phase(token, ConnectionPhase::WritingResponse, start + 100)
        .unwrap();
    worker.cancel_deadline(token).unwrap();
    worker.schedule_idle(token, start + 200).unwrap();
    assert_eq!(worker.phase(token).unwrap(), ConnectionPhase::KeepAliveIdle);
    worker.resume_idle(token, start + 300).unwrap();
    assert_eq!(
        worker.phase(token).unwrap(),
        ConnectionPhase::ReadingRequestHead
    );
    assert!(matches!(
        worker.resume_idle(token, start + 400),
        Err(WorkerError::Invalid)
    ));
    let due = drive(
        &mut worker,
        &mut listener,
        start + 1_300_301,
        0,
        &mut events,
        &mut expired,
        &mut admitted,
    )
    .unwrap();
    assert_eq!(due.expired_deadlines, 1);
    assert_eq!(expired[0].kind, ConnectionDeadlineKind::RequestHead);
    worker.close(token).unwrap();
}
