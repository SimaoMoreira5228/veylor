use super::{
    ShutdownPhase, ShutdownResult, ShutdownState, close_connection, destroy, force_shutdown,
    process_deadline, register_connection, register_connections, request_shutdown,
};
use crate::{TimerExpiration, TimerQueue};

#[test]
fn stops_admission_drains_until_deadline_then_completes() {
    let mut state = ShutdownState::new();
    assert_eq!(register_connection(&mut state), ShutdownResult::Accepted);
    assert_eq!(register_connection(&mut state), ShutdownResult::Accepted);
    let mut timer_storage = [TimerExpiration::default(); 1];
    let mut index_storage = [0; crate::TIMER_INDEX_STORAGE_FACTOR];
    let mut timers = TimerQueue::new(&mut timer_storage, &mut index_storage).unwrap();

    assert_eq!(
        request_shutdown(&mut state, Some(&mut timers), 71, 100, 50),
        ShutdownResult::Draining
    );
    assert_eq!(state.phase(), ShutdownPhase::Draining);
    assert_eq!(state.active_connections(), 2);
    assert_eq!(
        register_connection(&mut state),
        ShutdownResult::NotAccepting
    );
    assert_eq!(
        process_deadline(
            &mut state,
            TimerExpiration {
                timer_id: 71,
                deadline_ns: 149,
            },
        ),
        ShutdownResult::StaleExpiration
    );

    let timer = timers.pop_expired(150).unwrap().unwrap();
    assert_eq!(
        process_deadline(&mut state, timer),
        ShutdownResult::ForceConnections
    );
    assert_eq!(state.phase(), ShutdownPhase::Forcing);
    assert!(!state.has_deadline());
    assert_eq!(
        close_connection(&mut state, Some(&mut timers)),
        ShutdownResult::ConnectionClosed
    );
    assert_eq!(state.active_connections(), 1);
    assert_eq!(
        close_connection(&mut state, Some(&mut timers)),
        ShutdownResult::Completed
    );
    assert_eq!(state.phase(), ShutdownPhase::Complete);
    assert_eq!(destroy(&mut state), ShutdownResult::Destroyed);
    assert_eq!(state.phase(), ShutdownPhase::Destroyed);
}

#[test]
fn early_drain_completion_cancels_deadline_and_rejects_late_expiration() {
    let mut state = ShutdownState::new();
    assert_eq!(register_connection(&mut state), ShutdownResult::Accepted);
    let mut timer_storage = [TimerExpiration::default(); 1];
    let mut index_storage = [0; crate::TIMER_INDEX_STORAGE_FACTOR];
    let mut timers = TimerQueue::new(&mut timer_storage, &mut index_storage).unwrap();
    assert_eq!(
        request_shutdown(&mut state, Some(&mut timers), 81, 200, 75),
        ShutdownResult::Draining
    );

    assert_eq!(
        close_connection(&mut state, Some(&mut timers)),
        ShutdownResult::Completed
    );
    assert!(timers.is_empty());
    assert_eq!(state.phase(), ShutdownPhase::Complete);
    assert_eq!(
        process_deadline(&mut state, timer_storage[0]),
        ShutdownResult::StaleExpiration
    );
    assert_eq!(destroy(&mut state), ShutdownResult::Destroyed);
}

#[test]
fn failed_deadline_scheduling_leaves_running_state_unchanged() {
    let mut state = ShutdownState::new();
    assert_eq!(register_connection(&mut state), ShutdownResult::Accepted);
    let mut timer_storage = [TimerExpiration::default(); 1];
    let mut index_storage = [0; crate::TIMER_INDEX_STORAGE_FACTOR];
    let mut timers = TimerQueue::new(&mut timer_storage, &mut index_storage).unwrap();
    timers
        .schedule(TimerExpiration {
            timer_id: 91,
            deadline_ns: 10,
        })
        .unwrap();

    assert_eq!(
        request_shutdown(&mut state, Some(&mut timers), 92, 20, 10),
        ShutdownResult::TimerFull
    );
    assert_eq!(state.phase(), ShutdownPhase::Running);
    assert_eq!(state.active_connections(), 1);
    assert_eq!(register_connection(&mut state), ShutdownResult::Accepted);

    timers.cancel(91).unwrap();
    assert_eq!(
        request_shutdown(&mut state, Some(&mut timers), 92, 20, 10),
        ShutdownResult::Draining
    );
    assert_eq!(destroy(&mut state), ShutdownResult::Busy);
    let timer = timers.pop_expired(30).unwrap().unwrap();
    assert_eq!(
        process_deadline(&mut state, timer),
        ShutdownResult::ForceConnections
    );
    assert_eq!(
        close_connection(&mut state, Some(&mut timers)),
        ShutdownResult::ConnectionClosed
    );
    assert_eq!(
        close_connection(&mut state, Some(&mut timers)),
        ShutdownResult::Completed
    );
    assert_eq!(destroy(&mut state), ShutdownResult::Destroyed);
}

#[test]
fn zero_grace_forces_immediately_and_no_connections_complete_directly() {
    let mut state = ShutdownState::new();
    assert_eq!(register_connection(&mut state), ShutdownResult::Accepted);
    assert_eq!(
        request_shutdown(&mut state, None, 1, 0, 0),
        ShutdownResult::ForceConnections
    );
    assert_eq!(state.phase(), ShutdownPhase::Forcing);
    assert_eq!(
        close_connection(&mut state, None),
        ShutdownResult::Completed
    );
    assert_eq!(destroy(&mut state), ShutdownResult::Destroyed);

    let mut idle = ShutdownState::new();
    assert_eq!(
        request_shutdown(&mut idle, None, 2, 0, 100),
        ShutdownResult::Completed
    );
    assert_eq!(idle.phase(), ShutdownPhase::Complete);
    assert_eq!(destroy(&mut idle), ShutdownResult::Destroyed);
}

#[test]
fn force_shutdown_cancels_a_drain_timer_and_allows_late_timer_pop() {
    let mut state = ShutdownState::new();
    assert_eq!(register_connection(&mut state), ShutdownResult::Accepted);
    let mut timer_storage = [TimerExpiration::default(); 1];
    let mut index_storage = [0; crate::TIMER_INDEX_STORAGE_FACTOR];
    let mut timers = TimerQueue::new(&mut timer_storage, &mut index_storage).unwrap();
    assert_eq!(
        request_shutdown(&mut state, Some(&mut timers), 101, 500, 100),
        ShutdownResult::Draining
    );
    assert_eq!(timers.len(), 1);
    assert_eq!(
        force_shutdown(&mut state, Some(&mut timers)),
        ShutdownResult::ForceConnections
    );
    assert_eq!(state.phase(), ShutdownPhase::Forcing);
    assert!(!state.has_deadline());
    assert!(timers.is_empty());
    assert_eq!(
        close_connection(&mut state, Some(&mut timers)),
        ShutdownResult::Completed
    );
    assert_eq!(destroy(&mut state), ShutdownResult::Destroyed);
}

#[test]
fn deadline_overflow_duplicate_ids_and_counter_overflow_are_rejected() {
    let mut state = ShutdownState::new();
    assert_eq!(
        register_connections(&mut state, u64::MAX),
        ShutdownResult::Accepted
    );
    assert_eq!(register_connection(&mut state), ShutdownResult::Invalid);

    let mut timer_storage = [TimerExpiration::default(); 1];
    let mut index_storage = [0; crate::TIMER_INDEX_STORAGE_FACTOR];
    let mut timers = TimerQueue::new(&mut timer_storage, &mut index_storage).unwrap();
    assert_eq!(
        request_shutdown(&mut state, Some(&mut timers), 102, u64::MAX - 1, 2),
        ShutdownResult::DeadlineOverflow
    );
    assert_eq!(state.phase(), ShutdownPhase::Running);

    timers
        .schedule(TimerExpiration {
            timer_id: 103,
            deadline_ns: 100,
        })
        .unwrap();
    assert_eq!(
        request_shutdown(&mut state, Some(&mut timers), 103, 10, 5),
        ShutdownResult::DuplicateTimer
    );
    assert_eq!(state.phase(), ShutdownPhase::Running);
    assert_eq!(state.active_connections(), u64::MAX);
}

#[test]
fn force_without_connections_completes_and_destroy_is_idempotent() {
    let mut state = ShutdownState::new();
    assert_eq!(force_shutdown(&mut state, None), ShutdownResult::Completed);
    assert_eq!(state.phase(), ShutdownPhase::Complete);
    assert_eq!(state.active_connections(), 0);
    assert_eq!(destroy(&mut state), ShutdownResult::Destroyed);
    assert_eq!(destroy(&mut state), ShutdownResult::Destroyed);
    assert_eq!(force_shutdown(&mut state, None), ShutdownResult::Invalid);
}

#[test]
fn rejects_close_without_active_connections_and_destroy_before_completion() {
    let mut state = ShutdownState::new();
    assert_eq!(close_connection(&mut state, None), ShutdownResult::Invalid);
    assert_eq!(destroy(&mut state), ShutdownResult::Busy);
}

#[test]
fn handles_a_missing_drain_timer_without_stranding_connections() {
    let mut state = ShutdownState::new();
    assert_eq!(register_connection(&mut state), ShutdownResult::Accepted);
    let mut timer_storage = [TimerExpiration::default(); 1];
    let mut index_storage = [0; crate::TIMER_INDEX_STORAGE_FACTOR];
    let mut timers = TimerQueue::new(&mut timer_storage, &mut index_storage).unwrap();
    assert_eq!(
        request_shutdown(&mut state, Some(&mut timers), 104, 1, 10),
        ShutdownResult::Draining
    );
    let timer = timers.pop_expired(11).unwrap().unwrap();
    assert_eq!(
        force_shutdown(&mut state, Some(&mut timers)),
        ShutdownResult::ForceConnections
    );
    assert_eq!(
        process_deadline(&mut state, timer),
        ShutdownResult::StaleExpiration
    );
    assert_eq!(
        close_connection(&mut state, Some(&mut timers)),
        ShutdownResult::Completed
    );
}
