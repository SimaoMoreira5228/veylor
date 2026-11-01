use super::{
    ConnectionDeadlineKind, ConnectionPhase, ConnectionSlot, ConnectionTable, ConnectionTimeouts,
    ConnectionToken, DeadlineError, ReleaseOutcome, TableError, cancel_phase_deadline,
    deadline_timer_id, resume_keep_alive_request, schedule_phase_deadline,
    start_keep_alive_idle_deadline,
};

#[test]
fn allocates_releases_and_reuses_slots_with_fresh_generations() {
    let mut slots = [ConnectionSlot::EMPTY; 2];
    let mut free_stack = [0; 2];
    let mut table = ConnectionTable::new(7, 1, &mut slots, &mut free_stack).unwrap();

    let first = table.allocate(10).unwrap();
    let second = table.allocate(20).unwrap();
    assert_eq!(table.allocate(30), Err(TableError::Full));
    assert_eq!(table.lookup(first), Ok(10));
    assert_eq!(table.lookup(second), Ok(20));
    assert_eq!(table.phase(first), Ok(ConnectionPhase::ReadingRequestHead));
    assert_eq!(table.release(first), Ok(ReleaseOutcome::Released));
    assert_eq!(table.lookup(first), Err(TableError::Stale));
    assert_eq!(table.release(first), Err(TableError::Stale));

    let reused = table.allocate(30).unwrap();
    assert_eq!(reused.slot, first.slot);
    assert_eq!(reused.generation, first.generation + 1);
    assert_eq!(table.lookup(reused), Ok(30));
    assert_eq!(table.lookup(first), Err(TableError::Stale));
    assert_eq!(table.release(second), Ok(ReleaseOutcome::Released));
    assert_eq!(table.release(reused), Ok(ReleaseOutcome::Released));
}

#[test]
fn rejects_other_workers_and_worker_restarts() {
    let mut slots = [ConnectionSlot::EMPTY; 1];
    let mut free_stack = [0; 1];
    let mut table = ConnectionTable::new(4, 10, &mut slots, &mut free_stack).unwrap();
    let token = table.allocate(71).unwrap();

    let other_worker = ConnectionToken {
        worker_id: 5,
        ..token
    };
    assert_eq!(table.lookup(other_worker), Err(TableError::Stale));
    let restarted_worker = ConnectionToken {
        worker_generation: 11,
        ..token
    };
    assert_eq!(table.lookup(restarted_worker), Err(TableError::Stale));
    let out_of_range = ConnectionToken { slot: 1, ..token };
    assert_eq!(table.lookup(out_of_range), Err(TableError::Invalid));
}

#[test]
fn follows_http_phases_and_rejects_skips_and_reentry_after_close() {
    let mut slots = [ConnectionSlot::EMPTY; 1];
    let mut free_stack = [0; 1];
    let mut table = ConnectionTable::new(9, 2, &mut slots, &mut free_stack).unwrap();
    let token = table.allocate(81).unwrap();
    assert_eq!(
        table.transition(token, ConnectionPhase::KeepAliveIdle),
        Err(TableError::InvalidTransition)
    );
    assert_eq!(
        table.transition(token, ConnectionPhase::ReadingRequestBody),
        Ok(())
    );
    assert_eq!(
        table.transition(token, ConnectionPhase::AwaitingHandler),
        Ok(())
    );
    assert_eq!(
        table.transition(token, ConnectionPhase::WritingResponse),
        Ok(())
    );
    assert_eq!(
        table.transition(token, ConnectionPhase::KeepAliveIdle),
        Ok(())
    );
    assert_eq!(
        table.transition(token, ConnectionPhase::ReadingRequestHead),
        Ok(())
    );
    assert_eq!(table.transition(token, ConnectionPhase::Closing), Ok(()));
    assert_eq!(
        table.transition(token, ConnectionPhase::ReadingRequestHead),
        Err(TableError::InvalidTransition)
    );
}

#[test]
fn binds_clears_and_expires_deadlines_for_live_slot_generations() {
    let mut slots = [ConnectionSlot::EMPTY; 2];
    let mut free_stack = [0; 2];
    let mut table = ConnectionTable::new(11, 3, &mut slots, &mut free_stack).unwrap();
    let first = table.allocate(101).unwrap();
    let second = table.allocate(102).unwrap();

    let first_timer = table
        .bind_deadline(first, ConnectionDeadlineKind::RequestHead)
        .unwrap();
    assert_eq!(first_timer, deadline_timer_id(first));
    assert_eq!(
        table.bind_deadline(first, ConnectionDeadlineKind::RequestHead),
        Err(TableError::DuplicateDeadline)
    );
    assert_eq!(
        table.bind_deadline(second, ConnectionDeadlineKind::BodyProgress),
        Err(TableError::InvalidTransition)
    );
    assert_eq!(
        table.transition(first, ConnectionPhase::ReadingRequestBody),
        Err(TableError::InvalidTransition)
    );
    assert_eq!(table.clear_deadline(first), Ok(()));
    assert_eq!(
        table.expire_deadline(first_timer),
        Err(TableError::NotFound)
    );

    assert_eq!(
        table.transition(second, ConnectionPhase::ReadingRequestBody),
        Ok(())
    );
    let second_timer = table
        .bind_deadline(second, ConnectionDeadlineKind::BodyProgress)
        .unwrap();
    let deadline = table.expire_deadline(second_timer).unwrap();
    assert_eq!(deadline.token, second);
    assert_eq!(deadline.kind, ConnectionDeadlineKind::BodyProgress);
    assert_eq!(deadline.timer_id, second_timer);
    assert_eq!(
        table.expire_deadline(second_timer),
        Err(TableError::NotFound)
    );
    assert_eq!(table.release(first), Ok(ReleaseOutcome::Released));
    assert_eq!(table.release(second), Ok(ReleaseOutcome::Released));
}

#[test]
fn refuses_release_with_live_deadline_and_rejects_stale_expiration_after_reuse() {
    let mut slots = [ConnectionSlot::EMPTY; 1];
    let mut free_stack = [0; 1];
    let mut table = ConnectionTable::new(12, 4, &mut slots, &mut free_stack).unwrap();
    let first = table.allocate(110).unwrap();
    let stale_timer = table
        .bind_deadline(first, ConnectionDeadlineKind::RequestHead)
        .unwrap();
    assert_eq!(table.release(first), Err(TableError::InvalidTransition));
    table.clear_deadline(first).unwrap();
    assert_eq!(table.release(first), Ok(ReleaseOutcome::Released));

    let reused = table.allocate(111).unwrap();
    assert_eq!(reused.slot, first.slot);
    assert_eq!(reused.generation, first.generation + 1);
    assert_eq!(
        table.bind_deadline(reused, ConnectionDeadlineKind::RequestHead),
        Ok(deadline_timer_id(reused))
    );
    assert_eq!(
        table.expire_deadline(stale_timer),
        Err(TableError::NotFound)
    );
    assert_eq!(
        table
            .expire_deadline(deadline_timer_id(reused))
            .unwrap()
            .token,
        reused
    );
}

#[test]
fn retires_a_slot_before_generation_wrap() {
    let mut slots = [ConnectionSlot::EMPTY; 1];
    let mut free_stack = [0; 1];
    let mut table = ConnectionTable::new(13, 5, &mut slots, &mut free_stack).unwrap();
    let token = table.allocate(120).unwrap();
    let token = table.set_generation_for_test(token, 0x7fff_ffff).unwrap();
    assert_eq!(table.release(token), Ok(ReleaseOutcome::Retired));
    assert_eq!(table.allocate(121), Err(TableError::Full));
}

#[test]
fn validates_worker_generation_and_free_stack_storage_at_initialization() {
    let mut slots = [ConnectionSlot::EMPTY; 2];
    let mut short_free_stack = [0; 1];
    assert!(matches!(
        ConnectionTable::new(1, 1, &mut slots, &mut short_free_stack),
        Err(TableError::Invalid)
    ));
    let mut free_stack = [0; 2];
    assert!(matches!(
        ConnectionTable::new(1, 0, &mut slots, &mut free_stack),
        Err(TableError::Invalid)
    ));
}

#[test]
fn schedules_cancels_and_expires_deadlines() {
    let mut slots = [ConnectionSlot::EMPTY; 1];
    let mut free_stack = [0; 1];
    let mut table = ConnectionTable::new(14, 6, &mut slots, &mut free_stack).unwrap();
    let timeouts = ConnectionTimeouts {
        request_head_ns: 10,
        body_progress_ns: 20,
        response_write_ns: 30,
        keep_alive_idle_ns: 40,
    };
    let token = table.allocate(201).unwrap();

    let timer = schedule_phase_deadline(&mut table, token, 100, timeouts).unwrap();
    assert_eq!(timer.timer_id, deadline_timer_id(token));
    assert_eq!(timer.deadline_ns, 110);
    assert_eq!(table.deadline_ns(token), Ok(Some(110)));
    assert_eq!(
        schedule_phase_deadline(&mut table, token, 100, timeouts),
        Err(DeadlineError::ConnectionAlreadyTimed)
    );
    cancel_phase_deadline(&mut table, token).unwrap();
    assert_eq!(table.deadline_ns(token), Ok(None));

    let timer = schedule_phase_deadline(&mut table, token, 200, timeouts).unwrap();
    assert_eq!(timer.deadline_ns, 210);
    let deadline = table.expire_deadline(timer.timer_id).unwrap();
    assert_eq!(deadline.token, token);
    assert_eq!(deadline.kind, ConnectionDeadlineKind::RequestHead);

    table
        .transition(token, ConnectionPhase::ReadingRequestBody)
        .unwrap();
    let timer = schedule_phase_deadline(&mut table, token, 300, timeouts).unwrap();
    assert_eq!(timer.deadline_ns, 320);
    cancel_phase_deadline(&mut table, token).unwrap();
    table
        .transition(token, ConnectionPhase::AwaitingHandler)
        .unwrap();
    let timer = schedule_phase_deadline(&mut table, token, 350, timeouts).unwrap();
    assert_eq!(timer.deadline_ns, 370);
    cancel_phase_deadline(&mut table, token).unwrap();
    table
        .transition(token, ConnectionPhase::WritingResponse)
        .unwrap();
    let timer = schedule_phase_deadline(&mut table, token, 400, timeouts).unwrap();
    assert_eq!(timer.deadline_ns, 430);
    cancel_phase_deadline(&mut table, token).unwrap();
    table
        .transition(token, ConnectionPhase::KeepAliveIdle)
        .unwrap();
    let timer = schedule_phase_deadline(&mut table, token, 500, timeouts).unwrap();
    assert_eq!(timer.deadline_ns, 540);
    cancel_phase_deadline(&mut table, token).unwrap();
    assert_eq!(table.release(token), Ok(ReleaseOutcome::Released));
}

#[test]
fn keep_alive_deadline_is_cancelled_when_the_next_request_starts() {
    let mut slots = [ConnectionSlot::EMPTY; 1];
    let mut free_stack = [0; 1];
    let mut table = ConnectionTable::new(15, 7, &mut slots, &mut free_stack).unwrap();
    let timeouts = ConnectionTimeouts {
        request_head_ns: 100,
        keep_alive_idle_ns: 10,
        ..ConnectionTimeouts::default()
    };
    let token = table.allocate(202).unwrap();
    table
        .transition(token, ConnectionPhase::WritingResponse)
        .unwrap();

    let timer = start_keep_alive_idle_deadline(&mut table, token, 100, timeouts).unwrap();
    assert_eq!(timer.deadline_ns, 110);
    assert_eq!(table.phase(token), Ok(ConnectionPhase::KeepAliveIdle));
    resume_keep_alive_request(&mut table, token).unwrap();
    assert_eq!(table.deadline(token), Ok(None));
    assert_eq!(table.phase(token), Ok(ConnectionPhase::ReadingRequestHead));

    table
        .transition(token, ConnectionPhase::ReadingRequestBody)
        .unwrap();
    table
        .transition(token, ConnectionPhase::AwaitingHandler)
        .unwrap();
    table
        .transition(token, ConnectionPhase::WritingResponse)
        .unwrap();
    let zero_timeout = ConnectionTimeouts::default();
    assert_eq!(
        start_keep_alive_idle_deadline(&mut table, token, 200, zero_timeout),
        Err(DeadlineError::InvalidTimeout)
    );
    assert_eq!(table.phase(token), Ok(ConnectionPhase::Closing));
}

#[test]
fn deadline_scheduling_rejects_zero_overflow_full_and_stale_connections() {
    let mut slots = [ConnectionSlot::EMPTY; 1];
    let mut free_stack = [0; 1];
    let mut table = ConnectionTable::new(16, 8, &mut slots, &mut free_stack).unwrap();
    let token = table.allocate(203).unwrap();

    assert_eq!(
        schedule_phase_deadline(&mut table, token, 0, ConnectionTimeouts::default(),),
        Err(DeadlineError::InvalidTimeout)
    );
    assert_eq!(
        schedule_phase_deadline(
            &mut table,
            token,
            u64::MAX - 1,
            ConnectionTimeouts {
                request_head_ns: 2,
                ..ConnectionTimeouts::default()
            },
        ),
        Err(DeadlineError::DeadlineOverflow)
    );

    schedule_phase_deadline(
        &mut table,
        token,
        10,
        ConnectionTimeouts {
            request_head_ns: 2,
            ..ConnectionTimeouts::default()
        },
    )
    .unwrap();
    cancel_phase_deadline(&mut table, token).unwrap();
    assert_eq!(table.release(token), Ok(ReleaseOutcome::Released));
    assert_eq!(
        schedule_phase_deadline(
            &mut table,
            token,
            10,
            ConnectionTimeouts {
                request_head_ns: 2,
                ..ConnectionTimeouts::default()
            },
        ),
        Err(DeadlineError::StaleConnection)
    );
}

#[test]
fn production_timeouts_match_documented_defaults() {
    // NOTE: pins the values deployments cite: 5 s head/body/response budgets,
    // 30 s keep-alive idle. ServerConfig::default builds from these; changing a
    // default here changes every default server, so this fails loudly first.
    let timeouts = ConnectionTimeouts::production();
    assert_eq!(timeouts.request_head_ns, 5_000_000_000);
    assert_eq!(timeouts.body_progress_ns, 5_000_000_000);
    assert_eq!(timeouts.response_write_ns, 5_000_000_000);
    assert_eq!(timeouts.keep_alive_idle_ns, 30_000_000_000);
}
