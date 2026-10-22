use std::io;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use veylor_router::RouteSlot;
use veylor_runtime::{
    AcceptedConnection, AdmittedConnection, ConnectionDeadline, ConnectionDeadlineKind,
    ConnectionPhase, ConnectionSlot, ConnectionToken, PollInterest, Poller, Readiness,
    ShutdownPhase, ShutdownState, TIMER_INDEX_STORAGE_FACTOR, TimerExpiration, TurnInput,
    WAKE_TOKEN, Worker, WorkerError, WorkerStorage,
};

use crate::connection::{ConnClose, ConnState, PumpOutcome, PumpProgress, pump_connection};
use crate::host::HostShared;
use crate::multi_worker::{ThreadCtx, ThreadResult};
use crate::static_routes::MAX_ROUTES;

use super::{CloseCounts, LISTENER_TOKEN, MAX_CONNECTIONS, MAX_EVENTS_PER_TURN, SHUTDOWN_TIMER_ID};

const MAX_EXPIRED_PER_TURN: usize = 64;
const SENTINEL_SLOT: u32 = u32::MAX;

fn conn_poll_token(slot: usize) -> usize {
    slot + 1
}

pub(crate) fn worker_main(ctx: ThreadCtx) -> ThreadResult {
    let ThreadCtx {
        worker_id,
        mut listener,
        config,
        routes,
        host,
        mut worker_poller,
        stop,
        force,
        peers: _,
    } = ctx;
    let mut result = ThreadResult {
        counts: CloseCounts::default(),
        error: None,
    };
    let mut stream_poller = match Poller::new(MAX_CONNECTIONS * 2) {
        Ok(poller) => poller,
        Err(error) => {
            result.error = Some(error.to_string());
            return result;
        }
    };
    // NOTE: host respond/cancel stages a command and wakes this poller so a parked
    // worker picks it up promptly instead of sleeping out the poll cadence.
    match stream_poller.create_waker() {
        Ok(waker) => host.register_waker(worker_id, waker),
        Err(error) => {
            result.error = Some(error.to_string());
            return result;
        }
    };
    let mut route_slots = [RouteSlot::EMPTY; MAX_ROUTES];
    let table = match routes.build_table(&mut route_slots) {
        Ok(table) => table,
        Err(_) => {
            result.error = Some("route freeze failed".to_string());
            return result;
        }
    };

    let mut slots = vec![ConnectionSlot::EMPTY; MAX_CONNECTIONS];
    let mut free_stack = vec![0u32; MAX_CONNECTIONS];
    let timer_cap = MAX_CONNECTIONS + 2;
    let mut timer_storage = vec![TimerExpiration::default(); timer_cap];
    let mut timer_index = vec![0u32; timer_cap * TIMER_INDEX_STORAGE_FACTOR];
    let staging_cap = config.turn.max_accepts_per_turn.max(1);
    let mut staging: Vec<Option<AcceptedConnection>> = (0..staging_cap).map(|_| None).collect();
    let mut live: Vec<Option<ConnectionToken>> = (0..MAX_CONNECTIONS).map(|_| None).collect();
    let mut shutdown = ShutdownState::new();
    let storage = WorkerStorage {
        slots: &mut slots,
        free_stack: &mut free_stack,
        timer_storage: &mut timer_storage,
        timer_index_storage: &mut timer_index,
        staging: &mut staging,
        live: &mut live,
    };
    let mut worker = match Worker::new(
        worker_id,
        1,
        config.turn,
        config.timeouts,
        storage,
        &mut shutdown,
        &mut worker_poller,
    ) {
        Ok(worker) => worker,
        Err(_) => {
            result.error = Some("worker init failed".to_string());
            return result;
        }
    };

    let mut conns: Vec<Option<ConnState>> = (0..MAX_CONNECTIONS).map(|_| None).collect();
    let mut events: Vec<Readiness> = (0..MAX_EVENTS_PER_TURN)
        .map(|_| Readiness {
            token: WAKE_TOKEN,
            readable: false,
            writable: false,
            read_closed_hint: false,
            write_closed_hint: false,
            error_hint: false,
        })
        .collect();
    let mut expired: Vec<ConnectionDeadline> = (0..MAX_EXPIRED_PER_TURN)
        .map(|_| sentinel_deadline())
        .collect();
    let mut admitted: Vec<Option<AdmittedConnection>> = (0..staging_cap).map(|_| None).collect();
    let mut ready: Vec<Readiness> = Vec::new();
    let base = Instant::now();
    let mut turn_offset = 0usize;
    let mut stop_seen = false;
    let mut drain_start_ns = 0u64;

    loop {
        let now_ns = base.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        if force.load(Ordering::Acquire) {
            worker.force_shutdown();
        } else if stop.load(Ordering::Acquire) && worker.shutdown_phase() == ShutdownPhase::Running
        {
            // NOTE: the first observation runs one normal turn so SYNs already in
            // the backlog are admitted and their in-flight bytes served; only the
            // next iteration enters drain. Without the sweep, stop-before-accept
            // leaves backlog connections to RST on listener close.
            if !stop_seen {
                stop_seen = true;
            } else {
                let result =
                    worker.begin_shutdown(now_ns, config.shutdown_grace_ns, SHUTDOWN_TIMER_ID);
                if result == veylor_runtime::ShutdownResult::Draining {
                    drain_start_ns = now_ns;
                }
            }
        }
        expired.fill(sentinel_deadline());
        // NOTE: the loop wait rides the stream poller while streams are registered, so
        // stream readiness wakes the wait instead of every wave paying the worker-poller
        // cadence; Worker::turn then runs at zero wait and still drains accepts/expires
        // timers each iteration. With zero streams the stream poll is skipped and the
        // worker poller carries the wait as before (no spin either way). Stop/force wake
        // only worker-poller wakers, so a stop sleeping in the stream poll is observed at
        // loop top within one stream cadence; new accepts likewise wait out one cadence.
        let live_streams = worker.live_count();
        let pending = work_pending(&conns);
        let stream_wait_ms = if pending { 0 } else { config.poll_wait_ms };
        let worker_wait_ms = if live_streams == 0 { stream_wait_ms } else { 0 };
        if live_streams > 0 {
            ready.clear();
            let poll_error: Option<io::Error> =
                match stream_poller.poll(Some(Duration::from_millis(stream_wait_ms as u64))) {
                    Ok(readiness) => {
                        ready.extend(readiness.take(MAX_CONNECTIONS * 2));
                        None
                    }
                    Err(error) => Some(error),
                };
            for event in ready.drain(..) {
                if event.token == LISTENER_TOKEN || event.token == WAKE_TOKEN {
                    continue;
                }
                let Some(conn) = conns
                    .get_mut(event.token.wrapping_sub(1))
                    .and_then(|slot| slot.as_mut())
                else {
                    continue;
                };
                if event.readable || event.read_closed_hint {
                    conn.mark_readable();
                }
                if event.writable {
                    conn.mark_writable();
                }
                if event.error_hint {
                    conn.mark_error();
                }
            }
            if let Some(error) = poll_error {
                result.error = Some(error.to_string());
                cleanup_all(
                    &host,
                    &mut conns,
                    &mut stream_poller,
                    &mut worker,
                    &mut result.counts,
                );
                worker.destroy();
                host.push_shutdown(worker_id);
                break;
            }
        }
        let input = TurnInput {
            listener_token: LISTENER_TOKEN,
            now_ns,
            max_wait_ms: worker_wait_ms,
            events: &mut events,
            expired: &mut expired,
            admitted: &mut admitted,
        };
        let turn_result = worker.turn(&mut listener, input);
        drain_admitted(
            &mut admitted,
            &mut conns,
            &mut stream_poller,
            &mut worker,
            &mut result.counts,
        );
        drain_expired(
            &expired,
            &host,
            &mut conns,
            &mut stream_poller,
            &mut worker,
            &mut result.counts,
        );
        let draining = worker.shutdown_phase() != ShutdownPhase::Running;
        drain_host_staged(
            &host,
            worker_id,
            &mut conns,
            &mut stream_poller,
            &mut worker,
            &mut result.counts,
            now_ns,
        );
        match turn_result {
            Ok(outcome) => {
                if outcome.shutdown_complete {
                    cleanup_all(
                        &host,
                        &mut conns,
                        &mut stream_poller,
                        &mut worker,
                        &mut result.counts,
                    );
                    worker.destroy();
                    host.push_shutdown(worker_id);
                    break;
                }
            }
            Err(WorkerError::SlotCapacity) => {}
            Err(error) => {
                result.error = Some(format!("worker turn failed: {error:?}"));
                cleanup_all(
                    &host,
                    &mut conns,
                    &mut stream_poller,
                    &mut worker,
                    &mut result.counts,
                );
                worker.destroy();
                host.push_shutdown(worker_id);
                break;
            }
        }

        let live_count = worker.live_count().max(1) as u64;
        let per_conn_budget = config.turn.byte_budget_per_turn.div_ceil(live_count).max(1);
        for step in 0..MAX_CONNECTIONS {
            let index = (turn_offset + step) % MAX_CONNECTIONS;
            let Some(conn) = conns[index].as_mut() else {
                continue;
            };
            if draining {
                conn.begin_drain();
            }
            let hook_close = match pump_connection(
                conn,
                &routes,
                &table,
                per_conn_budget,
                worker_id,
                &host,
                draining,
            ) {
                PumpOutcome::Keep(progress) => {
                    let token = conn.token();
                    apply_pump_hooks(&mut worker, token, progress, now_ns)
                }
                PumpOutcome::Close(reason) => Some(reason),
            };
            if let Some(reason) = hook_close {
                close_conn(
                    index,
                    reason,
                    &host,
                    &mut conns,
                    &mut stream_poller,
                    &mut worker,
                    &mut result.counts,
                );
            }
        }
        turn_offset = (turn_offset + 1) % MAX_CONNECTIONS;

        // NOTE: drain serves in-flight exchanges to completion (each conn closes
        // after its current response via begin_drain); conns still idle past the
        // linger close promptly instead of waiting out their admission deadline.
        if draining
            && drain_start_ns > 0
            && now_ns.saturating_sub(drain_start_ns) >= config.drain_idle_linger_ns
        {
            close_idle_for_drain(
                &host,
                &mut conns,
                &mut stream_poller,
                &mut worker,
                &mut result.counts,
            );
        }
    }
    result
}

fn sentinel_deadline() -> ConnectionDeadline {
    ConnectionDeadline {
        token: ConnectionToken {
            worker_id: u32::MAX,
            slot: SENTINEL_SLOT,
            request_seq: 0,
            stream_id: 0,
            worker_generation: 0,
            generation: 0,
        },
        timer_id: 0,
        kind: ConnectionDeadlineKind::RequestHead,
    }
}

fn drain_admitted(
    admitted: &mut [Option<AdmittedConnection>],
    conns: &mut [Option<ConnState>],
    stream_poller: &mut Poller,
    worker: &mut Worker<'_>,
    counts: &mut CloseCounts,
) {
    for entry in admitted.iter_mut() {
        let Some(admitted_conn) = entry.take() else {
            continue;
        };
        let slot = admitted_conn.token.slot as usize;
        let mut registered = false;
        if slot < conns.len() && conns[slot].is_none() {
            let mut stream = admitted_conn.stream;
            if stream_poller
                .register(&mut stream, conn_poll_token(slot), PollInterest::Readable)
                .is_ok()
            {
                conns[slot] = Some(ConnState::new(admitted_conn.token, stream));
                registered = true;
            } else {
                let _ = worker.close(admitted_conn.token);
            }
        } else {
            let _ = worker.close(admitted_conn.token);
        }
        if !registered {
            counts.add(ConnClose::TransportError);
        }
    }
}

fn drain_expired(
    expired: &[ConnectionDeadline],
    host: &HostShared,
    conns: &mut [Option<ConnState>],
    stream_poller: &mut Poller,
    worker: &mut Worker<'_>,
    counts: &mut CloseCounts,
) {
    for deadline in expired {
        if deadline.token.slot == SENTINEL_SLOT {
            break;
        }
        let slot = deadline.token.slot as usize;
        let timed_out = conns
            .get(slot)
            .and_then(|entry| entry.as_ref())
            .is_some_and(|conn| conn.token() == deadline.token);
        if timed_out {
            // NOTE: an abandoned park (host never responded) reports ParkedTimeout, not
            // Timeout, so host health is distinguishable from slow networks.
            let reason = if deadline.kind == ConnectionDeadlineKind::AwaitingHandler {
                ConnClose::ParkedTimeout
            } else {
                ConnClose::Timeout
            };
            close_conn(slot, reason, host, conns, stream_poller, worker, counts);
        }
    }
}

// NOTE: a staged validation failure drops the work without touching the slot: the entry
// is already tombed, and the host got its outcome at stage time. Single-threaded worker
// order makes this unreachable except under table divergence, so fail closed silently.
fn drain_host_staged(
    host: &HostShared,
    worker_id: u32,
    conns: &mut [Option<ConnState>],
    stream_poller: &mut Poller,
    worker: &mut Worker<'_>,
    counts: &mut CloseCounts,
    now_ns: u64,
) {
    for work in host.take_staged(worker_id) {
        let valid = conns
            .get(work.slot)
            .and_then(|entry| entry.as_ref())
            .is_some_and(|conn| conn.token() == work.token && conn.is_parked())
            && worker
                .phase(work.token)
                .is_ok_and(|phase| phase == ConnectionPhase::AwaitingHandler);
        if !valid {
            continue;
        }
        if work.cancel {
            close_conn(
                work.slot,
                ConnClose::Cancelled,
                host,
                conns,
                stream_poller,
                worker,
                counts,
            );
            continue;
        }
        let encoded = conns
            .get_mut(work.slot)
            .and_then(|entry| entry.as_mut())
            .map(|conn| conn.complete_parked(work.status, &work.reason, &work.body));
        match encoded {
            Some(Ok(())) => {
                if worker
                    .transition_phase(work.token, ConnectionPhase::WritingResponse, now_ns)
                    .is_err()
                {
                    close_conn(
                        work.slot,
                        ConnClose::TransportError,
                        host,
                        conns,
                        stream_poller,
                        worker,
                        counts,
                    );
                }
            }
            Some(Err(reason)) => {
                close_conn(
                    work.slot,
                    reason,
                    host,
                    conns,
                    stream_poller,
                    worker,
                    counts,
                );
            }
            None => {}
        }
    }
}

// NOTE: hook failures mean the worker table diverged from connection state; a
// failed transition already parks the slot in Closing, so the server closes the
// connection instead of running it deadline-less or retrying into the park.
fn apply_pump_hooks(
    worker: &mut Worker<'_>,
    token: ConnectionToken,
    progress: PumpProgress,
    now_ns: u64,
) -> Option<ConnClose> {
    let bytes = progress.bytes_read.saturating_add(progress.bytes_written);
    if worker.note_progress(token, bytes, now_ns).is_err() {
        return Some(ConnClose::TransportError);
    }
    if progress.resumed_idle {
        // NOTE: pump marks a resume candidate on local head-phase read activity or an armed
        // buffered tail, so quiet turns cost zero lookups. Confirm via one phase query: idle
        // resumes in a single round-trip, head is a fresh read needing no transition, anything
        // else (or a stale token) fails closed without retrying into the park.
        match worker.phase(token) {
            Ok(ConnectionPhase::KeepAliveIdle) => {
                if worker.resume_idle(token, now_ns).is_err() {
                    return Some(ConnClose::TransportError);
                }
            }
            Ok(ConnectionPhase::ReadingRequestHead) => {}
            Ok(_) | Err(_) => return Some(ConnClose::TransportError),
        }
    }
    if progress.entered_body {
        let entered = worker.transition_phase(token, ConnectionPhase::ReadingRequestBody, now_ns);
        if entered.is_err() {
            return Some(ConnClose::TransportError);
        }
    }
    if progress.entered_response {
        let entered = worker.transition_phase(token, ConnectionPhase::WritingResponse, now_ns);
        if entered.is_err() {
            return Some(ConnClose::TransportError);
        }
    }
    // NOTE: the park enqueue already succeeded in the pump; this arms the parked
    // (AwaitingHandler) deadline. On failure close_conn unparks the entry, so the host
    // observes Gone rather than a wedged slot.
    if progress.entered_park {
        let entered = worker.transition_phase(token, ConnectionPhase::AwaitingHandler, now_ns);
        if entered.is_err() {
            return Some(ConnClose::TransportError);
        }
    }
    if progress.response_flushed {
        if worker.cancel_deadline(token).is_err() {
            return Some(ConnClose::TransportError);
        }
        if worker.schedule_idle(token, now_ns).is_err() {
            return Some(ConnClose::TransportError);
        }
    }
    None
}

fn close_conn(
    index: usize,
    reason: ConnClose,
    host: &HostShared,
    conns: &mut [Option<ConnState>],
    stream_poller: &mut Poller,
    worker: &mut Worker<'_>,
    counts: &mut CloseCounts,
) {
    let Some(conn) = conns[index].take() else {
        return;
    };
    let token = conn.token();
    let parked = conn.is_parked();
    let mut stream = conn.into_stream();
    let _ = stream_poller.deregister(&mut stream);
    let _ = worker.close(token);
    // NOTE: every parked close unparks exactly once (take_staged already removed staged
    // entries, so this is a no-op for applied responds/cancels): the host tomb records
    // Gone and a later respond can never report success for a dead connection.
    if parked {
        host.unpark_closed(token);
    }
    counts.add(reason);
}

fn cleanup_all(
    host: &HostShared,
    conns: &mut [Option<ConnState>],
    stream_poller: &mut Poller,
    worker: &mut Worker<'_>,
    counts: &mut CloseCounts,
) {
    for index in 0..conns.len() {
        if conns[index].is_some() {
            close_conn(
                index,
                ConnClose::Shutdown,
                host,
                conns,
                stream_poller,
                worker,
                counts,
            );
        }
    }
}

fn work_pending(conns: &[Option<ConnState>]) -> bool {
    conns
        .iter()
        .flatten()
        .any(|conn| conn.has_pending_output() || conn.has_unparsed_input() || conn.read_armed())
}

fn close_idle_for_drain(
    host: &HostShared,
    conns: &mut [Option<ConnState>],
    stream_poller: &mut Poller,
    worker: &mut Worker<'_>,
    counts: &mut CloseCounts,
) {
    for index in 0..conns.len() {
        let idle = conns[index].as_ref().is_some_and(|conn| conn.is_idle());
        if idle {
            close_conn(
                index,
                ConnClose::Shutdown,
                host,
                conns,
                stream_poller,
                worker,
                counts,
            );
        }
    }
}
