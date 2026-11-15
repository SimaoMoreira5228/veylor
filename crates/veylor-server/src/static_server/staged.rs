use veylor_runtime::{ConnectionPhase, ConnectionToken, Poller, Worker};

use crate::connection::{ConnClose, ConnState};
use crate::h2::H2Conn;
use crate::h2::slots as h2_slots;
use crate::h2::turn as h2_turn;
use crate::host::{HostShared, StagedWork};

use super::conn_lifecycle::close_conn;
use super::{CloseCounts, MAX_CONNECTIONS};

// NOTE: a staged validation failure drops the work without touching the slot: the entry
// is already tombed, and the host got its outcome at stage time. Single-threaded worker
// order makes this unreachable except under table divergence, so fail closed silently.
#[allow(clippy::too_many_arguments)]
pub(super) fn drain_host_staged(
    host: &HostShared,
    staged: Vec<StagedWork>,
    conns: &mut [Option<ConnState>],
    stream_poller: &mut Poller,
    worker: &mut Worker<'_>,
    counts: &mut CloseCounts,
    now_ns: u64,
    tls_acceptor: &mut Option<veylor_tls::TlsAcceptor>,
) {
    for work in staged {
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
                tls_acceptor,
            );
            continue;
        }
        let encoded = conns
            .get_mut(work.slot)
            .and_then(|entry| entry.as_mut())
            .map(|conn| conn.complete_parked(work.status, &work.reason, &work.body, &work.headers));
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
                        tls_acceptor,
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
                    tls_acceptor,
                );
            }
            None => {}
        }
    }
}

// NOTE: H2 staged hook: validate-then-complete per stream-keyed work item, fail closed
// silently on divergence (entries arrive tombed and the host already has its outcome).
pub(crate) fn apply_h2_staged(
    meta: &mut h2_slots::H2Meta,
    conn: &mut Option<H2Conn<'_>>,
    turn: &mut h2_turn::H2Turn<'_>,
) -> bool {
    if !meta.occupied {
        return false;
    }
    let index = meta.token.saturating_sub(h2_slots::H2_STREAM_TOKEN_BASE);
    // NOTE: H2 base mirrors the h2/host slice arithmetic, which keys H2 slots from the
    // const ceiling; max_conns never moves this line, so H1/H2 slot ids cannot collide.
    let base = MAX_CONNECTIONS + index * crate::h2::STREAM_SLOTS;
    let mut slot = 0;
    let mut applied = false;
    while slot < turn.staged.len() {
        if turn.staged[slot].slot < base || turn.staged[slot].slot >= base + crate::h2::STREAM_SLOTS
        {
            slot += 1;
            continue;
        }
        let work = turn.staged.remove(slot);
        let sub = work.slot - base;
        let stream = work.token.stream_id;
        let expect = ConnectionToken {
            worker_id: turn.worker_id,
            slot: work.slot as u32,
            request_seq: 0,
            stream_id: stream,
            worker_generation: 1,
            generation: 1,
        };
        if work.token != expect || (stream >> 1) as usize % crate::h2::STREAM_SLOTS != sub {
            continue;
        }
        let Some(live) = conn.as_mut().filter(|active| !active.is_closed()) else {
            continue;
        };
        let was_cancel = work.cancel;
        let outcome = live.apply_staged(h2_slots::pending_spare(meta), work);
        meta.pending_len += outcome.written;
        turn.counts.h2_served += u64::from(outcome.served);
        turn.counts.h2_refused += u64::from(outcome.refused);
        if was_cancel {
            turn.counts.h2_refused += 1;
        }
        applied = true;
    }
    applied
}
