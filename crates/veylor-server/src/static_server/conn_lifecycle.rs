use std::io::{Read as _, Write as _};

use veylor_runtime::{
    AdmittedConnection, ConnectionDeadline, ConnectionDeadlineKind, ConnectionPhase,
    ConnectionToken, PollInterest, Poller, Worker,
};

use crate::connection::{CLOSE_DRAIN_BYTES, ConnClose, ConnPhase, ConnState, PumpProgress};
use crate::host::HostShared;

use super::CloseCounts;
use super::turn::{SENTINEL_SLOT, conn_poll_token};

pub(super) fn drain_admitted(
    admitted: &mut [Option<AdmittedConnection>],
    conns: &mut [Option<ConnState>],
    stream_poller: &mut Poller,
    worker: &mut Worker<'_>,
    counts: &mut CloseCounts,
    tls_acceptor: &mut Option<veylor_tls::TlsAcceptor>,
) {
    for entry in admitted.iter_mut() {
        let Some(admitted_conn) = entry.take() else {
            continue;
        };
        let slot = admitted_conn.token.slot as usize;
        let mut registered = false;
        if slot < conns.len() && conns[slot].is_none() {
            let pending_tls = match tls_acceptor.as_mut() {
                None => None,
                Some(acceptor) => match acceptor.admit() {
                    Ok(tls_conn) => Some(tls_conn),
                    Err(_) => {
                        let _ = worker.close(admitted_conn.token);
                        counts.add(ConnClose::TransportError);
                        continue;
                    }
                },
            };
            let mut stream = admitted_conn.stream;
            if stream_poller
                .register(&mut stream, conn_poll_token(slot), PollInterest::Readable)
                .is_ok()
            {
                let mut state = ConnState::new(admitted_conn.token, stream);

                if let Some(tls_conn) = pending_tls {
                    state.attach_tls(tls_conn);
                }

                conns[slot] = Some(state);
                registered = true;
            } else {
                if pending_tls.is_some()
                    && let Some(acceptor) = tls_acceptor.as_mut()
                {
                    acceptor.release();
                }

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

pub(super) fn drain_expired(
    expired: &[ConnectionDeadline],
    host: &HostShared,
    conns: &mut [Option<ConnState>],
    stream_poller: &mut Poller,
    worker: &mut Worker<'_>,
    counts: &mut CloseCounts,
    tls_acceptor: &mut Option<veylor_tls::TlsAcceptor>,
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
    }
}

// NOTE: hook failures mean the worker table diverged from connection state; a
// failed transition already parks the slot in Closing, so the server closes the
// connection instead of running it deadline-less or retrying into the park.
pub(super) fn apply_pump_hooks(
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
    if progress.tunnel_upgraded {
        let entered = worker.transition_phase(token, ConnectionPhase::Tunnel, now_ns);
        if entered.is_err() {
            return Some(ConnClose::TransportError);
        }
    }
    if progress.tunnel_draining {
        let entered = worker.transition_phase(token, ConnectionPhase::TunnelDraining, now_ns);
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

#[allow(clippy::too_many_arguments)]
pub(super) fn close_conn(
    index: usize,
    reason: ConnClose,
    host: &HostShared,
    conns: &mut [Option<ConnState>],
    stream_poller: &mut Poller,
    worker: &mut Worker<'_>,
    counts: &mut CloseCounts,
    tls_acceptor: &mut Option<veylor_tls::TlsAcceptor>,
) {
    let Some(mut conn) = conns[index].take() else {
        return;
    };
    let token = conn.token();
    let parked = conn.is_parked();
    let tunnel = conn.phase == ConnPhase::Tunnel;
    let tunnel_token = conn.tunnel_token;
    let tunnel_notified = conn.tunnel_notified;
    let had_tls = conn.has_tls();
    if had_tls && let Some(tls) = conn.tls.as_mut() {
        let mut flight = Vec::with_capacity(64);
        if veylor_tls::queue_close_notify(tls, &mut flight).is_ok() {
            let mut sent = 0;
            while sent < flight.len() {
                match conn.stream.write(&flight[sent..]) {
                    Ok(0) => break,
                    Ok(wrote) => sent += wrote,
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(_) => break,
                }
            }
        }
    }
    if !had_tls && conn.close_drain_remaining != 0 {
        let mut scratch = [0u8; 4096];
        let mut budget = conn.close_drain_remaining.min(CLOSE_DRAIN_BYTES);
        while budget > 0 {
            match conn.stream.read(&mut scratch) {
                Ok(0) => break,
                Ok(count) => budget = budget.saturating_sub(count as u64),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(_) => break,
            }
        }
    }
    let mut stream = conn.into_stream();
    let _ = stream_poller.deregister(&mut stream);
    let _ = worker.close(token);
    if had_tls && let Some(acceptor) = tls_acceptor.as_mut() {
        acceptor.release();
    }
    // NOTE: every parked close unparks exactly once (take_staged already removed staged
    // entries, so this is a no-op for applied responds/cancels): the host tomb records
    // Gone and a later respond can never report success for a dead connection.
    if tunnel {
        if !tunnel_notified {
            let _ = host.stage_tunnel_closed(tunnel_token);
        }
        host.unpark_closed(tunnel_token.worker_id, tunnel_token.slot);
    } else if parked {
        host.unpark_closed(token.worker_id, token.slot);
    }
    counts.add(reason);
}

pub(super) fn cleanup_all(
    host: &HostShared,
    conns: &mut [Option<ConnState>],
    stream_poller: &mut Poller,
    worker: &mut Worker<'_>,
    counts: &mut CloseCounts,
    tls_acceptor: &mut Option<veylor_tls::TlsAcceptor>,
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
                tls_acceptor,
            );
        }
    }
}

pub(super) fn close_idle_for_drain(
    host: &HostShared,
    conns: &mut [Option<ConnState>],
    stream_poller: &mut Poller,
    worker: &mut Worker<'_>,
    counts: &mut CloseCounts,
    tls_acceptor: &mut Option<veylor_tls::TlsAcceptor>,
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
                tls_acceptor,
            );
        }
    }
}
