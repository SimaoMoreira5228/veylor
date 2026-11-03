use veylor_runtime::{
    ConnectionTimeouts, ConnectionToken, PollInterest, Poller, TcpListener, TcpStream,
};
use veylor_tls::TlsAcceptor;

use super::flush::{H2Flush, H2Read, flush_h2, read_once};
use super::slots::{
    H2_PENDING_CAP, H2_STREAM_TOKEN_BASE, H2Meta, H2Slots, idle_for_drain, pending_spare,
};
use super::{FrozenNativeRoute, FrozenRoute, FrozenRoutesView, H2Conn, STREAM_SLOTS};
use crate::connection::ConnClose;
use crate::connection::tls::tls_read_decrypted;
use crate::host::{HostShared, StagedWork};
use crate::static_routes::MAX_RESPONSE_BODY;
use crate::static_server::turn::apply_h2_staged;
use crate::static_server::{CloseCounts, H2StaticRoute, MAX_CONNECTIONS};
use veylor_http2::ErrorCode;

// NOTE: spanning driver borrows persist across turns: only same-site overwrites end
// old regions, so workers pre-allocate buffer sets and borrow FIELDS at one inline
// build site per slot; closes visibly clear the conn. 8x32 streams out-multiplex H1.
pub(crate) const H2C_LISTENER_TOKEN: usize = usize::MAX - 1;
pub(crate) const H2_FLUSH_THRESHOLD: usize = 1024;
pub(crate) const MAX_H2_STEPS_PER_TURN: usize = 4;
// NOTE: inbound flood trip-wire: per-connection sliding window over the engine's
// monotonic control-frame counters. Resets are cheap for honest peers (one per
// cancelled stream) and SETTINGS are near-free, so 64/16 per second only trips on
// abuse; the trip fails the connection open with GOAWAY(ENHANCE_YOUR_CALM).
pub(crate) const FLOOD_WINDOW_NS: u64 = 1_000_000_000;
pub(crate) const FLOOD_MAX_RESETS: u32 = 64;
pub(crate) const FLOOD_MAX_SETTINGS: u32 = 16;

pub(crate) struct H2Turn<'a> {
    pub(crate) routes: &'a FrozenRoutesView<'a>,
    pub(crate) host: &'a HostShared,
    pub(crate) worker_id: u32,
    pub(crate) staged: &'a mut Vec<StagedWork>,
    pub(crate) budget: u64,
    pub(crate) now_ns: u64,
    pub(crate) timeouts: ConnectionTimeouts,
    pub(crate) draining: bool,
    pub(crate) linger_due: bool,
    pub(crate) forcing: bool,
    pub(crate) counts: &'a mut CloseCounts,
    pub(crate) tls_acceptor: Option<&'a mut TlsAcceptor>,
}

pub(crate) fn open_slot(
    meta: &mut H2Meta,
    stream: TcpStream,
    index: usize,
    now_ns: u64,
    head_budget_ns: u64,
    stream_poller: &mut Poller,
    counts: &mut CloseCounts,
) -> bool {
    // NOTE: transient only: registers the stream and arms scalars (driver built at call site).
    let token = H2_STREAM_TOKEN_BASE + index;
    let mut owned = stream;
    if stream_poller
        .register(&mut owned, token, PollInterest::Readable)
        .is_err()
    {
        counts.add(ConnClose::TransportError);
        return false;
    }
    *meta = H2Meta {
        stream: Some(owned),
        occupied: true,
        readable: true,
        token,
        deadline_ns: now_ns.saturating_add(head_budget_ns),
        last_progress_ns: now_ns,
        pending: Some(Box::new([0; H2_PENDING_CAP])),
        ..Default::default()
    };
    true
}

pub(crate) fn abort_slot(meta: &mut H2Meta, stream_poller: &mut Poller, counts: &mut CloseCounts) {
    if let Some(mut stream) = meta.stream.take() {
        let _ = stream_poller.deregister(&mut stream);
        let _ = stream.shutdown(std::net::Shutdown::Both);
    }
    meta.occupied = false;
    meta.readable = false;
    counts.add(ConnClose::TransportError);
}

// NOTE: driver-chain construction expands inline once per slot over buffer FIELDS;
// Option temporaries and helper params are rejected; transient helpers stay plain fns.

// NOTE: teardown unparks the conn's full stream-keyed range (empty slots no-op).
fn unpark_conn(host: &HostShared, worker: u32, meta: &H2Meta) {
    let index = meta.token.saturating_sub(H2_STREAM_TOKEN_BASE);
    for sub in 0..STREAM_SLOTS {
        host.unpark_closed(
            worker,
            (MAX_CONNECTIONS + index * STREAM_SLOTS + sub) as u32,
        );
    }
}

pub(crate) fn abort_tls_slot(
    meta: &mut H2Meta,
    stream_poller: &mut Poller,
    counts: &mut CloseCounts,
    tls_acceptor: &mut Option<TlsAcceptor>,
) {
    if meta.tls.take().is_some()
        && let Some(acceptor) = tls_acceptor.as_mut()
    {
        acceptor.release();
    }
    abort_slot(meta, stream_poller, counts);
}

fn shut_slot(meta: &mut H2Meta, poller: &mut Poller, reason: ConnClose, turn: &mut H2Turn<'_>) {
    unpark_conn(turn.host, turn.worker_id, meta);
    if let Some(mut stream) = meta.stream.take() {
        let _ = poller.deregister(&mut stream);
        let _ = stream.shutdown(std::net::Shutdown::Both);
    }
    if meta.tls.take().is_some()
        && let Some(acceptor) = turn.tls_acceptor.as_mut()
    {
        acceptor.release();
    }
    // NOTE: the caller clears the conn visibly; stale tables keep dead borrows only.
    meta.occupied = false;
    meta.readable = false;
    turn.counts.add(reason);
}

// NOTE: sliding-window flood check over the engine's monotonic counters: the first
// sample arms the window, later samples trip when either delta exceeds its ceiling
// inside the window. Window rollover re-arms instead of tripping, so a slow trickle
// never accumulates into a false positive.
fn flood_tripped(meta: &mut H2Meta, conn: Option<&H2Conn<'_>>, now_ns: u64) -> bool {
    let Some(live) = conn else {
        return false;
    };
    let (resets, settings) = live.flood_counters();
    if now_ns.saturating_sub(meta.flood_window_start_ns) >= FLOOD_WINDOW_NS {
        meta.flood_window_start_ns = now_ns;
        meta.flood_rst_base = resets;
        meta.flood_settings_base = settings;
        return false;
    }
    resets.saturating_sub(meta.flood_rst_base) > FLOOD_MAX_RESETS
        || settings.saturating_sub(meta.flood_settings_base) > FLOOD_MAX_SETTINGS
}

// NOTE: polite close (timeouts, drain-idle): stage GOAWAY, flush once, then close.
fn goaway_close(
    meta: &mut H2Meta,
    conn: &mut Option<H2Conn<'_>>,
    stream_poller: &mut Poller,
    reason: ConnClose,
    code: ErrorCode,
    turn: &mut H2Turn<'_>,
) -> Option<ConnClose> {
    if let Some(active) = conn.as_mut().filter(|active| !active.is_closed()) {
        let staged = active.drain_with_code(pending_spare(meta), code);
        meta.pending_len += staged.written;
    }
    let fatal = match flush_h2(meta, stream_poller, &mut turn.budget) {
        H2Flush::Closed(fatal) => fatal,
        H2Flush::Done | H2Flush::Blocked | H2Flush::Retry => reason,
    };
    shut_slot(meta, stream_poller, fatal, turn);
    Some(fatal)
}

pub(crate) fn pump_slot(
    meta: &mut H2Meta,
    conn: &mut Option<H2Conn<'_>>,
    turn: &mut H2Turn<'_>,
    stream_poller: &mut Poller,
    index: usize,
) -> Option<ConnClose> {
    if !meta.occupied {
        return None;
    }
    if conn.is_none() {
        shut_slot(meta, stream_poller, ConnClose::TransportError, turn);
        return Some(ConnClose::TransportError);
    }
    if turn.forcing {
        shut_slot(meta, stream_poller, ConnClose::Shutdown, turn);
        return Some(ConnClose::Shutdown);
    }
    if meta.timed_out {
        return goaway_close(
            meta,
            conn,
            stream_poller,
            ConnClose::Timeout,
            ErrorCode::NoError,
            turn,
        );
    }
    if turn.draining && turn.linger_due && idle_for_drain(meta, conn.as_ref()) {
        return goaway_close(
            meta,
            conn,
            stream_poller,
            ConnClose::Shutdown,
            ErrorCode::NoError,
            turn,
        );
    }
    if meta.error {
        shut_slot(meta, stream_poller, ConnClose::TransportError, turn);
        return Some(ConnClose::TransportError);
    }
    if flood_tripped(meta, conn.as_ref(), turn.now_ns) {
        return goaway_close(
            meta,
            conn,
            stream_poller,
            ConnClose::Abused,
            ErrorCode::EnhanceYourCalm,
            turn,
        );
    }
    let mut close_reason: Option<ConnClose> = None;
    let mut eof = false;
    let mut progressed = apply_h2_staged(meta, conn, turn);
    progressed |= super::apply_h2_streams(meta, conn, turn);
    if let H2Flush::Closed(reason) = flush_h2(meta, stream_poller, &mut turn.budget) {
        close_reason = Some(reason);
    }
    // NOTE: keep reading even while output is pending. A response larger than the
    // connection window makes the peer's WINDOW_UPDATE the only thing that can unblock
    // the flush, so pausing reads here deadlocks any multi-stream file transfer.
    let wants_read = meta.readable && close_reason.is_none();
    if wants_read {
        if meta.tls.is_some() {
            let Some(live) = conn.as_mut() else {
                shut_slot(meta, stream_poller, ConnClose::TransportError, turn);
                return Some(ConnClose::TransportError);
            };
            let room = live.spare_mut().len().min(turn.budget as usize);
            if room > 0 {
                let spare = &mut live.spare_mut()[..room];
                let (Some(stream), Some(tls)) = (meta.stream.as_mut(), meta.tls.as_mut()) else {
                    shut_slot(meta, stream_poller, ConnClose::TransportError, turn);
                    return Some(ConnClose::TransportError);
                };
                match tls_read_decrypted(stream, tls, spare, turn.budget, &mut meta.tls_plain) {
                    Ok((wire, plain, blocked)) => {
                        live.commit_staged(plain);
                        turn.budget = turn.budget.saturating_sub(wire);
                        if blocked {
                            meta.readable = false;
                        }
                        if wire > 0 || plain > 0 {
                            progressed = true;
                        }
                    }
                    Err(reason) => close_reason = Some(reason),
                }
            }
        } else {
            let (Some(live), Some(stream)) = (conn.as_mut(), meta.stream.as_mut()) else {
                shut_slot(meta, stream_poller, ConnClose::TransportError, turn);
                return Some(ConnClose::TransportError);
            };
            let room = live.spare_mut().len().min(turn.budget as usize);
            if room > 0 {
                match read_once(stream, &mut live.spare_mut()[..room]) {
                    H2Read::Progress(read) => {
                        live.commit_staged(read);
                        turn.budget -= read as u64;
                        progressed = true;
                    }
                    H2Read::Blocked => meta.readable = false,
                    H2Read::Eof => eof = true,
                    H2Read::Closed(reason) => close_reason = Some(reason),
                }
            }
        }
    }
    let base = ConnectionToken {
        worker_id: turn.worker_id,
        slot: (MAX_CONNECTIONS + index * STREAM_SLOTS) as u32,
        worker_generation: 1,
        generation: 1,
        ..Default::default()
    };
    let mut steps = 0;
    while close_reason.is_none() && !eof && steps < MAX_H2_STEPS_PER_TURN && turn.budget > 0 {
        if pending_spare(meta).len() < H2_FLUSH_THRESHOLD && meta.pending_unflushed() {
            if let H2Flush::Closed(reason) = flush_h2(meta, stream_poller, &mut turn.budget) {
                close_reason = Some(reason);
                break;
            }
            if pending_spare(meta).len() < H2_FLUSH_THRESHOLD {
                break;
            }
        }
        let Some(live) = conn.as_mut() else {
            close_reason = Some(ConnClose::TransportError);
            break;
        };
        let staged_before = live.staged_len();
        let ctx = super::H2HostCtx {
            host: turn.host,
            base,
        };
        let outcome = live.step_host_timed(
            pending_spare(meta),
            turn.routes,
            ctx,
            turn.now_ns,
            turn.timeouts.response_write_ns,
        );
        turn.counts.h2_served += u64::from(outcome.served);
        turn.counts.h2_refused += u64::from(outcome.refused);
        meta.pending_len += outcome.written;
        let step_progressed = outcome.written > 0
            || outcome.served > 0
            || outcome.refused > 0
            || live.staged_len() != staged_before;
        progressed |= step_progressed;
        if outcome.closed {
            // NOTE: closed+goaway comes only from fail paths; bare close with output/refusal
            // is the post-refusal policy close, bare close with neither is peer going away.
            if outcome.goaway {
                close_reason = Some(ConnClose::ProtocolError);
            } else if outcome.refused > 0 || outcome.written > 0 {
                close_reason = Some(ConnClose::Completed);
            } else {
                close_reason = Some(ConnClose::PeerClosed);
            }
            break;
        }
        steps += 1;
        if !outcome.pending || !step_progressed {
            break;
        }
    }
    // NOTE: closed means flush-then-close: staged bytes go best-effort before the FIN.
    // Flush failure reports transport truth; a blocked remainder drops with the slot.
    if (meta.pending_unflushed() || meta.tls_unflushed())
        && let H2Flush::Closed(fatal) = flush_h2(meta, stream_poller, &mut turn.budget)
    {
        close_reason = Some(fatal);
    }
    if eof && close_reason.is_none() {
        close_reason = Some(ConnClose::PeerClosed);
    }
    if progressed {
        meta.last_progress_ns = turn.now_ns;
        meta.deadline_ns = turn.now_ns.saturating_add(turn.timeouts.keep_alive_idle_ns);
    }
    if !meta.serving {
        meta.serving = conn.as_ref().is_some_and(|live| live.handshake_complete());
    }
    meta.staged = conn.as_ref().is_some_and(|live| live.staged_len() > 0);
    if let Some(reason) = close_reason {
        shut_slot(meta, stream_poller, reason, turn);
        return Some(reason);
    }
    // NOTE: terminal without a reason is unreachable; fail closed, never wedge the slot.
    if conn.as_ref().is_some_and(|live| live.is_closed()) {
        shut_slot(meta, stream_poller, ConnClose::Completed, turn);
        return Some(ConnClose::Completed);
    }
    None
}

// NOTE: fatal-path abandon (worker exiting): streams drop, slots count Shutdown.
pub(crate) fn abandon_slots(
    slots: &mut H2Slots,
    host: &HostShared,
    worker: u32,
    counts: &mut CloseCounts,
    tls_acceptor: &mut Option<TlsAcceptor>,
) {
    for index in 0..slots.capacity() {
        let Some(slot) = slots.slot_mut(index) else {
            continue;
        };
        if slot.meta.occupied {
            unpark_conn(host, worker, &slot.meta);
            slot.meta.occupied = false;
            slot.meta.stream.take();
            if slot.meta.tls.take().is_some()
                && let Some(acceptor) = tls_acceptor.as_mut()
            {
                acceptor.release();
            }
            counts.add(ConnClose::Shutdown);
        }
    }
    for index in 0..slots.capacity() {
        slots.close(index);
    }
}

pub(crate) fn restart_h2_drain(
    drain_start_ns: &mut u64,
    h2_force_ns: &mut u64,
    now_ns: u64,
    shutdown_grace_ns: u64,
) {
    // NOTE: the shutdown counter tracks H1 slots only: restart the linger clock here so
    // h2c conns still get their GOAWAY drain (bounded by the same grace).
    *drain_start_ns = now_ns;
    *h2_force_ns = now_ns.saturating_add(shutdown_grace_ns);
}

pub(crate) fn register_listener(
    worker_poller: &mut Poller,
    h2c_listener: &mut Option<TcpListener>,
) -> Result<(), &'static str> {
    // NOTE: h2c readiness rides the worker poller under its own token, so h2c accepts
    // share the turn's event budget; Worker::turn ignores the token.
    if let Some(bound) = h2c_listener.as_mut() {
        worker_poller
            .register(bound, H2C_LISTENER_TOKEN, PollInterest::Readable)
            .map_err(|_| "h2c listener register failed")?;
    }
    Ok(())
}

pub(crate) fn project_routes<'a>(
    h2_routes: &'a [H2StaticRoute],
) -> (Vec<FrozenRoute<'a>>, Vec<FrozenNativeRoute<'a>>) {
    // NOTE: the h2c route projection is built once per worker from the owned mirror;
    // only status eligibility (200..=599 except 204) filters here. Natives skip the
    // status/body filter (no stored response) and project alongside, never inside.
    let flat = h2_routes
        .iter()
        .filter(|route| {
            route.native.is_none()
                && route.status != 204
                && (200..=599).contains(&route.status)
                && route.body.len() <= MAX_RESPONSE_BODY
        })
        .map(|route| FrozenRoute {
            method: &route.method,
            path: &route.path,
            token: route.token,
            status: route.status,
            body: &route.body,
            gzip_body: route.gzip_body.as_deref(),
            managed: route.managed,
            max_body_bytes: route.max_body_bytes,
            file_root: route.file_root.as_deref(),
            trailers: &route.trailers,
        })
        .collect();
    let natives = h2_routes
        .iter()
        .filter_map(|route| {
            route
                .native
                .map(|(handler, user_data, gzip)| FrozenNativeRoute {
                    method: &route.method,
                    path: &route.path,
                    handler,
                    user_data,
                    gzip,
                })
        })
        .collect();
    (flat, natives)
}
// NOTE: single accept-arm definition, invoked once per slot index below.
pub(crate) fn accept_slot(
    slots: &mut H2Slots,
    index: usize,
    accepted: veylor_runtime::AcceptedConnection,
    now_ns: u64,
    head_budget_ns: u64,
    poller: &mut Poller,
    counts: &mut CloseCounts,
) {
    let Some(slot) = slots.ensure(index) else {
        counts.add(ConnClose::TransportError);
        return;
    };
    if !open_slot(
        &mut slot.meta,
        accepted.stream,
        index,
        now_ns,
        head_budget_ns,
        poller,
        counts,
    ) {
        slots.close(index);
        return;
    }
    if !slot.open() {
        abort_slot(&mut slot.meta, poller, counts);
        slots.close(index);
    }
}

// NOTE: per-turn h2c accept dispatch. h2c shares the H1 accept budget; on an H1-error
// turn the budget is unknown, so h2c skips (fail closed).
#[allow(clippy::too_many_arguments)]
pub(crate) fn drive_accepts(
    slots: &mut H2Slots,
    listener: Option<&mut veylor_runtime::TcpListener>,
    enabled: bool,
    draining: bool,
    budget: usize,
    now_ns: u64,
    head_budget_ns: u64,
    poller: &mut Poller,
    counts: &mut CloseCounts,
    staging: &mut [Option<veylor_runtime::AcceptedConnection>],
) {
    if !(enabled && !draining && budget > 0) {
        return;
    }
    let Some(bound) = listener else {
        return;
    };
    // NOTE: one nonblocking accept per turn; WouldBlock short-circuits empty.
    if veylor_runtime::accept_ready(bound, staging, budget).is_err() {
        return;
    }
    for entry in staging.iter_mut() {
        let Some(accepted) = entry.take() else {
            continue;
        };
        let Some(index) = slots.free_slot() else {
            counts.add(ConnClose::TransportError);
            continue;
        };
        accept_slot(
            slots,
            index,
            accepted,
            now_ns,
            head_budget_ns,
            poller,
            counts,
        );
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn move_tls_slot(
    slots: &mut H2Slots,
    index: usize,
    stream: veylor_runtime::TcpStream,
    tls: Box<veylor_tls::TlsConn>,
    prefix: &[u8],
    now_ns: u64,
    head_budget_ns: u64,
    poller: &mut Poller,
    counts: &mut CloseCounts,
    tls_acceptor: &mut Option<TlsAcceptor>,
) -> bool {
    let Some(slot) = slots.ensure(index) else {
        if let Some(acceptor) = tls_acceptor.as_mut() {
            acceptor.release();
        }
        return false;
    };
    if !open_slot(
        &mut slot.meta,
        stream,
        index,
        now_ns,
        head_budget_ns,
        poller,
        counts,
    ) {
        if let Some(acceptor) = tls_acceptor.as_mut() {
            acceptor.release();
        }
        slots.close(index);
        return false;
    }
    slot.meta.tls = Some(tls);
    let moved = slot.open()
        && slot
            .conn
            .as_mut()
            .is_some_and(|live| live.push_input(prefix).is_ok());
    if !moved {
        abort_tls_slot(&mut slot.meta, poller, counts, tls_acceptor);
        slots.close(index);
    }
    moved
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn drive_pumps(
    slots: &mut H2Slots,
    view: Option<&FrozenRoutesView<'_>>,
    host: &HostShared,
    worker_id: u32,
    staged: &mut Vec<StagedWork>,
    budget: u64,
    now_ns: u64,
    timeouts: ConnectionTimeouts,
    draining: bool,
    linger_due: bool,
    forcing: bool,
    counts: &mut CloseCounts,
    tls_acceptor: Option<&mut TlsAcceptor>,
    poller: &mut Poller,
) {
    let Some(view) = view else {
        return;
    };
    slots.sweep_deadlines(now_ns, timeouts);
    let mut turn = H2Turn {
        routes: view,
        host,
        worker_id,
        staged,
        budget: 0,
        now_ns,
        timeouts,
        draining,
        linger_due,
        forcing,
        counts,
        tls_acceptor,
    };
    for index in 0..slots.capacity() {
        turn.budget = budget;
        let mut retire = false;
        if let Some(slot) = slots.slot_mut(index) {
            if slot.meta.occupied {
                let _ = pump_slot(&mut slot.meta, &mut slot.conn, &mut turn, poller, index);
                if let Some(conn) = slot.conn.as_mut() {
                    conn.compact_decoder();
                }
                retire = !slot.meta.occupied;
            } else {
                retire = true;
            }
        }
        if retire {
            slots.close(index);
        }
    }
}
