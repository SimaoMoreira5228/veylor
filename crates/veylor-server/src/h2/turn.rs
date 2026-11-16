use veylor_runtime::{ConnectionTimeouts, ConnectionToken, Poller};
use veylor_tls::TlsAcceptor;

pub(crate) use super::accept::{drive_accepts, move_tls_slot, register_listener};
use super::flush::{H2Flush, H2Read, flush_h2, read_once};

use super::slots::{
    H2_STREAM_TOKEN_BASE, H2Meta, H2Slabs, H2Slot, H2Slots, idle_for_drain, pending_spare,
};
use super::{FrozenNativeRoute, FrozenRoute, FrozenRoutesView, H2Conn, STREAM_SLOTS};
use crate::connection::ConnClose;
use crate::connection::tls::tls_read_decrypted;
use crate::host::{HostShared, StagedWork};
use crate::static_routes::MAX_RESPONSE_BODY;
use crate::static_server::apply_h2_staged;
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
    meta.defer_write = false;
    let fatal = match flush_h2(meta, stream_poller, &mut turn.budget) {
        H2Flush::Closed(fatal) => fatal,
        H2Flush::Done | H2Flush::Blocked | H2Flush::Retry => reason,
    };
    shut_slot(meta, stream_poller, fatal, turn);
    Some(fatal)
}

// NOTE: a slot-scoped index into the turn's staged work: a host response that arrives for a
// connection whose slabs are in the pool must take them before `apply_h2_staged` writes.
impl H2Turn<'_> {
    fn staged_for(&self, index: usize) -> bool {
        let base = MAX_CONNECTIONS + index * STREAM_SLOTS;
        self.staged
            .iter()
            .any(|work| work.slot >= base && work.slot < base + STREAM_SLOTS)
    }
}

pub(crate) fn pump_slot(
    meta: &mut H2Meta,
    conn: &mut Option<H2Conn<'_>>,
    slabs: &mut H2Slabs,
    turn: &mut H2Turn<'_>,
    stream_poller: &mut Poller,
    index: usize,
) -> Option<ConnClose> {
    if !meta.occupied {
        return None;
    }
    let batched_read = std::mem::take(&mut meta.batched_read);
    let mut eof = std::mem::take(&mut meta.eof);
    if conn.is_none() {
        shut_slot(meta, stream_poller, ConnClose::TransportError, turn);
        return Some(ConnClose::TransportError);
    }
    if turn.forcing {
        shut_slot(meta, stream_poller, ConnClose::Shutdown, turn);
        return Some(ConnClose::Shutdown);
    }
    // NOTE: a connection that just woke up after giving its slabs back takes them here, before
    // the staged application, the flush or the read can write through them.
    let staged_here = turn.staged_for(index);
    let wants = staged_here || meta.wants_pump() || meta.timed_out || turn.draining;
    let missing = meta.pending.is_none() || conn.as_ref().is_some_and(|live| !live.holds_slabs());
    if wants && missing {
        slabs.adopt(meta, conn, turn.now_ns);
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
    turn.budget = turn.budget.saturating_sub(batched_read as u64);
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
                        // NOTE: TLS never joins the batch pass, so this is the claim's only
                        // consumer; leaving it set would keep the slabs from ever releasing.
                        meta.claimed = false;
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
                        meta.claimed = false;
                        live.commit_staged(read);
                        turn.budget -= read as u64;
                        progressed = true;
                    }
                    H2Read::Blocked => {
                        meta.claimed = false;
                        meta.readable = false;
                    }
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
    if meta.pending_unflushed() || meta.tls_unflushed() {
        if close_reason.is_some() {
            meta.defer_write = false;
        }
        if let H2Flush::Closed(fatal) = flush_h2(meta, stream_poller, &mut turn.budget) {
            close_reason = Some(fatal);
        }
    }
    if eof && close_reason.is_none() {
        close_reason = Some(ConnClose::PeerClosed);
    }
    if progressed {
        meta.note_progress(turn.now_ns);
        meta.deadline_ns = turn.now_ns.saturating_add(turn.timeouts.keep_alive_idle_ns);
    } else {
        meta.note_quiet();
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
        if meta.pending_unflushed() || meta.tls_unflushed() {
            meta.defer_write = false;
            let _ = flush_h2(meta, stream_poller, &mut turn.budget);
        }
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
    // NOTE: slabs come back before the recv batch and the pump, because both write through
    // them; the pass is bounded by the connection ceiling and costs nothing when warm.
    slots.adopt_ready(now_ns);
    let batched = slots.batch_available();
    slots.recv_ready(poller, budget);
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
    {
        let entries = &mut slots.slots;
        let slabs = &mut slots.slabs;
        for index in 0..entries.len() {
            turn.budget = budget;
            let mut retire = false;
            if let Some(slot) = entries
                .get_mut(index)
                .and_then(|entry| entry.as_deref_mut())
            {
                if slot.meta.occupied {
                    slot.meta.defer_write = batched;
                    let H2Slot { meta, conn, .. } = &mut *slot;
                    let _ = pump_slot(meta, conn, slabs, &mut turn, poller, index);
                    meta.defer_write = false;
                    if let Some(live) = conn.as_mut() {
                        live.compact_decoder();
                    }
                    retire = !meta.occupied;
                } else {
                    retire = true;
                }
            }
            if retire && let Some(entry) = entries.get_mut(index) {
                *entry = None;
            }
        }
    }
    if batched && slots.send_deferred(poller, budget) {
        // NOTE: a deferred write that failed surfaces here, after the pump already left
        // the slot; close it on the same account the direct flush would have used.
        for index in 0..slots.capacity() {
            let failed = slots
                .slot_mut(index)
                .is_some_and(|slot| slot.meta.occupied && slot.meta.error);
            if !failed {
                continue;
            }
            if let Some(slot) = slots.slot_mut(index) {
                shut_slot(&mut slot.meta, poller, ConnClose::TransportError, &mut turn);
            }
            slots.close(index);
        }
    }
    if !draining {
        slots.reclaim_idle(now_ns);
    }
}
