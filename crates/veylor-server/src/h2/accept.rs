use veylor_runtime::{PollInterest, Poller, TcpListener, TcpStream};
use veylor_tls::TlsAcceptor;

use super::slots::{H2_STREAM_TOKEN_BASE, H2Meta, H2Slots};
use super::turn::H2C_LISTENER_TOKEN;
use crate::connection::ConnClose;
use crate::static_server::CloseCounts;

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

pub(crate) fn accept_slot(
    slots: &mut H2Slots,
    index: usize,
    accepted: veylor_runtime::AcceptedConnection,
    now_ns: u64,
    head_budget_ns: u64,
    poller: &mut Poller,
    counts: &mut CloseCounts,
) {
    let H2Slots {
        slots: entries,
        slabs,
        ..
    } = slots;
    let Some(entry) = entries.get_mut(index) else {
        counts.add(ConnClose::TransportError);
        return;
    };
    let slot = entry.get_or_insert_with(super::tables::H2Slot::boxed);
    if !open_slot(
        &mut slot.meta,
        accepted.stream,
        index,
        now_ns,
        head_budget_ns,
        poller,
        counts,
    ) {
        *entry = None;
        return;
    }
    // NOTE: prepare installs the pooled engine and decoder slabs, builds the tables on the
    // first pass, then fills the transport slabs; a warm pool makes the whole accept a set of
    // pointer moves.
    if !slot.prepare(slabs, now_ns) {
        abort_slot(&mut slot.meta, poller, counts);
        let (engine, decoder) = slot.retire();
        if let Some(engine) = engine {
            slabs.engine.give(engine);
        }
        if let Some(decoder) = decoder {
            slabs.decoder.give(decoder);
        }
        *entry = None;
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
    let H2Slots {
        slots: entries,
        slabs,
        ..
    } = slots;
    let Some(entry) = entries.get_mut(index) else {
        if let Some(acceptor) = tls_acceptor.as_mut() {
            acceptor.release();
        }
        return false;
    };
    let slot = entry.get_or_insert_with(super::tables::H2Slot::boxed);
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
        *entry = None;
        return false;
    }
    slot.meta.tls = Some(tls);
    let prepared = slot.prepare(slabs, now_ns);
    let moved = prepared
        && slot
            .conn
            .as_mut()
            .is_some_and(|live| live.push_input(prefix).is_ok());
    if !moved {
        abort_tls_slot(&mut slot.meta, poller, counts, tls_acceptor);
        let (engine, decoder) = slot.retire();
        if let Some(engine) = engine {
            slabs.engine.give(engine);
        }
        if let Some(decoder) = decoder {
            slabs.decoder.give(decoder);
        }
        *entry = None;
        return false;
    }
    true
}
