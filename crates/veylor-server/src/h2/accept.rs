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
    // NOTE: an accept proves a connection, not queued bytes. The registration above reports
    // the readiness edge once bytes are waiting, and that edge is what lets the batch read
    // without arming io_uring's own poll, so the slot takes no read claim here.
    *meta = H2Meta {
        stream: Some(owned),
        occupied: true,
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

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::net::{TcpListener as StdListener, TcpStream as StdStream};
    use std::time::Duration;

    use veylor_runtime::AcceptedConnection;

    use super::*;

    const CERT_DER: &[u8] = include_bytes!("../../../veylor-tls/tests/testdata/server_cert.der");
    const KEY_DER: &[u8] = include_bytes!("../../../veylor-tls/tests/testdata/server_key.der");
    const HEAD_BUDGET_NS: u64 = 5_000_000_000;

    fn rig() -> (AcceptedConnection, StdStream) {
        let listener = StdListener::bind("127.0.0.1:0").expect("bind rig");
        let addr = listener.local_addr().expect("rig addr");
        let client = StdStream::connect(addr).expect("rig connect");
        let (accepted, peer) = listener.accept().expect("rig accept");
        accepted.set_nonblocking(true).expect("nonblocking");
        (
            AcceptedConnection {
                stream: TcpStream::from_std(accepted),
                peer,
            },
            client,
        )
    }

    fn readable_edge(slots: &mut H2Slots, poller: &mut Poller, token: usize) {
        let mut polled = poller
            .poll(Some(Duration::from_millis(200)))
            .expect("poll rig");
        let edge = polled
            .find(|event| event.token == token)
            .expect("accepted socket reports its queued bytes");
        assert!(edge.readable);
        assert!(slots.note_readiness(&edge));
    }

    #[test]
    fn an_accept_waits_for_its_edge_before_it_claims_a_read() {
        let mut poller = Poller::new(4).expect("poller");
        let mut slots = H2Slots::new();
        let mut counts = CloseCounts::default();
        let (accepted, mut client) = rig();
        accept_slot(
            &mut slots,
            0,
            accepted,
            0,
            HEAD_BUDGET_NS,
            &mut poller,
            &mut counts,
        );
        assert!(slots.batch_available(), "rig needs a working batch ring");

        let slot = slots.slot_mut(0).expect("slot");
        assert!(!slot.meta.readable, "an accept proves no queued bytes");
        assert!(!slot.meta.claimed, "an accept arms no read claim");

        let preface = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
        client.write_all(preface).expect("write preface");
        slots.recv_ready(&mut poller, 1024);
        assert_eq!(
            slots.slot_mut(0).expect("slot").meta.batched_read,
            0,
            "an unclaimed slot is not read by the batch"
        );

        readable_edge(&mut slots, &mut poller, H2_STREAM_TOKEN_BASE);
        let slot = slots.slot_mut(0).expect("slot");
        assert!(slot.meta.claimed, "the edge is the read claim");

        slots.recv_ready(&mut poller, 1024);
        let slot = slots.slot_mut(0).expect("slot");
        assert_eq!(slot.meta.batched_read, preface.len());
        assert_eq!(
            slot.conn.as_ref().expect("conn").staged_len(),
            preface.len()
        );
        assert!(!slot.meta.readable);
        assert!(!slot.meta.claimed);
    }

    #[test]
    fn a_tls_slot_moved_from_h1_starts_without_a_read_claim() {
        let mut acceptor = TlsAcceptor::new(CERT_DER, KEY_DER, 1).expect("acceptor builds");
        let mut poller = Poller::new(4).expect("poller");
        let mut slots = H2Slots::new();
        let mut counts = CloseCounts::default();
        let (accepted, mut client) = rig();
        let tls = acceptor.admit().expect("tls slot available");
        let mut accepter = Some(acceptor);

        assert!(move_tls_slot(
            &mut slots,
            0,
            accepted.stream,
            Box::new(tls),
            &[],
            0,
            HEAD_BUDGET_NS,
            &mut poller,
            &mut counts,
            &mut accepter,
        ));
        assert!(slots.batch_available(), "rig needs a working batch ring");

        let slot = slots.slot_mut(0).expect("slot");
        assert!(!slot.meta.readable);
        assert!(!slot.meta.claimed);
        assert!(
            slot.meta.tls_cipher.is_some(),
            "the ciphertext slab is installed at the move"
        );
        assert!(slot.meta.pending.is_some());

        client.write_all(b"ciphertext").expect("write ciphertext");
        readable_edge(&mut slots, &mut poller, H2_STREAM_TOKEN_BASE);
        slots.recv_ready(&mut poller, 1024);
        let slot = slots.slot_mut(0).expect("slot");
        assert_eq!(slot.meta.tls_cipher_read, b"ciphertext".len());
        assert_eq!(slot.meta.tls_cipher_fed, 0);
        assert_eq!(slot.meta.batched_read, b"ciphertext".len());
        assert!(!slot.meta.claimed);
    }
}
