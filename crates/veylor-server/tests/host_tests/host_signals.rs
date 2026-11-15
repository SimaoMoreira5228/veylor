use crate::host_peer::*;
use std::io::Write;
use std::time::Instant;
use veylor_runtime::ConnectionToken;
use veylor_server::{
    CancelOutcome, EventSlot, HostEventKind, HostHandle, HostShared, RespondOutcome,
};

fn unit_token(slot: u32) -> ConnectionToken {
    ConnectionToken {
        worker_id: 0,
        slot,
        request_seq: 0,
        stream_id: 0,
        worker_generation: 1,
        generation: 1,
    }
}

fn unit_park(shared: &HostShared, slot: u32, total: u64) -> ConnectionToken {
    shared
        .try_park(0, unit_token(slot), 7, b"POST", b"/upload", total, &[])
        .expect("unit park stages request")
}

fn unit_poll(shared: &HostShared) -> Vec<EventSlot> {
    let mut events = vec![empty_slot(); 8];
    let mut methods = vec![0; 64];
    let mut paths = vec![0; 4096];
    let report = shared.poll(0, &mut events, &mut methods, &mut paths);
    events.truncate(report.copied);
    events
}

#[test]
fn host_body_available_staged_at_admission_coalesced_latest_wins() {
    let shared = HostShared::new(1);
    let token = unit_park(&shared, 3, 10);
    assert!(
        shared.push_body(token, b"abc"),
        "first admission stages bytes and signal"
    );
    assert!(
        shared.push_body(token, b"defg"),
        "second admission coalesces onto the pending signal"
    );
    let events = unit_poll(&shared);
    assert_eq!(
        events.len(),
        2,
        "one request plus one coalesced signal: {events:?}"
    );
    assert_eq!(events[0].kind, HostEventKind::Request);
    assert_eq!(events[1].kind, HostEventKind::BodyAvailable);
    assert_eq!(events[1].token, token);
    assert_eq!(events[1].route_id, 0);
    assert_eq!(
        events[1].method_len, 7,
        "latest-wins counts every staged byte"
    );
    assert_eq!(
        events[1].path_len, 0,
        "partial window never claims completion"
    );
    assert_eq!((events[1].method_offset, events[1].path_offset), (0, 0));
    let mut pulled = [0; 16];
    assert_eq!(
        shared.pull_body(token, &mut pulled),
        (RespondOutcome::Ok, 7, false)
    );
    assert!(shared.push_body(token, b"hij"), "tail admission stages");
    let events = unit_poll(&shared);
    assert_eq!(events.len(), 1, "tail stages exactly one terminal signal");
    assert_eq!(events[0].kind, HostEventKind::BodyAvailable);
    assert_eq!(events[0].method_len, 3);
    assert_eq!(events[0].path_len, 1, "pulled plus staged covers the total");
}

#[test]
fn host_body_available_full_queue_refuses_admission_fail_closed() {
    let shared = HostShared::new(1);
    let mut tokens = Vec::new();
    for slot in 0..64 {
        tokens.push(unit_park(&shared, slot, 64));
    }
    assert_eq!(shared.counters().queue_full, 0);
    assert!(
        !shared.push_body(tokens[0], b"x"),
        "signal with no coalesce target and no room refuses the bytes too"
    );
    assert_eq!(shared.counters().queue_full, 1);
    let mut scratch = [0; 8];
    assert_eq!(
        shared.pull_body(tokens[0], &mut scratch),
        (RespondOutcome::Ok, 0, false),
        "refused bytes never stage"
    );
    let mut events = vec![empty_slot(); 1];
    let mut methods = vec![0; 64];
    let mut paths = vec![0; 4096];
    let report = shared.poll(0, &mut events, &mut methods, &mut paths);
    assert_eq!(report.copied, 1, "one drain frees exactly one room");
    assert!(
        shared.push_body(tokens[0], b"x"),
        "retry after drain stages"
    );
    let mut events = vec![empty_slot(); 128];
    let mut methods = vec![0; 2048];
    let mut paths = vec![0; 4096];
    let report = shared.poll(0, &mut events, &mut methods, &mut paths);
    let signals = events[..report.copied]
        .iter()
        .filter(|event| event.kind == HostEventKind::BodyAvailable)
        .count();
    assert_eq!(signals, 1);
}

#[test]
fn host_body_available_retired_on_respond_and_close() {
    let shared = HostShared::new(1);
    let token = unit_park(&shared, 3, 5);
    assert!(shared.push_body(token, b"hello"));
    let mut events = vec![empty_slot(); 1];
    let mut methods = vec![0; 64];
    let mut paths = vec![0; 4096];
    let report = shared.poll(0, &mut events, &mut methods, &mut paths);
    assert_eq!(report.copied, 1);
    assert_eq!(events[0].kind, HostEventKind::Request);
    assert_eq!(
        shared.stage_respond(token, 200, b"OK", &[], b"x"),
        RespondOutcome::Ok
    );
    assert!(
        unit_poll(&shared).is_empty(),
        "respond retires the pending signal"
    );
    let token = unit_park(&shared, 5, 5);
    assert!(shared.push_body(token, b"hello"));
    let mut events = vec![empty_slot(); 1];
    let mut methods = vec![0; 64];
    let mut paths = vec![0; 4096];
    let report = shared.poll(0, &mut events, &mut methods, &mut paths);
    assert_eq!(report.copied, 1);
    assert_eq!(events[0].kind, HostEventKind::Request);
    assert!(shared.unpark_closed(0, 5), "close unparks");
    assert!(
        unit_poll(&shared).is_empty(),
        "close retires the pending signal"
    );
}

#[test]
fn host_stage_writable_helper_coalesces_and_maps() {
    let shared = HostShared::new(1);
    let token = unit_park(&shared, 3, 0);
    assert_eq!(
        shared.begin_stream(token, 200, b"OK"),
        RespondOutcome::Ok,
        "unit park stages a stream command"
    );
    assert!(shared.stage_writable(token), "first release stages");
    assert!(
        shared.stage_writable(token),
        "repeat coalesces without duplicating"
    );
    let bogus = ConnectionToken {
        request_seq: 9999,
        ..token
    };
    assert!(
        !shared.stage_writable(bogus),
        "unknown sequence stages nothing"
    );
    let events = unit_poll(&shared);
    assert_eq!(
        events.len(),
        2,
        "one request plus one coalesced writable: {events:?}"
    );
    assert_eq!(events[0].kind, HostEventKind::Request);
    assert_eq!(events[1].kind, HostEventKind::Writable);
    assert_eq!(events[1].token, token);
    assert_eq!(events[1].route_id, 0);
    assert_eq!((events[1].method_len, events[1].path_len), (0, 0));
    assert_eq!((events[1].method_offset, events[1].path_offset), (0, 0));
}

#[test]
fn managed_body_available_event_admits_pull_then_count() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    stream
        .write_all(
            b"POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 11\r\n\r\nhello world",
        )
        .expect("write post");
    // NOTE: the worker stages body bytes right behind the park, so the admission signal often
    // shares the first batches with its own REQUEST: a request-only poll would eat it. The loop
    // below keeps every signal seen before the REQUEST instead.
    let deadline = Instant::now() + DEADLINE;
    let mut kept = Vec::new();
    let (request, method, path) = loop {
        let (events, methods, paths, _) = poll_batch(&host, 100_000_000, 8);
        let mut found = None;
        for event in events {
            if event.kind == HostEventKind::Request {
                let method =
                    methods[event.method_offset..event.method_offset + event.method_len].to_vec();
                let path = paths[event.path_offset..event.path_offset + event.path_len].to_vec();
                found = Some((event, method, path));
            } else if event.kind == HostEventKind::BodyAvailable {
                kept.push(event);
            }
        }
        if let Some(found) = found {
            break found;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for request event"
        );
    };
    assert_eq!(method, b"POST");
    assert_eq!(path, b"/upload");
    // NOTE: segment splits may stage partial updates first; every signal carries the token with
    // reused counts, and the terminal full window always arrives because admission is atomic.
    // Signals kept from the request batches join the wait below.
    let deadline = Instant::now() + DEADLINE;
    let signal = loop {
        if let Some(done) = kept.iter().find(|event| {
            event.kind == HostEventKind::BodyAvailable
                && event.token == request.token
                && event.path_len == 1
        }) {
            break *done;
        }
        let (events, _, _, _) = poll_batch(&host, 100_000_000, 8);
        kept.extend(events.into_iter().filter(|event| {
            event.kind == HostEventKind::BodyAvailable && event.token == request.token
        }));
        assert!(Instant::now() < deadline, "terminal body signal stalls");
    };
    assert_eq!(signal.token, request.token);
    assert_eq!(signal.route_id, 0);
    assert_eq!(signal.method_len, 11);
    assert_eq!((signal.method_offset, signal.path_offset), (0, 0));
    assert_eq!(pull_body_complete(&host, request.token), b"hello world");
    assert_eq!(
        host.respond(request.token, 200, b"OK", b"done"),
        RespondOutcome::Ok
    );
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"done");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

fn wait_writable_event(host: &HostHandle, token: ConnectionToken) -> EventSlot {
    let deadline = Instant::now() + DEADLINE;
    loop {
        let (events, _, _, _) = poll_batch(host, 100_000_000, 8);
        for event in events {
            if event.kind == HostEventKind::Writable && event.token == token {
                return event;
            }
        }
        assert!(Instant::now() < deadline, "writable signal stalls");
    }
}

// NOTE: WRITABLE emission pin: the flushed head and each consumed write queue exactly one
// coalesced signal, and each signal admits exactly one write. The waiter above keeps advisory
// signals instead of assuming batch shape, so REQUEST-only polls stay single-event.
#[test]
fn managed_stream_writable_event_signals_each_release() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    let first = wait_writable_event(&host, event.token);
    assert_eq!(first.kind, HostEventKind::Writable);
    assert_eq!(first.token, event.token);
    assert_eq!(first.route_id, 0);
    assert_eq!((first.method_len, first.path_len), (0, 0));
    assert_eq!((first.method_offset, first.path_offset), (0, 0));
    assert_eq!(
        host.write_stream(event.token, b"hello "),
        RespondOutcome::Ok
    );
    let second = wait_writable_event(&host, event.token);
    assert_eq!(second.token, event.token);
    assert_eq!(second.route_id, 0);
    assert_eq!((second.method_len, second.path_len), (0, 0));
    assert_eq!(host.write_stream(event.token, b"world"), RespondOutcome::Ok);
    wait_writable(&host, event.token);
    assert_eq!(host.end_stream(event.token), RespondOutcome::Ok);
    let (status, raw, body) = read_chunked(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"hello world");
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("head end")
        + 4;
    stream_head_exact(&raw[..split], "HTTP/1.1 200 OK");
    assert_eq!(&raw[split..], b"6\r\nhello \r\n5\r\nworld\r\n0\r\n\r\n");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn host_writable_full_queue_refuses_and_resignals_after_drain() {
    let shared = HostShared::new(1);
    let token = unit_park(&shared, 3, 0);
    assert_eq!(
        shared.begin_stream(token, 200, b"OK"),
        RespondOutcome::Ok,
        "unit park stages a stream command"
    );
    for slot in 0..64 {
        if slot == 3 {
            continue;
        }
        unit_park(&shared, slot, 0);
    }
    assert!(
        !shared.stage_writable(token),
        "full queue with no coalesce target refuses"
    );
    assert_eq!(shared.counters().queue_full, 1);
    let mut events = vec![empty_slot(); 1];
    let mut methods = vec![0; 64];
    let mut paths = vec![0; 4096];
    let report = shared.poll(0, &mut events, &mut methods, &mut paths);
    assert_eq!(report.copied, 1, "one drain frees exactly one room");
    assert!(shared.stage_writable(token), "retry after drain stages");
    assert!(
        shared.stage_writable(token),
        "repeat coalesces without duplicating"
    );
    let mut events = vec![empty_slot(); 128];
    let mut methods = vec![0; 2048];
    let mut paths = vec![0; 4096];
    let report = shared.poll(0, &mut events, &mut methods, &mut paths);
    let signals = events[..report.copied]
        .iter()
        .filter(|event| event.kind == HostEventKind::Writable && event.token == token)
        .count();
    assert_eq!(signals, 1);
}

#[test]
fn host_writable_retired_on_cancel_and_close() {
    let shared = HostShared::new(1);
    let token = unit_park(&shared, 3, 0);
    assert_eq!(shared.begin_stream(token, 200, b"OK"), RespondOutcome::Ok);
    assert!(shared.stage_writable(token));
    let mut events = vec![empty_slot(); 1];
    let mut methods = vec![0; 64];
    let mut paths = vec![0; 4096];
    let report = shared.poll(0, &mut events, &mut methods, &mut paths);
    assert_eq!(report.copied, 1);
    assert_eq!(events[0].kind, HostEventKind::Request);
    assert_eq!(shared.stage_cancel(token), CancelOutcome::Ok);
    assert!(
        unit_poll(&shared).is_empty(),
        "cancel retires the pending signal"
    );
    let token = unit_park(&shared, 5, 0);
    assert_eq!(shared.begin_stream(token, 200, b"OK"), RespondOutcome::Ok);
    assert!(shared.stage_writable(token));
    let mut events = vec![empty_slot(); 1];
    let mut methods = vec![0; 64];
    let mut paths = vec![0; 4096];
    let report = shared.poll(0, &mut events, &mut methods, &mut paths);
    assert_eq!(report.copied, 1);
    assert_eq!(events[0].kind, HostEventKind::Request);
    assert!(shared.unpark_closed(0, 5), "close unparks");
    assert!(
        unit_poll(&shared).is_empty(),
        "close retires the pending signal"
    );
}
