use crate::frame_io::*;
use crate::h2_peer::*;
use crate::h2_response::*;
use std::io::Write;
use veylor_http2::{
    DATA_FLAG_END_STREAM, ErrorCode, FrameType, HEADERS_FLAG_END_STREAM, decode_rst_stream_payload,
};
use veylor_runtime::ConnectionToken;
use veylor_server::{CancelOutcome, FrozenRoute, FrozenRoutesView, H2HostCtx, RespondOutcome};

#[test]
fn host_park_poll_respond_round_trip() {
    let view = FrozenRoutesView::new(&ROUTES).expect("frozen routes");
    with_rig(|conn, shared, client, server| {
        rig_handshake(conn, client, server, &view);
        let ctx = H2HostCtx {
            host: shared,
            base: host_base(),
        };
        send_headers(client, 1, b"GET", b"/hello", true);
        let outcome = pump_until(conn, server, &view, Some(ctx), |outcome| outcome.parked > 0);
        assert_eq!(outcome.written, 0);
        assert_eq!(outcome.parked, 1);
        let (event, method, path) = poll_request_event(shared);
        assert_eq!(method, b"GET");
        assert_eq!(path, b"/hello");
        assert_eq!(event.token.stream_id, 1);
        assert_eq!(event.route_id, 0);
        assert_eq!(
            shared.stage_respond(event.token, 200, b"OK", &[], b"host-hello"),
            RespondOutcome::Ok
        );
        let (staged, _) = shared.take_staged(0);
        assert_eq!(staged.len(), 1);
        assert!(!staged[0].cancel);
        let mut output = [0u8; 16384];
        let work = staged.into_iter().next().expect("staged response");
        let applied = conn.apply_staged(&mut output, work);
        assert_eq!(applied.served, 1);
        server
            .write_all(&output[..applied.written])
            .expect("flush staged");
        let response = read_response(client, 1);
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"host-hello");
    });
}

#[test]
fn host_cancel_mid_park_rsts_sibling_unaffected() {
    let view = FrozenRoutesView::new(&ROUTES).expect("frozen routes");
    with_rig(|conn, shared, client, server| {
        rig_handshake(conn, client, server, &view);
        let ctx = H2HostCtx {
            host: shared,
            base: host_base(),
        };
        send_headers(client, 1, b"GET", b"/hello", true);
        send_headers(client, 3, b"GET", b"/missing", true);
        pump_until(conn, server, &view, Some(ctx), |outcome| {
            outcome.parked > 0 && outcome.served > 0
        });
        let (event, _, _) = poll_request_event(shared);
        assert_eq!(event.token.stream_id, 1);
        assert_eq!(shared.stage_cancel(event.token), CancelOutcome::Ok);
        let (staged, _) = shared.take_staged(0);
        assert_eq!(staged.len(), 1);
        assert!(staged[0].cancel);
        let mut output = [0u8; 16384];
        let work = staged.into_iter().next().expect("staged cancellation");
        let applied = conn.apply_staged(&mut output, work);
        server
            .write_all(&output[..applied.written])
            .expect("flush cancel");
        let sibling = read_response(client, 3);
        assert_eq!(sibling.status, 404);
        assert_eq!(sibling.body, b"Not Found");
        let (header, payload) = read_frame(client);
        assert_eq!(header.frame_type, FrameType::RstStream);
        assert_eq!(header.stream_id, 1);
        assert_eq!(
            decode_rst_stream_payload(header, &payload).expect("decode rst"),
            ErrorCode::Cancel
        );
    });
}

#[test]
fn host_stale_replay_and_duplicate_fail_closed() {
    let view = FrozenRoutesView::new(&ROUTES).expect("frozen routes");
    with_rig(|conn, shared, client, server| {
        rig_handshake(conn, client, server, &view);
        let ctx = H2HostCtx {
            host: shared,
            base: host_base(),
        };
        send_headers(client, 1, b"GET", b"/hello", true);
        pump_until(conn, server, &view, Some(ctx), |outcome| outcome.parked > 0);
        let (event, _, _) = poll_request_event(shared);
        let stale = ConnectionToken {
            request_seq: event.token.request_seq.wrapping_add(99),
            ..event.token
        };
        assert_eq!(
            shared.stage_respond(stale, 200, b"OK", &[], b"hijack"),
            RespondOutcome::Stale
        );
        assert_eq!(
            shared.stage_respond(event.token, 200, b"OK", &[], b"live"),
            RespondOutcome::Ok
        );
        let (staged, _) = shared.take_staged(0);
        assert_eq!(staged.len(), 1);
        let mut output = [0u8; 16384];
        let work = staged.into_iter().next().expect("staged response");
        let applied = conn.apply_staged(&mut output, work);
        server
            .write_all(&output[..applied.written])
            .expect("flush staged");
        let response = read_response(client, 1);
        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"live");
        assert_eq!(
            shared.stage_respond(event.token, 200, b"OK", &[], b"again"),
            RespondOutcome::Duplicate
        );
    });
}

#[test]
fn host_unknown_path_stays_sync_404() {
    let view = FrozenRoutesView::new(&ROUTES).expect("frozen routes");
    with_rig(|conn, shared, client, server| {
        rig_handshake(conn, client, server, &view);
        let ctx = H2HostCtx {
            host: shared,
            base: host_base(),
        };
        send_headers(client, 1, b"GET", b"/missing", true);
        let outcome = pump_until(conn, server, &view, Some(ctx), |outcome| outcome.served > 0);
        assert_eq!(outcome.parked, 0);
        assert_eq!(shared.parked_count(), 0);
        let response = read_response(client, 1);
        assert_eq!(response.status, 404);
        assert_eq!(response.body, b"Not Found");
    });
}

#[test]
fn host_request_body_refused_without_staging() {
    static STATIC: [FrozenRoute<'static>; 1] = [FrozenRoute {
        method: b"GET",
        path: b"/hello",
        token: 0,
        status: 200,
        body: b"hello",
        gzip_body: None,
        managed: false,
        max_body_bytes: 0,
        file_root: None,
        trailers: &[],
    }];
    let view = FrozenRoutesView::new(&STATIC).expect("frozen routes");
    with_rig(|conn, shared, client, server| {
        rig_handshake(conn, client, server, &view);
        let ctx = H2HostCtx {
            host: shared,
            base: host_base(),
        };
        send_headers(client, 1, b"GET", b"/hello", false);
        write_frame(client, FrameType::Data, DATA_FLAG_END_STREAM, 1, b"junk");
        let outcome = pump_until(conn, server, &view, Some(ctx), |outcome| outcome.closed);
        assert_eq!(outcome.parked, 0);
        assert_eq!(outcome.refused, 1);
        let (header, payload) = read_frame(client);
        assert_eq!(header.frame_type, FrameType::Headers);
        assert_eq!(header.flags & HEADERS_FLAG_END_STREAM, 0);
        assert_eq!(decode_response(&payload).status, 200);
        let (header, payload) = read_frame(client);
        assert_eq!(header.frame_type, FrameType::RstStream);
        assert_eq!(
            decode_rst_stream_payload(header, &payload).expect("decode rst"),
            ErrorCode::RefusedStream
        );
        read_eof(client);
    });
}
