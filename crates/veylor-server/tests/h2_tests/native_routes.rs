use crate::frame_io::*;
use crate::h2_peer::*;
use crate::h2_response::*;
use parking_lot::Mutex;
use std::ffi::c_void;
use std::io::Write;
use std::net::SocketAddr;
use veylor_http2::{ErrorCode, FrameType, HEADERS_FLAG_END_STREAM, decode_rst_stream_payload};
use veylor_server::{
    FrozenRoute, FrozenRoutesView, H2HostCtx, MAX_STATIC_BODY_BYTES, NativeRequestView,
    NativeResponseOut, RespondOutcome, RouteBuildError, Server, ServerConfig,
    h2::FrozenNativeRoute,
};

static H2N_ECHO: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
    method: b"POST",
    path: b"/echo",
    handler: h2n_record,
    user_data: 0,
    gzip: false,
}];

static H2N_END_PROBE: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
    method: b"POST",
    path: b"/echo",
    handler: h2n_probe_end,
    user_data: 0,
    gzip: false,
}];

static H2N_CAP_PROBE: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
    method: b"POST",
    path: b"/echo",
    handler: h2n_probe_cap,
    user_data: 0,
    gzip: false,
}];

static H2N_FAIL: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
    method: b"GET",
    path: b"/fail",
    handler: h2n_fail,
    user_data: 0,
    gzip: false,
}];

static H2N_BAD_STATUS: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
    method: b"GET",
    path: b"/fail",
    handler: h2n_bad_status,
    user_data: 0,
    gzip: false,
}];

static H2N_BIG_BODY: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
    method: b"GET",
    path: b"/fail",
    handler: h2n_big_body,
    user_data: 0,
    gzip: false,
}];

static H2N_BAD_REASON: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
    method: b"GET",
    path: b"/fail",
    handler: h2n_bad_reason,
    user_data: 0,
    gzip: false,
}];

static H2N_204: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
    method: b"GET",
    path: b"/fail",
    handler: h2n_204,
    user_data: 0,
    gzip: false,
}];

static H2N_204_BODY: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
    method: b"GET",
    path: b"/fail",
    handler: h2n_204_body,
    user_data: 0,
    gzip: false,
}];

static H2N_304_BODY: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
    method: b"GET",
    path: b"/fail",
    handler: h2n_304_body,
    user_data: 0,
    gzip: false,
}];

static H2N_SELECT: [FrozenNativeRoute<'static>; 2] = [
    FrozenNativeRoute {
        method: b"GET",
        path: b"/u1",
        handler: h2n_select,
        user_data: 1,
        gzip: false,
    },
    FrozenNativeRoute {
        method: b"GET",
        path: b"/u2",
        handler: h2n_select,
        user_data: 2,
        gzip: false,
    },
];

static H2N_PRE_ROUTES: [FrozenRoute<'static>; 1] = [FrozenRoute {
    method: b"HEAD",
    path: b"/x",
    token: 0,
    status: 200,
    body: b"static-head",
    gzip_body: None,
    managed: false,
    max_body_bytes: 0,
    file_root: None,
    trailers: &[],
}];

static H2N_PRE_NATIVES: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
    method: b"GET",
    path: b"/x",
    handler: h2n_ok,
    user_data: 0,
    gzip: false,
}];

static H2N_BIG: [u8; 15 * 1024 + 1] = [b'x'; 15 * 1024 + 1];

static H2N_BAD_REASON_BYTES: [u8; 3] = *b"B\nd";

static H2N_END_HIT: Mutex<bool> = Mutex::new(false);

static H2N_CAP_HIT: Mutex<bool> = Mutex::new(false);

struct H2nObserved {
    method: Vec<u8>,
    path: Vec<u8>,
    has_body: u8,
    body_len: u64,
    null_body: bool,
}

static H2N_OBSERVED: Mutex<Option<H2nObserved>> = Mutex::new(None);

#[allow(unsafe_code)]
unsafe extern "C" fn h2n_record(
    _user_data: *mut c_void,
    request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    if request.is_null() {
        return 1;
    }

    let view = unsafe { &*request };
    let method = unsafe { std::slice::from_raw_parts(view.method, view.method_len) }.to_vec();
    let path = unsafe { std::slice::from_raw_parts(view.path, view.path_len) }.to_vec();
    *H2N_OBSERVED.lock() = Some(H2nObserved {
        method,
        path,
        has_body: view.has_body,
        body_len: view.body_len,
        null_body: view.body.is_null(),
    });
    h2n_respond(response, 200, b"OK", b"")
}

#[allow(unsafe_code)]
unsafe extern "C" fn h2n_probe_end(
    _user_data: *mut c_void,
    _request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    *H2N_END_HIT.lock() = true;
    h2n_respond(response, 200, b"OK", b"")
}

#[allow(unsafe_code)]
unsafe extern "C" fn h2n_probe_cap(
    _user_data: *mut c_void,
    _request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    *H2N_CAP_HIT.lock() = true;
    h2n_respond(response, 200, b"OK", b"")
}

#[allow(unsafe_code)]
unsafe extern "C" fn h2n_fail(
    _user_data: *mut c_void,
    _request: *const NativeRequestView,
    _response: *mut NativeResponseOut,
) -> i32 {
    1
}

#[allow(unsafe_code)]
unsafe extern "C" fn h2n_bad_status(
    _user_data: *mut c_void,
    _request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    h2n_respond(response, 199, b"OK", b"")
}

#[allow(unsafe_code)]
unsafe extern "C" fn h2n_big_body(
    _user_data: *mut c_void,
    _request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    h2n_respond(response, 200, b"OK", &H2N_BIG)
}

#[allow(unsafe_code)]
unsafe extern "C" fn h2n_bad_reason(
    _user_data: *mut c_void,
    _request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    h2n_respond(response, 200, &H2N_BAD_REASON_BYTES, b"")
}

#[allow(unsafe_code)]
unsafe extern "C" fn h2n_204(
    _user_data: *mut c_void,
    _request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    h2n_respond(response, 204, b"No Content", b"")
}

#[allow(unsafe_code)]
unsafe extern "C" fn h2n_204_body(
    _user_data: *mut c_void,
    _request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    h2n_respond(response, 204, b"No Content", b"stray-body")
}

#[allow(unsafe_code)]
unsafe extern "C" fn h2n_304_body(
    _user_data: *mut c_void,
    _request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    h2n_respond(response, 304, b"Not Modified", b"stray-body")
}

#[allow(unsafe_code)]
unsafe extern "C" fn h2n_select(
    user_data: *mut c_void,
    _request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    if user_data as usize == 1 {
        h2n_respond(response, 200, b"OK", b"one")
    } else {
        h2n_respond(response, 200, b"OK", b"two")
    }
}

#[test]
fn h2_native_get_serves_handler_body() {
    let view = h2n_view(&H2N_OK);
    with_rig(|conn, _shared, client, server| {
        rig_handshake(conn, client, server, &view);
        send_headers(client, 1, b"GET", b"/native", true);
        let outcome = pump_until(conn, server, &view, None, |outcome| outcome.served > 0);
        assert_eq!(outcome.served, 1);
        assert_eq!(outcome.refused, 0);
        let response = read_response(client, 1);
        assert_eq!(response.status, 200);
        assert_eq!(response.content_length, 12);
        assert_eq!(response.body, b"native-hello");
    });
}

#[test]
fn h2_native_head_falls_back_to_get_without_body() {
    let view = h2n_view(&H2N_OK);
    with_rig(|conn, _shared, client, server| {
        rig_handshake(conn, client, server, &view);
        send_headers(client, 1, b"HEAD", b"/native", true);
        let outcome = pump_until(conn, server, &view, None, |outcome| outcome.served > 0);
        assert_eq!(outcome.served, 1);
        let response = read_response(client, 1);
        assert_eq!(response.status, 200);
        assert_eq!(response.content_length, 12);
        assert!(response.body.is_empty());
    });
}

#[test]
fn h2_native_user_data_selects_body() {
    let view = h2n_view(&H2N_SELECT);
    with_rig(|conn, _shared, client, server| {
        rig_handshake(conn, client, server, &view);
        send_headers(client, 1, b"GET", b"/u1", true);
        pump_until(conn, server, &view, None, |outcome| outcome.served > 0);
        let first = read_response(client, 1);
        assert_eq!(first.status, 200);
        assert_eq!(first.body, b"one");
        send_headers(client, 3, b"GET", b"/u2", true);
        pump_until(conn, server, &view, None, |outcome| outcome.served > 0);
        let second = read_response(client, 3);
        assert_eq!(second.status, 200);
        assert_eq!(second.body, b"two");
    });
}

#[test]
fn h2_native_open_tail_invokes_framing_only_then_refuses() {
    let view = h2n_view(&H2N_ECHO);
    with_rig(|conn, _shared, client, server| {
        rig_handshake(conn, client, server, &view);
        *H2N_OBSERVED.lock() = None;
        send_headers_len(client, 1, b"POST", b"/echo", Some(5), false);
        let outcome = pump_until(conn, server, &view, None, |outcome| outcome.closed);
        assert_eq!(outcome.refused, 1);
        let seen = H2N_OBSERVED.lock().take().expect("handler invoked");
        assert_eq!(seen.method, b"POST");
        assert_eq!(seen.path, b"/echo");
        assert_eq!(seen.has_body, 1);
        assert_eq!(seen.body_len, 5);
        assert!(seen.null_body);
        let (header, payload) = read_frame(client);
        assert_eq!(header.frame_type, FrameType::Headers);
        assert_eq!(header.stream_id, 1);
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

#[test]
fn h2_native_end_with_declared_body_is_protocol_error() {
    let view = h2n_view(&H2N_END_PROBE);
    with_rig(|conn, _shared, client, server| {
        rig_handshake(conn, client, server, &view);
        *H2N_END_HIT.lock() = false;
        send_headers_len(client, 1, b"POST", b"/echo", Some(5), true);
        pump_until(conn, server, &view, None, |outcome| outcome.refused > 0);
        assert!(!*H2N_END_HIT.lock());
        let (id, code) = read_rst(client);
        assert_eq!(id, 1);
        assert_eq!(code, ErrorCode::ProtocolError);
    });
}

#[test]
fn h2_native_over_cap_refuses_before_invoke() {
    let view = h2n_view(&H2N_CAP_PROBE);
    with_rig(|conn, _shared, client, server| {
        rig_handshake(conn, client, server, &view);
        *H2N_CAP_HIT.lock() = false;
        send_headers_len(
            client,
            1,
            b"POST",
            b"/echo",
            Some(MAX_STATIC_BODY_BYTES + 1),
            false,
        );
        pump_until(conn, server, &view, None, |outcome| outcome.refused > 0);
        assert!(!*H2N_CAP_HIT.lock());
        let (id, code) = read_rst(client);
        assert_eq!(id, 1);
        assert_eq!(code, ErrorCode::RefusedStream);
        send_headers(client, 3, b"GET", b"/empty", true);
        pump_until(conn, server, &view, None, |outcome| outcome.served > 0);
        let sibling = read_response(client, 3);
        assert_eq!(sibling.status, 200);
        assert!(sibling.body.is_empty());
    });
}

fn h2n_failure_rsts(natives: &'static [FrozenNativeRoute<'static>]) {
    let view = h2n_view(natives);
    with_rig(|conn, _shared, client, server| {
        rig_handshake(conn, client, server, &view);
        send_headers(client, 1, b"GET", b"/fail", true);
        pump_until(conn, server, &view, None, |outcome| outcome.refused > 0);
        let (header, payload) = read_frame(client);
        assert_eq!(header.frame_type, FrameType::RstStream);
        assert_eq!(header.stream_id, 1);
        assert_eq!(
            decode_rst_stream_payload(header, &payload).expect("decode rst"),
            ErrorCode::InternalError
        );
    });
}

#[test]
fn h2_native_handler_failures_rst_without_headers() {
    h2n_failure_rsts(&H2N_FAIL);
    h2n_failure_rsts(&H2N_BAD_STATUS);
    h2n_failure_rsts(&H2N_BIG_BODY);
    h2n_failure_rsts(&H2N_BAD_REASON);
}

// NOTE: 204 serves :status plus Date with END_STREAM (RFC 9113 forbids content-length on
// 204, never Date): the exact field-name list proves no length rides the wire, the Date value
// proves IMF-fixdate shape, and the sibling read proves no DATA follows (a stray DATA frame
// would misroute the sibling's stream assertion).
fn h2n_204_serves(natives: &'static [FrozenNativeRoute<'static>]) {
    let view = h2n_view(natives);
    with_rig(|conn, _shared, client, server| {
        rig_handshake(conn, client, server, &view);
        send_headers(client, 1, b"GET", b"/fail", true);
        let outcome = pump_until(conn, server, &view, None, |outcome| outcome.served > 0);
        assert_eq!(outcome.served, 1);
        assert_eq!(outcome.refused, 0);
        let (header, payload) = read_frame(client);
        assert_eq!(header.frame_type, FrameType::Headers);
        assert_eq!(header.stream_id, 1);
        assert_ne!(header.flags & HEADERS_FLAG_END_STREAM, 0);
        let fields = decode_fields(&payload);
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0], (b":status".to_vec(), b"204".to_vec()));
        assert_eq!(fields[1].0, b"date");
        date_value_shape(&fields[1].1);
        send_headers(client, 3, b"GET", b"/empty", true);
        pump_until(conn, server, &view, None, |outcome| outcome.served > 0);
        let sibling = read_response(client, 3);
        assert_eq!(sibling.status, 200);
        assert!(sibling.body.is_empty());
    });
}

#[test]
fn h2_native_204_serves_status_only_end_stream() {
    h2n_204_serves(&H2N_204);
}

#[test]
fn h2_native_204_drops_handler_body() {
    h2n_204_serves(&H2N_204_BODY);
}

// NOTE: native 304 with a handler body must shed it like H1 does (H1 native gates on
// body_allowed; RFC 9110 8.6 forbids the bytes): HEADERS carries END_STREAM with
// :status + Date only, no DATA follows, and the connection survives for a sibling.
#[test]
fn h2_native_304_drops_handler_body() {
    let view = h2n_view(&H2N_304_BODY);
    with_rig(|conn, _shared, client, server| {
        rig_handshake(conn, client, server, &view);
        send_headers(client, 1, b"GET", b"/fail", true);
        let outcome = pump_until(conn, server, &view, None, |outcome| outcome.served > 0);
        assert_eq!(outcome.served, 1);
        assert_eq!(outcome.refused, 0);
        let (header, _) = read_frame(client);
        assert_eq!(header.frame_type, FrameType::Headers);
        assert_eq!(header.stream_id, 1);
        assert_ne!(header.flags & HEADERS_FLAG_END_STREAM, 0);
        send_headers(client, 3, b"GET", b"/empty", true);
        pump_until(conn, server, &view, None, |outcome| outcome.served > 0);
        let sibling = read_response(client, 3);
        assert_eq!(sibling.status, 200);
        assert!(sibling.body.is_empty());
    });
}

// NOTE: managed 204 pin: stage_respond admits any 200..=599 so a host 204 lands in
// apply_staged, where the shared sender sheds the length and the staged body; the exact
// field list plus the sync sibling prove the wire shape and conn survival.
#[test]
fn h2_managed_respond_204_serves_status_only() {
    let view = FrozenRoutesView::new(&ROUTES).expect("frozen routes");
    with_rig(|conn, shared, client, server| {
        rig_handshake(conn, client, server, &view);
        let ctx = H2HostCtx {
            host: shared,
            base: host_base(),
        };
        send_headers(client, 1, b"GET", b"/hello", true);
        let outcome = pump_until(conn, server, &view, Some(ctx), |outcome| outcome.parked > 0);
        assert_eq!(outcome.parked, 1);
        let (event, _, _) = poll_request_event(shared);
        assert_eq!(event.token.stream_id, 1);
        assert_eq!(
            shared.stage_respond(event.token, 204, b"No Content", &[], b"stray"),
            RespondOutcome::Ok
        );
        let (staged, _) = shared.take_staged(0);
        assert_eq!(staged.len(), 1);
        let mut output = [0u8; 16384];
        let work = staged.into_iter().next().expect("staged response");
        let applied = conn.apply_staged(&mut output, work);
        assert_eq!(applied.served, 1);
        server
            .write_all(&output[..applied.written])
            .expect("flush staged");
        let (header, payload) = read_frame(client);
        assert_eq!(header.frame_type, FrameType::Headers);
        assert_eq!(header.stream_id, 1);
        assert_ne!(header.flags & HEADERS_FLAG_END_STREAM, 0);
        let fields = decode_fields(&payload);
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0], (b":status".to_vec(), b"204".to_vec()));
        assert_eq!(fields[1].0, b"date");
        date_value_shape(&fields[1].1);
        send_headers(client, 3, b"GET", b"/missing", true);
        pump_until(conn, server, &view, Some(ctx), |outcome| outcome.served > 0);
        let sibling = read_response(client, 3);
        assert_eq!(sibling.status, 404);
        assert_eq!(sibling.body, b"Not Found");
    });
}

#[test]
fn h2_native_wins_over_head_static_on_fallback() {
    let view = FrozenRoutesView::new(&H2N_PRE_ROUTES)
        .expect("frozen routes")
        .with_natives(&H2N_PRE_NATIVES)
        .expect("natives");
    with_rig(|conn, _shared, client, server| {
        rig_handshake(conn, client, server, &view);
        send_headers(client, 1, b"HEAD", b"/x", true);
        pump_until(conn, server, &view, None, |outcome| outcome.served > 0);
        let response = read_response(client, 1);
        assert_eq!(response.status, 200);
        assert_eq!(response.content_length, 12);
        assert!(response.body.is_empty());
    });
}

#[test]
fn h2_native_view_rejects_overlaps_and_bad_input() {
    static OVER_STATIC: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
        method: b"GET",
        path: b"/empty",
        handler: h2n_ok,
        user_data: 0,
        gzip: false,
    }];
    assert_eq!(
        FrozenRoutesView::new(&ROUTES)
            .expect("routes")
            .with_natives(&OVER_STATIC)
            .unwrap_err(),
        RouteBuildError::Duplicate
    );
    static OVER_MANAGED: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
        method: b"GET",
        path: b"/hello",
        handler: h2n_ok,
        user_data: 0,
        gzip: false,
    }];
    assert_eq!(
        FrozenRoutesView::new(&ROUTES)
            .expect("routes")
            .with_natives(&OVER_MANAGED)
            .unwrap_err(),
        RouteBuildError::Duplicate
    );
    static INTERNAL_DUPE: [FrozenNativeRoute<'static>; 2] = [
        FrozenNativeRoute {
            method: b"GET",
            path: b"/a",
            handler: h2n_ok,
            user_data: 0,
            gzip: false,
        },
        FrozenNativeRoute {
            method: b"GET",
            path: b"/a",
            handler: h2n_ok,
            user_data: 1,
            gzip: false,
        },
    ];
    assert_eq!(
        FrozenRoutesView::new(&ROUTES)
            .expect("routes")
            .with_natives(&INTERNAL_DUPE)
            .unwrap_err(),
        RouteBuildError::Duplicate
    );
    static BAD_METHOD: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
        method: b"GE T",
        path: b"/a",
        handler: h2n_ok,
        user_data: 0,
        gzip: false,
    }];
    assert_eq!(
        FrozenRoutesView::new(&ROUTES)
            .expect("routes")
            .with_natives(&BAD_METHOD)
            .unwrap_err(),
        RouteBuildError::Invalid
    );
    static EMPTY_PATH: [FrozenNativeRoute<'static>; 1] = [FrozenNativeRoute {
        method: b"GET",
        path: b"",
        handler: h2n_ok,
        user_data: 0,
        gzip: false,
    }];
    assert_eq!(
        FrozenRoutesView::new(&ROUTES)
            .expect("routes")
            .with_natives(&EMPTY_PATH)
            .unwrap_err(),
        RouteBuildError::Invalid
    );
}

#[test]
fn h2c_serves_native_routes_live() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let config = ServerConfig {
        h2c_port: Some(0),
        ..ServerConfig::default()
    };
    let mut server = Server::bind(addr, config).expect("bind loopback");
    server
        .register_native(b"GET", b"/native", Some(live_native_hello), 0)
        .expect("register native");
    server
        .register_route(
            b"GET",
            b"/hello",
            200,
            b"OK",
            b"hello",
            MAX_STATIC_BODY_BYTES,
        )
        .expect("register hello");
    let server = server.start().expect("start server");
    let mut client = h2_connect(&server);
    handshake(&mut client);
    send_headers(&mut client, 1, b"GET", b"/native", true);
    let response = read_response(&mut client, 1);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"h2-native");
    send_headers(&mut client, 3, b"GET", b"/hello", true);
    let static_response = read_response(&mut client, 3);
    assert_eq!(static_response.status, 200);
    assert_eq!(static_response.body, b"hello");
    let counts = stop_join(server);
    assert_eq!(counts.h2_served, 2);
}
