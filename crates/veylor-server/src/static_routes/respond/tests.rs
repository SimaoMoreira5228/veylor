// NOTE: drift alarm for the converged 100-continue policy: the managed pump stages this
// exact shape ahead of the final response while the host-driven conn crate encodes the
// same shared shape for its continue handshake, so any shape edit upstream fails here
// until the pump follows.
use super::encode_continue;
use veylor_http1::{
    ALLOW_BYTES, ALLOW_METHODS, CONTENT_RANGE_BYTES, MULTIPART_CTYPE_BYTES, continue_interim,
    encode_response_head,
};

#[test]
fn interim_stages_shared_continue_shape_ahead_of_final() {
    let mut staged = [0u8; 64];
    let len = encode_continue(&mut staged).expect("interim fits");
    let mut expected = [0u8; 64];
    let encoded =
        encode_response_head(&continue_interim(), &mut expected).expect("shared interim encodes");
    assert_eq!(&staged[..len], &expected[..encoded.length]);
    assert_eq!(&staged[..len], b"HTTP/1.1 100 Continue\r\n\r\n");
}

#[test]
fn keep_open_verdict_matches_shared_reuse_policy() {
    // NOTE: drift alarm for the converged keep-alive policy: every static keep_open below
    // must equal veylor_http1::response_permits_reuse on the same verdict, so a local
    // verdict edit fails here until the shared predicate follows. The upgrade input stays
    // false (this path never upgrades) and drain rides drain_total, not the booleans.
    use super::respond_to_head;
    use crate::static_routes::StaticRoutes;
    use veylor_http1::{
        Header, Limits, Parser, RequestHead, ResponseFraming, connection_header_for_response,
        response_permits_reuse,
    };
    use veylor_router::RouteSlot;

    fn drive(routes: &StaticRoutes, input: &[u8]) -> (RequestHead, bool) {
        let mut slots = [RouteSlot::EMPTY; 8];
        let table = routes.build_table(&mut slots).expect("table builds");
        let mut headers = [Header::default(); 16];
        let mut parser = Parser::new();
        let head = parser
            .parse(input, &mut headers, Limits::default())
            .expect("head parses")
            .expect("head complete");
        let mut output = [0u8; 2048];
        let encoded = respond_to_head(routes, &table, head, &headers, input, &mut output)
            .expect("static encodes");
        (head, encoded.keep_open)
    }

    let mut routes = StaticRoutes::new();
    routes
        .register(b"GET", b"/hello", 200, b"OK", b"hi", 1024)
        .expect("register static");

    let plain = b"GET /hello HTTP/1.1\r\nHost: test\r\n\r\n";
    let (head, keep_open) = drive(&routes, plain);
    let verdict =
        connection_header_for_response(head, plain, 200, ResponseFraming::FixedLength, true, false);
    assert!(keep_open);
    assert_eq!(keep_open, response_permits_reuse(false, verdict));

    let close = b"GET /hello HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n";
    let (head, keep_open) = drive(&routes, close);
    let verdict =
        connection_header_for_response(head, close, 200, ResponseFraming::FixedLength, true, false);
    assert!(!keep_open);
    assert_eq!(keep_open, response_permits_reuse(false, verdict));

    let missing = b"GET /nope HTTP/1.1\r\nHost: test\r\n\r\n";
    let (head, keep_open) = drive(&routes, missing);
    let verdict = connection_header_for_response(
        head,
        missing,
        404,
        ResponseFraming::FixedLength,
        true,
        true,
    );
    assert!(!keep_open);
    assert_eq!(keep_open, response_permits_reuse(false, verdict));
}

#[test]
fn allow_scratch_sizes_match_shared_bounds() {
    // NOTE: drift alarm for the converged Allow scratch policy: this engine sizes
    // every Allow, Content-Range, and multipart buffer from the shared veylor_http1
    // bounds, so a shared-bound edit fails here until this consumer follows.
    assert_eq!(ALLOW_METHODS, 16);
    assert_eq!(ALLOW_BYTES, 320);
    assert_eq!(CONTENT_RANGE_BYTES, 80);
    assert_eq!(MULTIPART_CTYPE_BYTES, 128);
}

#[test]
fn static_response_uses_http10_and_preserves_keep_alive() {
    use super::respond_to_head;
    use crate::static_routes::StaticRoutes;
    use veylor_http1::{Header, Limits, Parser};
    use veylor_router::RouteSlot;

    let mut routes = StaticRoutes::new();
    routes
        .register(b"GET", b"/hello", 200, b"OK", b"hi", 1024)
        .expect("register static");
    let input = b"GET /hello HTTP/1.0\r\nHost: test\r\nConnection: keep-alive\r\n\r\n";
    let mut slots = [RouteSlot::EMPTY; 8];
    let table = routes.build_table(&mut slots).expect("table builds");
    let mut headers = [Header::default(); 16];
    let mut parser = Parser::new();
    let head = parser
        .parse(input, &mut headers, Limits::default())
        .expect("head parses")
        .expect("head complete");
    let mut output = [0u8; 2048];

    let encoded = respond_to_head(&routes, &table, head, &headers, input, &mut output)
        .expect("static response encodes");
    let response = &output[..encoded.length];

    assert!(encoded.keep_open);
    assert!(response.starts_with(b"HTTP/1.0 200 OK\r\n"));
    assert!(
        response
            .windows(b"Connection: keep-alive\r\n".len())
            .any(|window| window == b"Connection: keep-alive\r\n")
    );
    assert!(
        !response
            .windows(b"Transfer-Encoding: chunked".len())
            .any(|window| window == b"Transfer-Encoding: chunked")
    );
}

#[test]
fn http10_options_and_error_responses_use_request_version() {
    use super::{
        respond_bad_request, respond_expectation_failed, respond_payload_too_large,
        respond_request_header_fields_too_large, respond_to_head, respond_uri_too_long,
    };
    use crate::static_routes::StaticRoutes;
    use veylor_http1::{Header, Limits, Parser, RequestVersion};
    use veylor_router::RouteSlot;

    let mut routes = StaticRoutes::new();
    routes
        .register(b"GET", b"/hello", 200, b"OK", b"hi", 1024)
        .expect("register static");
    let input = b"OPTIONS * HTTP/1.0\r\nHost: test\r\nConnection: keep-alive\r\n\r\n";
    let mut slots = [RouteSlot::EMPTY; 8];
    let table = routes.build_table(&mut slots).expect("table builds");
    let mut headers = [Header::default(); 16];
    let mut parser = Parser::new();
    let head = parser
        .parse(input, &mut headers, Limits::default())
        .expect("head parses")
        .expect("head complete");
    let mut output = [0u8; 2048];

    let encoded = respond_to_head(&routes, &table, head, &headers, input, &mut output)
        .expect("OPTIONS response encodes");
    let response = &output[..encoded.length];

    assert!(encoded.keep_open);
    assert!(response.starts_with(b"HTTP/1.0 200 OK\r\n"));
    assert!(
        response
            .windows(b"Connection: keep-alive\r\n".len())
            .any(|window| window == b"Connection: keep-alive\r\n")
    );

    fn assert_closed_http10(output: &[u8], length: usize) {
        let response = &output[..length];
        assert!(response.starts_with(b"HTTP/1.0 "));
        assert!(
            response
                .windows(b"Connection: close\r\n".len())
                .any(|window| window == b"Connection: close\r\n")
        );
        assert!(
            !response
                .windows(b"Transfer-Encoding: chunked".len())
                .any(|window| window == b"Transfer-Encoding: chunked")
        );
    }

    let length = respond_bad_request(&mut output, RequestVersion::Http10).expect("400 encodes");
    assert_closed_http10(&output, length);
    let length = respond_payload_too_large(&mut output, b"POST", RequestVersion::Http10)
        .expect("413 encodes");
    assert_closed_http10(&output, length);
    let length =
        respond_expectation_failed(&mut output, RequestVersion::Http10).expect("417 encodes");
    assert_closed_http10(&output, length);
    let length = respond_request_header_fields_too_large(&mut output, RequestVersion::Http10)
        .expect("431 encodes");
    assert_closed_http10(&output, length);
    let length = respond_uri_too_long(&mut output, RequestVersion::Http10).expect("414 encodes");
    assert_closed_http10(&output, length);
}
