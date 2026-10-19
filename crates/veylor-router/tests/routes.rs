use veylor_http1::{
    BodyLimits, Header, Limits, ParseError, Parser, RequestBodyAdmission, RequestBodyDecoder,
    RequestHead,
};
use veylor_router::{
    RouteBodyPolicy, RouteCapture, RouteConfig, RouteError, RouteKind, RouteSlot, RouteTable,
};

fn parse_request(input: &[u8]) -> (RequestHead, [Header; 16]) {
    let mut parser = Parser::new();
    let mut headers = [Header::default(); 16];
    let head = parser
        .parse(input, &mut headers, Limits::default())
        .unwrap()
        .unwrap();
    (head, headers)
}

fn route_config(kind: RouteKind) -> RouteConfig<'static> {
    RouteConfig {
        kind,
        body: RouteBodyPolicy::Reject,
    }
}

#[test]
fn freezes_exact_routes_rejects_duplicates_and_matches_normalized_paths() {
    let mut slots = [RouteSlot::EMPTY; 4];
    let mut routes = RouteTable::new(&mut slots);
    routes
        .register(b"GET", b"/health", route_config(RouteKind::Static))
        .unwrap();
    routes
        .register(b"GET", b"/a", route_config(RouteKind::Static))
        .unwrap();
    assert_eq!(routes.freeze(), Ok(()));
    assert_eq!(
        routes.register(b"GET", b"/late", route_config(RouteKind::Static)),
        Err(RouteError::Frozen)
    );

    let input = b"GET /%61 HTTP/1.1\r\nHost: test\r\n\r\n";
    let (head, _) = parse_request(input);
    let mut captures = [RouteCapture::default(); 8];
    let matched = routes.find_request(head, input, &mut captures).unwrap();
    assert_eq!(matched.kind, RouteKind::Static);
    assert_eq!(matched.pattern, b"/a");
    assert!(matched.captures.is_empty());

    let mut duplicate_slots = [RouteSlot::EMPTY; 2];
    let mut duplicates = RouteTable::new(&mut duplicate_slots);
    duplicates
        .register(b"GET", b"/a", route_config(RouteKind::Static))
        .unwrap();
    duplicates
        .register(b"GET", b"/%61", route_config(RouteKind::Static))
        .unwrap();
    assert_eq!(duplicates.freeze(), Err(RouteError::Duplicate));
}

#[test]
fn resolves_dynamic_routes_with_literal_precedence_and_borrowed_captures() {
    let mut slots = [RouteSlot::EMPTY; 5];
    let mut routes = RouteTable::new(&mut slots);
    routes
        .register(b"GET", b"/:section/edit", route_config(RouteKind::Managed))
        .unwrap();
    routes
        .register(b"GET", b"/users/:id/edit", route_config(RouteKind::Static))
        .unwrap();
    routes
        .register(b"GET", b"/users/:id", route_config(RouteKind::Managed))
        .unwrap();
    routes
        .register(b"GET", b"/users/edit", route_config(RouteKind::Static))
        .unwrap();
    routes
        .register(b"GET", b"/*rest", route_config(RouteKind::Static))
        .unwrap();
    routes.freeze().unwrap();

    let input = b"GET /users/42/edit?tab=full HTTP/1.1\r\nHost: test\r\n\r\n";
    let (head, _) = parse_request(input);
    let mut captures = [RouteCapture::default(); 8];
    let matched = routes.find_request(head, input, &mut captures).unwrap();
    assert_eq!(matched.pattern, b"/users/:id/edit");
    assert_eq!(matched.captures[0].value(input).unwrap(), b"42");

    let input = b"GET /users/edit HTTP/1.1\r\nHost: test\r\n\r\n";
    let (head, _) = parse_request(input);
    let matched = routes.find_request(head, input, &mut captures).unwrap();
    assert_eq!(matched.pattern, b"/users/edit");
    assert!(matched.captures.is_empty());

    let input = b"GET /users/42 HTTP/1.1\r\nHost: test\r\n\r\n";
    let (head, _) = parse_request(input);
    let matched = routes.find_request(head, input, &mut captures).unwrap();
    assert_eq!(matched.pattern, b"/users/:id");
    assert_eq!(matched.captures.len(), 1);
    assert_eq!(matched.captures[0].name(matched.pattern).unwrap(), b"id");
    assert_eq!(matched.captures[0].value(input).unwrap(), b"42");
    let mut no_capture_storage = [];
    assert_eq!(
        routes
            .find_request(head, input, &mut no_capture_storage)
            .err(),
        Some(RouteError::CaptureCapacity)
    );

    let absolute = b"GET http://test/users/7?tab=full HTTP/1.1\r\nHost: test\r\n\r\n";
    let (head, _) = parse_request(absolute);
    let matched = routes.find_request(head, absolute, &mut captures).unwrap();
    assert_eq!(matched.captures[0].value(absolute).unwrap(), b"7");

    let absolute_root = b"GET http://test HTTP/1.1\r\nHost: test\r\n\r\n";
    let (head, _) = parse_request(absolute_root);
    let matched = routes
        .find_request(head, absolute_root, &mut captures)
        .unwrap();
    assert_eq!(matched.pattern, b"/*rest");
    assert_eq!(matched.captures[0].value(absolute_root).unwrap(), b"");
    assert_eq!(
        matched.captures[0].value.offset,
        head.target.offset + head.target.length
    );

    let head_input = b"HEAD /users/9 HTTP/1.1\r\nHost: test\r\n\r\n";
    let (head, _) = parse_request(head_input);
    let matched = routes
        .find_request(head, head_input, &mut captures)
        .unwrap();
    assert_eq!(matched.pattern, b"/users/:id");

    let absolute_query = b"GET http://test?tab=full HTTP/1.1\r\nHost: test\r\n\r\n";
    let (head, _) = parse_request(absolute_query);
    let matched = routes
        .find_request(head, absolute_query, &mut captures)
        .unwrap();
    assert_eq!(matched.pattern, b"/*rest");
    assert_eq!(matched.captures[0].value(absolute_query).unwrap(), b"");
    assert_eq!(
        matched.captures[0].value.offset,
        head.target.offset + b"http://test".len()
    );
}

#[test]
fn route_admission_policy_gates_expect_continue_body_reads() {
    let allowed = [b"Digest".as_slice()];
    let config = RouteConfig {
        kind: RouteKind::Managed,
        body: RouteBodyPolicy::Stream {
            max_body_bytes: 16,
            permitted_trailers: &allowed,
        },
    };
    let mut slots = [RouteSlot::EMPTY; 1];
    let mut routes = RouteTable::new(&mut slots);
    routes.register(b"POST", b"/upload", config).unwrap();
    routes.freeze().unwrap();

    let input =
        b"POST /upload HTTP/1.1\r\nHost: test\r\nContent-Length: 5\r\nExpect: 100-continue\r\n\r\n";
    let (head, headers) = parse_request(input);
    let mut captures = [RouteCapture::default(); 2];
    let matched = routes.find_request(head, input, &mut captures).unwrap();
    assert!(matched.body.permits(head));
    let RouteBodyPolicy::Stream {
        max_body_bytes,
        permitted_trailers,
    } = matched.body
    else {
        unreachable!()
    };
    let body_limits = BodyLimits {
        total_body_bytes: max_body_bytes,
        ..BodyLimits::default()
    };
    let mut decoder = RequestBodyDecoder::new(
        head,
        input,
        &headers[..head.header_count],
        permitted_trailers,
        body_limits,
    )
    .unwrap();
    assert_eq!(
        decoder.admit_body(true),
        Ok(RequestBodyAdmission::SendContinue)
    );
    assert_eq!(
        decoder.next(
            b"hello",
            false,
            &mut [Header::default(); 2],
            Limits::default()
        ),
        Err(ParseError::AdmissionPending)
    );
    decoder.continue_sent().unwrap();
    assert!(
        decoder
            .next(
                b"hello",
                false,
                &mut [Header::default(); 2],
                Limits::default()
            )
            .unwrap()
            .event
            .is_some()
    );

    let oversized = b"POST /upload HTTP/1.1\r\nHost: test\r\nContent-Length: 17\r\nExpect: 100-continue\r\n\r\n";
    let (head, headers) = parse_request(oversized);
    let mut captures = [RouteCapture::default(); 2];
    let matched = routes.find_request(head, oversized, &mut captures).unwrap();
    assert!(!matched.body.permits(head));
    let RouteBodyPolicy::Stream {
        permitted_trailers, ..
    } = matched.body
    else {
        unreachable!()
    };
    let mut decoder = RequestBodyDecoder::new(
        head,
        oversized,
        &headers[..head.header_count],
        permitted_trailers,
        BodyLimits::default(),
    )
    .unwrap();
    assert_eq!(
        decoder.admit_body(matched.body.permits(head)),
        Ok(RequestBodyAdmission::Rejected)
    );
}

#[test]
fn rejects_invalid_and_ambiguous_dynamic_patterns() {
    let mut slots = [RouteSlot::EMPTY; 2];
    let mut routes = RouteTable::new(&mut slots);
    for path in [b"/bad%xx".as_slice(), b"/:id/:id", b"/files/*rest/more"] {
        assert_eq!(
            routes.register(b"GET", path, route_config(RouteKind::Static)),
            Err(RouteError::Invalid)
        );
    }

    routes
        .register(b"GET", b"/users/:id", route_config(RouteKind::Static))
        .unwrap();
    routes
        .register(b"GET", b"/users/:name", route_config(RouteKind::Managed))
        .unwrap();
    assert_eq!(routes.freeze(), Err(RouteError::Duplicate));
}

#[test]
fn freezes_and_searches_exact_tables_at_8_100_and_10000_routes() {
    let paths: Vec<String> = (0..10_000)
        .map(|index| format!("/route/{index:05}"))
        .collect();
    let mut slots = vec![RouteSlot::EMPTY; 10_000];

    for route_count in [8, 100, 10_000] {
        let mut routes = RouteTable::new(&mut slots[..route_count]);
        for path in &paths[..route_count] {
            routes
                .register(b"GET", path.as_bytes(), route_config(RouteKind::Static))
                .unwrap();
        }
        routes.freeze().unwrap();

        let target = paths[route_count - 1].as_bytes();
        let request = [
            b"GET ".as_slice(),
            target,
            b" HTTP/1.1\r\nHost: test\r\n\r\n",
        ]
        .concat();
        let (head, _) = parse_request(&request);
        let mut captures = [RouteCapture::default(); 1];
        let matched = routes.find_request(head, &request, &mut captures).unwrap();
        assert_eq!(matched.pattern, target);
    }
}
