use super::{RouteBodyPolicy, RouteConfig, RouteError, RouteKind, RouteSlot, RouteTable};

fn config() -> RouteConfig<'static> {
    RouteConfig {
        kind: RouteKind::Static,
        body: RouteBodyPolicy::Reject,
    }
}

#[test]
fn nine_capture_segments_rejected_at_eight() {
    let mut slots = [RouteSlot::EMPTY; 4];
    let mut table = RouteTable::new(&mut slots);
    // NOTE: eight dynamic segments register (the bound); the ninth fails closed
    // at pattern inspection, before any slot or capture storage is touched.
    table
        .register(b"GET", b"/a/:p1/:p2/:p3/:p4/:p5/:p6/:p7/:p8", config())
        .expect("eight captures fit");
    assert_eq!(
        table.register(b"GET", b"/a/:p1/:p2/:p3/:p4/:p5/:p6/:p7/:p8/:p9", config()),
        Err(RouteError::Invalid)
    );
}

#[test]
fn trailer_allowlist_bounds_reject_overflow() {
    let mut slots = [RouteSlot::EMPTY; 4];
    let mut table = RouteTable::new(&mut slots);
    let names: [&[u8]; 33] = [&b"x-tag"[..]; 33];
    // NOTE: over-long allowlists fail at registration, before any route or
    // trailer state exists; over-long names fail the same way.
    assert_eq!(
        table.register(
            b"POST",
            b"/upload",
            RouteConfig {
                kind: RouteKind::Managed,
                body: RouteBodyPolicy::Stream {
                    max_body_bytes: u64::MAX,
                    permitted_trailers: &names,
                },
            },
        ),
        Err(RouteError::Invalid)
    );
    let long = [b'a'; 129];
    assert_eq!(
        table.register(
            b"POST",
            b"/upload",
            RouteConfig {
                kind: RouteKind::Managed,
                body: RouteBodyPolicy::Stream {
                    max_body_bytes: u64::MAX,
                    permitted_trailers: &[&long],
                },
            },
        ),
        Err(RouteError::Invalid)
    );
}

#[test]
fn validated_matcher_agrees_with_route_matching() {
    use super::{MAX_ROUTE_CAPTURES, RouteCapture, match_route, matches_route};

    let patterns: [&[u8]; 9] = [
        b"/",
        b"/a",
        b"/a/b",
        b"/a/b/",
        b"/a/:name",
        b"/a/:name/b",
        b"/a/:name/:value",
        b"/a/*rest",
        b"/json/:count",
    ];
    let paths: [&[u8]; 15] = [
        b"/",
        b"//",
        b"/a",
        b"/a/",
        b"/a/b",
        b"/a/b/",
        b"/a/b/c",
        b"/a/x",
        b"/a/x/b",
        b"/a/x/y",
        b"/json/25",
        b"/json/25/extra",
        b"/A/B",
        b"/a/x?from=10",
        b"/json/25?m=4",
    ];
    let mut captures = [RouteCapture::default(); MAX_ROUTE_CAPTURES];
    for pattern in patterns {
        for path in paths {
            let fast = matches_route(pattern, path);
            let full = match_route(pattern, path, &mut captures).is_some();
            assert_eq!(
                fast,
                full,
                "pattern={} path={} fast={fast} full={full}",
                String::from_utf8_lossy(pattern),
                String::from_utf8_lossy(path)
            );
        }
    }
}
