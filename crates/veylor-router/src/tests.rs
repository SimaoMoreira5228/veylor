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
