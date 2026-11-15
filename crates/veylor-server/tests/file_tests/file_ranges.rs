use crate::file_fixture::*;
use std::fs;
use veylor_server::{RangeApply, read_chunk};

#[test]
fn range_exact_open_suffix_and_clamp_set_partial_windows() {
    let fixture = Fixture::build();
    let mounts = fixture.mounts();
    let mut exact = mounts
        .resolve(b"GET", b"/assets/index.html")
        .expect("index plan");
    assert_eq!(exact.apply_range(Some(b"bytes=0-3")), RangeApply::Ranged);
    assert!(exact.partial);
    assert_eq!((exact.start, exact.size, exact.total), (0, 4, 18));
    let mut open = mounts
        .resolve(b"GET", b"/assets/index.html")
        .expect("index plan");
    assert_eq!(open.apply_range(Some(b"bytes=4-")), RangeApply::Ranged);
    assert_eq!((open.start, open.size, open.total), (4, 14, 18));
    let mut suffix = mounts
        .resolve(b"GET", b"/assets/index.html")
        .expect("index plan");
    assert_eq!(suffix.apply_range(Some(b"bytes=-4")), RangeApply::Ranged);
    assert_eq!((suffix.start, suffix.size, suffix.total), (14, 4, 18));
    let mut clamp = mounts
        .resolve(b"GET", b"/assets/index.html")
        .expect("index plan");
    assert_eq!(clamp.apply_range(Some(b"bytes=10-100")), RangeApply::Ranged);
    assert_eq!((clamp.start, clamp.size, clamp.total), (10, 8, 18));
    let mut oversuffix = mounts
        .resolve(b"GET", b"/assets/index.html")
        .expect("index plan");
    assert_eq!(
        oversuffix.apply_range(Some(b"bytes=-100")),
        RangeApply::Ranged
    );
    assert_eq!(
        (oversuffix.start, oversuffix.size, oversuffix.total),
        (0, 18, 18)
    );
    let mut upper = mounts
        .resolve(b"GET", b"/assets/index.html")
        .expect("index plan");
    assert_eq!(upper.apply_range(Some(b"BYTES=0-3")), RangeApply::Ranged);
    assert_eq!((upper.start, upper.size), (0, 4));
}

#[test]
fn range_unsatisfiable_reports_without_mutating_plan() {
    let fixture = Fixture::build();
    let mounts = fixture.mounts();
    for header in [b"bytes=18-".as_slice(), b"bytes=20-30", b"bytes=18-18"] {
        let mut plan = mounts
            .resolve(b"GET", b"/assets/index.html")
            .expect("index plan");
        assert_eq!(plan.apply_range(Some(header)), RangeApply::Unsatisfiable);
        assert!(!plan.partial);
        assert_eq!((plan.start, plan.size, plan.total), (0, 18, 18));
    }
    let mut empty = mounts
        .resolve(b"GET", b"/assets/empty.txt")
        .expect("empty plan");
    assert_eq!(
        empty.apply_range(Some(b"bytes=0-")),
        RangeApply::Unsatisfiable
    );
}

#[test]
fn range_ignored_headers_leave_full_plan() {
    let fixture = Fixture::build();
    let mounts = fixture.mounts();
    let ignored: Vec<Option<&[u8]>> = vec![
        None,
        Some(b"items=0-1"),
        Some(b"bytes=abc"),
        Some(b"bytes=5-3"),
        Some(b"bytes=-0"),
        Some(b"bytes="),
        Some(b"bytes"),
        Some(b"bytes=0 - 3"),
    ];
    for header in ignored {
        let mut plan = mounts
            .resolve(b"GET", b"/assets/index.html")
            .expect("index plan");
        assert_eq!(plan.apply_range(header), RangeApply::Ignored);
        assert!(!plan.partial);
        assert_eq!((plan.start, plan.size, plan.total), (0, 18, 18));
    }
}

#[test]
fn range_overflow_saturates_to_deterministic_outcomes() {
    let fixture = Fixture::build();
    let mounts = fixture.mounts();
    let mut past = mounts
        .resolve(b"GET", b"/assets/index.html")
        .expect("index plan");
    assert_eq!(
        past.apply_range(Some(b"bytes=18446744073709551615-")),
        RangeApply::Unsatisfiable
    );
    let mut clamped = mounts
        .resolve(b"GET", b"/assets/index.html")
        .expect("index plan");
    assert_eq!(
        clamped.apply_range(Some(b"bytes=0-18446744073709551615")),
        RangeApply::Ranged
    );
    assert_eq!((clamped.start, clamped.size), (0, 18));
    let mut suffix = mounts
        .resolve(b"GET", b"/assets/index.html")
        .expect("index plan");
    assert_eq!(
        suffix.apply_range(Some(b"bytes=-18446744073709551615")),
        RangeApply::Ranged
    );
    assert_eq!((suffix.start, suffix.size), (0, 18));
}

#[test]
fn range_window_reads_exact_slice_bytes() {
    let fixture = Fixture::build();
    let mounts = fixture.mounts();
    let mut plan = mounts
        .resolve(b"GET", b"/assets/index.html")
        .expect("index plan");
    assert_eq!(plan.apply_range(Some(b"bytes=2-5")), RangeApply::Ranged);
    let mut assembled = Vec::new();
    let mut offset = 0u64;
    let mut out = [0u8; 2];
    while offset < plan.size {
        let got = read_chunk(&plan, offset, &mut out).expect("window read");
        assert!(got > 0, "window stalls before its length");
        assembled.extend_from_slice(&out[..got]);
        offset += got as u64;
    }
    assert_eq!(assembled, b"tml>");
}

fn multipart_body(
    boundary: &[u8],
    content_type: &[u8],
    total: u64,
    windows: &[(u64, u64)],
    file: &[u8],
) -> Vec<u8> {
    let mut body = Vec::new();
    for (start, end) in windows.iter().copied() {
        body.extend_from_slice(b"--");
        body.extend_from_slice(boundary);
        body.extend_from_slice(b"\r\nContent-Type: ");
        body.extend_from_slice(content_type);
        body.extend_from_slice(b"\r\nContent-Range: bytes ");
        body.extend_from_slice(format!("{start}-{}", end - 1).as_bytes());
        body.extend_from_slice(b"/");
        body.extend_from_slice(total.to_string().as_bytes());
        body.extend_from_slice(b"\r\n\r\n");
        body.extend_from_slice(&file[start as usize..end as usize]);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(b"--");
    body.extend_from_slice(boundary);
    body.extend_from_slice(b"--\r\n");
    body
}

fn multipart_boundary_of(plan: &veylor_server::FilePlan) -> Vec<u8> {
    let mut first = [0u8; 512];
    let got = read_chunk(plan, 0, &mut first).expect("first stage");
    assert!(first[..got].starts_with(b"--"), "opens with delimiter");
    let crlf = first[..got]
        .iter()
        .position(|byte| *byte == b'\r')
        .expect("first line ends");
    first[2..crlf].to_vec()
}

#[test]
fn range_set_two_valid_reports_multipart_and_pages_exact() {
    let fixture = Fixture::build();
    let mounts = fixture.mounts();
    let mut plan = mounts
        .resolve(b"GET", b"/assets/index.html")
        .expect("index plan");
    assert_eq!(
        plan.apply_range(Some(b"bytes=0-3,6-9")),
        RangeApply::Multipart
    );
    assert!(plan.partial && plan.multipart);
    assert_eq!(plan.range_count, 2);
    assert_eq!((plan.ranges[0], plan.ranges[1]), ((0, 4), (6, 10)));
    assert_eq!(plan.total, 18);
    let file = fs::read(fixture.root.join("index.html")).expect("index bytes");
    let boundary = multipart_boundary_of(&plan);
    let expected = multipart_body(&boundary, b"text/html", 18, &[(0, 4), (6, 10)], &file);
    assert_eq!(plan.size, expected.len() as u64);
    let mut repeat = mounts
        .resolve(b"GET", b"/assets/index.html")
        .expect("index plan");
    assert_eq!(
        repeat.apply_range(Some(b"bytes=0-3,6-9")),
        RangeApply::Multipart
    );
    assert_eq!(multipart_boundary_of(&repeat), boundary);
    let mut assembled = Vec::new();
    let mut offset = 0u64;
    let mut turns = 0u32;
    let mut out = [0u8; 7];
    while offset < plan.size {
        let got = read_chunk(&plan, offset, &mut out).expect("part page");
        assert!(got > 0 && got <= out.len(), "bounded staging");
        assembled.extend_from_slice(&out[..got]);
        offset += got as u64;
        turns += 1;
    }
    assert_eq!(assembled, expected);
    assert!(turns > 1, "pages across turns: {turns}");
    assert_eq!(read_chunk(&plan, plan.size, &mut out).expect("at end"), 0);
    assert!(read_chunk(&plan, plan.size + 1, &mut out).is_err());
}

#[test]
fn range_set_mixed_unsatisfiable_and_malformed_split_outcomes() {
    let fixture = Fixture::build();
    let mounts = fixture.mounts();
    for (header, window) in [
        (b"bytes=0-3,99-".as_slice(), (0, 4)),
        (b"bytes=99-,0-3".as_slice(), (0, 4)),
        (b"bytes=0-1,".as_slice(), (0, 2)),
        (b"bytes=0-1 ,99-".as_slice(), (0, 2)),
    ] {
        let mut plan = mounts
            .resolve(b"GET", b"/assets/index.html")
            .expect("index plan");
        assert_eq!(plan.apply_range(Some(header)), RangeApply::Ranged);
        assert!(!plan.multipart);
        assert_eq!((plan.start, plan.size), (window.0, window.1 - window.0));
    }
    for header in [
        b"bytes=99-,100-200".as_slice(),
        b"bytes=99-,nope".as_slice(),
    ] {
        let mut plan = mounts
            .resolve(b"GET", b"/assets/index.html")
            .expect("index plan");
        assert_eq!(plan.apply_range(Some(header)), RangeApply::Unsatisfiable);
        assert!(!plan.partial && !plan.multipart);
        assert_eq!((plan.start, plan.size, plan.total), (0, 18, 18));
    }
    for header in [b"bytes=a-b,c-d".as_slice(), b"bytes=,".as_slice()] {
        let mut plan = mounts
            .resolve(b"GET", b"/assets/index.html")
            .expect("index plan");
        assert_eq!(plan.apply_range(Some(header)), RangeApply::Ignored);
        assert!(!plan.partial && !plan.multipart);
        assert_eq!((plan.start, plan.size, plan.total), (0, 18, 18));
    }
    let mut over = mounts
        .resolve(b"GET", b"/assets/index.html")
        .expect("index plan");
    assert_eq!(
        over.apply_range(Some(b"bytes=0-0,1-1,2-2,3-3,4-4,5-5,6-6,7-7,8-8")),
        RangeApply::Ignored
    );
    assert!(!over.partial && !over.multipart);
    assert_eq!((over.start, over.size, over.total), (0, 18, 18));
    let mut spaced = mounts
        .resolve(b"GET", b"/assets/index.html")
        .expect("index plan");
    assert_eq!(
        spaced.apply_range(Some(b"bytes=0-1, 3-4")),
        RangeApply::Multipart
    );
    assert_eq!((spaced.ranges[0], spaced.ranges[1]), ((0, 2), (3, 5)));
    let mut empty = mounts
        .resolve(b"GET", b"/assets/empty.txt")
        .expect("empty plan");
    assert_eq!(
        empty.apply_range(Some(b"bytes=0-,1-")),
        RangeApply::Unsatisfiable
    );
}

#[test]
fn multipart_large_body_pages_in_bounded_turns_without_buffering() {
    let fixture = Fixture::build();
    let mounts = fixture.mounts();
    let mut plan = mounts
        .resolve(b"GET", b"/assets/blob.bin")
        .expect("blob plan");
    assert_eq!(
        plan.apply_range(Some(b"bytes=0-262143,262144-524287,524288-786431")),
        RangeApply::Multipart
    );
    let boundary = multipart_boundary_of(&plan);
    let expected = multipart_body(
        &boundary,
        b"application/octet-stream",
        BLOB_LEN as u64,
        &[(0, 262144), (262144, 524288), (524288, 786432)],
        &fixture.blob,
    );
    assert_eq!(plan.size, expected.len() as u64);
    let mut assembled = Vec::with_capacity(expected.len());
    let mut offset = 0u64;
    let mut turns = 0u32;
    let mut high = 0usize;
    let mut out = [0u8; 1024];
    while offset < plan.size {
        let got = read_chunk(&plan, offset, &mut out).expect("blob page");
        assert!(got > 0 && got <= out.len(), "bounded staging");
        high = high.max(got);
        assembled.extend_from_slice(&out[..got]);
        offset += got as u64;
        turns += 1;
    }
    assert_eq!(assembled, expected);
    assert!(turns > 700, "streams across turns: {turns}");
    assert_eq!(high, out.len(), "fills the staging buffer");
}
