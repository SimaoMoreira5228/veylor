use super::super::*;
use super::ffi_server::*;
use super::h2c_peer::*;
use super::managed_peer::*;
use crate::abi::VylResult;
use veylor_http2::{HpackEntrySlot, HpackHeader};

#[test]
fn file_mount_h2c_serves_full_range_conditional_matrix() {
    let fixture = FixtureDir::new("h2c-file");
    fixture.write("data.txt", b"0123456789");
    let root = mount_root_bytes(&fixture.path);
    let config = test_config(0);
    let server = unsafe { create(&config) };
    assert_eq!(
        unsafe { mount_raw(server, b"/assets/", &root) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { vyl_managed_static_enable_h2c(server, 0) },
        VylResult::Ok
    );
    assert_eq!(unsafe { vyl_managed_static_start(server) }, VylResult::Ok);
    let h2c = unsafe { h2c_port_of(server) };
    let mut stream = connect_loopback(h2c);
    h2_handshake(&mut stream);
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    // NOTE: 9 date-carrying responses need 521 scratch bytes; 512 predates H2 Date.
    let mut scratch = [0; 1024];
    let mut decoder = h2_decoder(&mut entries, &mut scratch);
    let mut decoded = [HpackHeader::EMPTY; 32];

    h2_send_file(&mut stream, 1, b"GET", b"/assets/data.txt", &[]);
    let (status, headers, body) = h2c_read_full(&mut stream, &mut decoder, &mut decoded, 1);
    assert_eq!(status, 200);
    assert_eq!(body, b"0123456789");
    assert_eq!(h2_header(&headers, "content-type"), Some("text/plain"));
    assert_eq!(h2_header(&headers, "accept-ranges"), Some("bytes"));
    assert_eq!(h2_header(&headers, "content-length"), Some("10"));
    let last_modified = h2_header(&headers, "last-modified")
        .expect("last-modified")
        .to_string();
    assert_eq!(last_modified.len(), 29, "IMF-fixdate: {last_modified:?}");

    h2_send_file(&mut stream, 3, b"HEAD", b"/assets/data.txt", &[]);
    let (status, headers, body) = h2c_read_full(&mut stream, &mut decoder, &mut decoded, 3);
    assert_eq!(status, 200);
    assert!(body.is_empty(), "HEAD carries no body");
    assert_eq!(h2_header(&headers, "content-length"), Some("10"));
    assert_eq!(h2_header(&headers, "content-type"), Some("text/plain"));
    assert_eq!(h2_header(&headers, "content-range"), None);

    h2_send_file(
        &mut stream,
        5,
        b"HEAD",
        b"/assets/data.txt",
        &[(b"range" as &[u8], b"bytes=0-1" as &[u8])],
    );
    let (status, headers, body) = h2c_read_full(&mut stream, &mut decoder, &mut decoded, 5);
    assert_eq!(status, 200, "HEAD ignores Range");
    assert!(body.is_empty(), "HEAD carries no body");
    assert_eq!(h2_header(&headers, "content-range"), None);
    assert_eq!(h2_header(&headers, "content-length"), Some("10"));

    h2_send_file(
        &mut stream,
        7,
        b"GET",
        b"/assets/data.txt",
        &[(b"range" as &[u8], b"bytes=2-5" as &[u8])],
    );
    let (status, headers, body) = h2c_read_full(&mut stream, &mut decoder, &mut decoded, 7);
    assert_eq!(status, 206);
    assert_eq!(body, b"2345");
    assert_eq!(h2_header(&headers, "content-range"), Some("bytes 2-5/10"));
    assert_eq!(h2_header(&headers, "accept-ranges"), Some("bytes"));
    assert_eq!(h2_header(&headers, "content-length"), Some("4"));

    h2_send_file(
        &mut stream,
        9,
        b"GET",
        b"/assets/data.txt",
        &[(b"range" as &[u8], b"bytes=0-1, 4-5" as &[u8])],
    );
    let (status, headers, body) = h2c_read_full(&mut stream, &mut decoder, &mut decoded, 9);
    assert_eq!(status, 206);
    let ctype = h2_header(&headers, "content-type").expect("multipart content-type");
    assert!(
        ctype.starts_with("multipart/byteranges; boundary="),
        "ctype: {ctype:?}"
    );
    assert_eq!(h2_header(&headers, "content-range"), None);
    let framed: usize = h2_header(&headers, "content-length")
        .expect("multipart length")
        .parse()
        .expect("length int");
    assert_eq!(framed, body.len(), "multipart length frames the whole body");
    let text = String::from_utf8(body).expect("multipart utf8");
    assert!(
        text.contains("Content-Range: bytes 0-1/10"),
        "body: {text:?}"
    );
    assert!(
        text.contains("Content-Range: bytes 4-5/10"),
        "body: {text:?}"
    );
    assert!(text.contains("01"), "body: {text:?}");
    assert!(text.contains("45"), "body: {text:?}");

    h2_send_file(
        &mut stream,
        11,
        b"GET",
        b"/assets/data.txt",
        &[(b"range" as &[u8], b"bytes=99-" as &[u8])],
    );
    let (status, headers, body) = h2c_read_full(&mut stream, &mut decoder, &mut decoded, 11);
    assert_eq!(status, 416);
    assert_eq!(h2_header(&headers, "content-range"), Some("bytes */10"));
    assert_eq!(h2_header(&headers, "content-type"), None);
    assert!(body.is_empty(), "416 carries no body");

    h2_send_file(
        &mut stream,
        13,
        b"GET",
        b"/assets/data.txt",
        &[(b"if-modified-since" as &[u8], last_modified.as_bytes())],
    );
    let (status, headers, body) = h2c_read_full(&mut stream, &mut decoder, &mut decoded, 13);
    assert_eq!(status, 304);
    assert!(body.is_empty(), "304 carries no body");
    assert_eq!(
        h2_header(&headers, "last-modified"),
        Some(last_modified.as_str())
    );

    h2_send_file(
        &mut stream,
        15,
        b"GET",
        b"/assets/data.txt",
        &[(
            b"if-modified-since" as &[u8],
            b"Thu, 01 Jan 1970 00:00:00 GMT" as &[u8],
        )],
    );
    let (status, _, body) = h2c_read_full(&mut stream, &mut decoder, &mut decoded, 15);
    assert_eq!(status, 200);
    assert_eq!(body, b"0123456789");

    h2_send_file(&mut stream, 17, b"GET", b"/assets/missing.txt", &[]);
    let (status, _, body) = h2c_read_full(&mut stream, &mut decoder, &mut decoded, 17);
    assert_eq!(status, 404);
    assert_eq!(body, b"Not Found");

    let counts = unsafe { stop_join(server) };
    assert_eq!(counts.shutdown, 1, "counts: {counts:?}");
    assert_eq!(unsafe { vyl_managed_static_destroy(server) }, VylResult::Ok);
}
