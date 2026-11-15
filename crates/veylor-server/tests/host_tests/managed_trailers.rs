use crate::host_peer::*;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use veylor_runtime::ConnectionToken;
use veylor_server::{
    HostHandle, MAX_STATIC_BODY_BYTES, RespondOutcome, Server, ServerConfig, ServerError,
    TrailerOutcome, TrailerReport,
};

fn pull_trailer_set(
    host: &HostHandle,
    token: ConnectionToken,
) -> (TrailerReport, Vec<u8>, Vec<u8>) {
    let mut names = vec![0; 256];
    let mut values = vec![0; 256];
    let report = host.pull_trailers(token, &mut names, &mut values);
    names.truncate(report.names_copied);
    values.truncate(report.values_copied);
    (report, names, values)
}

#[test]
fn managed_trailers_listed_stage_and_copy_exact() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    stream
        .write_all(b"POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n5\r\nhello\r\n0\r\nX-Checksum: abc123\r\n\r\n")
        .expect("write chunks with listed trailer");
    let (event, _, _) = poll_request(&host);
    assert_eq!(pull_body_complete(&host, event.token), b"hello");
    let mut tiny_names = [0; 4];
    let mut tiny_values = [0; 64];
    let short = host.pull_trailers(event.token, &mut tiny_names, &mut tiny_values);
    assert_eq!(short.outcome, TrailerOutcome::Ok);
    assert_eq!(short.fields, 1);
    assert_eq!(short.names_copied, 0);
    assert_eq!(short.values_copied, 0);
    assert_eq!(short.names_required, 10);
    assert_eq!(short.values_required, 6);
    let (report, names, values) = pull_trailer_set(&host, event.token);
    assert_eq!(report.outcome, TrailerOutcome::Ok);
    assert_eq!(report.fields, 1);
    assert_eq!(report.names_copied, 10);
    assert_eq!(report.values_copied, 6);
    assert_eq!(names, b"X-Checksum");
    assert_eq!(values, b"abc123");
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"done"),
        RespondOutcome::Ok
    );
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, b"done");
    get(&mut stream, "/work");
    let (second, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(second.token, 200, b"OK", b"again"),
        RespondOutcome::Ok
    );
    let reused = read_response(&mut stream);
    assert_eq!(reused.status, 200);
    assert_eq!(reused.body, b"again");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_trailers_pull_before_complete_not_ready() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    stream
        .write_all(b"POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n5\r\nhello\r\n")
        .expect("write data chunk without terminal");
    let (event, _, _) = poll_request(&host);
    let mut names = [0; 64];
    let mut values = [0; 64];
    assert_eq!(
        host.pull_trailers(event.token, &mut names, &mut values)
            .outcome,
        TrailerOutcome::NotReady
    );
    stream
        .write_all(b"0\r\n\r\n")
        .expect("terminal chunk without trailers");
    assert_eq!(pull_body_complete(&host, event.token), b"hello");
    let (report, names, values) = pull_trailer_set(&host, event.token);
    assert_eq!(report.outcome, TrailerOutcome::Ok);
    assert_eq!(report.fields, 0);
    assert!(names.is_empty() && values.is_empty());
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"done"),
        RespondOutcome::Ok
    );
    let response = read_response(&mut stream);
    assert_eq!(response.body, b"done");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_trailers_over_count_closes() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    let mut request = b"POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n3\r\nabc\r\n0\r\n".to_vec();
    for index in 0..9 {
        request.extend_from_slice(format!("X-Checksum: v{index}\r\n").as_bytes());
    }
    request.extend_from_slice(b"\r\n");
    stream
        .write_all(&request)
        .expect("write nine listed trailers");
    let (event, _, _) = poll_request(&host);
    expect_eof(&mut stream);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"late"),
        RespondOutcome::Gone,
        "ninth staged field breaches slot capacity and fails closed"
    );
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_trailers_over_bytes_close() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    let mut request = b"POST /upload HTTP/1.1\r\nHost: 127.0.0.1\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n3\r\nabc\r\n0\r\nX-Checksum: ".to_vec();
    request.extend_from_slice(&[b'a'; 2048]);
    request.extend_from_slice(b"\r\n\r\n");
    stream
        .write_all(&request)
        .expect("write oversized trailer value");
    let (event, _, _) = poll_request(&host);
    expect_eof(&mut stream);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"late"),
        RespondOutcome::Gone,
        "staged trailer bytes past 1 KiB fail closed"
    );
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_trailers_registration_rejects_bad_allowlist() {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, ServerConfig::default()).expect("bind loopback");
    let overlong: Vec<Vec<u8>> = (0..33)
        .map(|index| format!("x-t{index:02}").into_bytes())
        .collect();
    let refs: Vec<&[u8]> = overlong.iter().map(Vec::as_slice).collect();
    assert!(
        matches!(
            server.register_managed_with_trailers(b"POST", b"/t", MAX_STATIC_BODY_BYTES, &refs),
            Err(ServerError::InvalidRoute)
        ),
        "allowlist past 32 names rejects at registration"
    );
    assert!(
        matches!(
            server.register_managed_with_trailers(
                b"POST",
                b"/t",
                MAX_STATIC_BODY_BYTES,
                &[b"bad name"]
            ),
            Err(ServerError::InvalidRoute)
        ),
        "non-token trailer name rejects at registration"
    );
    assert!(
        matches!(
            server.register_managed_with_trailers(
                b"POST",
                b"/t",
                MAX_STATIC_BODY_BYTES,
                &[b"x-a", b"X-A"]
            ),
            Err(ServerError::InvalidRoute)
        ),
        "duplicate trailer name rejects at registration"
    );
    server
        .register_managed_with_trailers(b"POST", b"/t", MAX_STATIC_BODY_BYTES, &[b"x-a"])
        .expect("valid allowlist registers");
}

fn read_trailer_block(stream: &mut TcpStream) -> Vec<u8> {
    let mut block = Vec::new();
    let mut byte = [0; 1];
    while !block.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte) {
            Ok(0) => panic!("eof mid-trailers"),
            Ok(_) => block.push(byte[0]),
            Err(error) => panic!("read trailers: {error:?}"),
        }
        assert!(block.len() < 65536, "trailer overflow");
    }
    block
}

#[test]
fn managed_stream_end_with_trailers_exact_and_reuses() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    wait_writable(&host, event.token);
    assert_eq!(host.write_stream(event.token, b"foo"), RespondOutcome::Ok);
    wait_writable(&host, event.token);
    let fields: &[(&[u8], &[u8])] = &[(b"x-sum", b"ok"), (b"x-n", b"7")];
    assert_eq!(
        host.end_stream_with_trailers(event.token, fields),
        RespondOutcome::Ok
    );
    let head = read_raw_head(&mut stream);
    stream_head_exact(&head, "HTTP/1.1 200 OK");
    assert_eq!(read_chunk_frame(&mut stream), Some(b"foo".to_vec()));
    let mut size = [0; 3];
    stream.read_exact(&mut size).expect("terminal size");
    assert_eq!(&size, b"0\r\n");
    assert_eq!(
        read_trailer_block(&mut stream),
        b"x-sum: ok\r\nx-n: 7\r\n\r\n",
        "terminal chunk carries the trailer block"
    );
    assert_eq!(
        host.end_stream(event.token),
        RespondOutcome::Gone,
        "applied terminal unparks"
    );
    get(&mut stream, "/work");
    let (second, _, _) = poll_request(&host);
    assert_eq!(
        host.respond(second.token, 200, b"OK", b"again"),
        RespondOutcome::Ok
    );
    let reused = read_response(&mut stream);
    assert_eq!(reused.status, 200);
    assert_eq!(reused.body, b"again");
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_stream_end_with_trailers_invalid_stages_nothing() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    wait_writable(&host, event.token);
    let bad_names: &[&[u8]] = &[
        b"authorization",
        b"content-encoding",
        b"content-length",
        b"content-range",
        b"content-type",
        b"transfer-encoding",
        b"proxy-authorization",
        b"proxy-connection",
        b"te",
        b"trailer",
        b"expect",
        b"host",
        b"connection",
        b"upgrade",
        b"keep-alive",
        b"set-cookie",
        b"",
        b":pseudo",
        b"bad name",
    ];
    for name in bad_names {
        let fields: &[(&[u8], &[u8])] = &[(*name, b"v")];
        assert_eq!(
            host.end_stream_with_trailers(event.token, fields),
            RespondOutcome::Invalid,
            "prohibited trailer name rejects"
        );
        assert!(
            host.is_writable(event.token),
            "rejected trailers stage nothing"
        );
    }
    let bad_value: &[(&[u8], &[u8])] = &[(b"x-ok", b"a\r\nb")];
    assert_eq!(
        host.end_stream_with_trailers(event.token, bad_value),
        RespondOutcome::Invalid,
        "CRLF value rejects"
    );
    assert!(host.is_writable(event.token));
    let many: &[(&[u8], &[u8])] = &[
        (b"x-0", b"v"),
        (b"x-1", b"v"),
        (b"x-2", b"v"),
        (b"x-3", b"v"),
        (b"x-4", b"v"),
        (b"x-5", b"v"),
        (b"x-6", b"v"),
        (b"x-7", b"v"),
        (b"x-8", b"v"),
    ];
    assert_eq!(
        host.end_stream_with_trailers(event.token, many),
        RespondOutcome::Invalid,
        "ninth field rejects"
    );
    assert!(host.is_writable(event.token));
    let big = vec![b'v'; 1025];
    let big_field: &[(&[u8], &[u8])] = &[(b"x-big", &big)];
    assert_eq!(
        host.end_stream_with_trailers(event.token, big_field),
        RespondOutcome::Invalid,
        "past-1KiB staged bytes reject"
    );
    assert!(host.is_writable(event.token));
    assert_eq!(
        host.write_stream(event.token, b"recovered"),
        RespondOutcome::Ok
    );
    wait_writable(&host, event.token);
    assert_eq!(host.end_stream(event.token), RespondOutcome::Ok);
    let (status, raw, body) = read_chunked(&mut stream);
    assert_eq!(status, 200);
    assert_eq!(body, b"recovered");
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("head end")
        + 4;
    stream_head_exact(&raw[..split], "HTTP/1.1 200 OK");
    assert_eq!(
        &raw[split..],
        b"9\r\nrecovered\r\n0\r\n\r\n",
        "bare end unchanged after rejected trailers"
    );
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_stream_write_after_trailers_fails_closed() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    wait_writable(&host, event.token);
    assert_eq!(host.write_stream(event.token, b"a"), RespondOutcome::Ok);
    wait_writable(&host, event.token);
    let fields: &[(&[u8], &[u8])] = &[(b"x-done", b"yes")];
    assert_eq!(
        host.end_stream_with_trailers(event.token, fields),
        RespondOutcome::Ok
    );
    // NOTE: the staged terminal races the apply like a bare end: WouldBlock while queued, Gone
    // after; a repeat terminal is Duplicate while queued. Every iteration is legal, so neither
    // loop can flake, and no write is ever admitted past the terminal.
    let mut spins = 0u32;
    loop {
        match host.write_stream(event.token, b"late") {
            RespondOutcome::WouldBlock | RespondOutcome::NotWritable => spins += 1,
            RespondOutcome::Gone => break,
            other => panic!("unexpected write outcome: {other:?}"),
        }
        assert!(spins < 1_000_000, "staged terminal never applied");
    }
    loop {
        match host.end_stream_with_trailers(event.token, fields) {
            RespondOutcome::Duplicate => spins += 1,
            RespondOutcome::Gone => break,
            other => panic!("unexpected terminal outcome: {other:?}"),
        }
        assert!(spins < 1_000_000, "staged terminal never applied");
    }
    let head = read_raw_head(&mut stream);
    stream_head_exact(&head, "HTTP/1.1 200 OK");
    assert_eq!(read_chunk_frame(&mut stream), Some(b"a".to_vec()));
    let mut size = [0; 3];
    stream.read_exact(&mut size).expect("terminal size");
    assert_eq!(&size, b"0\r\n");
    assert_eq!(
        read_trailer_block(&mut stream),
        b"x-done: yes\r\n\r\n",
        "refused writes leave the trailer block exact"
    );
    server.stop_accepting();
    let _ = server.join().expect("join server");
}

#[test]
fn managed_stream_end_then_trailers_fails_closed() {
    let server = default_managed_server();
    let host = server.host();
    let mut stream = connect(&server);
    get(&mut stream, "/work");
    let (event, _, _) = poll_request(&host);
    assert_eq!(
        host.begin_stream(event.token, 200, b"OK"),
        RespondOutcome::Ok
    );
    wait_writable(&host, event.token);
    assert_eq!(host.end_stream(event.token), RespondOutcome::Ok);
    let fields: &[(&[u8], &[u8])] = &[(b"x-late", b"1")];
    let mut spins = 0u32;
    loop {
        match host.end_stream_with_trailers(event.token, fields) {
            RespondOutcome::Duplicate => spins += 1,
            RespondOutcome::Gone => break,
            other => panic!("unexpected terminal outcome: {other:?}"),
        }
        assert!(spins < 1_000_000, "staged end never applied");
    }
    let head = read_raw_head(&mut stream);
    stream_head_exact(&head, "HTTP/1.1 200 OK");
    let mut terminal = [0; 5];
    stream.read_exact(&mut terminal).expect("terminal");
    assert_eq!(&terminal, b"0\r\n\r\n", "bare end carries no trailer block");
    assert_eq!(host.end_stream(event.token), RespondOutcome::Gone);
    server.stop_accepting();
    let _ = server.join().expect("join server");
}
