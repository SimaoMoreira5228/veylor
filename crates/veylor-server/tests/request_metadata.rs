use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use veylor_router::{MAX_ROUTE_CAPTURES, RouteCapture, match_route};
use veylor_server::{
    HostEventKind, MAX_REQUEST_HEADER_BYTES, MAX_REQUEST_HEADERS, RequestHeadError, RespondOutcome,
    Server, ServerConfig,
};

fn request_event(host: &veylor_server::HostHandle) -> veylor_server::EventSlot {
    let empty = veylor_server::EventSlot {
        kind: HostEventKind::Shutdown,
        token: veylor_runtime::ConnectionToken {
            worker_id: 0,
            slot: 0,
            request_seq: 0,
            stream_id: 0,
            worker_generation: 0,
            generation: 0,
        },
        route_id: 0,
        route_token: 0,
        method_len: 0,
        path_len: 0,
        method_offset: 0,
        path_offset: 0,
    };
    let mut events = [empty; 8];
    let mut methods = [0; 128];
    let mut paths = [0; 16_384];
    let deadline = Instant::now() + Duration::from_secs(5);

    loop {
        let report = host.poll(10_000_000, &mut events, &mut methods, &mut paths);
        if let Some(event) = events[..report.copied]
            .iter()
            .find(|event| event.kind == HostEventKind::Request)
        {
            return *event;
        }

        assert!(Instant::now() < deadline, "request event did not arrive");
    }
}

fn request(server: &veylor_server::RunningServer) -> TcpStream {
    let mut stream = TcpStream::connect(server.addr()).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .write_all(
            b"GET /accounts/42/statement?from=10 HTTP/1.1\r\n\
              Host: localhost\r\n\
              Authorization: Bearer test\r\n\
              X-Trace-Id: abc123\r\n\
              Connection: close\r\n\r\n",
        )
        .unwrap();
    stream
}

fn field(header: veylor_server::RequestHeader, bytes: &[u8]) -> (&[u8], &[u8]) {
    (
        &bytes[header.name.offset..header.name.offset + header.name.length],
        &bytes[header.value.offset..header.value.offset + header.value.length],
    )
}

#[test]
fn managed_h1_copies_bounded_target_query_and_fields() {
    let mut server = Server::bind(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        ServerConfig::default(),
    )
    .unwrap();
    server
        .register_managed(b"GET", b"/accounts/:id/statement", 0)
        .unwrap();
    let running = server.start().unwrap();
    let host = running.host();
    let mut client = request(&running);
    let event = request_event(&host);

    let mut short_headers = [];
    let mut short_bytes = [0xA5; 1];
    assert_eq!(
        host.request_head(event.token, &mut short_headers, &mut short_bytes),
        Err(RequestHeadError::Capacity {
            headers_required: 4,
            bytes_required: 98,
        })
    );
    assert_eq!(short_bytes, [0xA5]);

    let mut headers = [veylor_server::RequestHeader::default(); MAX_REQUEST_HEADERS];
    let mut bytes = [0; MAX_REQUEST_HEADER_BYTES];
    let report = host
        .request_head(event.token, &mut headers, &mut bytes)
        .unwrap();
    let target = &bytes[..report.target.length];
    assert_eq!(target, b"/accounts/42/statement?from=10");
    let query = report.query.unwrap();
    assert_eq!(
        &bytes[query.offset..query.offset + query.length],
        b"from=10"
    );
    assert!(headers[..report.header_count].iter().any(|header| {
        let (name, value) = field(*header, &bytes);
        name.eq_ignore_ascii_case(b"authorization") && value == b"Bearer test"
    }));
    assert!(headers[..report.header_count].iter().any(|header| {
        let (name, value) = field(*header, &bytes);
        name.eq_ignore_ascii_case(b"x-trace-id") && value == b"abc123"
    }));

    let mut captures = [RouteCapture::default(); MAX_ROUTE_CAPTURES];
    let count = match_route(b"/accounts/:id/statement", target, &mut captures).unwrap();
    assert_eq!(count, 1);
    assert_eq!(captures[0].value(target), Some(b"42".as_slice()));
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"done"),
        RespondOutcome::Ok
    );

    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"));

    running.stop_accepting();
    running.join().unwrap();
}
