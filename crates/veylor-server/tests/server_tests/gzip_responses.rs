use crate::h1_peer::*;
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::sync::OnceLock;
use veylor_server::{NativeRequestView, NativeResponseOut, RunningServer, Server, ServerConfig};

fn get_with_encoding(stream: &mut TcpStream, path: &str, extra: &str) {
    let request =
        format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: keep-alive\r\n{extra}\r\n");
    stream.write_all(request.as_bytes()).expect("write request");
}

fn gunzip(body: &[u8]) -> Vec<u8> {
    use std::io::Read;
    let mut decoder = flate2::read::GzDecoder::new(body);
    let mut plain = Vec::new();
    decoder.read_to_end(&mut plain).expect("gunzip body");
    plain
}

#[test]
fn gzip_static_body_round_trips_with_encoding_headers() {
    let server = gzip_server();
    let mut stream = connect(&server);
    get_with_encoding(&mut stream, "/gzip", "Accept-Encoding: gzip\r\n");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(header(&response, "content-encoding"), "gzip");
    assert_eq!(header(&response, "vary"), "Accept-Encoding");
    date_shape(&response);
    assert!(response.body.len() < gzip_body().len());
    assert_eq!(gunzip(&response.body), gzip_body());
    let length: usize = header(&response, "content-length")
        .parse()
        .expect("length int");
    assert_eq!(length, response.body.len());
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed + counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn gzip_identity_fallback_without_accept_encoding() {
    let server = gzip_server();
    let mut stream = connect(&server);
    get_with_encoding(&mut stream, "/gzip", "");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, gzip_body());
    assert!(
        response
            .headers
            .iter()
            .all(|(name, _)| !name.eq_ignore_ascii_case("content-encoding")),
        "no content-encoding"
    );
    assert_eq!(header(&response, "vary"), "Accept-Encoding");
    date_shape(&response);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed + counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn gzip_q0_disables_twin() {
    let server = gzip_server();
    let mut stream = connect(&server);
    get_with_encoding(&mut stream, "/gzip", "Accept-Encoding: gzip;q=0\r\n");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, gzip_body());
    assert!(
        response
            .headers
            .iter()
            .all(|(name, _)| !name.eq_ignore_ascii_case("content-encoding")),
        "no content-encoding"
    );
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed + counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn gzip_head_reports_compressed_length() {
    let server = gzip_server();
    let mut stream = connect(&server);
    let request = "HEAD /gzip HTTP/1.1\r\nHost: 127.0.0.1\r\nAccept-Encoding: gzip\r\nConnection: close\r\n\r\n";
    stream.write_all(request.as_bytes()).expect("write HEAD");
    let text = read_head(&mut stream);
    assert!(text.starts_with("HTTP/1.1 200 OK\r\n"), "status: {text}");
    assert!(text.contains("Content-Encoding: gzip"), "encoding: {text}");
    assert!(text.contains("Vary: Accept-Encoding"), "vary: {text}");
    let gzipped = {
        use std::io::Write;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&gzip_body()).expect("gzip expected");
        encoder.finish().expect("finish expected")
    };
    assert!(
        text.contains(&format!("Content-Length: {}", gzipped.len())),
        "length: {text}"
    );
    read_eof(&mut stream);
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
}

static GZIP_NATIVE_BODY: OnceLock<Vec<u8>> = OnceLock::new();

fn gzip_native_body() -> &'static [u8] {
    GZIP_NATIVE_BODY.get_or_init(gzip_body)
}

#[allow(unsafe_code)]
unsafe extern "C" fn native_gzip(
    _user_data: *mut std::ffi::c_void,
    request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    if request.is_null() || response.is_null() {
        return 1;
    }
    let view = unsafe { &*request };
    let out = unsafe { &mut *response };
    if view.method_len == 0 || view.path_len == 0 {
        return 1;
    }
    let body = gzip_native_body();
    out.status = 200;
    out.reason = b"OK".as_ptr();
    out.reason_len = 2;
    out.body = body.as_ptr();
    out.body_len = body.len();
    0
}

fn gzip_native_server() -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let mut server = Server::bind(addr, ServerConfig::default()).expect("bind loopback");
    server
        .register_native_gzip(b"GET", b"/gzip-native", Some(native_gzip), 0)
        .expect("register gzip native");
    server.start().expect("start server")
}

#[test]
fn gzip_native_route_round_trips_with_encoding_headers() {
    let server = gzip_native_server();
    let mut stream = connect(&server);
    get_with_encoding(&mut stream, "/gzip-native", "Accept-Encoding: gzip\r\n");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(header(&response, "content-encoding"), "gzip");
    assert_eq!(header(&response, "vary"), "Accept-Encoding");
    date_shape(&response);
    assert!(response.body.len() < gzip_body().len());
    assert_eq!(gunzip(&response.body), gzip_body());
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed + counts.shutdown, 1, "counts: {counts:?}");
}

#[test]
fn gzip_native_identity_fallback_without_accept_encoding() {
    let server = gzip_native_server();
    let mut stream = connect(&server);
    get_with_encoding(&mut stream, "/gzip-native", "");
    let response = read_response(&mut stream);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, gzip_body());
    assert!(
        response
            .headers
            .iter()
            .all(|(name, _)| !name.eq_ignore_ascii_case("content-encoding")),
        "no content-encoding"
    );
    assert_eq!(header(&response, "vary"), "Accept-Encoding");
    server.stop_accepting();
    let counts = server.join().expect("join server");
    assert_eq!(counts.completed + counts.shutdown, 1, "counts: {counts:?}");
}
