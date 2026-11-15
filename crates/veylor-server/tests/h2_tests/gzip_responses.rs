use crate::frame_io::*;
use crate::h2_peer::*;
use crate::h2_response::*;
use std::ffi::c_void;
use std::net::SocketAddr;
use std::sync::OnceLock;
use veylor_http2::ErrorCode;
use veylor_server::{
    MAX_STATIC_BODY_BYTES, NativeRequestView, NativeResponseOut, RunningServer, Server,
    ServerConfig,
};

fn gzip_plain_body() -> Vec<u8> {
    b"The quick brown fox jumps over the lazy dog. ".repeat(50)
}

fn gzip_h2_server() -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let config = ServerConfig {
        h2c_port: Some(0),
        ..ServerConfig::default()
    };
    let mut server = Server::bind(addr, config).expect("bind loopback");
    server
        .register_gzip_route(
            b"GET",
            b"/gzip",
            200,
            b"OK",
            &gzip_plain_body(),
            MAX_STATIC_BODY_BYTES,
        )
        .expect("register gzip");
    server.start().expect("start server")
}

fn gunzip_plain(body: &[u8]) -> Vec<u8> {
    use std::io::Read;
    let mut decoder = flate2::read::GzDecoder::new(body);
    let mut plain = Vec::new();
    decoder.read_to_end(&mut plain).expect("gunzip body");
    plain
}

#[test]
fn gzip_static_route_round_trips_over_h2c() {
    let server = gzip_h2_server();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    send_headers_extra(
        &mut client,
        1,
        b"GET",
        b"/gzip",
        &[(b"accept-encoding".as_slice(), b"gzip".as_slice())],
        true,
    );
    let response = read_response(&mut client, 1);
    assert_eq!(response.status, 200);
    assert_eq!(response.content_encoding, b"gzip");
    assert_eq!(response.vary, b"Accept-Encoding");
    assert_date_first(&response.order, true);
    assert!(response.body.len() < gzip_plain_body().len());
    assert_eq!(response.content_length as usize, response.body.len());
    assert_eq!(gunzip_plain(&response.body), gzip_plain_body());
    server.stop_accepting();
    let (_, code) = read_goaway(&mut client);
    assert_eq!(code, ErrorCode::NoError);
    read_eof(&mut client);
    let counts = server.join().expect("join server");
    assert_eq!(counts.h2_served, 1);
    assert_eq!(counts.shutdown, 1);
}

#[test]
fn gzip_static_route_identity_without_accept_encoding_over_h2c() {
    let server = gzip_h2_server();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    send_headers(&mut client, 1, b"GET", b"/gzip", true);
    let response = read_response(&mut client, 1);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, gzip_plain_body());
    assert!(response.content_encoding.is_empty());
    assert_eq!(response.vary, b"Accept-Encoding");
    assert_date_first(&response.order, true);
    server.stop_accepting();
    let (_, code) = read_goaway(&mut client);
    assert_eq!(code, ErrorCode::NoError);
    read_eof(&mut client);
    let counts = server.join().expect("join server");
    assert_eq!(counts.h2_served, 1);
    assert_eq!(counts.shutdown, 1);
}

static H2N_GZIP_BODY: OnceLock<Vec<u8>> = OnceLock::new();

fn h2n_gzip_body() -> &'static [u8] {
    H2N_GZIP_BODY.get_or_init(gzip_plain_body)
}

#[allow(unsafe_code)]
unsafe extern "C" fn h2n_gzip(
    _user_data: *mut c_void,
    _request: *const NativeRequestView,
    response: *mut NativeResponseOut,
) -> i32 {
    h2n_respond(response, 200, b"OK", h2n_gzip_body())
}

fn gzip_native_h2_server() -> RunningServer {
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let config = ServerConfig {
        h2c_port: Some(0),
        ..ServerConfig::default()
    };
    let mut server = Server::bind(addr, config).expect("bind loopback");
    server
        .register_native_gzip(b"GET", b"/gzip-native", Some(h2n_gzip), 0)
        .expect("register gzip native");
    server.start().expect("start server")
}

#[test]
fn gzip_native_route_round_trips_over_h2c() {
    let server = gzip_native_h2_server();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    send_headers_extra(
        &mut client,
        1,
        b"GET",
        b"/gzip-native",
        &[(b"accept-encoding".as_slice(), b"gzip".as_slice())],
        true,
    );
    let response = read_response(&mut client, 1);
    assert_eq!(response.status, 200);
    assert_eq!(response.content_encoding, b"gzip");
    assert_eq!(response.vary, b"Accept-Encoding");
    assert_date_first(&response.order, true);
    assert!(response.body.len() < gzip_plain_body().len());
    assert_eq!(gunzip_plain(&response.body), gzip_plain_body());
    server.stop_accepting();
    let (_, code) = read_goaway(&mut client);
    assert_eq!(code, ErrorCode::NoError);
    read_eof(&mut client);
    let counts = server.join().expect("join server");
    assert_eq!(counts.h2_served, 1);
    assert_eq!(counts.shutdown, 1);
}

#[test]
fn gzip_native_route_identity_without_accept_encoding_over_h2c() {
    let server = gzip_native_h2_server();
    let mut client = h2_connect(&server);
    handshake(&mut client);
    send_headers(&mut client, 1, b"GET", b"/gzip-native", true);
    let response = read_response(&mut client, 1);
    assert_eq!(response.status, 200);
    assert_eq!(response.body, gzip_plain_body());
    assert!(response.content_encoding.is_empty());
    assert_eq!(response.vary, b"Accept-Encoding");
    assert_date_first(&response.order, true);
    server.stop_accepting();
    let (_, code) = read_goaway(&mut client);
    assert_eq!(code, ErrorCode::NoError);
    read_eof(&mut client);
    let counts = server.join().expect("join server");
    assert_eq!(counts.h2_served, 1);
    assert_eq!(counts.shutdown, 1);
}
