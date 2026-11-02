use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use veylor_h3::{Client, ClientConfig, ClientTls, Limits};
use veylor_runtime::ConnectionToken;
use veylor_server::{
    EventSlot, HostEventKind, HostHandle, RespondOutcome, Server, ServerConfig, TlsConfig,
};

fn fixture(name: &str) -> Vec<u8> {
    let path = format!(
        "{}/../veylor-tls/tests/testdata/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read(path).expect("test fixture")
}

fn blank_event() -> EventSlot {
    EventSlot {
        kind: HostEventKind::Request,
        token: ConnectionToken::default(),
        route_id: 0,
        route_token: 0,
        method_len: 0,
        path_len: 0,
        method_offset: 0,
        path_offset: 0,
    }
}

fn dispatch(host: HostHandle, stop: Arc<AtomicBool>) {
    let mut events = [blank_event(); 16];
    let mut methods = [0u8; 256];
    let mut paths = [0u8; 4096];
    while !stop.load(Ordering::Acquire) {
        let report = host.poll(10_000_000, &mut events, &mut methods, &mut paths);
        for event in &events[..report.copied] {
            match event.kind {
                HostEventKind::Request => {
                    assert_eq!(
                        host.upgrade_websocket_with_protocol(event.token, b"chat"),
                        RespondOutcome::Ok
                    );
                    assert_eq!(
                        host.tunnel_write(event.token, b"welcome"),
                        RespondOutcome::Ok
                    );
                }
                HostEventKind::TunnelData => {
                    let mut buffer = [0u8; 128];
                    let (outcome, count) = host.tunnel_read(event.token, &mut buffer);
                    if outcome == RespondOutcome::Ok && count > 0 {
                        assert_eq!(
                            host.tunnel_write(event.token, &buffer[..count]),
                            RespondOutcome::Ok
                        );
                    }
                }
                HostEventKind::TunnelClosed => {
                    let _ = host.tunnel_close(event.token);
                }
                _ => {}
            }
        }
    }
}

#[test]
fn h3_extended_connect_tunnels_bytes() {
    let cert = veylor_tls::load_cert_chain(&fixture("leaf_cert.der")).expect("cert");
    let key = veylor_tls::load_private_key(&fixture("leaf_key.der")).expect("key");
    let config = ServerConfig {
        h3_port: Some(0),
        tls: Some(TlsConfig {
            cert_chain: cert.clone(),
            key,
            max_handshakes: 8,
        }),
        ..ServerConfig::default()
    };
    let mut server = Server::bind(SocketAddr::from(([127, 0, 0, 1], 0)), config).expect("bind");
    server
        .register_managed(b"CONNECT", b"/ws", 1024)
        .expect("register connect");
    let running = server.start().expect("start");
    let address = running.h3_addr().expect("h3 address");

    let stop = Arc::new(AtomicBool::new(false));
    let host = running.host();
    let dispatch_stop = Arc::clone(&stop);
    let dispatch = thread::spawn(move || dispatch(host, dispatch_stop));

    let client = Client::connect(ClientConfig {
        server: address,
        server_name: "localhost".to_owned(),
        tls: ClientTls::new(cert),
        limits: Limits::default(),
    })
    .expect("connect client");

    let request = veylor_h3::http::Request::builder()
        .method(veylor_h3::http::Method::CONNECT)
        .uri("https://localhost/ws")
        .header("sec-websocket-protocol", "chat")
        .body(())
        .expect("request");
    let tunnel = client
        .connect_tunnel(request, b"websocket")
        .expect("tunnel");

    let welcome = tunnel.read().expect("read welcome").expect("welcome bytes");
    assert_eq!(welcome, b"welcome");

    tunnel.write(b"ping").expect("write");
    let echoed = tunnel.read().expect("read echo").expect("echo bytes");
    assert_eq!(echoed, b"ping");

    tunnel.close();
    drop(tunnel);
    drop(client);

    stop.store(true, Ordering::Release);
    dispatch.join().expect("dispatch join");
    running.stop_accepting();
    let _ = running.join().expect("join server");
}
