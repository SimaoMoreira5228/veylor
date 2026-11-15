use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use veylor_fetch::{Agent, Protocol};
use veylor_tls::{TlsAcceptor, TlsStream};

const CERT: &[u8] = include_bytes!("../../../veylor-tls/tests/testdata/leaf_cert.der");
const KEY: &[u8] = include_bytes!("../../../veylor-tls/tests/testdata/leaf_key.der");

const DATA: u8 = 0;
const HEADERS: u8 = 1;
const SETTINGS: u8 = 4;
const WINDOW_UPDATE: u8 = 8;
const END_STREAM: u8 = 1;
const END_HEADERS: u8 = 4;
const ACK: u8 = 1;

struct Peer {
    port: u16,
    accepted: Arc<AtomicUsize>,
}

fn frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
    let length = payload.len() as u32;
    let mut out = vec![
        (length >> 16) as u8,
        (length >> 8) as u8,
        length as u8,
        kind,
        flags,
    ];

    out.extend_from_slice(&stream.to_be_bytes());
    out.extend_from_slice(payload);

    out
}

fn read_frame(stream: &mut TlsStream<TcpStream>) -> Option<(u8, u8, u32, Vec<u8>)> {
    let mut head = [0u8; 9];

    stream.read_exact(&mut head).ok()?;

    let length = usize::from(head[0]) << 16 | usize::from(head[1]) << 8 | usize::from(head[2]);
    let id = u32::from_be_bytes([head[5] & 0x7f, head[6], head[7], head[8]]);
    let mut payload = vec![0u8; length];

    stream.read_exact(&mut payload).ok()?;

    Some((head[3], head[4], id, payload))
}

fn serve(mut stream: TlsStream<TcpStream>, requests_per_connection: usize) {
    let mut preface = [0u8; 24];

    if stream.read_exact(&mut preface).is_err() {
        return;
    }

    let _ = stream.write_all(&frame(SETTINGS, 0, 0, &[]));

    let mut received = 0usize;
    let mut served = 0usize;

    while let Some((kind, flags, id, payload)) = read_frame(&mut stream) {
        let mut finished = false;

        match kind {
            SETTINGS if flags & ACK == 0 => {
                let _ = stream.write_all(&frame(SETTINGS, ACK, 0, &[]));
            }
            HEADERS => finished = flags & END_STREAM != 0,
            DATA => {
                received += payload.len();

                let credit = (payload.len() as u32).to_be_bytes();

                if !payload.is_empty() {
                    let _ = stream.write_all(&frame(WINDOW_UPDATE, 0, 0, &credit));
                    let _ = stream.write_all(&frame(WINDOW_UPDATE, 0, id, &credit));
                }

                finished = flags & END_STREAM != 0;
            }
            _ => {}
        }

        if !finished {
            continue;
        }

        let body = format!("got:{received}");
        let mut reply = frame(HEADERS, END_HEADERS, id, &[0x88]);

        reply.extend(frame(DATA, END_STREAM, id, body.as_bytes()));

        received = 0;
        served += 1;

        if stream.write_all(&reply).is_err() || stream.flush().is_err() {
            return;
        }

        if served >= requests_per_connection {
            return;
        }
    }
}

fn spawn_peer(requests_per_connection: usize) -> Peer {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let accepted = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&accepted);
    let mut acceptor = TlsAcceptor::new(CERT, KEY, usize::MAX)
        .expect("acceptor")
        .with_alpn(&[b"h2"]);

    std::thread::spawn(move || {
        for socket in listener.incoming() {
            let Ok(socket) = socket else {
                return;
            };
            let conn = acceptor.admit().expect("conn");

            counter.fetch_add(1, Ordering::SeqCst);

            std::thread::spawn(move || {
                serve(TlsStream::new(socket, conn), requests_per_connection);
            });
        }
    });

    Peer { port, accepted }
}

fn agent() -> Agent {
    Agent::builder()
        .builtin_root_certificates(false)
        .add_root_certificate(CERT)
        .build()
        .expect("agent")
}

#[test]
fn uploads_respect_flow_control_credit() {
    let peer = spawn_peer(usize::MAX);
    let agent = agent();
    let body = vec![b'u'; 400_000];

    for _ in 0..3 {
        let response = agent
            .post(&format!("https://localhost:{}/up", peer.port))
            .send(&body)
            .expect("upload");

        assert_eq!(response.protocol(), Protocol::Http2);
        assert_eq!(response.text().expect("body"), "got:400000");
    }

    assert_eq!(peer.accepted.load(Ordering::SeqCst), 1);
    assert_eq!(agent.pool_stats().h2, 1);
}

#[test]
fn empty_and_small_posts_carry_a_content_length() {
    let peer = spawn_peer(usize::MAX);
    let agent = agent();
    let url = format!("https://localhost:{}/small", peer.port);

    let empty = agent.post(&url).call().expect("empty");

    assert_eq!(empty.text().expect("body"), "got:0");

    let small = agent.put(&url).send_string("tiny").expect("small");

    assert_eq!(small.text().expect("body"), "got:4");
}

#[test]
fn a_pooled_h2_connection_closed_by_the_server_is_replaced() {
    let peer = spawn_peer(1);
    let agent = agent();
    let url = format!("https://localhost:{}/", peer.port);

    for _ in 0..3 {
        let text = agent
            .get(&url)
            .call()
            .expect("response")
            .text()
            .expect("body");

        assert_eq!(text, "got:0");
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    assert_eq!(peer.accepted.load(Ordering::SeqCst), 3);
}
