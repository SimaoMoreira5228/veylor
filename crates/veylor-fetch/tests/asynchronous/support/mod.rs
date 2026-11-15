#![allow(dead_code)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

pub const CERT: &[u8] = include_bytes!("../../../../veylor-tls/tests/testdata/leaf_cert.der");
pub const KEY: &[u8] = include_bytes!("../../../../veylor-tls/tests/testdata/leaf_key.der");

pub struct Req {
    pub method: String,
    pub target: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Req {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

pub struct TestServer {
    pub addr: SocketAddr,
    pub connections: Arc<AtomicUsize>,
}

impl TestServer {
    pub fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }
}

pub fn serve<F>(handler: F) -> TestServer
where
    F: Fn(&Req, &mut TcpStream) + Send + Sync + 'static,
{
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind");
    let addr = listener.local_addr().expect("addr");
    let connections = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&connections);
    let handler = Arc::new(handler);

    std::thread::spawn(move || {
        for socket in listener.incoming() {
            let Ok(socket) = socket else {
                return;
            };
            let handler = Arc::clone(&handler);

            count.fetch_add(1, Ordering::SeqCst);

            std::thread::spawn(move || handle(socket, &*handler));
        }
    });

    TestServer { addr, connections }
}

fn handle(mut socket: TcpStream, handler: &(dyn Fn(&Req, &mut TcpStream) + Send + Sync)) {
    let mut pending: Vec<u8> = Vec::new();

    loop {
        let Some(request) = read_request(&mut socket, &mut pending) else {
            return;
        };

        handler(&request, &mut socket);
    }
}

fn read_request(socket: &mut TcpStream, pending: &mut Vec<u8>) -> Option<Req> {
    let head_end = loop {
        if let Some(at) = pending.windows(4).position(|w| w == b"\r\n\r\n") {
            break at + 4;
        }

        let mut buf = [0u8; 4096];
        let read = socket.read(&mut buf).ok()?;

        if read == 0 {
            return None;
        }

        pending.extend_from_slice(&buf[..read]);
    };

    let head = String::from_utf8_lossy(&pending[..head_end]).into_owned();
    let mut lines = head.split("\r\n");
    let mut first = lines.next()?.split(' ');
    let method = first.next()?.to_owned();
    let target = first.next()?.to_owned();
    let headers: Vec<(String, String)> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(n, v)| (n.trim().to_owned(), v.trim().to_owned()))
        .collect();
    let length = headers
        .iter()
        .find(|(n, _)| n.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.parse::<usize>().ok())
        .unwrap_or(0);

    pending.drain(..head_end);

    while pending.len() < length {
        let mut buf = [0u8; 4096];
        let read = socket.read(&mut buf).ok()?;

        if read == 0 {
            return None;
        }

        pending.extend_from_slice(&buf[..read]);
    }

    let body = pending.drain(..length).collect();

    Some(Req {
        method,
        target,
        headers,
        body,
    })
}

pub fn respond(socket: &mut TcpStream, status: &str, headers: &[(&str, &str)], body: &[u8]) {
    let mut head = format!("HTTP/1.1 {status}\r\ncontent-length: {}\r\n", body.len());

    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }

    head.push_str("\r\n");

    let _ = socket.write_all(head.as_bytes());
    let _ = socket.write_all(body);
    let _ = socket.flush();
}

pub fn pause(millis: u64) {
    std::thread::sleep(Duration::from_millis(millis));
}
