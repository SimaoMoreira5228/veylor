#![allow(dead_code)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

pub struct Raw {
    pub head: String,
    pub body: Vec<u8>,
}

impl Raw {
    pub fn request_line(&self) -> &str {
        self.head.lines().next().unwrap_or("")
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.head.lines().skip(1).find_map(|line| {
            let (key, value) = line.split_once(':')?;

            key.eq_ignore_ascii_case(name).then(|| value.trim())
        })
    }
}

pub struct TestServer {
    pub addr: SocketAddr,
    pub accepted: Arc<AtomicUsize>,
}

impl TestServer {
    pub fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    pub fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }
}

pub fn spawn<F>(handler: F) -> TestServer
where
    F: Fn(usize, TcpStream) + Send + Sync + 'static,
{
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind");
    let addr = listener.local_addr().expect("addr");
    let accepted = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&accepted);
    let handler = Arc::new(handler);

    std::thread::spawn(move || {
        for socket in listener.incoming() {
            let Ok(socket) = socket else {
                return;
            };
            let index = counter.fetch_add(1, Ordering::SeqCst);
            let handler = Arc::clone(&handler);

            std::thread::spawn(move || handler(index, socket));
        }
    });

    TestServer { addr, accepted }
}

pub fn read_request(stream: &mut TcpStream) -> Option<Raw> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];

    let end = loop {
        if let Some(position) = buf.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }

        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return None,
            Ok(count) => buf.extend_from_slice(&chunk[..count]),
        }
    };

    let head = String::from_utf8_lossy(&buf[..end]).into_owned();
    let mut raw = Raw {
        head,
        body: Vec::new(),
    };
    let length = raw
        .header("content-length")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);

    raw.body = buf[end..].to_vec();

    while raw.body.len() < length {
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return None,
            Ok(count) => raw.body.extend_from_slice(&chunk[..count]),
        }
    }

    Some(raw)
}

pub fn respond(stream: &mut TcpStream, status: &str, headers: &[&str], body: &[u8]) {
    let mut out = format!("HTTP/1.1 {status}\r\ncontent-length: {}\r\n", body.len());

    for header in headers {
        out.push_str(header);
        out.push_str("\r\n");
    }

    out.push_str("\r\n");

    let _ = stream.write_all(out.as_bytes());
    let _ = stream.write_all(body);
}

pub fn serve_loop(mut stream: TcpStream, answer: impl Fn(&Raw, &mut TcpStream)) {
    while let Some(request) = read_request(&mut stream) {
        answer(&request, &mut stream);
    }
}

pub fn pause(millis: u64) {
    std::thread::sleep(Duration::from_millis(millis));
}
