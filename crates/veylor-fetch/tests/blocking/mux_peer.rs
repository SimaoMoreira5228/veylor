use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::Mutex;
use veylor_tls::{TlsAcceptor, TlsStream};

pub const CERT: &[u8] = include_bytes!("../../../veylor-tls/tests/testdata/leaf_cert.der");
const KEY: &[u8] = include_bytes!("../../../veylor-tls/tests/testdata/leaf_key.der");

const DATA: u8 = 0;
const HEADERS: u8 = 1;
const RST_STREAM: u8 = 3;
const SETTINGS: u8 = 4;
const GOAWAY: u8 = 7;
const WINDOW_UPDATE: u8 = 8;
const END_STREAM: u8 = 1;
const END_HEADERS: u8 = 4;
const ACK: u8 = 1;
const SETTINGS_MAX_CONCURRENT_STREAMS: u16 = 3;
const SETTINGS_INITIAL_WINDOW_SIZE: u16 = 4;
const MAX_FRAME: usize = 16_384;

#[derive(Clone, Copy)]
pub enum Plan {
    Body(usize),
    Echo,
    Silent,
    Reset(u32),
}

pub struct Script {
    pub max_concurrent: Option<u32>,
    pub release_at: usize,
    pub goaway_at: Option<usize>,
    pub plan: Box<dyn Fn(usize) -> Plan + Send + Sync>,
}

impl Script {
    pub fn new(plan: impl Fn(usize) -> Plan + Send + Sync + 'static) -> Self {
        Self {
            max_concurrent: None,
            release_at: 0,
            goaway_at: None,
            plan: Box::new(plan),
        }
    }
}

pub struct Peer {
    pub port: u16,
    pub accepted: Arc<AtomicUsize>,
    pub closed: Arc<AtomicUsize>,
    pub opened: Arc<AtomicUsize>,
    pub max_open: Arc<AtomicUsize>,
    pub violations: Arc<AtomicUsize>,
    pub resets: Arc<Mutex<Vec<(u32, u32)>>>,
}

struct Out {
    headers_sent: bool,
    remaining: usize,
    fill: u8,
    text: Option<Vec<u8>>,
}

struct Stream {
    index: usize,
    plan: Plan,
    got: usize,
    window: i64,
    out: Option<Out>,
    refused: bool,
}

struct Session {
    tls: TlsStream<TcpStream>,
    script: Arc<Script>,
    peer: Shared,
    streams: HashMap<u32, Stream>,
    held: Vec<u32>,
    conn_window: i64,
    initial_window: i64,
    open: usize,
    last_id: u32,
}

#[derive(Clone)]
struct Shared {
    opened: Arc<AtomicUsize>,
    max_open: Arc<AtomicUsize>,
    violations: Arc<AtomicUsize>,
    resets: Arc<Mutex<Vec<(u32, u32)>>>,
}

pub fn frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
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

pub fn spawn(script: Script) -> Peer {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let accepted = Arc::new(AtomicUsize::new(0));
    let closed = Arc::new(AtomicUsize::new(0));
    let shared = Shared {
        opened: Arc::new(AtomicUsize::new(0)),
        max_open: Arc::new(AtomicUsize::new(0)),
        violations: Arc::new(AtomicUsize::new(0)),
        resets: Arc::new(Mutex::new(Vec::new())),
    };
    let peer = Peer {
        port,
        accepted: Arc::clone(&accepted),
        closed: Arc::clone(&closed),
        opened: Arc::clone(&shared.opened),
        max_open: Arc::clone(&shared.max_open),
        violations: Arc::clone(&shared.violations),
        resets: Arc::clone(&shared.resets),
    };
    let script = Arc::new(script);
    let mut acceptor = TlsAcceptor::new(CERT, KEY, usize::MAX)
        .expect("acceptor")
        .with_alpn(&[b"h2"]);

    std::thread::spawn(move || {
        for socket in listener.incoming() {
            let Ok(socket) = socket else {
                return;
            };
            let conn = acceptor.admit().expect("conn");

            accepted.fetch_add(1, Ordering::SeqCst);

            let session = Session {
                tls: TlsStream::new(socket, conn),
                script: Arc::clone(&script),
                peer: shared.clone(),
                streams: HashMap::new(),
                held: Vec::new(),
                conn_window: 65_535,
                initial_window: 65_535,
                open: 0,
                last_id: 0,
            };

            let closed = Arc::clone(&closed);

            std::thread::spawn(move || {
                session.run();
                closed.fetch_add(1, Ordering::SeqCst);
            });
        }
    });

    peer
}

impl Session {
    fn send(&mut self, bytes: &[u8]) -> bool {
        self.tls.write_all(bytes).is_ok() && self.tls.flush().is_ok()
    }

    fn run(mut self) {
        let mut preface = [0u8; 24];

        if self.tls.read_exact(&mut preface).is_err() {
            return;
        }

        let mut settings = Vec::new();

        if let Some(limit) = self.script.max_concurrent {
            settings.extend_from_slice(&SETTINGS_MAX_CONCURRENT_STREAMS.to_be_bytes());
            settings.extend_from_slice(&limit.to_be_bytes());
        }

        if !self.send(&frame(SETTINGS, 0, 0, &settings)) {
            return;
        }

        while let Some((kind, flags, id, payload)) = read_frame(&mut self.tls) {
            if !self.handle(kind, flags, id, &payload) || !self.pump() {
                return;
            }
        }
    }

    fn handle(&mut self, kind: u8, flags: u8, id: u32, payload: &[u8]) -> bool {
        match kind {
            SETTINGS if flags & ACK == 0 => {
                for pair in payload.as_chunks::<6>().0 {
                    let key = u16::from_be_bytes([pair[0], pair[1]]);
                    let value = u32::from_be_bytes([pair[2], pair[3], pair[4], pair[5]]);

                    if key == SETTINGS_INITIAL_WINDOW_SIZE {
                        self.initial_window = i64::from(value);
                    }
                }

                self.send(&frame(SETTINGS, ACK, 0, &[]))
            }
            WINDOW_UPDATE => {
                let increment = i64::from(
                    u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]])
                        & 0x7fff_ffff,
                );

                if id == 0 {
                    self.conn_window += increment;
                } else if let Some(stream) = self.streams.get_mut(&id) {
                    stream.window += increment;
                }

                true
            }
            HEADERS => self.opened(id, flags & END_STREAM != 0),
            DATA => {
                let length = payload.len();

                if let Some(stream) = self.streams.get_mut(&id) {
                    stream.got += length;
                }

                if length > 0 {
                    let credit = (length as u32).to_be_bytes();
                    let mut out = frame(WINDOW_UPDATE, 0, 0, &credit);

                    out.extend(frame(WINDOW_UPDATE, 0, id, &credit));

                    if !self.send(&out) {
                        return false;
                    }
                }

                flags & END_STREAM == 0 || self.request_ended(id)
            }
            RST_STREAM => {
                let code = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);

                self.peer.resets.lock().push((id, code));
                self.forget(id);

                true
            }
            _ => true,
        }
    }

    fn forget(&mut self, id: u32) {
        if self.streams.remove(&id).is_some() {
            self.open -= 1;
        }

        self.held.retain(|held| *held != id);
    }

    fn opened(&mut self, id: u32, ended: bool) -> bool {
        let index = self.peer.opened.fetch_add(1, Ordering::SeqCst);
        let refused = self.script.goaway_at == Some(index);

        self.open += 1;
        self.peer.max_open.fetch_max(self.open, Ordering::SeqCst);

        if self
            .script
            .max_concurrent
            .is_some_and(|limit| self.open > limit as usize)
        {
            self.peer.violations.fetch_add(1, Ordering::SeqCst);
        }

        self.streams.insert(
            id,
            Stream {
                index,
                plan: (self.script.plan)(index),
                got: 0,
                window: self.initial_window,
                out: None,
                refused,
            },
        );

        if refused {
            let mut payload = self.last_id.to_be_bytes().to_vec();

            payload.extend_from_slice(&0u32.to_be_bytes());

            return self.send(&frame(GOAWAY, 0, 0, &payload));
        }

        self.last_id = id;

        !ended || self.request_ended(id)
    }

    fn request_ended(&mut self, id: u32) -> bool {
        let Some(stream) = self.streams.get_mut(&id) else {
            return true;
        };

        if stream.refused {
            return true;
        }

        let out = match stream.plan {
            Plan::Silent => return true,
            Plan::Reset(code) => {
                self.forget(id);

                return self.send(&frame(RST_STREAM, 0, id, &code.to_be_bytes()));
            }
            Plan::Body(length) => Out {
                headers_sent: false,
                remaining: length,
                fill: (stream.index % 251) as u8,
                text: None,
            },
            Plan::Echo => {
                let text = format!("got:{}", stream.got).into_bytes();

                Out {
                    headers_sent: false,
                    remaining: text.len(),
                    fill: 0,
                    text: Some(text),
                }
            }
        };

        stream.out = Some(out);

        if self.script.release_at > 0 {
            self.held.push(id);
        }

        true
    }

    fn pump(&mut self) -> bool {
        if self.script.release_at > 0 && self.held.len() >= self.script.release_at {
            self.held.clear();
        }

        let mut bytes = Vec::new();
        let mut finished = Vec::new();
        let mut progressed = true;
        let ids: Vec<u32> = self.streams.keys().copied().collect();

        while progressed {
            progressed = false;

            for id in &ids {
                if self.held.contains(id) {
                    continue;
                }

                let Some(stream) = self.streams.get_mut(id) else {
                    continue;
                };
                let Some(out) = stream.out.as_mut() else {
                    continue;
                };

                if !out.headers_sent {
                    out.headers_sent = true;
                    bytes.extend(frame(HEADERS, END_HEADERS, *id, &[0x88]));
                    progressed = true;
                }

                let take = out
                    .remaining
                    .min(MAX_FRAME)
                    .min(usize::try_from(stream.window.max(0)).unwrap_or(0))
                    .min(usize::try_from(self.conn_window.max(0)).unwrap_or(0));

                if take == 0 && out.remaining > 0 {
                    continue;
                }

                let sent = match &out.text {
                    Some(text) => text[text.len() - out.remaining..][..take].to_vec(),
                    None => vec![out.fill; take],
                };

                out.remaining -= take;
                stream.window -= take as i64;
                self.conn_window -= take as i64;

                let flag = if out.remaining == 0 { END_STREAM } else { 0 };

                bytes.extend(frame(DATA, flag, *id, &sent));
                progressed = true;

                if out.remaining == 0 {
                    stream.out = None;
                    finished.push(*id);
                }
            }
        }

        for id in finished {
            self.streams.remove(&id);
            self.open -= 1;
        }

        bytes.is_empty() || self.send(&bytes)
    }
}
