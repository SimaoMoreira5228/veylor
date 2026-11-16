use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::Value;

static TEST_REQUEST_KEY: AtomicUsize = AtomicUsize::new(0);

pub struct Service {
    pub child: Child,
    pub address: SocketAddr,
    _output: BufReader<std::process::ChildStdout>,
}

impl Service {
    pub fn start(database: &Path) -> Self {
        Self::start_with_options(database, &[])
    }

    pub fn start_with_options(database: &Path, options: &[&str]) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_veylor-ledger"))
            .args([
                "--address",
                "127.0.0.1:0",
                "--database",
                database.to_str().unwrap(),
                "--workers",
                "2",
            ])
            .args(options)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();

        let mut output = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        output.read_line(&mut line).unwrap();

        let address = line.trim().split_once("://").unwrap().1.parse().unwrap();

        Self {
            child,
            address,
            _output: output,
        }
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        if self.child.try_wait().unwrap().is_none() {
            let status = Command::new("kill")
                .args(["-TERM", &self.child.id().to_string()])
                .status()
                .unwrap();
            assert!(status.success());
        }

        let deadline = std::time::Instant::now() + Duration::from_secs(10);

        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                if !std::thread::panicking() {
                    assert!(status.success());
                }
                return;
            }

            if std::time::Instant::now() >= deadline {
                self.child.kill().unwrap();
                self.child.wait().unwrap();
                panic!("service shutdown exceeded deadline");
            }

            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

pub struct Database(PathBuf);

impl Database {
    pub fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("veylor-ledger-{}-{nonce}", std::process::id()));
        std::fs::create_dir(&path).unwrap();

        Self(path)
    }

    pub fn file(&self) -> PathBuf {
        self.0.join("accounts.sqlite3")
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct WriterStats {
    pub commits: u64,
    pub writes: u64,
    pub max_batch: u64,
}

// NOTE: read after the service exits, when SQLite's last close has checkpointed the WAL, so
// the counters and the transactions describe the same durable state.
#[allow(dead_code)]
pub fn writer_stats(database: &Database) -> WriterStats {
    let connection = rusqlite::Connection::open_with_flags(
        database.file(),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .expect("open database for stats");
    let (commits, writes, max_batch): (i64, i64, i64) = connection
        .query_row(
            "SELECT commits, writes, max_batch FROM writer_stats WHERE id = 0",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("writer stats row");

    WriterStats {
        commits: u64::try_from(commits).expect("commit count is non-negative"),
        writes: u64::try_from(writes).expect("write count is non-negative"),
        max_batch: u64::try_from(max_batch).expect("batch size is non-negative"),
    }
}

#[allow(dead_code)]
pub fn durable_keys(database: &Database, account: i64) -> Vec<String> {
    let connection = rusqlite::Connection::open_with_flags(
        database.file(),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .expect("open database for keys");
    let mut statement = connection
        .prepare(
            "SELECT idempotency_key FROM transactions
             WHERE account_id = ?1 AND idempotency_key IS NOT NULL",
        )
        .expect("prepare key query");
    statement
        .query_map([account], |row| row.get::<_, String>(0))
        .expect("query keys")
        .collect::<Result<Vec<_>, _>>()
        .expect("read keys")
}

pub fn request(
    address: SocketAddr,
    method: &str,
    path: &str,
    body: &[u8],
    fragmented: bool,
) -> (u16, Value) {
    let key = format!("test-{}", TEST_REQUEST_KEY.fetch_add(1, Ordering::Relaxed));
    request_with_idempotency_key(address, method, path, body, fragmented, &key)
}

#[allow(dead_code)]
pub fn request_with_content_type(
    address: SocketAddr,
    method: &str,
    path: &str,
    content_type: &str,
    body: &[u8],
    fragmented: bool,
) -> (u16, Value) {
    let key = format!("test-{}", TEST_REQUEST_KEY.fetch_add(1, Ordering::Relaxed));
    request_with_headers(
        address,
        method,
        path,
        content_type,
        body,
        fragmented,
        &[&key],
    )
}

pub fn request_with_idempotency_key(
    address: SocketAddr,
    method: &str,
    path: &str,
    body: &[u8],
    fragmented: bool,
    idempotency_key: &str,
) -> (u16, Value) {
    request_with_headers(
        address,
        method,
        path,
        "application/json",
        body,
        fragmented,
        &[idempotency_key],
    )
}

pub fn request_with_headers(
    address: SocketAddr,
    method: &str,
    path: &str,
    content_type: &str,
    body: &[u8],
    fragmented: bool,
    idempotency_keys: &[&str],
) -> (u16, Value) {
    try_request_with_headers(
        address,
        method,
        path,
        content_type,
        body,
        fragmented,
        idempotency_keys,
    )
    .expect("service answered with a complete response")
}

#[allow(dead_code)]
pub fn try_request_with_idempotency_key(
    address: SocketAddr,
    method: &str,
    path: &str,
    body: &[u8],
    fragmented: bool,
    idempotency_key: &str,
) -> Option<(u16, Value)> {
    try_request_with_headers(
        address,
        method,
        path,
        "application/json",
        body,
        fragmented,
        &[idempotency_key],
    )
}

// NOTE: an overloaded server answers 503 after a parsed head and only closes silently when
// it never got a complete head (transport failure, request-head deadline); both are
// unacknowledged attempts, so the flood tests read this None instead of panicking.
fn try_request_with_headers(
    address: SocketAddr,
    method: &str,
    path: &str,
    content_type: &str,
    body: &[u8],
    fragmented: bool,
    idempotency_keys: &[&str],
) -> Option<(u16, Value)> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(5)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .ok()?;

    let mut idempotency_headers = String::new();
    for key in idempotency_keys {
        idempotency_headers.push_str(&format!("Idempotency-Key: {key}\r\n"));
    }

    let head = format!(
        "{method} {path} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Content-Type: {content_type}\r\n\
         {idempotency_headers}\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n",
        body.len(),
    );
    stream.write_all(head.as_bytes()).ok()?;
    if fragmented && !body.is_empty() {
        let middle = body.len() / 2;
        stream.write_all(&body[..middle]).ok()?;
        std::thread::sleep(Duration::from_millis(5));
        stream.write_all(&body[middle..]).ok()?;
    } else {
        stream.write_all(body).ok()?;
    }

    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes).ok()?;

    let end = bytes.windows(4).position(|part| part == b"\r\n\r\n")?;
    let head = std::str::from_utf8(&bytes[..end]).ok()?;
    let status = head.split_whitespace().nth(1)?.parse().ok()?;
    let value = serde_json::from_slice(&bytes[end + 4..]).unwrap_or(Value::Null);

    Some((status, value))
}
