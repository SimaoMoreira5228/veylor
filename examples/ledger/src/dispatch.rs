use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::Serialize;
use veylor_runtime::ConnectionToken;
use veylor_server::{EventSlot, HostEventKind, HostHandle, RespondOutcome};

use crate::store::{Payment, Store, StoreError};

pub const BODY_CAP: usize = 512;
const PENDING_CAP: usize = 128;

struct Pending {
    token: ConnectionToken,
    account: i64,
    payment: bool,
    body: [u8; BODY_CAP],
    used: usize,
}

pub fn run(host: HostHandle, mut store: Store, stopped: Arc<AtomicBool>) {
    let empty = EventSlot {
        kind: HostEventKind::Shutdown,
        token: ConnectionToken {
            worker_id: 0,
            slot: 0,
            request_seq: 0,
            stream_id: 0,
            worker_generation: 0,
            generation: 0,
        },
        route_id: 0,
        method_len: 0,
        path_len: 0,
        method_offset: 0,
        path_offset: 0,
    };
    let mut events = [empty; 64];
    let mut methods = [0; 64 * 16];
    let mut paths = [0; 64 * 2048];
    let mut pending = Vec::with_capacity(PENDING_CAP);

    while !stopped.load(Ordering::Acquire) {
        let report = host.poll(1_000_000, &mut events, &mut methods, &mut paths);

        for event in &events[..report.copied] {
            if event.kind != HostEventKind::Request {
                continue;
            }

            if pending.len() == PENDING_CAP {
                reply(
                    &host,
                    event.token,
                    503,
                    b"{\"error\":\"capacity exhausted\"}",
                );
                continue;
            }

            let path = &paths[event.path_offset..event.path_offset + event.path_len];
            let Some(account) = path.get(10).filter(|id| (b'1'..=b'5').contains(id)) else {
                reply(
                    &host,
                    event.token,
                    404,
                    b"{\"error\":\"account not found\"}",
                );
                continue;
            };

            let method = &methods[event.method_offset..event.method_offset + event.method_len];

            pending.push(Pending {
                token: event.token,
                account: i64::from(account - b'0'),
                payment: method == b"POST",
                body: [0; BODY_CAP],
                used: 0,
            });
        }

        let mut index = 0;

        while index < pending.len() {
            let request = &mut pending[index];
            let (outcome, count, done) =
                host.pull_body(request.token, &mut request.body[request.used..]);
            if outcome != RespondOutcome::Ok {
                pending.swap_remove(index);
                continue;
            }

            request.used += count;

            if !done {
                index += 1;
                continue;
            }

            let request = pending.swap_remove(index);
            let mut response = [0; 4096];

            let result = execute(&mut store, &request, &mut response);

            match result {
                Ok(length) => reply(&host, request.token, 200, &response[..length]),
                Err(422) => reply(
                    &host,
                    request.token,
                    422,
                    b"{\"error\":\"invalid transaction\"}",
                ),
                Err(404) => reply(
                    &host,
                    request.token,
                    404,
                    b"{\"error\":\"account not found\"}",
                ),
                Err(status) => reply(
                    &host,
                    request.token,
                    status,
                    b"{\"error\":\"storage unavailable\"}",
                ),
            }
        }
    }
}

fn execute(store: &mut Store, request: &Pending, response: &mut [u8]) -> Result<usize, u16> {
    if !request.payment {
        let statement = store
            .statement(request.account)
            .map_err(|error| error_status(&error))?;

        return encode(&statement, response);
    }

    let body = &request.body[..request.used];
    let payment = serde_json::from_slice::<Payment<'_>>(body).map_err(|_| 422u16)?;

    let balance = store
        .apply(request.account, &payment)
        .map_err(|error| error_status(&error))?;

    encode(&balance, response)
}

fn encode(value: &impl Serialize, output: &mut [u8]) -> Result<usize, u16> {
    let mut cursor = Cursor::new(output);
    serde_json::to_writer(&mut cursor, value).map_err(|_| 500u16)?;

    Ok(cursor.position() as usize)
}

fn error_status(error: &StoreError) -> u16 {
    match error {
        StoreError::Missing => 404,
        StoreError::Rejected => 422,
        StoreError::Database(error) => {
            eprintln!("database operation failed: {error}");
            503
        }
        StoreError::Clock => 500,
    }
}

fn reply(host: &HostHandle, token: ConnectionToken, status: u16, body: &[u8]) {
    let reason = match status {
        200 => b"OK".as_slice(),
        404 => b"Not Found",
        422 => b"Unprocessable Content",
        503 => b"Service Unavailable",
        _ => b"Internal Server Error",
    };

    let outcome = host.respond_with_headers(
        token,
        status,
        reason,
        &[(b"content-type", b"application/json")],
        body,
    );

    if !matches!(
        outcome,
        RespondOutcome::Ok | RespondOutcome::Gone | RespondOutcome::Stale
    ) {
        eprintln!("response staging failed: {outcome:?}");
        host.cancel(token);
    }
}
