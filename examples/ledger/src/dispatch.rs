use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::Serialize;
use veylor_runtime::ConnectionToken;
use veylor_server::{
    EventSlot, HostEventKind, HostHandle, MAX_REQUEST_HEADER_BYTES, MAX_REQUEST_HEADERS,
    RequestHeadError, RequestHeader, RequestSpan, RespondOutcome,
};

use crate::store::{Payment, Store, StoreError};

pub const BODY_CAP: usize = 512;
const PENDING_CAP: usize = 128;

struct Pending {
    token: ConnectionToken,
    account: i64,
    payment: bool,
    idempotency_key: [u8; 64],
    idempotency_key_len: usize,
    body: [u8; BODY_CAP],
    used: usize,
}

struct RequestInfo {
    account: i64,
    payment: bool,
    idempotency_key: [u8; 64],
    idempotency_key_len: usize,
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
    let mut request_headers = [RequestHeader::default(); MAX_REQUEST_HEADERS];
    let mut request_bytes = [0; MAX_REQUEST_HEADER_BYTES];
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

            let method = &methods[event.method_offset..event.method_offset + event.method_len];
            let request = match request(
                &host,
                event.token,
                method,
                &mut request_headers,
                &mut request_bytes,
            ) {
                Ok(request) => request,
                Err(status) => {
                    reply(&host, event.token, status, error_body(status));
                    continue;
                }
            };

            pending.push(Pending {
                token: event.token,
                account: request.account,
                payment: request.payment,
                idempotency_key: request.idempotency_key,
                idempotency_key_len: request.idempotency_key_len,
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
                Err(409) => reply(
                    &host,
                    request.token,
                    409,
                    b"{\"error\":\"idempotency key reused\"}",
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
        .apply(
            request.account,
            &request.idempotency_key[..request.idempotency_key_len],
            &payment,
        )
        .map_err(|error| error_status(&error))?;

    encode(&balance, response)
}

fn encode(value: &impl Serialize, output: &mut [u8]) -> Result<usize, u16> {
    let mut cursor = Cursor::new(output);
    serde_json::to_writer(&mut cursor, value).map_err(|_| 500u16)?;

    Ok(cursor.position() as usize)
}

fn request(
    host: &HostHandle,
    token: ConnectionToken,
    method: &[u8],
    headers: &mut [RequestHeader],
    bytes: &mut [u8],
) -> Result<RequestInfo, u16> {
    let head = match host.request_head(token, headers, bytes) {
        Ok(head) => head,
        Err(RequestHeadError::Capacity { .. }) => return Err(431),
        Err(RequestHeadError::Unavailable(_)) => return Err(500),
    };

    let target = span_bytes(bytes, head.target);
    let path = target.split(|byte| *byte == b'?').next().ok_or(400u16)?;
    let account = path
        .strip_prefix(b"/accounts/")
        .and_then(|path| path.split(|byte| *byte == b'/').next())
        .and_then(|account| std::str::from_utf8(account).ok())
        .and_then(|account| account.parse::<i64>().ok())
        .filter(|account| *account > 0)
        .ok_or(404u16)?;
    let payment = method == b"POST";
    let mut key = None;
    if payment {
        for header in &headers[..head.header_count] {
            if !span_bytes(bytes, header.name).eq_ignore_ascii_case(b"idempotency-key") {
                continue;
            }

            if key.is_some() {
                return Err(422);
            }

            key = Some(span_bytes(bytes, header.value).trim_ascii());
        }

        let value = key.filter(|value| {
            !value.is_empty()
                && value.len() <= 64
                && value
                    .iter()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        });
        let Some(value) = value else {
            return Err(422);
        };

        if !json_content_type(headers, head.header_count, bytes) {
            return Err(415);
        }

        let mut idempotency_key = [0; 64];
        idempotency_key[..value.len()].copy_from_slice(value);

        return Ok(RequestInfo {
            account,
            payment,
            idempotency_key,
            idempotency_key_len: value.len(),
        });
    }

    Ok(RequestInfo {
        account,
        payment,
        idempotency_key: [0; 64],
        idempotency_key_len: 0,
    })
}

fn json_content_type(headers: &[RequestHeader], count: usize, bytes: &[u8]) -> bool {
    let mut content_type = None;

    for header in &headers[..count] {
        let name = span_bytes(bytes, header.name);
        if !name.eq_ignore_ascii_case(b"content-type") {
            continue;
        }

        if content_type.is_some() {
            return false;
        }

        let value = span_bytes(bytes, header.value);
        let media_type = value
            .split(|byte| *byte == b';')
            .next()
            .unwrap_or_default()
            .trim_ascii();
        content_type = Some(media_type);
    }

    content_type.is_some_and(|media_type| media_type.eq_ignore_ascii_case(b"application/json"))
}

fn span_bytes(bytes: &[u8], span: RequestSpan) -> &[u8] {
    &bytes[span.offset..span.offset + span.length]
}

fn error_body(status: u16) -> &'static [u8] {
    match status {
        400 => b"{\"error\":\"bad request\"}",
        409 => b"{\"error\":\"idempotency key reused\"}",
        404 => b"{\"error\":\"account not found\"}",
        415 => b"{\"error\":\"expected JSON\"}",
        422 => b"{\"error\":\"invalid idempotency key\"}",
        431 => b"{\"error\":\"request headers too large\"}",
        _ => b"{\"error\":\"request unavailable\"}",
    }
}

fn error_status(error: &StoreError) -> u16 {
    match error {
        StoreError::Missing => 404,
        StoreError::Rejected => 422,
        StoreError::KeyConflict => 409,
        StoreError::Corrupt => 500,
        StoreError::Busy => 503,
        StoreError::Database(error) => {
            eprintln!("database operation failed: {error}");
            500
        }
        StoreError::Clock => 500,
    }
}

fn reply(host: &HostHandle, token: ConnectionToken, status: u16, body: &[u8]) {
    let reason = match status {
        200 => b"OK".as_slice(),
        409 => b"Conflict",
        404 => b"Not Found",
        415 => b"Unsupported Media Type",
        422 => b"Unprocessable Content",
        431 => b"Request Header Fields Too Large",
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
