use parking_lot::Mutex;
use veylor_web::{Bytes, HeadersOwned, Path, Request, Response, State};

use crate::store::{Payment, Store, StoreError, Write};
use crate::writer::Writer;

pub const BODY_CAP: usize = 512;

pub struct App {
    pub writer: Writer,
    pub reader: Mutex<Store>,
}

fn account(param: &str) -> Option<i64> {
    param.parse::<i64>().ok().filter(|account| *account > 0)
}

fn idempotency_key(headers: &HeadersOwned) -> Result<String, u16> {
    let mut found = None;
    for (name, value) in &headers.0 {
        if !name.eq_ignore_ascii_case(b"idempotency-key") {
            continue;
        }
        if found.is_some() {
            return Err(422);
        }
        found = Some(value.trim_ascii());
    }

    let value = found.ok_or(422u16)?;
    let valid = !value.is_empty()
        && value.len() <= 64
        && value
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'));
    if !valid {
        return Err(422);
    }

    String::from_utf8(value.to_vec()).map_err(|_| 422)
}

fn json_content_type(headers: &HeadersOwned) -> bool {
    let mut found = None;
    for (name, value) in &headers.0 {
        if !name.eq_ignore_ascii_case(b"content-type") {
            continue;
        }
        if found.is_some() {
            return false;
        }
        let media = value
            .split(|byte| *byte == b';')
            .next()
            .unwrap_or_default()
            .trim_ascii();
        found = Some(media.eq_ignore_ascii_case(b"application/json"));
    }
    found.unwrap_or(false)
}

fn json(status: u16, body: Vec<u8>) -> Response {
    Response::new(status, body).header("content-type", "application/json")
}

fn error(status: u16) -> Response {
    let response = json(status, error_body(status).to_vec());
    if status == 503 {
        return response.header("retry-after", "1");
    }

    response
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
        StoreError::Busy | StoreError::Timeout => 503,
        StoreError::Database(error) => {
            eprintln!("database operation failed: {error}");
            500
        }
        StoreError::Clock => 500,
    }
}

fn encode(value: &impl serde::Serialize) -> Response {
    match serde_json::to_vec(value) {
        Ok(body) => json(200, body),
        Err(_) => error(500),
    }
}

pub fn ready() -> &'static str {
    "ready\n"
}

pub fn statement(request: Request<'_, App>) -> Response {
    let Some(account) = request
        .param("account")
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(account)
    else {
        return error(404);
    };

    match request.state().reader.lock().statement(account) {
        Ok(statement) => encode(&statement),
        Err(failure) => error(error_status(&failure)),
    }
}

pub async fn transaction(
    Path(account_param): Path<String>,
    State(app): State<App>,
    headers: HeadersOwned,
    Bytes(body): Bytes,
) -> Response {
    let Some(account) = account(&account_param) else {
        return error(404);
    };
    let key = match idempotency_key(&headers) {
        Ok(key) => key,
        Err(status) => return error(status),
    };
    if !json_content_type(&headers) {
        return error(415);
    }
    let payment = match serde_json::from_slice::<Payment>(&body) {
        Ok(payment) => payment.into_owned(),
        Err(_) => return error(422),
    };
    let write = Write {
        account,
        key,
        payment,
    };

    let ticket = match app.writer.submit(write) {
        Ok(ticket) => ticket,
        Err(failure) => return error(error_status(&failure)),
    };

    match ticket.await {
        Ok(balance) => encode(&balance),
        Err(failure) => error(error_status(&failure)),
    }
}
