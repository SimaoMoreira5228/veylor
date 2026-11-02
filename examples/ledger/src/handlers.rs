use std::sync::Mutex;

use veylor_web::{Request, Response};

use crate::store::{Payment, Store, StoreError};

pub const BODY_CAP: usize = 512;

pub type App = Mutex<Store>;

fn account(request: &Request<'_, App>) -> Option<i64> {
    let [param] = request.params() else {
        return None;
    };
    std::str::from_utf8(param.value)
        .ok()?
        .parse::<i64>()
        .ok()
        .filter(|account| *account > 0)
}

fn idempotency_key<'a>(request: &'a Request<'_, App>) -> Result<&'a [u8], u16> {
    let mut found = None;
    for (name, value) in request.headers() {
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
    valid.then_some(value).ok_or(422)
}

fn json_content_type(request: &Request<'_, App>) -> bool {
    let mut found = None;
    for (name, value) in request.headers() {
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
    json(status, error_body(status).to_vec())
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

fn encode(value: &impl serde::Serialize) -> Response {
    match serde_json::to_vec(value) {
        Ok(body) => json(200, body),
        Err(_) => error(500),
    }
}

fn locked(request: &Request<'_, App>) -> Result<std::sync::MutexGuard<'static, Store>, Response> {
    request.state().lock().map_err(|_| error(500))
}

pub fn ready() -> &'static str {
    "ready\n"
}

pub fn statement(request: Request<'_, App>) -> Response {
    let Some(account) = account(&request) else {
        return error(404);
    };
    let mut store = match locked(&request) {
        Ok(store) => store,
        Err(response) => return response,
    };

    match store.statement(account) {
        Ok(statement) => encode(&statement),
        Err(failure) => error(error_status(&failure)),
    }
}

pub fn transaction(request: Request<'_, App>) -> Response {
    let Some(account) = account(&request) else {
        return error(404);
    };
    let key = match idempotency_key(&request) {
        Ok(key) => key,
        Err(status) => return error(status),
    };
    if !json_content_type(&request) {
        return error(415);
    }
    let payment = match serde_json::from_slice::<Payment>(request.body()) {
        Ok(payment) => payment,
        Err(_) => return error(422),
    };
    let mut store = match locked(&request) {
        Ok(store) => store,
        Err(response) => return response,
    };

    match store.apply(account, key, &payment) {
        Ok(balance) => encode(&balance),
        Err(failure) => error(error_status(&failure)),
    }
}
