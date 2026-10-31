use std::sync::Arc;

use bytes::Bytes;
use http::header::{CONTENT_LENGTH, HeaderName, HeaderValue};
use http::{Method, Request, Response, StatusCode, Uri};
use quinn::{Connection, RecvStream, SendStream, VarInt};
use veylor_http3::frame::SettingsError;
use veylor_http3::{
    CONTROL_STREAM, H3_CLOSED_CRITICAL_STREAM, H3_EXCESSIVE_LOAD, H3_FRAME_ERROR,
    H3_FRAME_UNEXPECTED, H3_MESSAGE_ERROR, H3_MISSING_SETTINGS, H3_REQUEST_INCOMPLETE,
    H3_SETTINGS_ERROR, H3_STREAM_CREATION_ERROR, PUSH_STREAM, QPACK_DECODER_STREAM,
    QPACK_DECODER_STREAM_ERROR, QPACK_DECOMPRESSION_FAILED, QPACK_ENCODER_STREAM,
    QPACK_ENCODER_STREAM_ERROR, SETTINGS_MAX_FIELD_SECTION_SIZE, SETTINGS_QPACK_BLOCKED_STREAMS,
    SETTINGS_QPACK_MAX_TABLE_CAPACITY, frame, qpack,
};

use crate::frame_io::{close, read_byte, read_continuation, read_frame, read_varint};
use crate::{Body, Handler, Limits, ManagedOutcome, RequestTrailers};

const MAX_TRAILER_FIELDS: usize = 8;
const MAX_TRAILER_BYTES: usize = 1024;

pub(crate) async fn serve(connection: Connection, handler: Arc<dyn Handler>, limits: Limits) {
    if setup_control(&connection, limits).await.is_err() {
        return;
    }

    let uni = connection.clone();
    tokio::spawn(async move {
        accept_uni(uni, limits).await;
    });

    loop {
        let Ok((send, recv)) = connection.accept_bi().await else {
            return;
        };
        let handler = Arc::clone(&handler);
        let control = connection.clone();
        tokio::spawn(async move {
            let _ = handle_request(send, recv, handler, control, limits).await;
        });
    }
}

async fn setup_control(connection: &Connection, limits: Limits) -> Result<(), ()> {
    let mut control = connection.open_uni().await.map_err(|_| ())?;
    control
        .write_all(&[CONTROL_STREAM as u8])
        .await
        .map_err(|_| ())?;

    let settings = frame::encode_settings(&[
        (SETTINGS_QPACK_MAX_TABLE_CAPACITY, 0),
        (SETTINGS_QPACK_BLOCKED_STREAMS, 0),
        (SETTINGS_MAX_FIELD_SECTION_SIZE, limits.max_field_section),
    ]);
    let mut encoded = Vec::with_capacity(settings.len() + 16);
    frame::encode_frame(frame::SETTINGS, &settings, &mut encoded);
    control.write_all(&encoded).await.map_err(|_| ())?;

    let mut encoder = connection.open_uni().await.map_err(|_| ())?;
    encoder
        .write_all(&[QPACK_ENCODER_STREAM as u8])
        .await
        .map_err(|_| ())?;
    let mut decoder = connection.open_uni().await.map_err(|_| ())?;
    decoder
        .write_all(&[QPACK_DECODER_STREAM as u8])
        .await
        .map_err(|_| ())?;

    let connection = connection.clone();
    tokio::spawn(async move {
        let _ = connection.closed().await;
        drop((control, encoder, decoder));
    });
    Ok(())
}

async fn accept_uni(connection: Connection, limits: Limits) {
    let mut control_seen = false;
    let mut encoder_seen = false;
    let mut decoder_seen = false;

    loop {
        let Ok(mut recv) = connection.accept_uni().await else {
            return;
        };
        let Ok(Some(kind)) = read_varint(&mut recv).await else {
            continue;
        };
        match kind {
            CONTROL_STREAM => {
                if control_seen {
                    close(
                        &connection,
                        H3_STREAM_CREATION_ERROR,
                        "second control stream",
                    );
                    return;
                }
                control_seen = true;
                let control = connection.clone();
                tokio::spawn(async move {
                    read_control(control, recv, limits).await;
                });
            }
            QPACK_ENCODER_STREAM => {
                if encoder_seen {
                    close(
                        &connection,
                        H3_STREAM_CREATION_ERROR,
                        "second encoder stream",
                    );
                    return;
                }
                encoder_seen = true;
                let control = connection.clone();
                tokio::spawn(async move {
                    read_qpack(control, recv, kind).await;
                });
            }
            QPACK_DECODER_STREAM => {
                if decoder_seen {
                    close(
                        &connection,
                        H3_STREAM_CREATION_ERROR,
                        "second decoder stream",
                    );
                    return;
                }
                decoder_seen = true;
                let control = connection.clone();
                tokio::spawn(async move {
                    read_qpack(control, recv, kind).await;
                });
            }
            PUSH_STREAM => {
                close(&connection, H3_STREAM_CREATION_ERROR, "client push stream");
                return;
            }
            _ => {
                let _ = recv.stop(VarInt::from_u32(0));
            }
        }
    }
}

async fn read_control(connection: Connection, mut recv: RecvStream, limits: Limits) {
    let mut first = true;
    let limit = (limits.max_field_section as usize)
        .saturating_mul(2)
        .max(1024);

    loop {
        match read_frame(&mut recv, limit).await {
            Ok(Some((kind, payload))) => match kind {
                frame::SETTINGS => {
                    if !first {
                        close(&connection, H3_FRAME_UNEXPECTED, "second settings");
                        return;
                    }
                    match frame::decode_settings(&payload) {
                        Ok(_) => {}
                        Err(SettingsError::Truncated) => {
                            close(&connection, H3_FRAME_ERROR, "settings truncated");
                            return;
                        }
                        Err(_) => {
                            close(&connection, H3_SETTINGS_ERROR, "invalid settings");
                            return;
                        }
                    }
                    first = false;
                }
                frame::DATA | frame::HEADERS => {
                    close(&connection, H3_FRAME_UNEXPECTED, "request frame on control");
                    return;
                }
                _ => {
                    if first {
                        close(&connection, H3_MISSING_SETTINGS, "settings must be first");
                        return;
                    }
                }
            },
            Ok(None) => {
                close(
                    &connection,
                    H3_CLOSED_CRITICAL_STREAM,
                    "control stream closed",
                );
                return;
            }
            Err(()) => {
                close(&connection, H3_EXCESSIVE_LOAD, "control frame too large");
                return;
            }
        }
    }
}

async fn read_qpack(connection: Connection, mut recv: RecvStream, kind: u64) {
    if kind == QPACK_ENCODER_STREAM {
        loop {
            let Some(first) = read_byte(&mut recv).await else {
                close(
                    &connection,
                    H3_CLOSED_CRITICAL_STREAM,
                    "encoder stream closed",
                );
                return;
            };
            if first & 0xE0 != 0x20 {
                close(
                    &connection,
                    QPACK_ENCODER_STREAM_ERROR,
                    "dynamic qpack insert",
                );
                return;
            }
            let mut capacity = u64::from(first & 0x1f);
            if capacity == 0x1f {
                let Some(value) = read_continuation(&mut recv).await else {
                    close(
                        &connection,
                        H3_CLOSED_CRITICAL_STREAM,
                        "encoder stream truncated",
                    );
                    return;
                };
                capacity = value;
            }
            if capacity != 0 {
                close(
                    &connection,
                    QPACK_ENCODER_STREAM_ERROR,
                    "dynamic qpack capacity",
                );
                return;
            }
        }
    }

    loop {
        let Some(first) = read_byte(&mut recv).await else {
            close(
                &connection,
                H3_CLOSED_CRITICAL_STREAM,
                "decoder stream closed",
            );
            return;
        };
        if first & 0x80 != 0 {
            if first & 0x7f == 0x7f {
                let Some(_) = read_continuation(&mut recv).await else {
                    close(
                        &connection,
                        H3_CLOSED_CRITICAL_STREAM,
                        "decoder stream truncated",
                    );
                    return;
                };
            }
        } else if first & 0x40 != 0 {
            if first & 0x3f == 0x3f {
                let Some(_) = read_continuation(&mut recv).await else {
                    close(
                        &connection,
                        H3_CLOSED_CRITICAL_STREAM,
                        "decoder stream truncated",
                    );
                    return;
                };
            }
        } else {
            close(
                &connection,
                QPACK_DECODER_STREAM_ERROR,
                "insert count increment",
            );
            return;
        }
    }
}

async fn handle_request(
    mut send: SendStream,
    mut recv: RecvStream,
    handler: Arc<dyn Handler>,
    connection: Connection,
    limits: Limits,
) -> Result<(), ()> {
    let limit = (limits.max_field_section as usize).saturating_mul(2);
    let Some((kind, payload)) = read_frame(&mut recv, limit).await.map_err(|_| ())? else {
        close(
            &connection,
            H3_REQUEST_INCOMPLETE,
            "request finished before headers",
        );
        return Ok(());
    };
    if kind != frame::HEADERS {
        close(&connection, H3_FRAME_UNEXPECTED, "expected headers");
        return Ok(());
    }
    let Some(fields) = qpack::decode(&payload, limits.max_field_section as usize).ok() else {
        close(
            &connection,
            QPACK_DECOMPRESSION_FAILED,
            "header block decode failed",
        );
        return Ok(());
    };
    let Some(mut request) = request_from_fields(&fields) else {
        close(&connection, H3_MESSAGE_ERROR, "malformed request");
        return Ok(());
    };

    let mut body = Vec::new();
    let mut trailers = Vec::new();
    loop {
        match read_frame(&mut recv, limits.max_request_body.saturating_add(1)).await {
            Ok(Some((kind, payload))) => match kind {
                frame::DATA => {
                    if body.len().saturating_add(payload.len()) > limits.max_request_body {
                        close(&connection, H3_EXCESSIVE_LOAD, "request body too large");
                        return Ok(());
                    }
                    body.extend_from_slice(&payload);
                }
                frame::HEADERS => {
                    if payload.len() > limits.max_field_section as usize
                        || !append_trailers(
                            &mut trailers,
                            &payload,
                            limits.max_field_section as usize,
                        )
                    {
                        close(&connection, H3_MESSAGE_ERROR, "invalid request trailers");
                        return Ok(());
                    }
                }
                frame::SETTINGS
                | frame::GOAWAY
                | frame::MAX_PUSH_ID
                | frame::CANCEL_PUSH
                | frame::PUSH_PROMISE => {
                    close(&connection, H3_FRAME_UNEXPECTED, "unexpected request frame");
                    return Ok(());
                }
                _ => {}
            },
            Ok(None) => break,
            Err(()) => {
                close(&connection, H3_FRAME_ERROR, "invalid request frame");
                return Ok(());
            }
        }
    }

    if !trailers.is_empty() {
        request.extensions_mut().insert(RequestTrailers(trailers));
    }
    let is_head = request.method() == Method::HEAD;
    let outcome = {
        let mut writer = crate::ResponseWriter::new(&mut send);
        handler
            .managed(request, Bytes::from(body), &mut writer)
            .await
    };
    let response = match outcome {
        ManagedOutcome::Handled(response) => response,
        ManagedOutcome::Pass(request, body) => handler.handle(request, body),
        ManagedOutcome::Streamed => return Ok(()),
    };
    let response = if response
        .body()
        .content_length()
        .is_some_and(|length| length > limits.max_response_body as u64)
    {
        crate::text_response(StatusCode::INTERNAL_SERVER_ERROR, "response too large")
    } else {
        response
    };
    write_response(&mut send, response, is_head).await;
    Ok(())
}

const STREAM_CHUNK_BYTES: usize = 64 * 1024;

async fn write_response(send: &mut SendStream, response: Response<Body>, is_head: bool) {
    let (parts, body) = response.into_parts();
    let status = parts.status;
    let status_text = status.as_u16().to_string();
    let mut fields: Vec<(&[u8], &[u8])> = Vec::with_capacity(parts.headers.len() + 2);
    fields.push((b":status", status_text.as_bytes()));
    let header_length = is_head
        .then(|| parts.headers.get(CONTENT_LENGTH))
        .flatten()
        .and_then(|value| value.to_str().ok());
    let length = header_length
        .map(str::to_owned)
        .or_else(|| body.content_length().map(|length| length.to_string()));
    let sheds_length = matches!(status, StatusCode::NO_CONTENT | StatusCode::NOT_MODIFIED);
    if let Some(length) = length.as_deref() {
        if !sheds_length {
            fields.push((b"content-length", length.as_bytes()));
        }
    }
    for (name, value) in &parts.headers {
        if matches!(
            name.as_str(),
            "connection"
                | "keep-alive"
                | "proxy-connection"
                | "transfer-encoding"
                | "upgrade"
                | "content-length"
                | ":status"
        ) {
            continue;
        }
        fields.push((name.as_str().as_bytes(), value.as_bytes()));
    }

    let block = qpack::encode(&fields);
    let mut encoded = Vec::with_capacity(block.len() + 16);
    frame::encode_frame(frame::HEADERS, &block, &mut encoded);
    if send.write_all(&encoded).await.is_err() {
        return;
    }

    let sends_body = !is_head && !sheds_length && status != StatusCode::RESET_CONTENT;
    if sends_body {
        match body {
            Body::Bytes(body) => {
                if !body.is_empty() {
                    let mut data = Vec::with_capacity(body.len() + 16);
                    frame::encode_frame(frame::DATA, &body, &mut data);
                    if send.write_all(&data).await.is_err() {
                        return;
                    }
                }
            }
            Body::Stream(mut body) => {
                let mut scratch = vec![0u8; STREAM_CHUNK_BYTES];
                let mut head = [0u8; 16];
                loop {
                    let read = match body.read(&mut scratch) {
                        Ok(0) => break,
                        Ok(read) => read,
                        Err(_) => {
                            let _ = send.reset(VarInt::from_u32(0));
                            return;
                        }
                    };
                    let header = frame::encode_frame_header(frame::DATA, read as u64, &mut head);
                    if send.write_all(&head[..header]).await.is_err() {
                        return;
                    }
                    if send.write_all(&scratch[..read]).await.is_err() {
                        return;
                    }
                }
            }
        }
    }
    let _ = send.finish();
}

fn append_trailers(
    trailers: &mut Vec<(Vec<u8>, Vec<u8>)>,
    payload: &[u8],
    field_limit: usize,
) -> bool {
    let Some(fields) = qpack::decode(payload, field_limit).ok() else {
        return false;
    };
    if trailers.len().saturating_add(fields.len()) > MAX_TRAILER_FIELDS {
        return false;
    }
    let mut total: usize = trailers.iter().map(|(name, _)| name.len()).sum();
    total += trailers.iter().map(|(_, value)| value.len()).sum::<usize>();
    for (name, value) in fields {
        if name.first() == Some(&b':') || !valid_name(&name) || !valid_value(&value) {
            return false;
        }
        total = total.saturating_add(name.len()).saturating_add(value.len());
        if total > MAX_TRAILER_BYTES {
            return false;
        }
        trailers.push((name, value));
    }
    true
}

fn request_from_fields(fields: &[qpack::Field]) -> Option<Request<()>> {
    let mut method = None;
    let mut scheme = None;
    let mut authority = None;
    let mut path = None;
    let mut regular = false;
    let mut headers = Vec::new();

    for (name, value) in fields {
        if name.first() == Some(&b':') {
            if regular {
                return None;
            }
            match name.as_slice() {
                b":method" => set_once(&mut method, value)?,
                b":scheme" => set_once(&mut scheme, value)?,
                b":authority" => set_once(&mut authority, value)?,
                b":path" => set_once(&mut path, value)?,
                _ => return None,
            }
            continue;
        }

        regular = true;
        if !valid_name(name) || !valid_value(value) {
            return None;
        }
        if matches!(
            name.as_slice(),
            b"connection" | b"keep-alive" | b"proxy-connection" | b"transfer-encoding" | b"upgrade"
        ) {
            return None;
        }
        if name == b"te" && value != b"trailers" {
            return None;
        }
        headers.push((name, value));
    }

    let method = method?;
    let scheme = scheme?;
    let authority = authority?;
    let path = path?;
    if method == b"CONNECT" || (scheme != b"https" && scheme != b"http") {
        return None;
    }
    if path != b"*" && path.first() != Some(&b'/') {
        return None;
    }

    let mut uri = Vec::with_capacity(scheme.len() + authority.len() + path.len() + 3);
    uri.extend_from_slice(scheme);
    uri.extend_from_slice(b"://");
    uri.extend_from_slice(authority);
    uri.extend_from_slice(path);

    let mut request = Request::new(());
    *request.method_mut() = Method::from_bytes(method).ok()?;
    *request.uri_mut() = Uri::try_from(uri).ok()?;
    for (name, value) in headers {
        let name = HeaderName::from_bytes(name).ok()?;
        let value = HeaderValue::from_bytes(value).ok()?;
        request.headers_mut().append(name, value);
    }
    Some(request)
}

fn set_once<'a>(slot: &mut Option<&'a [u8]>, value: &'a [u8]) -> Option<()> {
    if slot.is_some() {
        return None;
    }
    *slot = Some(value);
    Some(())
}

fn valid_name(name: &[u8]) -> bool {
    !name.is_empty()
        && name.iter().copied().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(
                    byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
}

fn valid_value(value: &[u8]) -> bool {
    !value.iter().any(|byte| {
        *byte == 0
            || *byte == b'\r'
            || *byte == b'\n'
            || *byte == 0x7f
            || (*byte < 0x20 && *byte != b'\t')
    })
}
