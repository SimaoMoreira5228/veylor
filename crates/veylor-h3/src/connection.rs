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
    QPACK_ENCODER_STREAM_ERROR, SETTINGS_ENABLE_CONNECT_PROTOCOL, SETTINGS_MAX_FIELD_SECTION_SIZE,
    SETTINGS_QPACK_BLOCKED_STREAMS, SETTINGS_QPACK_MAX_TABLE_CAPACITY, frame, qpack,
};

use crate::frame_io::{close, read_byte, read_continuation, read_frame, read_varint};
use crate::{
    Body, ConnectProtocol, Handler, Limits, ManagedOutcome, RequestStream, RequestTrailers,
};

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
        let _ = handle_request(send, recv, Arc::clone(&handler), connection.clone(), limits).await;
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
        (SETTINGS_ENABLE_CONNECT_PROTOCOL, 1),
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
    let mut header_scratch = Vec::with_capacity(1024);
    let mut fields = Vec::with_capacity(16);
    if qpack::decode_into(
        &payload,
        limits.max_field_section as usize,
        &mut header_scratch,
        &mut fields,
    )
    .is_err()
    {
        close(
            &connection,
            QPACK_DECOMPRESSION_FAILED,
            "header block decode failed",
        );
        return Ok(());
    }
    let Some(mut request) = request_from_fields(&fields, &header_scratch) else {
        close(&connection, H3_MESSAGE_ERROR, "malformed request");
        return Ok(());
    };

    if request.method() == Method::CONNECT {
        let outcome = {
            let mut writer = crate::ResponseWriter::new(&mut send);
            handler
                .connect(request, RequestStream::new(recv), &mut writer)
                .await
        };
        match outcome {
            ManagedOutcome::Handled(response) => write_response(&mut send, response, false).await,
            ManagedOutcome::Pass(_, _) => {
                write_response(
                    &mut send,
                    crate::text_response(StatusCode::NOT_IMPLEMENTED, "connect unsupported"),
                    false,
                )
                .await;
            }
            ManagedOutcome::Streamed => {}
        }
        return Ok(());
    }
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
    let body = Bytes::from(body);
    let response = if handler.is_direct(&request) {
        handler.handle(request, body)
    } else {
        let outcome = {
            let mut writer = crate::ResponseWriter::new(&mut send);
            handler.managed(request, body, &mut writer).await
        };
        match outcome {
            ManagedOutcome::Handled(response) => response,
            ManagedOutcome::Pass(request, body) => handler.handle(request, body),
            ManagedOutcome::Streamed => return Ok(()),
        }
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
    let mut status_buf = [0u8; 3];
    let status_len = write_decimal(&mut status_buf, u64::from(status.as_u16()));
    let status_text = &status_buf[..status_len];
    let sheds_length = matches!(status, StatusCode::NO_CONTENT | StatusCode::NOT_MODIFIED);

    let mut length_buf = [0u8; 20];
    let declared = is_head.then(|| parts.headers.get(CONTENT_LENGTH)).flatten();
    let length: Option<&[u8]> = match declared {
        Some(value) => Some(value.as_bytes()),
        None => body.content_length().map(|length| {
            let written = write_decimal(&mut length_buf, length);
            &length_buf[..written]
        }),
    };

    let mut fields: Vec<(&[u8], &[u8])> = Vec::with_capacity(parts.headers.len() + 2);
    fields.push((b":status", status_text));
    if let Some(length) = length
        && !sheds_length
    {
        fields.push((b"content-length", length));
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

    let block_length = qpack::encoded_len(&fields);
    let sends_body = !is_head && !sheds_length && status != StatusCode::RESET_CONTENT;
    let mut head = Vec::with_capacity(12 + block_length);
    let mut header = [0u8; 12];
    let written = frame::encode_frame_header(frame::HEADERS, block_length as u64, &mut header);
    head.extend_from_slice(&header[..written]);
    qpack::encode_into(&fields, &mut head);

    match body {
        Body::Bytes(body) if sends_body && !body.is_empty() => {
            let written = frame::encode_frame_header(frame::DATA, body.len() as u64, &mut header);
            head.extend_from_slice(&header[..written]);
            let mut chunks = [Bytes::from(head), body];
            if send.write_all_chunks(&mut chunks).await.is_err() {
                return;
            }
        }
        Body::Bytes(_) => {
            if send.write_all(&head).await.is_err() {
                return;
            }
        }
        Body::Stream(mut body) => {
            if send.write_all(&head).await.is_err() {
                return;
            }
            let mut scratch = vec![0u8; STREAM_CHUNK_BYTES];
            let mut frame_header = [0u8; 16];
            loop {
                let read = match body.read(&mut scratch) {
                    Ok(0) => break,
                    Ok(read) => read,
                    Err(_) => {
                        let _ = send.reset(VarInt::from_u32(0));
                        return;
                    }
                };
                let written =
                    frame::encode_frame_header(frame::DATA, read as u64, &mut frame_header);
                if send.write_all(&frame_header[..written]).await.is_err() {
                    return;
                }
                if send.write_all(&scratch[..read]).await.is_err() {
                    return;
                }
            }
        }
    }
    let _ = send.finish();
}

fn write_decimal(out: &mut [u8], mut value: u64) -> usize {
    let mut digits = [0u8; 20];
    let mut count = 0;
    loop {
        digits[count] = b'0' + (value % 10) as u8;
        count += 1;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    for index in 0..count {
        out[index] = digits[count - 1 - index];
    }
    count
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

fn request_from_fields(fields: &[qpack::FieldView], scratch: &[u8]) -> Option<Request<()>> {
    let mut method = None;
    let mut scheme = None;
    let mut authority = None;
    let mut path = None;
    let mut protocol = None;
    let mut regular = false;
    let mut headers = Vec::new();

    for view in fields {
        let name = &scratch[view.name.clone()];
        let value = &scratch[view.value.clone()];
        if name.first() == Some(&b':') {
            if regular {
                return None;
            }
            match name {
                b":method" => set_once(&mut method, value)?,
                b":scheme" => set_once(&mut scheme, value)?,
                b":authority" => set_once(&mut authority, value)?,
                b":path" => set_once(&mut path, value)?,
                b":protocol" => set_once(&mut protocol, value)?,
                _ => return None,
            }
            continue;
        }

        regular = true;
        if !valid_name(name) || !valid_value(value) {
            return None;
        }
        if matches!(
            name,
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
    if scheme != b"https" && scheme != b"http" {
        return None;
    }
    if path != b"*" && path.first() != Some(&b'/') {
        return None;
    }
    let connect = method == b"CONNECT";
    if connect {
        protocol?;
    } else if protocol.is_some() {
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
    if let Some(protocol) = protocol {
        if protocol.is_empty() {
            return None;
        }
        request
            .extensions_mut()
            .insert(ConnectProtocol(protocol.to_vec()));
    }
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
