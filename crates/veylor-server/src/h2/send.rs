use std::borrow::Cow;

use veylor_http1::{CONTENT_RANGE_BYTES, MULTIPART_CTYPE_BYTES};
use veylor_http2::{
    ErrorCode, H2ServerDriver, HpackFieldToEncode, HpackRepresentation, status_sheds_length,
};

use crate::static_files::{
    FileHead, FilePlan, HTTP_DATE_BYTES, write_content_range, write_http_date,
};
use crate::static_routes::now_http_date;

use super::pending::PendingResponses;
use super::routes::{MAX_BEGIN_FIELDS, MAX_RESPONSE_FIELDS, METHOD_NOT_ALLOWED_BODY};
use super::{DECIMAL_CAP, HPACK_BLOCK_CAP};

// NOTE: file HEADERS assembly next to send_full: every status carries Date first (H1 parity:
// respond_file 200/206, respond_not_modified 304, respond_range_unsatisfiable 416 all emit
// Date first); 200/206 add content-length, content-type, accept-ranges, and last-modified;
// 304 carries no content-length (RFC 9110 8.6: only the would-be-200 length is legal, so the
// engine omits it on both protocols, gated below by status_sheds_length shared with
// send_full) plus last-modified; 416 carries content-length 0 plus
// content-range */total. Single-range
// 206 adds content-range; multipart 206 carries the dynamic boundary content-type instead
// of the file type and no top-level content-range (H1 parity, body pages via read_chunk).
pub(crate) fn send_file_head(
    server: &mut H2ServerDriver<'_, '_, '_, '_>,
    stream: u32,
    plan: &FilePlan,
    head: &FileHead,
    end_stream: bool,
    output: &mut [u8],
    used: &mut usize,
) -> Result<(), ErrorCode> {
    let mut status_buf = [0; DECIMAL_CAP];
    let status_len = push_decimal(&mut status_buf, u64::from(head.status));
    let mut length_buf = [0; DECIMAL_CAP];
    let mut mtime_buf = [0; HTTP_DATE_BYTES];
    let mtime_len = head
        .mtime
        .map(|mtime| write_http_date(mtime, &mut mtime_buf))
        .unwrap_or(0);
    let mut now_buf = [0; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now_buf);
    let mut range_buf = [0; CONTENT_RANGE_BYTES];
    let range_len = if head.status == 416 || head.window.is_some() {
        write_content_range(&mut range_buf, head.window, head.total)
    } else {
        0
    };
    let mut ctype_buf = [0; MULTIPART_CTYPE_BYTES];
    let ctype_len = if head.multipart {
        plan.multipart_content_type(&mut ctype_buf)
    } else {
        0
    };
    let mut fields = [field(b"", b""); 9];
    fields[0] = field(b":status", &status_buf[..status_len]);
    fields[1] = field(b"date", &now_buf[..now_len]);
    let mut count = 2;
    if !status_sheds_length(head.status) {
        let length_len = push_decimal(&mut length_buf, head.content_length);
        fields[count] = field(b"content-length", &length_buf[..length_len]);
        count += 1;
    }
    if head.multipart {
        fields[count] = field(b"content-type", &ctype_buf[..ctype_len]);
        count += 1;
    } else if let Some(content_type) = head.content_type {
        fields[count] = field(b"content-type", content_type.as_bytes());
        count += 1;
    }
    if head.accept_ranges {
        fields[count] = field(b"accept-ranges", b"bytes");
        count += 1;
    }
    if head.mtime.is_some() {
        fields[count] = field(b"last-modified", &mtime_buf[..mtime_len]);
        count += 1;
    }
    if head.status == 416 || head.window.is_some() {
        fields[count] = field(b"content-range", &range_buf[..range_len]);
        count += 1;
    }
    if let Some(encoding) = head.content_encoding {
        fields[count] = field(b"content-encoding", encoding.as_bytes());
        count += 1;
    }
    fields[count] = field(b"vary", b"accept-encoding");
    count += 1;
    let mut block = [0; HPACK_BLOCK_CAP];
    let sent = server
        .send_response(
            stream,
            &fields[..count],
            &mut block,
            &mut output[*used..],
            end_stream,
        )
        .map_err(|_| ErrorCode::InternalError)?;
    *used += sent.writer.pending_bytes().len();
    Ok(())
}

// NOTE: RFC 9113 forbids content-length on 1xx/204 responses and the driver rejects it
// at send_response time, so 204 emits :status plus Date with END_STREAM and never DATA;
// H1 carries Date on 204 the same way (body_allowed gates the body, never the Date).
pub(crate) fn send_status_only(
    server: &mut H2ServerDriver<'_, '_, '_, '_>,
    stream: u32,
    status: u16,
    output: &mut [u8],
    used: &mut usize,
) -> Result<(), ErrorCode> {
    let mut status_buf = [0; DECIMAL_CAP];
    let status_len = push_decimal(&mut status_buf, u64::from(status));
    let mut now_buf = [0; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now_buf);
    let response_fields = [
        field(b":status", &status_buf[..status_len]),
        field(b"date", &now_buf[..now_len]),
    ];
    let mut block = [0; HPACK_BLOCK_CAP];
    let sent = server
        .send_response(
            stream,
            &response_fields,
            &mut block,
            &mut output[*used..],
            true,
        )
        .map_err(|_| ErrorCode::InternalError)?;
    *used += sent.writer.pending_bytes().len();
    Ok(())
}

// NOTE: RFC 9113 section 8.2.2 length-match pin: a length-unknown Begin carries :status
// plus Date with END_STREAM clear, so the DATA that follows never contradicts a declared
// length (a content-length: 0 would make the first DATA a client-side PROTOCOL_ERROR).
// H1 Begin mirrors this with chunked framing plus Date (streams.rs Begin arm), and H2
// has no reason phrase, so :status plus Date is the whole head.
pub(crate) fn send_begin(
    server: &mut H2ServerDriver<'_, '_, '_, '_>,
    stream: u32,
    status: u16,
    extra: &[(&[u8], &[u8])],
    output: &mut [u8],
    used: &mut usize,
) -> Result<(), ErrorCode> {
    let mut status_buf = [0; DECIMAL_CAP];
    let status_len = push_decimal(&mut status_buf, u64::from(status));
    let mut now_buf = [0; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now_buf);
    let mut response_fields = [field(b"", b""); MAX_BEGIN_FIELDS];
    response_fields[0] = field(b":status", &status_buf[..status_len]);
    response_fields[1] = field(b"date", &now_buf[..now_len]);
    let mut field_count = 2;
    if extra.len() > response_fields.len() - field_count {
        return Err(ErrorCode::InternalError);
    }
    for (name, value) in extra {
        response_fields[field_count] = field(name, value);
        field_count += 1;
    }
    let mut block = [0; HPACK_BLOCK_CAP];
    let sent = server
        .send_response(
            stream,
            &response_fields[..field_count],
            &mut block,
            &mut output[*used..],
            false,
        )
        .map_err(|_| ErrorCode::InternalError)?;
    *used += sent.writer.pending_bytes().len();
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn send_full<'body>(
    server: &mut H2ServerDriver<'_, '_, '_, '_>,
    stream: u32,
    status: u16,
    body: Cow<'body, [u8]>,
    content_encoding: Option<&[u8]>,
    vary: bool,
    extra: &[(&[u8], &[u8])],
    end_stream: bool,
    send_body: bool,
    output: &mut [u8],
    used: &mut usize,
    pending: &mut PendingResponses,
) -> Result<bool, ErrorCode> {
    // NOTE: 204-only fast path (END_STREAM shape, not the shared length rule): 204 answers
    // :status plus Date with END_STREAM and never DATA through send_status_only, so it never
    // reaches the length gate below. status_sheds_length still covers it, and the gate below
    // agrees because 204 cannot fall through.
    if status == 204 {
        send_status_only(server, stream, status, output, used)?;
        return Ok(true);
    }
    // NOTE: a 304 never carries a body (RFC 9110 8.6 forbids the bytes; H1 drops them in
    // the shared sender the same way). Shed any staged body here so neither native nor
    // managed senders can emit DATA after the terminal HEADERS, and close the stream on
    // the head itself.
    let (body, end_stream): (Cow<'body, [u8]>, bool) = if status == 304 {
        (Cow::Borrowed(&[]), true)
    } else {
        (body, end_stream)
    };
    let body_bytes = body.as_ref();
    let mut status_buf = [0; DECIMAL_CAP];
    let status_len = push_decimal(&mut status_buf, u64::from(status));
    let mut length_buf = [0; DECIMAL_CAP];
    let mut now_buf = [0; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now_buf);
    let mut response_fields = [field(b"", b""); MAX_RESPONSE_FIELDS];
    response_fields[0] = field(b":status", &status_buf[..status_len]);
    response_fields[1] = field(b"date", &now_buf[..now_len]);
    let mut field_count = 2;
    if !status_sheds_length(status) {
        let length_len = push_decimal(&mut length_buf, body_bytes.len() as u64);
        response_fields[field_count] = field(b"content-length", &length_buf[..length_len]);
        field_count += 1;
    }
    if let Some(encoding) = content_encoding {
        response_fields[field_count] = field(b"content-encoding", encoding);
        field_count += 1;
    }
    if vary {
        response_fields[field_count] = field(b"vary", b"Accept-Encoding");
        field_count += 1;
    }
    if extra.len() > response_fields.len() - field_count {
        return Err(ErrorCode::InternalError);
    }
    for (name, value) in extra {
        response_fields[field_count] = field(name, value);
        field_count += 1;
    }
    let mut block = [0; HPACK_BLOCK_CAP];
    let sent = server
        .send_response(
            stream,
            &response_fields[..field_count],
            &mut block,
            &mut output[*used..],
            end_stream,
        )
        .map_err(|_| ErrorCode::InternalError)?;
    *used += sent.writer.pending_bytes().len();
    if end_stream || !send_body || body_bytes.is_empty() {
        return Ok(true);
    }

    pending.send(server, stream, body, output, used)
}

// NOTE: 405 HEADERS assembly beside send_full: :status plus Date plus content-length
// plus one comma-joined Allow literal (H1 encode_method_not_allowed shape). Date rides
// the existing now_http_date sender, no new Date machinery; per-stream fail-closed like
// send_full (HEADERS errors read InternalError, DATA errors FlowControlError, never GOAWAY
// here — the caller maps those to stream RSTs and the conn survives).
#[allow(clippy::too_many_arguments)]
pub(crate) fn send_method_not_allowed(
    server: &mut H2ServerDriver<'_, '_, '_, '_>,
    stream: u32,
    allow: &[u8],
    end_stream: bool,
    send_body: bool,
    output: &mut [u8],
    used: &mut usize,
    pending: &mut PendingResponses,
) -> Result<bool, ErrorCode> {
    let mut status_buf = [0; DECIMAL_CAP];
    let status_len = push_decimal(&mut status_buf, 405);
    let mut length_buf = [0; DECIMAL_CAP];
    let length_len = push_decimal(&mut length_buf, METHOD_NOT_ALLOWED_BODY.len() as u64);
    let mut now_buf = [0; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now_buf);
    let response_fields = [
        field(b":status", &status_buf[..status_len]),
        field(b"date", &now_buf[..now_len]),
        field(b"content-length", &length_buf[..length_len]),
        field(b"allow", allow),
    ];
    let mut block = [0; HPACK_BLOCK_CAP];
    let sent = server
        .send_response(
            stream,
            &response_fields,
            &mut block,
            &mut output[*used..],
            end_stream,
        )
        .map_err(|_| ErrorCode::InternalError)?;
    *used += sent.writer.pending_bytes().len();
    if end_stream || !send_body {
        return Ok(true);
    }

    pending.send(
        server,
        stream,
        Cow::Borrowed(METHOD_NOT_ALLOWED_BODY),
        output,
        used,
    )
}

pub(crate) fn send_options(
    server: &mut H2ServerDriver<'_, '_, '_, '_>,
    stream: u32,
    allow: &[u8],
    end_stream: bool,
    output: &mut [u8],
    used: &mut usize,
) -> Result<(), ErrorCode> {
    let mut status_buf = [0; DECIMAL_CAP];
    let status_len = push_decimal(&mut status_buf, 200);
    let mut length_buf = [0; DECIMAL_CAP];
    let length_len = push_decimal(&mut length_buf, 0);
    let mut now_buf = [0; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now_buf);
    let response_fields = [
        field(b":status", &status_buf[..status_len]),
        field(b"date", &now_buf[..now_len]),
        field(b"content-length", &length_buf[..length_len]),
        field(b"allow", allow),
    ];
    let mut block = [0; HPACK_BLOCK_CAP];
    let sent = server
        .send_response(
            stream,
            &response_fields,
            &mut block,
            &mut output[*used..],
            end_stream,
        )
        .map_err(|_| ErrorCode::InternalError)?;
    *used += sent.writer.pending_bytes().len();
    Ok(())
}

pub(crate) fn field<'a>(name: &'a [u8], value: &'a [u8]) -> HpackFieldToEncode<'a> {
    HpackFieldToEncode {
        name,
        value,
        representation: HpackRepresentation::Default,
        huffman_name: false,
        huffman_value: false,
    }
}

fn push_decimal(buf: &mut [u8; DECIMAL_CAP], mut value: u64) -> usize {
    let mut end = DECIMAL_CAP;
    loop {
        end -= 1;
        buf[end] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    let len = DECIMAL_CAP - end;
    buf.copy_within(end.., 0);
    len
}
