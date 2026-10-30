use veylor_http1::{CONTENT_RANGE_BYTES, MULTIPART_CTYPE_BYTES};
use veylor_http2::{
    ErrorCode, H2ServerDriver, HpackFieldToEncode, HpackRepresentation, status_sheds_length,
};

use crate::static_files::{FilePlan, HTTP_DATE_BYTES, write_http_date};
use crate::static_routes::now_http_date;

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
pub(crate) struct FileHead {
    pub(crate) status: u16,
    pub(crate) content_length: u64,
    pub(crate) content_type: Option<&'static str>,
    pub(crate) mtime: Option<u64>,
    pub(crate) total: u64,
    pub(crate) window: Option<(u64, u64)>,
    pub(crate) accept_ranges: bool,
    pub(crate) multipart: bool,
}

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
        write_file_content_range(&mut range_buf, head.window, head.total)
    } else {
        0
    };
    let mut ctype_buf = [0; MULTIPART_CTYPE_BYTES];
    let ctype_len = if head.multipart {
        plan.multipart_content_type(&mut ctype_buf)
    } else {
        0
    };
    let mut fields = [field(b"", b""); 7];
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

fn write_file_content_range(out: &mut [u8], window: Option<(u64, u64)>, total: u64) -> usize {
    use crate::static_files::{push_bytes, push_decimal};
    let mut len = push_bytes(out, 0, b"bytes ");
    match window {
        Some((start, last)) => {
            len = push_decimal(out, len, start);
            len = push_bytes(out, len, b"-");
            len = push_decimal(out, len, last);
            len = push_bytes(out, len, b"/");
        }
        None => len = push_bytes(out, len, b"*/"),
    }
    push_decimal(out, len, total)
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
pub(crate) fn send_full(
    server: &mut H2ServerDriver<'_, '_, '_, '_>,
    stream: u32,
    status: u16,
    body: &[u8],
    content_encoding: Option<&[u8]>,
    vary: bool,
    extra: &[(&[u8], &[u8])],
    end_stream: bool,
    send_body: bool,
    output: &mut [u8],
    used: &mut usize,
) -> Result<(), ErrorCode> {
    // NOTE: 204-only fast path (END_STREAM shape, not the shared length rule): 204 answers
    // :status plus Date with END_STREAM and never DATA through send_status_only, so it never
    // reaches the length gate below. status_sheds_length still covers it, and the gate below
    // agrees because 204 cannot fall through.
    if status == 204 {
        return send_status_only(server, stream, status, output, used);
    }
    // NOTE: a 304 never carries a body (RFC 9110 8.6 forbids the bytes; H1 drops them in
    // the shared sender the same way). Shed any staged body here so neither native nor
    // managed senders can emit DATA after the terminal HEADERS, and close the stream on
    // the head itself.
    let (body, end_stream) = if status == 304 {
        (&[][..], true)
    } else {
        (body, end_stream)
    };
    // NOTE: pre-flight the whole body against both send windows before staging anything:
    // a peer that shrank (or never opened) its windows gets a per-stream refusal it may
    // retry, never a torn-down connection or a truncated response. Zero output has been
    // staged at this point, so refusing is side-effect free; the DATA loop below can no
    // longer hit NoSendCredit because only our own exact debits move the windows.
    if !end_stream
        && send_body
        && !body.is_empty()
        && !response_fits_window(server, stream, body.len())
    {
        return Err(ErrorCode::RefusedStream);
    }
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
        let length_len = push_decimal(&mut length_buf, body.len() as u64);
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
    if end_stream || !send_body {
        return Ok(());
    }
    let mut offset = 0;
    while offset < body.len() {
        let room = output.len() - *used;
        if room < 10 {
            return Err(ErrorCode::EnhanceYourCalm);
        }
        let take = (body.len() - offset).min(room - 9);
        let last = offset + take == body.len();
        let sent = server
            .send_data(
                stream,
                &body[offset..offset + take],
                last,
                &mut output[*used..],
            )
            .map_err(|_| ErrorCode::FlowControlError)?;
        if sent.frame.payload_length == 0 {
            return Err(ErrorCode::FlowControlError);
        }
        *used += sent.writer.pending_bytes().len();
        offset += sent.frame.payload_length;
    }
    Ok(())
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
) -> Result<(), ErrorCode> {
    if !end_stream
        && send_body
        && !response_fits_window(server, stream, METHOD_NOT_ALLOWED_BODY.len())
    {
        return Err(ErrorCode::RefusedStream);
    }
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
        return Ok(());
    }
    let mut offset = 0;
    while offset < METHOD_NOT_ALLOWED_BODY.len() {
        let room = output.len() - *used;
        if room < 10 {
            return Err(ErrorCode::EnhanceYourCalm);
        }
        let take = (METHOD_NOT_ALLOWED_BODY.len() - offset).min(room - 9);
        let last = offset + take == METHOD_NOT_ALLOWED_BODY.len();
        let sent = server
            .send_data(
                stream,
                &METHOD_NOT_ALLOWED_BODY[offset..offset + take],
                last,
                &mut output[*used..],
            )
            .map_err(|_| ErrorCode::FlowControlError)?;
        if sent.frame.payload_length == 0 {
            return Err(ErrorCode::FlowControlError);
        }
        *used += sent.writer.pending_bytes().len();
        offset += sent.frame.payload_length;
    }
    Ok(())
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

// NOTE: single pre-flight gate for the H2 response senders below: the whole body must
// fit both send windows before anything is staged, so a stalled peer gets a per-stream
// refusal instead of a torn-down connection. Unknown streams read as zero credit, which
// the callers map to a silent drop through their usual closed/unknown handling.
fn response_fits_window(
    server: &H2ServerDriver<'_, '_, '_, '_>,
    stream: u32,
    body_len: usize,
) -> bool {
    let need = body_len as u64;
    let connection_open =
        u64::try_from(server.driver().connection_send_window().value()).unwrap_or(0);
    let stream_open = server.driver().streams().find(stream).map_or(0, |found| {
        u64::try_from(found.send_window.value()).unwrap_or(0)
    });
    connection_open >= need && stream_open >= need
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
