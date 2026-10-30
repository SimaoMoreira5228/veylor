use veylor_http1::{
    CONTENT_RANGE_BYTES, Field, Header, MULTIPART_CTYPE_BYTES, RequestHead, ResponseFraming,
    ResponseToWrite, TargetForm, connection_header_for_response, encode_response_head,
    response_permits_reuse,
};

use crate::static_files::{
    FilePlan, FileReject, HTTP_DATE_BYTES, RangeApply, parse_http_date, write_http_date,
};

use super::respond::{
    EncodedStaticResponse, encode_continue, encode_method_not_allowed, header_value, reject_payload,
};
use super::{MAX_STATIC_BODY_BYTES, RouteBuildError, StaticRoutes, now_http_date};

pub(crate) fn respond_file(
    routes: &StaticRoutes,
    head: RequestHead,
    headers: &[Header],
    input: &[u8],
    output: &mut [u8],
) -> Result<Option<EncodedStaticResponse>, RouteBuildError> {
    let Some((method, path)) = file_target(head, input) else {
        return Ok(None);
    };
    let request_version = head
        .protocol_version(input)
        .ok_or(RouteBuildError::Invalid)?;
    if head.is_chunked {
        return Ok(Some(reject_payload(output, method, request_version)?));
    }
    let drain_total = match head.content_length {
        Some(length) if length > MAX_STATIC_BODY_BYTES => {
            return Ok(Some(reject_payload(output, method, request_version)?));
        }
        Some(length) => length,
        None => 0,
    };
    let interim_len = if head.expect_continue {
        encode_continue(output)?
    } else {
        0
    };
    // NOTE: resolve reads non-GET/HEAD as no match without touching the fs; a GET probe
    // reuses the mount logic (prefix, trailing slash, decoding), so only a real file
    // resource answers 405 with Allow: GET, HEAD while traversal, directories, and
    // misses stay 404 with no existence oracle.
    if method != b"GET" && method != b"HEAD" {
        let probe = routes.files.resolve(b"GET", path);
        if matches!(probe, Ok(_) | Err(FileReject::TooLarge)) {
            let denied = encode_method_not_allowed(
                head,
                input,
                method,
                b"GET, HEAD",
                drain_total,
                interim_len,
                output,
            )?;
            return Ok(Some(denied));
        }
        return Ok(None);
    }
    let mut plan = match routes.files.resolve(method, path) {
        Ok(plan) => plan,
        Err(FileReject::TooLarge) => {
            return Ok(Some(reject_payload(output, method, request_version)?));
        }
        Err(FileReject::NotFound | FileReject::Forbidden | FileReject::NotAFile) => {
            return Ok(None);
        }
    };
    // NOTE: a satisfiable single range narrows the plan window for a 206 while the pump
    // keeps 0-based offsets; ignored headers fall back to 200 full and unsatisfiable
    // windows answer 416 with bytes */size and no body, closed like a 404.
    // NOTE (RFC 9110 section 14.2): range handling is defined only for GET; HEAD (the only
    // other file method here) ignores Range and answers 200 full headers.
    // NOTE: If-None-Match ignores unconditionally in v1: no ETags emit anywhere, so comparison is
    // impossible and If-Modified-Since alone drives 304. Preconditions evaluate before Range, for GET
    // and HEAD alike; malformed dates ignore to 200 full while future dates 304 by the same mtime <=
    // date rule, both sides already whole seconds so the comparison is per-second per RFC 9110 13.1.
    let fresh = header_value(headers, head, input, b"if-modified-since")
        .and_then(parse_http_date)
        .is_some_and(|since| plan.mtime <= since);
    if fresh {
        return Ok(Some(respond_not_modified(
            head,
            input,
            method,
            &plan,
            drain_total,
            interim_len,
            output,
        )?));
    }
    let range = (method == b"GET")
        .then(|| range_header_value(headers, head, input))
        .flatten();
    let outcome = plan.apply_range(range);
    if outcome == RangeApply::Unsatisfiable {
        return Ok(Some(respond_range_unsatisfiable(
            head,
            input,
            method,
            &plan,
            drain_total,
            interim_len,
            output,
        )?));
    }
    // NOTE: two or more satisfiable ranges frame as one multipart body: the virtual length rides plan.size and the pump pages it
    // through read_chunk, so this path only swaps the Content-Type.
    let multipart = outcome == RangeApply::Multipart;
    let status = if plan.partial { 206 } else { 200 };
    let reason = if plan.partial {
        b"Partial Content".as_slice()
    } else {
        b"OK".as_slice()
    };
    let connection = connection_header_for_response(
        head,
        input,
        status,
        ResponseFraming::FixedLength,
        true,
        false,
    );
    let mut value = [0u8; MULTIPART_CTYPE_BYTES];
    let mut date = [0u8; HTTP_DATE_BYTES];
    let date_len = write_http_date(plan.mtime, &mut date);
    let mut now = [0u8; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now);
    let range_len = if plan.partial && !multipart {
        write_content_range(
            &mut value,
            Some((plan.start, plan.start + plan.size - 1)),
            plan.total,
        )
    } else {
        0
    };
    let ctype_len = if multipart {
        plan.multipart_content_type(&mut value)
    } else {
        0
    };
    let ctype_value = if multipart {
        &value[..ctype_len]
    } else {
        plan.content_type.as_bytes()
    };
    let mut fields = [Field {
        name: b"Date",
        value: &now[..now_len],
    }; 5];
    fields[1] = Field {
        name: b"Content-Type",
        value: ctype_value,
    };
    fields[2] = Field {
        name: b"Accept-Ranges",
        value: b"bytes",
    };
    fields[3] = Field {
        name: b"Last-Modified",
        value: &date[..date_len],
    };
    let field_len = if plan.partial && !multipart {
        fields[4] = Field {
            name: b"Content-Range",
            value: &value[..range_len],
        };
        5
    } else {
        4
    };
    let response = ResponseToWrite {
        request_method: method,
        request_version,
        requested_upgrade: b"",
        status,
        reason,
        fields: &fields[..field_len],
        framing: ResponseFraming::FixedLength,
        content_length: plan.size,
        connection,
    };
    let final_buf = output
        .get_mut(interim_len..)
        .ok_or(RouteBuildError::Invalid)?;
    let encoded =
        encode_response_head(&response, final_buf).map_err(|_| RouteBuildError::Invalid)?;
    let length = interim_len + encoded.length;
    // NOTE: HEAD and empty files complete with the head alone; only GET with bytes
    // streams, so the pump never stages zero-length transfers.
    let file = if plan.head_only || plan.size == 0 {
        None
    } else {
        Some(plan)
    };
    Ok(Some(EncodedStaticResponse {
        length,
        keep_open: response_permits_reuse(false, connection),
        drain_total,
        interim_len,
        file,
    }))
}

fn range_header_value<'a>(
    headers: &'a [Header],
    head: RequestHead,
    input: &'a [u8],
) -> Option<&'a [u8]> {
    header_value(headers, head, input, b"range")
}

// NOTE: 304 mirrors the 416 small-fixed-response shape (no body, no file staging) but keeps alive:
// a cache revalidation is a normal response, not an error, so only the Last-Modified validator that
// the 200 would have carried rides along per RFC 9110 13.1, and the connection reuses as usual.
fn respond_not_modified(
    head: RequestHead,
    input: &[u8],
    method: &[u8],
    plan: &FilePlan,
    drain_total: u64,
    interim_len: usize,
    output: &mut [u8],
) -> Result<EncodedStaticResponse, RouteBuildError> {
    let request_version = head
        .protocol_version(input)
        .ok_or(RouteBuildError::Invalid)?;
    let mut date = [0u8; HTTP_DATE_BYTES];
    let date_len = write_http_date(plan.mtime, &mut date);
    let mut now = [0u8; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now);
    let fields = [
        Field {
            name: b"Date",
            value: &now[..now_len],
        },
        Field {
            name: b"Last-Modified",
            value: &date[..date_len],
        },
    ];
    let connection =
        connection_header_for_response(head, input, 304, ResponseFraming::FixedLength, true, false);
    let response = ResponseToWrite {
        request_method: method,
        request_version,
        requested_upgrade: b"",
        status: 304,
        reason: b"Not Modified",
        fields: &fields,
        framing: ResponseFraming::FixedLength,
        content_length: 0,
        connection,
    };
    let final_buf = output
        .get_mut(interim_len..)
        .ok_or(RouteBuildError::Invalid)?;
    let encoded =
        encode_response_head(&response, final_buf).map_err(|_| RouteBuildError::Invalid)?;
    Ok(EncodedStaticResponse {
        length: interim_len + encoded.length,
        keep_open: response_permits_reuse(false, connection),
        drain_total,
        interim_len,
        file: None,
    })
}

fn respond_range_unsatisfiable(
    head: RequestHead,
    input: &[u8],
    method: &[u8],
    plan: &FilePlan,
    drain_total: u64,
    interim_len: usize,
    output: &mut [u8],
) -> Result<EncodedStaticResponse, RouteBuildError> {
    let request_version = head
        .protocol_version(input)
        .ok_or(RouteBuildError::Invalid)?;
    let mut value = [0u8; CONTENT_RANGE_BYTES];
    let value_len = write_content_range(&mut value, None, plan.total);
    let mut now = [0u8; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now);
    let fields = [
        Field {
            name: b"Date",
            value: &now[..now_len],
        },
        Field {
            name: b"Content-Range",
            value: &value[..value_len],
        },
    ];
    let connection =
        connection_header_for_response(head, input, 416, ResponseFraming::FixedLength, true, true);
    let response = ResponseToWrite {
        request_method: method,
        request_version,
        requested_upgrade: b"",
        status: 416,
        reason: b"Range Not Satisfiable",
        fields: &fields,
        framing: ResponseFraming::FixedLength,
        content_length: 0,
        connection,
    };
    let final_buf = output
        .get_mut(interim_len..)
        .ok_or(RouteBuildError::Invalid)?;
    let encoded =
        encode_response_head(&response, final_buf).map_err(|_| RouteBuildError::Invalid)?;
    Ok(EncodedStaticResponse {
        length: interim_len + encoded.length,
        keep_open: response_permits_reuse(false, connection),
        drain_total,
        interim_len,
        file: None,
    })
}

fn write_content_range(out: &mut [u8], range: Option<(u64, u64)>, total: u64) -> usize {
    use crate::static_files::{push_bytes, push_decimal};
    let mut len = push_bytes(out, 0, b"bytes ");
    match range {
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

// NOTE: query strings strip like the router so /assets/a.css?x=1 names the file;
// fragments never arrive on the wire and anything else rejects inside resolve.
fn file_target(head: RequestHead, input: &[u8]) -> Option<(&[u8], &[u8])> {
    let method = head.method(input)?;
    let target = head.target(input)?;
    let path = match head.target_form {
        TargetForm::Origin => strip_query(target),
        TargetForm::Absolute => absolute_path(target)?,
        TargetForm::Authority | TargetForm::Asterisk => return None,
    };
    if path.is_empty() || path[0] != b'/' {
        return None;
    }
    Some((method, path))
}

fn strip_query(target: &[u8]) -> &[u8] {
    let end = target
        .iter()
        .position(|byte| *byte == b'?')
        .unwrap_or(target.len());
    &target[..end]
}

fn absolute_path(target: &[u8]) -> Option<&[u8]> {
    let colon = target.iter().position(|byte| *byte == b':')?;
    let after = target.get(colon + 1..)?;
    let authority = after.strip_prefix(b"//")?;
    let end = authority
        .iter()
        .position(|byte| matches!(byte, b'/' | b'?'))
        .unwrap_or(authority.len());
    if end == authority.len() || authority[end] == b'?' {
        return Some(b"/");
    }
    Some(strip_query(&authority[end..]))
}
