use veylor_http1::{
    CONTENT_RANGE_BYTES, Field, Header, MULTIPART_CTYPE_BYTES, RequestHead, ResponseFraming,
    ResponseToWrite, TargetForm, connection_header_for_response, encode_response_head_trusted,
    response_permits_reuse,
};

use crate::static_files::{
    FileReject, HTTP_DATE_BYTES, plan_file_head, write_content_range, write_http_date,
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
    let plan = match routes.files.resolve(method, path) {
        Ok(plan) => plan,
        Err(FileReject::TooLarge) => {
            return Ok(Some(reject_payload(output, method, request_version)?));
        }
        Err(FileReject::NotFound | FileReject::Forbidden | FileReject::NotAFile) => {
            return Ok(None);
        }
    };
    let since = header_value(headers, head, input, b"if-modified-since");
    let range = header_value(headers, head, input, b"range");
    let (plan, file_head, send_body) = plan_file_head(plan, method, range, since);
    let (status, reason) = match file_head.status {
        304 => (304, b"Not Modified".as_slice()),
        416 => (416, b"Range Not Satisfiable".as_slice()),
        206 => (206, b"Partial Content".as_slice()),
        _ => (200, b"OK".as_slice()),
    };
    let connection = connection_header_for_response(
        head,
        input,
        status,
        ResponseFraming::FixedLength,
        true,
        status == 416,
    );

    let mut ctype = [0u8; MULTIPART_CTYPE_BYTES];
    let mut range_value = [0u8; CONTENT_RANGE_BYTES];
    let mut date = [0u8; HTTP_DATE_BYTES];
    let mut now = [0u8; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now);
    let mut fields = [Field {
        name: b"",
        value: &[],
    }; 5];
    let mut field_len = 0;
    fields[field_len] = Field {
        name: b"Date",
        value: &now[..now_len],
    };
    field_len += 1;
    if file_head.multipart {
        let len = plan.multipart_content_type(&mut ctype);
        fields[field_len] = Field {
            name: b"Content-Type",
            value: &ctype[..len],
        };
        field_len += 1;
    } else if let Some(content_type) = file_head.content_type {
        fields[field_len] = Field {
            name: b"Content-Type",
            value: content_type.as_bytes(),
        };
        field_len += 1;
    }
    if file_head.accept_ranges {
        fields[field_len] = Field {
            name: b"Accept-Ranges",
            value: b"bytes",
        };
        field_len += 1;
    }
    if let Some(mtime) = file_head.mtime {
        let len = write_http_date(mtime, &mut date);
        fields[field_len] = Field {
            name: b"Last-Modified",
            value: &date[..len],
        };
        field_len += 1;
    }
    if status == 416 || file_head.window.is_some() {
        let len = write_content_range(&mut range_value, file_head.window, file_head.total);
        fields[field_len] = Field {
            name: b"Content-Range",
            value: &range_value[..len],
        };
        field_len += 1;
    }

    let content_length = file_head.content_length;
    let response = ResponseToWrite {
        request_method: method,
        request_version,
        requested_upgrade: b"",
        status,
        reason,
        fields: &fields[..field_len],
        framing: ResponseFraming::FixedLength,
        content_length,
        connection,
    };
    let final_buf = output
        .get_mut(interim_len..)
        .ok_or(RouteBuildError::Invalid)?;
    let encoded =
        encode_response_head_trusted(&response, final_buf).map_err(|_| RouteBuildError::Invalid)?;
    let length = interim_len + encoded.length;
    let file = if send_body && plan.size > 0 {
        Some(plan)
    } else {
        None
    };
    Ok(Some(EncodedStaticResponse {
        length,
        keep_open: response_permits_reuse(false, connection),
        drain_total,
        interim_len,
        file,
    }))
}

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
