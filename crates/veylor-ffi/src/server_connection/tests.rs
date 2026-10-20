use super::*;
use crate::abi::{VylHeader, VylResult};

#[test]
fn parses_split_request_heads_and_resets_without_reallocating_buffers() {
    let mut connection = std::ptr::null_mut();
    let create = unsafe { vyl_http1_server_connection_new(&mut connection) };
    assert_eq!(create, VylResult::Ok);

    let input = b"GET /health HTTP/1.1\r\nHost: example.test\r\nAccept: */*\r\n\r\n";
    let prefix_length = b"GET /health HTTP/1.1\r\nHost: example.test\r\nAcc".len();
    let mut headers = [VylHeader::default(); 4];
    let mut head = VylRequestHead::default();
    let mut headers_required = usize::MAX;
    let partial = unsafe {
        vyl_http1_server_connection_parse_head(
            connection,
            input.as_ptr(),
            prefix_length,
            headers.as_mut_ptr(),
            headers.len(),
            &mut head,
            &mut headers_required,
        )
    };
    assert_eq!(partial, VylResult::NeedMore);
    assert_eq!(headers_required, 0);

    let complete = unsafe {
        vyl_http1_server_connection_parse_head(
            connection,
            input.as_ptr(),
            input.len(),
            headers.as_mut_ptr(),
            headers.len(),
            &mut head,
            &mut headers_required,
        )
    };
    assert_eq!(complete, VylResult::Ok);
    assert_eq!(head.target_form, VylTargetForm::Origin);
    assert_eq!(head.header_count, 2);
    assert_eq!(headers_required, 2);
    assert_eq!(
        &input[head.method.offset..head.method.offset + head.method.length],
        b"GET"
    );

    assert_eq!(
        unsafe { vyl_http1_server_connection_reset(connection) },
        VylResult::Ok
    );
    assert_eq!(
        unsafe { vyl_http1_server_connection_destroy(connection) },
        VylResult::Ok
    );
}

#[test]
fn reports_header_storage_requirements_and_resumes_with_the_same_base() {
    let mut connection = std::ptr::null_mut();
    assert_eq!(
        unsafe { vyl_http1_server_connection_new(&mut connection) },
        VylResult::Ok
    );

    let input = b"GET / HTTP/1.1\r\nHost: example.test\r\nAccept: */*\r\n\r\n";
    let mut headers = [VylHeader::default(); 2];
    let mut head = VylRequestHead::default();
    let mut headers_required = 0;
    let short = unsafe {
        vyl_http1_server_connection_parse_head(
            connection,
            input.as_ptr(),
            input.len(),
            headers.as_mut_ptr(),
            1,
            &mut head,
            &mut headers_required,
        )
    };
    assert_eq!(short, VylResult::BufferTooSmall);
    assert_eq!(headers_required, 2);

    let complete = unsafe {
        vyl_http1_server_connection_parse_head(
            connection,
            input.as_ptr(),
            input.len(),
            headers.as_mut_ptr(),
            headers.len(),
            &mut head,
            &mut headers_required,
        )
    };
    assert_eq!(complete, VylResult::Ok);
    assert_eq!(head.header_count, 2);

    unsafe {
        vyl_http1_server_connection_destroy(connection);
    }
}

#[test]
fn rejects_null_outputs_and_destroys_null_handles() {
    assert_eq!(
        unsafe { vyl_http1_server_connection_new(std::ptr::null_mut()) },
        VylResult::InvalidArgument
    );
    assert_eq!(
        unsafe { vyl_http1_server_connection_destroy(std::ptr::null_mut()) },
        VylResult::Ok
    );
}
