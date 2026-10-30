use std::boxed::Box;
use std::ptr;
use std::slice;

use crate::abi::{
    VylBytes, VylField, VylHeader, VylResult, VylSpan, catch_ffi, slice_length_valid,
};
use veylor_http1::{Limits, RequestHead, TargetForm};
use veylor_server_conn::{ResponseFraming, ResponseHeadParams, ServerConnection, ServerError};

use super::map_error;

pub const VYL_HTTP1_HAS_CONTENT_LENGTH: u32 = 1;
pub const VYL_HTTP1_CHUNKED: u32 = 2;
pub const VYL_HTTP1_EXPECT_CONTINUE: u32 = 4;
pub const VYL_HTTP1_CONNECTION_CLOSE: u32 = 8;
pub const VYL_HTTP1_CONNECTION_KEEP_ALIVE: u32 = 16;
pub const VYL_HTTP1_CONNECTION_UPGRADE: u32 = 32;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VylRequestHead {
    pub method: VylSpan,
    pub target: VylSpan,
    pub authority: VylSpan,
    pub version: VylSpan,
    pub target_form: VylTargetForm,
    pub header_count: usize,
    pub length: usize,
    pub content_length: u64,
    pub flags: u32,
}

#[repr(u32)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum VylTargetForm {
    #[default]
    Origin = 0,
    Absolute = 1,
    Authority = 2,
    Asterisk = 3,
}

pub struct VylServerConnection {
    pub(super) inner: ServerConnection<'static>,
    pub(super) headers_base: *const VylHeader,
}

struct ServerResponseHeadArguments {
    input: *const u8,
    input_length: usize,
    request_headers: *const VylHeader,
    request_header_capacity: usize,
    status: u16,
    reason: VylBytes,
    fields: *const VylField,
    field_count: usize,
    framing: u32,
    content_length: u64,
    request_body_complete: u32,
    response_close: u32,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_body_allowed: *mut u32,
    out_required: *mut usize,
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_server_connection_new(
    out_connection: *mut *mut VylServerConnection,
) -> VylResult {
    catch_ffi(|| unsafe { create_connection(out_connection) })
}

unsafe fn create_connection(out_connection: *mut *mut VylServerConnection) -> VylResult {
    if out_connection.is_null() {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_connection.write(ptr::null_mut());
    }

    let connection = Box::new(VylServerConnection {
        inner: ServerConnection::new(),
        headers_base: ptr::null(),
    });
    unsafe {
        out_connection.write(Box::into_raw(connection));
    }
    VylResult::Ok
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_server_connection_parse_head(
    connection: *mut VylServerConnection,
    input: *const u8,
    input_length: usize,
    headers: *mut VylHeader,
    header_capacity: usize,
    out_head: *mut VylRequestHead,
    out_headers_required: *mut usize,
) -> VylResult {
    catch_ffi(|| unsafe {
        parse_request_head(
            connection,
            input,
            input_length,
            headers,
            header_capacity,
            out_head,
            out_headers_required,
        )
    })
}

unsafe fn parse_request_head(
    connection: *mut VylServerConnection,
    input: *const u8,
    input_length: usize,
    headers: *mut VylHeader,
    header_capacity: usize,
    out_head: *mut VylRequestHead,
    out_headers_required: *mut usize,
) -> VylResult {
    if connection.is_null()
        || (input.is_null() && input_length != 0)
        || (headers.is_null() && header_capacity != 0)
        || !slice_length_valid::<u8>(input_length)
        || !slice_length_valid::<VylHeader>(header_capacity)
        || out_head.is_null()
        || out_headers_required.is_null()
    {
        return VylResult::InvalidArgument;
    }

    unsafe {
        out_head.write(VylRequestHead::default());
        out_headers_required.write(0);
    }

    let connection = unsafe { &mut *connection };
    // SECURITY: the input and header borrows are erased to 'static under the documented
    // caller contract that both bases stay stable until the parser completes or resets.
    let input = if input_length == 0 {
        &[]
    } else {
        unsafe {
            std::mem::transmute::<&[u8], &'static [u8]>(slice::from_raw_parts(input, input_length))
        }
    };
    let headers = if header_capacity == 0 {
        &mut []
    } else {
        unsafe {
            std::mem::transmute::<&mut [VylHeader], &'static mut [VylHeader]>(
                slice::from_raw_parts_mut(headers, header_capacity),
            )
        }
    };
    let headers_base = headers.as_ptr();

    match connection.inner.parse_head(input, headers) {
        Ok(head) => {
            connection.headers_base = headers_base;
            unsafe {
                out_headers_required.write(head.header_count);
                out_head.write(to_ffi_head(head));
            }
            VylResult::Ok
        }
        Err(ServerError::NeedMore) => VylResult::NeedMore,
        Err(ServerError::HeaderCapacity {
            headers_required, ..
        }) => {
            unsafe {
                out_headers_required.write(headers_required);
            }
            VylResult::BufferTooSmall
        }
        Err(error) => map_error(error),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_server_connection_encode_response_head(
    connection: *mut VylServerConnection,
    input: *const u8,
    input_length: usize,
    request_headers: *const VylHeader,
    request_header_capacity: usize,
    status: u16,
    reason: VylBytes,
    fields: *const VylField,
    field_count: usize,
    framing: u32,
    content_length: u64,
    request_body_complete: u32,
    response_close: u32,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_body_allowed: *mut u32,
    out_required: *mut usize,
) -> VylResult {
    let arguments = ServerResponseHeadArguments {
        input,
        input_length,
        request_headers,
        request_header_capacity,
        status,
        reason,
        fields,
        field_count,
        framing,
        content_length,
        request_body_complete,
        response_close,
        output,
        output_capacity,
        out_written,
        out_body_allowed,
        out_required,
    };
    catch_ffi(|| unsafe { encode_response_head(connection, arguments) })
}

unsafe fn encode_response_head(
    connection: *mut VylServerConnection,
    arguments: ServerResponseHeadArguments,
) -> VylResult {
    let ServerResponseHeadArguments {
        input,
        input_length,
        request_headers,
        request_header_capacity,
        status,
        reason,
        fields,
        field_count,
        framing,
        content_length,
        request_body_complete,
        response_close,
        output,
        output_capacity,
        out_written,
        out_body_allowed,
        out_required,
    } = arguments;
    if connection.is_null()
        || (input.is_null() && input_length != 0)
        || (request_headers.is_null() && request_header_capacity != 0)
        || (fields.is_null() && field_count != 0)
        || (output.is_null() && output_capacity != 0)
        || out_written.is_null()
        || out_body_allowed.is_null()
        || out_required.is_null()
        || request_body_complete > 1
        || response_close > 1
        || !slice_length_valid::<u8>(input_length)
        || !slice_length_valid::<VylHeader>(request_header_capacity)
        || !slice_length_valid::<VylField>(field_count)
        || !slice_length_valid::<u8>(output_capacity)
    {
        return VylResult::InvalidArgument;
    }

    unsafe {
        out_written.write(0);
        out_body_allowed.write(0);
        out_required.write(0);
    }
    if field_count > Limits::DEFAULT_HEADER_COUNT {
        return VylResult::Capacity;
    }
    let connection = unsafe { &mut *connection };
    if connection.inner.is_cancelled() {
        return VylResult::Cancelled;
    }
    let Some(request) = connection.inner.request_head() else {
        return VylResult::BadState;
    };
    if request_headers != connection.headers_base {
        return VylResult::BadState;
    }
    // SECURITY: the trusted request-header slice is rebuilt from the stored parse-time base
    // (equality checked above) and erased to 'static; it stays valid until reset or
    // destruction under the documented caller contract.
    let request_headers = if request.header_count == 0 {
        &[]
    } else {
        unsafe {
            std::mem::transmute::<&[VylHeader], &'static [VylHeader]>(slice::from_raw_parts(
                connection.headers_base,
                request.header_count,
            ))
        }
    };
    let input = if input_length == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(input, input_length) }
    };
    if let Err(error) = connection.inner.check_encode_head(input, request_headers) {
        if let ServerError::BufferTooSmall { required } = error {
            unsafe {
                out_required.write(required);
            }
            return VylResult::BufferTooSmall;
        }
        return map_error(error);
    }
    let reason = match unsafe { read_bytes(reason) } {
        Ok(reason) => reason,
        Err(error) => return error,
    };
    let fields = if field_count == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(fields, field_count) }
    };
    let empty_field = veylor_http1::Field {
        name: &[],
        value: &[],
    };
    let mut response_fields = [empty_field; Limits::DEFAULT_HEADER_COUNT];
    for (destination, field) in response_fields.iter_mut().zip(fields) {
        let name = match unsafe { read_bytes(field.name) } {
            Ok(name) => name,
            Err(error) => return error,
        };
        let value = match unsafe { read_bytes(field.value) } {
            Ok(value) => value,
            Err(error) => return error,
        };
        *destination = veylor_http1::Field { name, value };
    }
    // NOTE: reason/field/framing conversions report InvalidArgument before the native
    // method-span check (which reports ProtocolError) runs. The method span always resolves
    // when the input base and length guards hold, so only the defensive unreachable arm is
    // affected and every reachable outcome is unchanged.
    let framing = match framing {
        0 => ResponseFraming::FixedLength,
        1 => ResponseFraming::Chunked,
        2 => ResponseFraming::CloseDelimited,
        _ => return VylResult::InvalidArgument,
    };
    let output = if output_capacity == 0 {
        &mut []
    } else {
        unsafe { slice::from_raw_parts_mut(output, output_capacity) }
    };
    let params = ResponseHeadParams {
        status,
        reason,
        fields: &response_fields[..field_count],
        framing,
        content_length,
        request_body_complete: request_body_complete != 0,
        response_close: response_close != 0,
    };
    match connection
        .inner
        .encode_response_head(input, request_headers, params, output)
    {
        Ok(encoded) => {
            unsafe {
                out_written.write(encoded.written);
                out_body_allowed.write(u32::from(encoded.body_allowed));
                out_required.write(encoded.required);
            }
            VylResult::Ok
        }
        Err(ServerError::BufferTooSmall { required }) => {
            unsafe {
                out_required.write(required);
            }
            VylResult::BufferTooSmall
        }
        Err(error) => map_error(error),
    }
}

unsafe fn read_bytes<'a>(bytes: VylBytes) -> Result<&'a [u8], VylResult> {
    if (bytes.data.is_null() && bytes.length != 0) || !slice_length_valid::<u8>(bytes.length) {
        return Err(VylResult::InvalidArgument);
    }
    if bytes.length == 0 {
        return Ok(&[]);
    }
    Ok(unsafe { slice::from_raw_parts(bytes.data, bytes.length) })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_server_connection_reset(
    connection: *mut VylServerConnection,
) -> VylResult {
    catch_ffi(|| unsafe { reset_connection(connection) })
}

unsafe fn reset_connection(connection: *mut VylServerConnection) -> VylResult {
    if connection.is_null() {
        return VylResult::InvalidArgument;
    }

    let connection = unsafe { &mut *connection };
    match connection.inner.reset() {
        Ok(()) => {
            connection.headers_base = ptr::null();
            VylResult::Ok
        }
        Err(error) => map_error(error),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_server_connection_destroy(
    connection: *mut VylServerConnection,
) -> VylResult {
    catch_ffi(|| unsafe { destroy_connection(connection) })
}

unsafe fn destroy_connection(connection: *mut VylServerConnection) -> VylResult {
    if !connection.is_null() {
        unsafe {
            drop(Box::from_raw(connection));
        }
    }
    VylResult::Ok
}

fn to_ffi_head(head: RequestHead) -> VylRequestHead {
    let mut flags = 0;
    if head.content_length.is_some() {
        flags |= VYL_HTTP1_HAS_CONTENT_LENGTH;
    }
    if head.is_chunked {
        flags |= VYL_HTTP1_CHUNKED;
    }
    if head.expect_continue {
        flags |= VYL_HTTP1_EXPECT_CONTINUE;
    }
    if head.connection.close {
        flags |= VYL_HTTP1_CONNECTION_CLOSE;
    }
    if head.connection.keep_alive {
        flags |= VYL_HTTP1_CONNECTION_KEEP_ALIVE;
    }
    if head.connection.upgrade {
        flags |= VYL_HTTP1_CONNECTION_UPGRADE;
    }

    VylRequestHead {
        method: head.method,
        target: head.target,
        authority: head.authority,
        version: head.version,
        target_form: match head.target_form {
            TargetForm::Origin => VylTargetForm::Origin,
            TargetForm::Absolute => VylTargetForm::Absolute,
            TargetForm::Authority => VylTargetForm::Authority,
            TargetForm::Asterisk => VylTargetForm::Asterisk,
        },
        header_count: head.header_count,
        length: head.length,
        content_length: head.content_length.unwrap_or_default(),
        flags,
    }
}

const _: () = assert!(std::mem::size_of::<VylRequestHead>() > 0);
