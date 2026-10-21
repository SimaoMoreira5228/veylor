use std::boxed::Box;
use std::ptr;
use std::slice;

use crate::abi::{
    VylBytes, VylField, VylHeader, VylResult, VylSpan, catch_ffi, slice_length_valid,
};
use veylor_http1::{
    ConnectionHeader, Limits, Parser, RequestHead, ResponseEncodeError, ResponseFraming,
    ResponseToWrite, TargetForm, connection_header_for_response,
    encode_response_head_with_required_length,
};

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
    pub(super) parser: Parser,
    pub(super) request: Option<RequestHead>,
    pub(super) input_base: *const u8,
    pub(super) input_length: usize,
    pub(super) headers_base: *const VylHeader,
    pub(super) response_started: bool,
    pub(super) body: Option<veylor_http1::RequestBodyDecoder<'static>>,
    pub(super) response_writer: super::response_write::ResponseWriter,
    pub(super) cancelled: bool,
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

impl VylServerConnection {
    fn new() -> Self {
        Self {
            parser: Parser::new(),
            request: None,
            input_base: core::ptr::null(),
            input_length: 0,
            headers_base: core::ptr::null(),
            response_started: false,
            body: None,
            response_writer: super::response_write::ResponseWriter::new(),
            cancelled: false,
        }
    }
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

    let connection = Box::new(VylServerConnection::new());
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
    if connection.cancelled {
        return VylResult::Cancelled;
    }
    if connection.response_started || connection.body.is_some() {
        return VylResult::BadState;
    }

    let input = if input_length == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(input, input_length) }
    };
    let headers = if header_capacity == 0 {
        &mut []
    } else {
        unsafe { std::slice::from_raw_parts_mut(headers, header_capacity) }
    };

    let parsed = match connection.parser.parse(input, headers, Limits::default()) {
        Ok(Some(head)) => head,
        Ok(None) => return VylResult::NeedMore,
        Err(veylor_http1::ParseError::HeaderCapacity) => {
            let required = connection.parser.required_header_capacity();
            unsafe {
                out_headers_required.write(required);
            }
            return VylResult::BufferTooSmall;
        }
        Err(_) => return VylResult::ProtocolError,
    };

    unsafe {
        out_headers_required.write(parsed.header_count);
    }
    if header_capacity < parsed.header_count {
        return VylResult::BufferTooSmall;
    }

    connection.request = Some(parsed);
    connection.input_base = input.as_ptr();
    connection.input_length = input.len();
    connection.headers_base = headers.as_ptr();
    connection.response_started = false;

    unsafe {
        out_head.write(to_ffi_head(parsed));
    }
    VylResult::Ok
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
    let connection = unsafe { &mut *connection };
    if connection.cancelled {
        return VylResult::Cancelled;
    }
    if field_count > Limits::DEFAULT_HEADER_COUNT {
        return VylResult::Capacity;
    }
    let Some(request) = connection.request else {
        return VylResult::BadState;
    };
    if connection.response_started
        || !connection.response_writer.can_start_head()
        || input != connection.input_base
        || input_length < request.length
        || request_headers != connection.headers_base
    {
        return VylResult::BadState;
    }
    if request_header_capacity < request.header_count {
        unsafe {
            out_required.write(request.header_count);
        }
        return VylResult::BufferTooSmall;
    }

    let input = if input_length == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(input, input_length) }
    };
    let request_headers = if request.header_count == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(request_headers, request.header_count) }
    };
    let mut requested_upgrade = &[][..];
    let mut upgrade_seen = false;
    for header in request_headers {
        let Some(name) = slice_for_span(input, header.name) else {
            return VylResult::ProtocolError;
        };
        let Some(value) = slice_for_span(input, header.value) else {
            return VylResult::ProtocolError;
        };
        if name.eq_ignore_ascii_case(b"upgrade") {
            if upgrade_seen {
                return VylResult::InvalidArgument;
            }
            requested_upgrade = value;
            upgrade_seen = true;
        }
    }
    if status == 101 && (!request.connection.upgrade || !upgrade_seen) {
        return VylResult::InvalidArgument;
    }

    let method = request.method(input).ok_or(VylResult::ProtocolError);
    let method = match method {
        Ok(method) => method,
        Err(error) => return error,
    };
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

    let framing = match framing {
        0 => ResponseFraming::FixedLength,
        1 => ResponseFraming::Chunked,
        2 => ResponseFraming::CloseDelimited,
        _ => return VylResult::InvalidArgument,
    };
    let connection_header = if status == 101 {
        ConnectionHeader::Upgrade
    } else {
        connection_header_for_response(
            request,
            input,
            status,
            framing,
            request_body_complete != 0,
            response_close != 0,
        )
    };
    let response = ResponseToWrite {
        request_method: method,
        requested_upgrade,
        status,
        reason,
        fields: &response_fields[..field_count],
        framing,
        content_length,
        connection: connection_header,
    };
    let output = if output_capacity == 0 {
        &mut []
    } else {
        unsafe { slice::from_raw_parts_mut(output, output_capacity) }
    };
    let mut required = 0;
    match encode_response_head_with_required_length(&response, output, &mut required) {
        Ok(encoded) => {
            connection.response_writer.start_head(
                status,
                framing,
                content_length,
                encoded.body_allowed,
                encoded.length,
            );
            connection.response_started = status >= 200 || status == 101;
            unsafe {
                out_written.write(encoded.length);
                out_body_allowed.write(u32::from(encoded.body_allowed));
                out_required.write(required);
            }
            VylResult::Ok
        }
        Err(ResponseEncodeError::BufferTooSmall) => {
            unsafe {
                out_required.write(required);
            }
            VylResult::BufferTooSmall
        }
        Err(_) => VylResult::InvalidArgument,
    }
}

fn slice_for_span(input: &[u8], span: veylor_http1::Span) -> Option<&[u8]> {
    input.get(span.offset..span.offset.checked_add(span.length)?)
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
    if connection.cancelled {
        return VylResult::Cancelled;
    }
    connection.parser.reset();
    connection.request = None;
    connection.input_base = core::ptr::null();
    connection.input_length = 0;
    connection.headers_base = core::ptr::null();
    connection.response_started = false;
    connection.body = None;
    connection.response_writer = super::response_write::ResponseWriter::new();
    VylResult::Ok
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
