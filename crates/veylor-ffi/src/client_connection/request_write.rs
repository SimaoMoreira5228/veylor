use std::slice;

use veylor_http1::{
    BodyFraming, Field, Request, RequestEncodeError, encode_chunk,
    encode_head_with_required_length, finish_chunks_with_required_length,
};

use crate::abi::{VylBytes, VylField, VylResult, catch_ffi, slice_length_valid};

use super::{
    VYL_HTTP1_MAX_REQUEST_FIELDS, VylClientConnection, VylRequestBodyKind, VylRequestMethod,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RequestBodyState {
    Finished,
    Fixed(u64),
    Chunked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RequestWriteState {
    Idle,
    HeadPending {
        pending: usize,
        body: RequestBodyState,
    },
    Ready(RequestBodyState),
    Pending {
        pending: usize,
        body: RequestBodyState,
    },
    Complete,
    Cancelled,
}

impl RequestWriteState {
    pub(super) const fn new() -> Self {
        Self::Idle
    }
}

struct ClientRequestHeadArguments {
    method: VylBytes,
    target: VylBytes,
    fields: *const VylField,
    field_count: usize,
    body_kind: u32,
    body_length: u64,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_required: *mut usize,
}









#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_client_connection_encode_request_head(
    connection: *mut VylClientConnection,
    method: VylBytes,
    target: VylBytes,
    fields: *const VylField,
    field_count: usize,
    body_kind: u32,
    body_length: u64,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_required: *mut usize,
) -> VylResult {
    let arguments = ClientRequestHeadArguments {
        method,
        target,
        fields,
        field_count,
        body_kind,
        body_length,
        output,
        output_capacity,
        out_written,
        out_required,
    };
    catch_ffi(|| unsafe { encode_request_head(connection, arguments) })
}

unsafe fn encode_request_head(
    connection: *mut VylClientConnection,
    arguments: ClientRequestHeadArguments,
) -> VylResult {
    let ClientRequestHeadArguments {
        method,
        target,
        fields,
        field_count,
        body_kind,
        body_length,
        output,
        output_capacity,
        out_written,
        out_required,
    } = arguments;
    if connection.is_null()
        || (fields.is_null() && field_count != 0)
        || (output.is_null() && output_capacity != 0)
        || out_written.is_null()
        || out_required.is_null()
        || !slice_length_valid::<VylField>(field_count)
        || !slice_length_valid::<u8>(output_capacity)
    {
        return VylResult::InvalidArgument;
    }

    unsafe {
        out_written.write(0);
        out_required.write(0);
    }
    if field_count > VYL_HTTP1_MAX_REQUEST_FIELDS {
        return VylResult::Capacity;
    }

    let method = match unsafe { read_bytes(method) } {
        Ok(method) => method,
        Err(error) => return error,
    };
    let target = match unsafe { read_bytes(target) } {
        Ok(target) => target,
        Err(error) => return error,
    };
    let body = match body_kind {
        value if value == VylRequestBodyKind::None as u32 && body_length == 0 => BodyFraming::None,
        value if value == VylRequestBodyKind::Fixed as u32 => BodyFraming::Fixed(body_length),
        value if value == VylRequestBodyKind::Chunked as u32 && body_length == 0 => {
            BodyFraming::Chunked
        }
        _ => return VylResult::InvalidArgument,
    };

    let connection = unsafe { &mut *connection };
    if connection.request_writer == RequestWriteState::Cancelled {
        return VylResult::Cancelled;
    }
    if connection.request_writer != RequestWriteState::Idle {
        return VylResult::BadState;
    }
    if !matches_request_method(connection.request_method, method) {
        return VylResult::InvalidArgument;
    }

    let fields = if field_count == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(fields, field_count) }
    };
    let empty_field = Field {
        name: &[],
        value: &[],
    };
    let mut request_fields = [empty_field; VYL_HTTP1_MAX_REQUEST_FIELDS];
    for (destination, field) in request_fields.iter_mut().zip(fields) {
        let name = match unsafe { read_bytes(field.name) } {
            Ok(name) => name,
            Err(error) => return error,
        };
        let value = match unsafe { read_bytes(field.value) } {
            Ok(value) => value,
            Err(error) => return error,
        };
        *destination = Field { name, value };
    }

    let request = Request {
        method,
        target,
        fields: &request_fields[..field_count],
        body,
    };
    let output = if output_capacity == 0 {
        &mut []
    } else {
        unsafe { slice::from_raw_parts_mut(output, output_capacity) }
    };
    let mut required = 0;
    match encode_head_with_required_length(&request, output, &mut required) {
        Ok(written) => {
            connection.request_writer = RequestWriteState::HeadPending {
                pending: written,
                body: body_state(body),
            };
            unsafe {
                out_written.write(written);
                out_required.write(required);
            }
            VylResult::Ok
        }
        Err(RequestEncodeError::BufferTooSmall) => {
            unsafe {
                out_required.write(required);
            }
            VylResult::BufferTooSmall
        }
        Err(_) => VylResult::InvalidArgument,
    }
}






#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_client_connection_advance_request_write(
    connection: *mut VylClientConnection,
    transferred_bytes: usize,
    out_pending: *mut usize,
) -> VylResult {
    catch_ffi(|| unsafe { advance_request_write(connection, transferred_bytes, out_pending) })
}

unsafe fn advance_request_write(
    connection: *mut VylClientConnection,
    transferred_bytes: usize,
    out_pending: *mut usize,
) -> VylResult {
    if connection.is_null() || out_pending.is_null() {
        return VylResult::InvalidArgument;
    }

    let connection = unsafe { &mut *connection };
    let (pending, body, is_head) = match connection.request_writer {
        RequestWriteState::HeadPending { pending, body } => (pending, body, true),
        RequestWriteState::Pending { pending, body } => (pending, body, false),
        RequestWriteState::Idle | RequestWriteState::Ready(_) | RequestWriteState::Complete => {
            return VylResult::BadState;
        }
        RequestWriteState::Cancelled => return VylResult::Cancelled,
    };
    if transferred_bytes > pending {
        return VylResult::WriteOverrun;
    }

    let pending = pending - transferred_bytes;
    connection.request_writer = if pending != 0 {
        if is_head {
            RequestWriteState::HeadPending { pending, body }
        } else {
            RequestWriteState::Pending { pending, body }
        }
    } else if body == RequestBodyState::Finished {
        RequestWriteState::Complete
    } else {
        RequestWriteState::Ready(body)
    };
    unsafe {
        out_pending.write(pending);
    }
    VylResult::Ok
}








#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_client_connection_encode_request_body(
    connection: *mut VylClientConnection,
    input: VylBytes,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_required: *mut usize,
) -> VylResult {
    catch_ffi(|| unsafe {
        encode_request_body(
            connection,
            input,
            output,
            output_capacity,
            out_written,
            out_required,
        )
    })
}

unsafe fn encode_request_body(
    connection: *mut VylClientConnection,
    input: VylBytes,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_required: *mut usize,
) -> VylResult {
    if connection.is_null()
        || (output.is_null() && output_capacity != 0)
        || out_written.is_null()
        || out_required.is_null()
        || !slice_length_valid::<u8>(output_capacity)
    {
        return VylResult::InvalidArgument;
    }

    unsafe {
        out_written.write(0);
        out_required.write(0);
    }
    let input = match unsafe { read_bytes(input) } {
        Ok(input) => input,
        Err(error) => return error,
    };
    let output = if output_capacity == 0 {
        &mut []
    } else {
        unsafe { slice::from_raw_parts_mut(output, output_capacity) }
    };
    let connection = unsafe { &mut *connection };
    let body = match connection.request_writer {
        RequestWriteState::Ready(body) => body,
        RequestWriteState::HeadPending { .. } | RequestWriteState::Pending { .. } => {
            return VylResult::WouldBlock;
        }
        RequestWriteState::Cancelled => return VylResult::Cancelled,
        RequestWriteState::Idle | RequestWriteState::Complete => return VylResult::BadState,
    };

    let (written, next_body) = match body {
        RequestBodyState::Fixed(remaining) => {
            let input_length = match u64::try_from(input.len()) {
                Ok(length) => length,
                Err(_) => return VylResult::BodyTooLong,
            };
            if input_length > remaining {
                return VylResult::BodyTooLong;
            }
            if input.is_empty() {
                return VylResult::Ok;
            }
            if output.is_empty() {
                unsafe {
                    out_required.write(input.len());
                }
                return VylResult::BufferTooSmall;
            }

            let written = input.len().min(output.len());
            output[..written].copy_from_slice(&input[..written]);
            let written_u64 = match u64::try_from(written) {
                Ok(length) => length,
                Err(_) => return VylResult::BodyTooLong,
            };
            (written, RequestBodyState::Fixed(remaining - written_u64))
        }
        RequestBodyState::Chunked => {
            if input.is_empty() {
                return VylResult::InvalidArgument;
            }
            let Some(required) = chunk_length(input.len()) else {
                return VylResult::BodyTooLong;
            };
            if output.len() < required {
                unsafe {
                    out_required.write(required);
                }
                return VylResult::BufferTooSmall;
            }
            if encode_chunk(input, output).is_err() {
                return VylResult::InvalidArgument;
            }
            (required, RequestBodyState::Chunked)
        }
        RequestBodyState::Finished => return VylResult::BadState,
    };

    connection.request_writer = RequestWriteState::Pending {
        pending: written,
        body: next_body,
    };
    unsafe {
        out_written.write(written);
        out_required.write(written);
    }
    VylResult::Ok
}








#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_client_connection_finish_request_body(
    connection: *mut VylClientConnection,
    trailers: *const VylField,
    trailer_count: usize,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_required: *mut usize,
) -> VylResult {
    catch_ffi(|| unsafe {
        finish_request_body(
            connection,
            trailers,
            trailer_count,
            output,
            output_capacity,
            out_written,
            out_required,
        )
    })
}

unsafe fn finish_request_body(
    connection: *mut VylClientConnection,
    trailers: *const VylField,
    trailer_count: usize,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_required: *mut usize,
) -> VylResult {
    if connection.is_null()
        || (trailers.is_null() && trailer_count != 0)
        || (output.is_null() && output_capacity != 0)
        || out_written.is_null()
        || out_required.is_null()
        || !slice_length_valid::<VylField>(trailer_count)
        || !slice_length_valid::<u8>(output_capacity)
    {
        return VylResult::InvalidArgument;
    }

    unsafe {
        out_written.write(0);
        out_required.write(0);
    }
    if trailer_count > VYL_HTTP1_MAX_REQUEST_FIELDS {
        return VylResult::Capacity;
    }
    let connection = unsafe { &mut *connection };
    let body = match connection.request_writer {
        RequestWriteState::Ready(body) => body,
        RequestWriteState::HeadPending { .. } | RequestWriteState::Pending { .. } => {
            return VylResult::WouldBlock;
        }
        RequestWriteState::Cancelled => return VylResult::Cancelled,
        RequestWriteState::Idle | RequestWriteState::Complete => return VylResult::BadState,
    };

    match body {
        RequestBodyState::Fixed(_) => VylResult::BadState,
        RequestBodyState::Finished => VylResult::BadState,
        RequestBodyState::Chunked => {
            let trailers = if trailer_count == 0 {
                &[]
            } else {
                unsafe { slice::from_raw_parts(trailers, trailer_count) }
            };
            let empty_field = Field {
                name: &[],
                value: &[],
            };
            let mut trailer_fields = [empty_field; VYL_HTTP1_MAX_REQUEST_FIELDS];
            for (destination, field) in trailer_fields.iter_mut().zip(trailers) {
                let name = match unsafe { read_bytes(field.name) } {
                    Ok(name) => name,
                    Err(error) => return error,
                };
                let value = match unsafe { read_bytes(field.value) } {
                    Ok(value) => value,
                    Err(error) => return error,
                };
                *destination = Field { name, value };
            }

            let output = unsafe { slice::from_raw_parts_mut(output, output_capacity) };
            let mut required = 0;
            let written = match finish_chunks_with_required_length(
                &trailer_fields[..trailer_count],
                output,
                &mut required,
            ) {
                Ok(written) => written,
                Err(RequestEncodeError::BufferTooSmall) => {
                    unsafe {
                        out_required.write(required);
                    }
                    return VylResult::BufferTooSmall;
                }
                Err(_) => return VylResult::InvalidArgument,
            };
            connection.request_writer = RequestWriteState::Pending {
                pending: written,
                body: RequestBodyState::Finished,
            };
            unsafe {
                out_written.write(written);
                out_required.write(required);
            }
            VylResult::Ok
        }
    }
}






#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_client_connection_cancel(
    connection: *mut VylClientConnection,
) -> VylResult {
    catch_ffi(|| unsafe { cancel_connection(connection) })
}

unsafe fn cancel_connection(connection: *mut VylClientConnection) -> VylResult {
    if connection.is_null() {
        return VylResult::InvalidArgument;
    }

    let connection = unsafe { &mut *connection };
    if connection.request_writer == RequestWriteState::Cancelled {
        return VylResult::Ok;
    }
    connection.response.cancel();
    connection.request_writer = RequestWriteState::Cancelled;
    VylResult::Ok
}

fn body_state(framing: BodyFraming) -> RequestBodyState {
    match framing {
        BodyFraming::None | BodyFraming::Fixed(0) => RequestBodyState::Finished,
        BodyFraming::Fixed(length) => RequestBodyState::Fixed(length),
        BodyFraming::Chunked => RequestBodyState::Chunked,
    }
}

fn chunk_length(length: usize) -> Option<usize> {
    let mut digits = 1;
    let mut remaining = length;
    while remaining >= 16 {
        digits += 1;
        remaining >>= 4;
    }
    length.checked_add(digits)?.checked_add(4)
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

fn matches_request_method(request_method: VylRequestMethod, method: &[u8]) -> bool {
    match request_method {
        VylRequestMethod::Head => method == b"HEAD",
        VylRequestMethod::Connect => method == b"CONNECT",
        VylRequestMethod::Other => method != b"HEAD" && method != b"CONNECT",
    }
}
