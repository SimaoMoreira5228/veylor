use std::slice;

use veylor_client::{BodyFraming, ClientError, Field};

use crate::abi::{VylBytes, VylField, VylResult, catch_ffi, slice_length_valid};

use super::{VYL_HTTP1_MAX_REQUEST_FIELDS, VylClientConnection, VylRequestBodyKind};

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

    let output = if output_capacity == 0 {
        &mut []
    } else {
        unsafe { slice::from_raw_parts_mut(output, output_capacity) }
    };
    match connection.inner.encode_request_head(
        method,
        target,
        &request_fields[..field_count],
        body,
        output,
    ) {
        Ok(encoded) => {
            unsafe {
                out_written.write(encoded.written);
                out_required.write(encoded.required);
            }
            VylResult::Ok
        }
        Err(ClientError::BufferTooSmall { required }) => {
            unsafe {
                out_required.write(required);
            }
            VylResult::BufferTooSmall
        }
        Err(ClientError::Capacity) => VylResult::Capacity,
        Err(ClientError::Cancelled) => VylResult::Cancelled,
        Err(ClientError::BadState) => VylResult::BadState,
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
    match connection.inner.advance_request_write(transferred_bytes) {
        Ok(pending) => {
            unsafe {
                out_pending.write(pending);
            }
            VylResult::Ok
        }
        Err(ClientError::WriteOverrun) => VylResult::WriteOverrun,
        Err(ClientError::Cancelled) => VylResult::Cancelled,
        Err(_) => VylResult::BadState,
    }
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
    match connection.inner.encode_request_body(input, output) {
        Ok(encoded) => {
            unsafe {
                out_written.write(encoded.written);
                out_required.write(encoded.required);
            }
            VylResult::Ok
        }
        Err(ClientError::BufferTooSmall { required }) => {
            unsafe {
                out_required.write(required);
            }
            VylResult::BufferTooSmall
        }
        Err(ClientError::BodyTooLong) => VylResult::BodyTooLong,
        Err(ClientError::WouldBlock) => VylResult::WouldBlock,
        Err(ClientError::Cancelled) => VylResult::Cancelled,
        Err(ClientError::BadState) => VylResult::BadState,
        Err(_) => VylResult::InvalidArgument,
    }
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
    let output = if output_capacity == 0 {
        &mut []
    } else {
        unsafe { slice::from_raw_parts_mut(output, output_capacity) }
    };
    match connection
        .inner
        .finish_request_body(&trailer_fields[..trailer_count], output)
    {
        Ok(finished) => {
            unsafe {
                out_written.write(finished.written);
                out_required.write(finished.required);
            }
            VylResult::Ok
        }
        Err(ClientError::BufferTooSmall { required }) => {
            unsafe {
                out_required.write(required);
            }
            VylResult::BufferTooSmall
        }
        Err(ClientError::Capacity) => VylResult::Capacity,
        Err(ClientError::WouldBlock) => VylResult::WouldBlock,
        Err(ClientError::Cancelled) => VylResult::Cancelled,
        Err(ClientError::BadState) => VylResult::BadState,
        Err(_) => VylResult::InvalidArgument,
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
    connection.inner.cancel();
    VylResult::Ok
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
