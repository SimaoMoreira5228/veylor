use std::slice;

use veylor_server_conn::ServerError;

use crate::abi::{VylBytes, VylField, VylResult, catch_ffi, slice_length_valid};

use super::api::VylServerConnection;
use super::map_error;

struct EncodeBodyArguments {
    input: VylBytes,
    output: *mut u8,
    output_capacity: usize,
    out_consumed: *mut usize,
    out_written: *mut usize,
    out_required: *mut usize,
}

struct FinishBodyArguments {
    trailers: *const VylField,
    trailer_count: usize,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_required: *mut usize,
}






#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_server_connection_advance_response_write(
    connection: *mut VylServerConnection,
    transferred: usize,
    out_pending: *mut usize,
) -> VylResult {
    catch_ffi(|| unsafe { advance_response_write(connection, transferred, out_pending) })
}

unsafe fn advance_response_write(
    connection: *mut VylServerConnection,
    transferred: usize,
    out_pending: *mut usize,
) -> VylResult {
    if out_pending.is_null() {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_pending.write(0);
    }
    if connection.is_null() {
        return VylResult::InvalidArgument;
    }
    let connection = unsafe { &mut *connection };
    match connection.inner.advance_response_write(transferred) {
        Ok(pending) => {
            unsafe {
                out_pending.write(pending);
            }
            VylResult::Ok
        }
        Err(error) => map_error(error),
    }
}







#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_server_connection_encode_response_body(
    connection: *mut VylServerConnection,
    input: VylBytes,
    output: *mut u8,
    output_capacity: usize,
    out_consumed: *mut usize,
    out_written: *mut usize,
    out_required: *mut usize,
) -> VylResult {
    let arguments = EncodeBodyArguments {
        input,
        output,
        output_capacity,
        out_consumed,
        out_written,
        out_required,
    };
    catch_ffi(|| unsafe { encode_response_body(connection, arguments) })
}

unsafe fn encode_response_body(
    connection: *mut VylServerConnection,
    arguments: EncodeBodyArguments,
) -> VylResult {
    let EncodeBodyArguments {
        input,
        output,
        output_capacity,
        out_consumed,
        out_written,
        out_required,
    } = arguments;
    if connection.is_null()
        || (output.is_null() && output_capacity != 0)
        || out_consumed.is_null()
        || out_written.is_null()
        || out_required.is_null()
        || !slice_length_valid::<u8>(output_capacity)
    {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_consumed.write(0);
        out_written.write(0);
        out_required.write(0);
    }
    let connection = unsafe { &mut *connection };
    if let Err(error) = connection.inner.check_write_body() {
        return map_error(error);
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
    match connection.inner.encode_response_body(input, output) {
        Ok(encoded) => {
            unsafe {
                out_consumed.write(encoded.consumed);
                out_written.write(encoded.written);
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







#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_server_connection_finish_response_body(
    connection: *mut VylServerConnection,
    trailers: *const VylField,
    trailer_count: usize,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_required: *mut usize,
) -> VylResult {
    let arguments = FinishBodyArguments {
        trailers,
        trailer_count,
        output,
        output_capacity,
        out_written,
        out_required,
    };
    catch_ffi(|| unsafe { finish_response_body(connection, arguments) })
}

unsafe fn finish_response_body(
    connection: *mut VylServerConnection,
    arguments: FinishBodyArguments,
) -> VylResult {
    let FinishBodyArguments {
        trailers,
        trailer_count,
        output,
        output_capacity,
        out_written,
        out_required,
    } = arguments;
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
    let connection = unsafe { &mut *connection };
    if let Err(error) = connection.inner.check_write_body() {
        return map_error(error);
    }
    if trailer_count > veylor_http1::Limits::DEFAULT_HEADER_COUNT {
        return VylResult::Capacity;
    }
    let trailers = if trailer_count == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(trailers, trailer_count) }
    };
    let empty_field = veylor_http1::Field {
        name: &[],
        value: &[],
    };
    let mut fields = [empty_field; veylor_http1::Limits::DEFAULT_HEADER_COUNT];
    for (destination, field) in fields.iter_mut().zip(trailers) {
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
    let output = if output_capacity == 0 {
        &mut []
    } else {
        unsafe { slice::from_raw_parts_mut(output, output_capacity) }
    };
    match connection
        .inner
        .finish_response_body(&fields[..trailer_count], output)
    {
        Ok(encoded) => {
            unsafe {
                out_written.write(encoded.written);
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
