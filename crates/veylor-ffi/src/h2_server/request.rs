use std::slice;

use super::{VylH2Server, split_error};
use crate::abi::{
    VYL_H2_MAX_FIELDS, VYL_H2_NO_FIELD, VylBytes, VylH2FieldToEncode, VylH2ValidatedRequest,
    VylResult, catch_ffi, slice_length_valid,
};

unsafe fn read_bytes(bytes: VylBytes) -> Result<&'static [u8], VylResult> {
    if (bytes.data.is_null() && bytes.length != 0) || !slice_length_valid::<u8>(bytes.length) {
        return Err(VylResult::InvalidArgument);
    }
    if bytes.length == 0 {
        return Ok(&[]);
    }
    Ok(unsafe { slice::from_raw_parts(bytes.data, bytes.length) })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h2_server_accept_request(
    handle: *mut VylH2Server,
    stream_id: u32,
    header_start: usize,
    header_count: usize,
    out_request: *mut VylH2ValidatedRequest,
) -> VylResult {
    catch_ffi(|| unsafe {
        accept_request(handle, stream_id, header_start, header_count, out_request)
    })
}

unsafe fn accept_request(
    handle: *mut VylH2Server,
    stream_id: u32,
    header_start: usize,
    header_count: usize,
    out_request: *mut VylH2ValidatedRequest,
) -> VylResult {
    if handle.is_null() || out_request.is_null() {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_request.write(VylH2ValidatedRequest::default());
    }
    let handle = unsafe { &mut *handle };
    let Some(connection) = handle.connection.as_mut() else {
        return VylResult::InternalError;
    };
    if connection.is_cancelled() {
        return VylResult::Cancelled;
    }
    let Some(end) = header_start.checked_add(header_count) else {
        return VylResult::InvalidArgument;
    };
    if end > connection.feed_header_count() {
        return VylResult::InvalidArgument;
    }
    // SECURITY: the header slice aliases the handle's feed scratch and stays valid until the
    // next poll under the documented caller contract; the validated indexes below are relative
    // to this same slice.
    let headers = &handle.feed_scratch.headers()[header_start..end];
    let validated = match connection.accept_request(stream_id, headers) {
        Ok(validated) => validated,
        Err(error) => return split_error(error).0,
    };
    let (content_length, has_content_length) = match validated.content_length {
        Some(length) => (length, 1),
        None => (0, 0),
    };
    unsafe {
        out_request.write(VylH2ValidatedRequest {
            method_field: validated
                .method
                .map_or(VYL_H2_NO_FIELD, |index| header_start.saturating_add(index)),
            scheme_field: validated
                .scheme
                .map_or(VYL_H2_NO_FIELD, |index| header_start.saturating_add(index)),
            authority_field: validated
                .authority
                .map_or(VYL_H2_NO_FIELD, |index| header_start.saturating_add(index)),
            path_field: validated
                .path
                .map_or(VYL_H2_NO_FIELD, |index| header_start.saturating_add(index)),
            content_length,
            has_content_length,
        });
    }
    VylResult::Ok
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h2_server_send_response(
    handle: *mut VylH2Server,
    stream_id: u32,
    fields: *const VylH2FieldToEncode,
    field_count: usize,
    end_stream: u32,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_required: *mut usize,
) -> VylResult {
    catch_ffi(|| unsafe {
        send_response(
            handle,
            stream_id,
            ResponseArguments {
                fields,
                field_count,
                end_stream,
                output,
                output_capacity,
                out_written,
                out_required,
            },
        )
    })
}

struct ResponseArguments {
    fields: *const VylH2FieldToEncode,
    field_count: usize,
    end_stream: u32,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_required: *mut usize,
}

unsafe fn send_response(
    handle: *mut VylH2Server,
    stream_id: u32,
    arguments: ResponseArguments,
) -> VylResult {
    let ResponseArguments {
        fields,
        field_count,
        end_stream,
        output,
        output_capacity,
        out_written,
        out_required,
    } = arguments;
    if handle.is_null()
        || (fields.is_null() && field_count != 0)
        || end_stream > 1
        || (output.is_null() && output_capacity != 0)
        || out_written.is_null()
        || out_required.is_null()
        || !slice_length_valid::<VylH2FieldToEncode>(field_count)
        || !slice_length_valid::<u8>(output_capacity)
    {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_written.write(0);
        out_required.write(0);
    }
    if field_count > VYL_H2_MAX_FIELDS {
        return VylResult::Capacity;
    }
    let fields = if field_count == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(fields, field_count) }
    };
    // NOTE: content errors are recorded, not returned, so state errors reported by
    // `check_writable` keep their historical precedence over field content errors.
    let mut encode_fields = [veylor_http2::HpackFieldToEncode {
        name: &[][..],
        value: &[][..],
        representation: veylor_http2::HpackRepresentation::WithoutIndexing,
        huffman_name: false,
        huffman_value: false,
    }; VYL_H2_MAX_FIELDS];
    let mut content_error: Option<VylResult> = None;
    for (destination, field) in encode_fields.iter_mut().zip(fields.iter()) {
        let name = match unsafe { read_bytes(field.name) } {
            Ok(name) => name,
            Err(error) => {
                content_error.get_or_insert(error);
                &[][..]
            }
        };
        let value = match unsafe { read_bytes(field.value) } {
            Ok(value) => value,
            Err(error) => {
                content_error.get_or_insert(error);
                &[][..]
            }
        };
        let representation = match field.representation {
            0 => veylor_http2::HpackRepresentation::Indexed,
            1 => veylor_http2::HpackRepresentation::Incremental,
            2 => veylor_http2::HpackRepresentation::WithoutIndexing,
            3 => veylor_http2::HpackRepresentation::NeverIndexed,
            _ => {
                content_error.get_or_insert(VylResult::InvalidArgument);
                veylor_http2::HpackRepresentation::WithoutIndexing
            }
        };
        if field.huffman_name > 1 || field.huffman_value > 1 {
            content_error.get_or_insert(VylResult::InvalidArgument);
        }
        *destination = veylor_http2::HpackFieldToEncode {
            name,
            value,
            representation,
            huffman_name: field.huffman_name != 0,
            huffman_value: field.huffman_value != 0,
        };
    }
    let handle = unsafe { &mut *handle };
    let Some(connection) = handle.connection.as_mut() else {
        return VylResult::InternalError;
    };
    if let Err(error) = connection.check_writable() {
        return split_error(error).0;
    }
    if let Some(error) = content_error {
        return error;
    }
    let output = if output_capacity == 0 {
        &mut []
    } else {
        unsafe { slice::from_raw_parts_mut(output, output_capacity) }
    };
    match connection.send_response(
        stream_id,
        &encode_fields[..field_count],
        end_stream != 0,
        output,
    ) {
        Ok(sent) => {
            unsafe {
                out_written.write(sent.written);
                out_required.write(sent.required);
            }
            VylResult::Ok
        }
        Err(error) => {
            let (result, required) = split_error(error);
            if result == VylResult::BufferTooSmall {
                unsafe {
                    out_required.write(required);
                }
            }
            result
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h2_server_send_data(
    handle: *mut VylH2Server,
    stream_id: u32,
    input: VylBytes,
    end_stream: u32,
    output: *mut u8,
    output_capacity: usize,
    out_consumed: *mut usize,
    out_written: *mut usize,
    out_end_stream: *mut u32,
    out_required: *mut usize,
) -> VylResult {
    catch_ffi(|| unsafe {
        send_data(
            handle,
            stream_id,
            DataArguments {
                input,
                end_stream,
                output,
                output_capacity,
                out_consumed,
                out_written,
                out_end_stream,
                out_required,
            },
        )
    })
}

struct DataArguments {
    input: VylBytes,
    end_stream: u32,
    output: *mut u8,
    output_capacity: usize,
    out_consumed: *mut usize,
    out_written: *mut usize,
    out_end_stream: *mut u32,
    out_required: *mut usize,
}

unsafe fn send_data(
    handle: *mut VylH2Server,
    stream_id: u32,
    arguments: DataArguments,
) -> VylResult {
    let DataArguments {
        input,
        end_stream,
        output,
        output_capacity,
        out_consumed,
        out_written,
        out_end_stream,
        out_required,
    } = arguments;
    if handle.is_null()
        || end_stream > 1
        || (output.is_null() && output_capacity != 0)
        || out_consumed.is_null()
        || out_written.is_null()
        || out_end_stream.is_null()
        || out_required.is_null()
        || !slice_length_valid::<u8>(output_capacity)
    {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_consumed.write(0);
        out_written.write(0);
        out_end_stream.write(0);
        out_required.write(0);
    }
    let input = match unsafe { read_bytes(input) } {
        Ok(input) => input,
        Err(error) => return error,
    };
    let handle = unsafe { &mut *handle };
    let Some(connection) = handle.connection.as_mut() else {
        return VylResult::InternalError;
    };
    let output = if output_capacity == 0 {
        &mut []
    } else {
        unsafe { slice::from_raw_parts_mut(output, output_capacity) }
    };
    match connection.send_data(stream_id, input, end_stream != 0, output) {
        Ok(sent) => {
            unsafe {
                out_consumed.write(sent.consumed);
                out_written.write(sent.written);
                out_end_stream.write(u32::from(sent.end_stream));
                out_required.write(sent.required);
            }
            VylResult::Ok
        }
        Err(error) => {
            let (result, required) = split_error(error);
            if result == VylResult::BufferTooSmall {
                unsafe {
                    out_required.write(required);
                }
            }
            result
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h2_server_refuse(
    handle: *mut VylH2Server,
    stream_id: u32,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_required: *mut usize,
) -> VylResult {
    catch_ffi(|| unsafe {
        reset_stream(
            handle,
            stream_id,
            veylor_http2::ErrorCode::RefusedStream,
            output,
            output_capacity,
            out_written,
            out_required,
        )
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h2_server_send_reset(
    handle: *mut VylH2Server,
    stream_id: u32,
    error_code: u32,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_required: *mut usize,
) -> VylResult {
    catch_ffi(|| unsafe {
        let code = veylor_http2::ErrorCode::from(error_code);
        if let veylor_http2::ErrorCode::Unknown(value) = code
            && value <= 13
        {
            return VylResult::InvalidArgument;
        }
        reset_stream(
            handle,
            stream_id,
            code,
            output,
            output_capacity,
            out_written,
            out_required,
        )
    })
}

unsafe fn reset_stream(
    handle: *mut VylH2Server,
    stream_id: u32,
    error_code: veylor_http2::ErrorCode,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_required: *mut usize,
) -> VylResult {
    if handle.is_null()
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
    let handle = unsafe { &mut *handle };
    let Some(connection) = handle.connection.as_mut() else {
        return VylResult::InternalError;
    };
    let output = if output_capacity == 0 {
        &mut []
    } else {
        unsafe { slice::from_raw_parts_mut(output, output_capacity) }
    };
    let result = if error_code == veylor_http2::ErrorCode::RefusedStream {
        connection.refuse(stream_id, output)
    } else {
        connection.send_reset(stream_id, error_code, output)
    };
    match result {
        Ok(sent) => {
            unsafe {
                out_written.write(sent.written);
                out_required.write(sent.required);
            }
            VylResult::Ok
        }
        Err(error) => {
            let (result, required) = split_error(error);
            if result == VylResult::BufferTooSmall {
                unsafe {
                    out_required.write(required);
                }
            }
            result
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h2_server_goaway_drain(
    handle: *mut VylH2Server,
    last_stream_id: u32,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_required: *mut usize,
) -> VylResult {
    catch_ffi(|| unsafe {
        goaway_drain(
            handle,
            last_stream_id,
            output,
            output_capacity,
            out_written,
            out_required,
        )
    })
}

unsafe fn goaway_drain(
    handle: *mut VylH2Server,
    last_stream_id: u32,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_required: *mut usize,
) -> VylResult {
    if handle.is_null()
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
    let handle = unsafe { &mut *handle };
    let Some(connection) = handle.connection.as_mut() else {
        return VylResult::InternalError;
    };
    let output = if output_capacity == 0 {
        &mut []
    } else {
        unsafe { slice::from_raw_parts_mut(output, output_capacity) }
    };
    match connection.goaway_drain(last_stream_id, output) {
        Ok(sent) => {
            unsafe {
                out_written.write(sent.written);
                out_required.write(sent.required);
            }
            VylResult::Ok
        }
        Err(error) => {
            let (result, required) = split_error(error);
            if result == VylResult::BufferTooSmall {
                unsafe {
                    out_required.write(required);
                }
            }
            result
        }
    }
}
