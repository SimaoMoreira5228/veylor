use std::slice;

use super::{VylH2Client, split_error};
use crate::abi::{
    VYL_H2_MAX_FIELDS, VylBytes, VylH2FieldToEncode, VylResult, catch_ffi, slice_length_valid,
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
pub unsafe extern "C" fn vyl_h2_client_start_request(
    handle: *mut VylH2Client,
    fields: *const VylH2FieldToEncode,
    field_count: usize,
    end_stream: u32,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_stream_id: *mut u32,
    out_required: *mut usize,
) -> VylResult {
    catch_ffi(|| unsafe {
        start_request(
            handle,
            StartArguments {
                fields,
                field_count,
                end_stream,
                output,
                output_capacity,
                out_written,
                out_stream_id,
                out_required,
            },
        )
    })
}

struct StartArguments {
    fields: *const VylH2FieldToEncode,
    field_count: usize,
    end_stream: u32,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_stream_id: *mut u32,
    out_required: *mut usize,
}

unsafe fn start_request(handle: *mut VylH2Client, arguments: StartArguments) -> VylResult {
    let StartArguments {
        fields,
        field_count,
        end_stream,
        output,
        output_capacity,
        out_written,
        out_stream_id,
        out_required,
    } = arguments;
    if handle.is_null()
        || (fields.is_null() && field_count != 0)
        || end_stream > 1
        || (output.is_null() && output_capacity != 0)
        || out_written.is_null()
        || out_stream_id.is_null()
        || out_required.is_null()
        || !slice_length_valid::<VylH2FieldToEncode>(field_count)
        || !slice_length_valid::<u8>(output_capacity)
    {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_written.write(0);
        out_stream_id.write(0);
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
    // `check_start_request` keep their historical precedence over field content errors.
    let mut encode_fields = [veylor_client::h2::HpackFieldToEncode {
        name: &[][..],
        value: &[][..],
        representation: veylor_client::h2::HpackRepresentation::WithoutIndexing,
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
            0 => veylor_client::h2::HpackRepresentation::Indexed,
            1 => veylor_client::h2::HpackRepresentation::Incremental,
            2 => veylor_client::h2::HpackRepresentation::WithoutIndexing,
            3 => veylor_client::h2::HpackRepresentation::NeverIndexed,
            _ => {
                content_error.get_or_insert(VylResult::InvalidArgument);
                veylor_client::h2::HpackRepresentation::WithoutIndexing
            }
        };
        if field.huffman_name > 1 || field.huffman_value > 1 {
            content_error.get_or_insert(VylResult::InvalidArgument);
        }
        *destination = veylor_client::h2::HpackFieldToEncode {
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
    if let Err(error) = connection.check_start_request() {
        let (result, _) = split_error(error);
        return result;
    }
    if let Some(error) = content_error {
        return error;
    }
    let output = if output_capacity == 0 {
        &mut []
    } else {
        unsafe { slice::from_raw_parts_mut(output, output_capacity) }
    };
    match connection.start_request(&encode_fields[..field_count], output, end_stream != 0) {
        Ok(started) => {
            unsafe {
                out_written.write(started.written);
                out_stream_id.write(started.stream_id);
                out_required.write(started.required);
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
pub unsafe extern "C" fn vyl_h2_client_send_data(
    handle: *mut VylH2Client,
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
    handle: *mut VylH2Client,
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
pub unsafe extern "C" fn vyl_h2_client_cancel_stream(
    handle: *mut VylH2Client,
    stream_id: u32,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_required: *mut usize,
) -> VylResult {
    catch_ffi(|| unsafe {
        send_reset(
            handle,
            stream_id,
            veylor_http2::ErrorCode::Cancel,
            output,
            output_capacity,
            out_written,
            out_required,
        )
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h2_client_send_reset(
    handle: *mut VylH2Client,
    stream_id: u32,
    error_code: u32,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_required: *mut usize,
) -> VylResult {
    catch_ffi(|| unsafe {
        send_reset(
            handle,
            stream_id,
            veylor_http2::ErrorCode::from(error_code),
            output,
            output_capacity,
            out_written,
            out_required,
        )
    })
}

unsafe fn send_reset(
    handle: *mut VylH2Client,
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
        out_required.write(veylor_http2::FRAME_HEADER_LEN + veylor_http2::RST_STREAM_PAYLOAD_LEN);
    }
    if let veylor_http2::ErrorCode::Unknown(value) = error_code
        && value <= 13
    {
        return VylResult::InvalidArgument;
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
    match connection.send_reset(stream_id, error_code, output) {
        Ok(out) => {
            unsafe {
                out_written.write(out.written);
                out_required.write(out.required);
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
pub unsafe extern "C" fn vyl_h2_client_goaway(
    handle: *mut VylH2Client,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_required: *mut usize,
) -> VylResult {
    catch_ffi(|| unsafe { goaway(handle, output, output_capacity, out_written, out_required) })
}

unsafe fn goaway(
    handle: *mut VylH2Client,
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
    match connection.goaway(output) {
        Ok(out) => {
            unsafe {
                out_written.write(out.written);
                out_required.write(out.required);
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
