use std::slice;

use veylor_http2::{DataWriteError, HeadersWriteError, ServerError};

use super::{PendingKind, PendingWrite, VylH2Server};
use crate::abi::{
    VYL_H2_MAX_FIELDS, VYL_H2_NO_FIELD, VylBytes, VylH2FieldToEncode, VylH2ValidatedRequest,
    VylResult, catch_ffi, slice_length_valid,
};

fn ensure_live(handle: &VylH2Server) -> Result<(), VylResult> {
    if handle.cancelled {
        return Err(VylResult::Cancelled);
    }
    Ok(())
}

fn ensure_writable(handle: &VylH2Server) -> Result<(), VylResult> {
    ensure_live(handle)?;
    if handle.pending.kind != PendingKind::None {
        return Err(VylResult::WouldBlock);
    }
    if handle.driver.is_none() {
        return Err(VylResult::InternalError);
    }
    Ok(())
}

fn map_server_error(error: ServerError, required: &mut usize) -> VylResult {
    match error {
        ServerError::OutputTooSmall { required: need } => {
            *required = need;
            VylResult::BufferTooSmall
        }
        ServerError::Headers(error) => map_headers_error(error, required),
        ServerError::Data(error) => map_data_error(error, required),
        ServerError::Control(error) => match error {
            veylor_http2::FrameWriteError::OutputTooSmall { required: need } => {
                *required = need;
                VylResult::BufferTooSmall
            }
            veylor_http2::FrameWriteError::InvalidHeader(_)
            | veylor_http2::FrameWriteError::InvalidPayload(_) => VylResult::InvalidArgument,
            veylor_http2::FrameWriteError::PendingFrame => VylResult::WouldBlock,
            veylor_http2::FrameWriteError::OverAcknowledged => VylResult::WriteOverrun,
            veylor_http2::FrameWriteError::Terminal => VylResult::BadState,
            veylor_http2::FrameWriteError::UnsupportedAction => VylResult::InternalError,
        },
        ServerError::Request(_) => VylResult::ProtocolError,
        ServerError::Response(_) => VylResult::InvalidArgument,
        ServerError::SettingsNotSent | ServerError::NewStreamsClosed => VylResult::BadState,
        ServerError::RefusedStream => VylResult::Capacity,
        ServerError::UnknownStream => VylResult::InvalidArgument,
        ServerError::StreamClosed => VylResult::BadState,
        ServerError::WrongRole | ServerError::Stream(_) | ServerError::Flow(_) => {
            VylResult::InternalError
        }
    }
}

fn map_headers_error(error: HeadersWriteError, required: &mut usize) -> VylResult {
    match error {
        HeadersWriteError::OutputTooSmall { required: need } => {
            *required = need;
            VylResult::BufferTooSmall
        }
        HeadersWriteError::SettingsNotSent => VylResult::BadState,
        HeadersWriteError::PendingFrame => VylResult::WouldBlock,
        HeadersWriteError::Terminal | HeadersWriteError::OverAcknowledged => {
            VylResult::WriteOverrun
        }
        HeadersWriteError::StreamRejected(_) => VylResult::ProtocolError,
        HeadersWriteError::InvalidStreamId
        | HeadersWriteError::LengthOverflow
        | HeadersWriteError::InvalidHeader(_) => VylResult::InternalError,
    }
}

fn map_data_error(error: DataWriteError, required: &mut usize) -> VylResult {
    match error {
        DataWriteError::OutputTooSmall { required: need } => {
            *required = need;
            VylResult::BufferTooSmall
        }
        DataWriteError::InvalidStreamId | DataWriteError::UnknownStream => {
            VylResult::InvalidArgument
        }
        DataWriteError::StreamClosed => VylResult::BadState,
        DataWriteError::SettingsNotSent => VylResult::BadState,
        DataWriteError::NoSendCredit | DataWriteError::PendingFrame => VylResult::WouldBlock,
        DataWriteError::OverAcknowledged | DataWriteError::Terminal => VylResult::WriteOverrun,
        DataWriteError::InvalidHeader(_) => VylResult::InternalError,
    }
}

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
    if let Err(error) = ensure_live(handle) {
        return error;
    }
    let Some(end) = header_start.checked_add(header_count) else {
        return VylResult::InvalidArgument;
    };
    if end > handle.feed_header_count {
        return VylResult::InvalidArgument;
    }
    let driver = match handle.driver.as_ref() {
        Some(driver) => driver,
        None => return VylResult::InternalError,
    };
    let validated = match driver.accept_request(stream_id, &handle.feed_headers[header_start..end])
    {
        Ok(validated) => validated,
        Err(error) => {
            let mut required = 0;
            return map_server_error(error, &mut required);
        }
    };
    let mut method_field = VYL_H2_NO_FIELD;
    let mut scheme_field = VYL_H2_NO_FIELD;
    let mut authority_field = VYL_H2_NO_FIELD;
    let mut path_field = VYL_H2_NO_FIELD;
    for (index, header) in handle.feed_headers[header_start..end].iter().enumerate() {
        let Some((name, value)) = driver.driver().resolve_header(*header) else {
            return VylResult::InternalError;
        };
        if name == b":method" && value == validated.method {
            method_field = header_start + index;
        } else if name == b":scheme" && Some(value) == validated.scheme {
            scheme_field = header_start + index;
        } else if name == b":authority" && Some(value) == validated.authority {
            authority_field = header_start + index;
        } else if name == b":path" && Some(value) == validated.path {
            path_field = header_start + index;
        }
    }
    if method_field == VYL_H2_NO_FIELD {
        return VylResult::InternalError;
    }
    let (content_length, has_content_length) = match validated.content_length {
        Some(length) => (length, 1),
        None => (0, 0),
    };
    unsafe {
        out_request.write(VylH2ValidatedRequest {
            method_field,
            scheme_field,
            authority_field,
            path_field,
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
    let handle = unsafe { &mut *handle };
    if let Err(error) = ensure_writable(handle) {
        return error;
    }
    let fields = if field_count == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(fields, field_count) }
    };
    let mut encode_fields = [veylor_http2::HpackFieldToEncode {
        name: &[][..],
        value: &[][..],
        representation: veylor_http2::HpackRepresentation::WithoutIndexing,
        huffman_name: false,
        huffman_value: false,
    }; VYL_H2_MAX_FIELDS];
    for (destination, field) in encode_fields.iter_mut().zip(fields.iter()) {
        let name = match unsafe { read_bytes(field.name) } {
            Ok(name) => name,
            Err(error) => return error,
        };
        let value = match unsafe { read_bytes(field.value) } {
            Ok(value) => value,
            Err(error) => return error,
        };
        let representation = match field.representation {
            0 => veylor_http2::HpackRepresentation::Indexed,
            1 => veylor_http2::HpackRepresentation::Incremental,
            2 => veylor_http2::HpackRepresentation::WithoutIndexing,
            3 => veylor_http2::HpackRepresentation::NeverIndexed,
            _ => return VylResult::InvalidArgument,
        };
        if field.huffman_name > 1 || field.huffman_value > 1 {
            return VylResult::InvalidArgument;
        }
        *destination = veylor_http2::HpackFieldToEncode {
            name,
            value,
            representation,
            huffman_name: field.huffman_name != 0,
            huffman_value: field.huffman_value != 0,
        };
    }
    let output = if output_capacity == 0 {
        &mut []
    } else {
        unsafe { slice::from_raw_parts_mut(output, output_capacity) }
    };
    let driver = match handle.driver.as_mut() {
        Some(driver) => driver,
        None => return VylResult::InternalError,
    };
    let mut required = 0;
    let sent = match driver.send_response(
        stream_id,
        &encode_fields[..field_count],
        &mut handle.block,
        output,
        end_stream != 0,
    ) {
        Ok(sent) => sent,
        Err(error) => {
            let result = map_server_error(error, &mut required);
            unsafe {
                out_required.write(required);
            }
            return result;
        }
    };
    handle.pending = PendingWrite {
        kind: PendingKind::Headers,
        remaining: sent.frame.output_length,
        ..PendingWrite::default()
    };
    unsafe {
        out_written.write(sent.frame.output_length);
        out_required.write(sent.frame.output_length);
    }
    VylResult::Ok
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
    if let Err(error) = ensure_writable(handle) {
        return error;
    }
    let output = if output_capacity == 0 {
        &mut []
    } else {
        unsafe { slice::from_raw_parts_mut(output, output_capacity) }
    };
    let driver = match handle.driver.as_mut() {
        Some(driver) => driver,
        None => return VylResult::InternalError,
    };
    let mut required = 0;
    let sent = match driver.send_data(stream_id, input, end_stream != 0, output) {
        Ok(sent) => sent,
        Err(error) => {
            let result = map_server_error(error, &mut required);
            unsafe {
                out_required.write(required);
            }
            return result;
        }
    };
    handle.pending = PendingWrite {
        kind: PendingKind::Data,
        remaining: sent.frame.frame_length,
        ..PendingWrite::default()
    };
    unsafe {
        out_consumed.write(sent.frame.payload_length);
        out_written.write(sent.frame.frame_length);
        out_end_stream.write(u32::from(sent.frame.end_stream));
        out_required.write(sent.frame.frame_length);
    }
    VylResult::Ok
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
        if let veylor_http2::ErrorCode::Unknown(value) = code {
            if value <= 13 {
                return VylResult::InvalidArgument;
            }
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
    if let Err(error) = ensure_writable(handle) {
        return error;
    }
    let output = if output_capacity == 0 {
        &mut []
    } else {
        unsafe { slice::from_raw_parts_mut(output, output_capacity) }
    };
    let driver = match handle.driver.as_mut() {
        Some(driver) => driver,
        None => return VylResult::InternalError,
    };
    let mut required = 0;
    let writer = match driver.rst_stream(stream_id, error_code, output) {
        Ok(writer) => writer,
        Err(error) => {
            let result = map_server_error(error, &mut required);
            unsafe {
                out_required.write(required);
            }
            return result;
        }
    };
    let written = writer.pending_bytes().len();
    handle.pending = PendingWrite {
        kind: PendingKind::Control,
        remaining: written,
        ..PendingWrite::default()
    };
    unsafe {
        out_written.write(written);
        out_required.write(written);
    }
    VylResult::Ok
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
    if let Err(error) = ensure_writable(handle) {
        return error;
    }
    let output = if output_capacity == 0 {
        &mut []
    } else {
        unsafe { slice::from_raw_parts_mut(output, output_capacity) }
    };
    let driver = match handle.driver.as_mut() {
        Some(driver) => driver,
        None => return VylResult::InternalError,
    };
    let mut required = 0;
    let writer = match driver.goaway_drain(last_stream_id, output) {
        Ok(writer) => writer,
        Err(error) => {
            let result = map_server_error(error, &mut required);
            unsafe {
                out_required.write(required);
            }
            return result;
        }
    };
    let written = writer.pending_bytes().len();
    handle.pending = PendingWrite {
        kind: PendingKind::Control,
        remaining: written,
        ..PendingWrite::default()
    };
    unsafe {
        out_written.write(written);
        out_required.write(written);
    }
    VylResult::Ok
}
