use std::slice;

use veylor_http2::{
    ClientError, DataWriteError, ErrorCode, FRAME_HEADER_LEN, HeadersWriteError,
    RST_STREAM_PAYLOAD_LEN,
};

use super::{PendingKind, PendingWrite, VylH2Client};
use crate::abi::{
    VYL_H2_MAX_FIELDS, VylBytes, VylH2FieldToEncode, VylResult, catch_ffi, slice_length_valid,
};

fn ensure_live(handle: &VylH2Client) -> Result<(), VylResult> {
    if handle.cancelled {
        return Err(VylResult::Cancelled);
    }
    Ok(())
}

fn ensure_writable(handle: &VylH2Client) -> Result<(), VylResult> {
    ensure_live(handle)?;
    if handle.pending.kind != PendingKind::None {
        return Err(VylResult::WouldBlock);
    }
    let failed = handle
        .driver
        .as_ref()
        .is_some_and(|driver| driver.driver().failed().is_some());
    if failed {
        return Err(VylResult::BadState);
    }
    if handle.driver.is_none() {
        return Err(VylResult::InternalError);
    }
    Ok(())
}

fn map_client_error(error: ClientError, required: &mut usize) -> VylResult {
    match error {
        ClientError::OutputTooSmall { required: need } => {
            *required = need;
            VylResult::BufferTooSmall
        }
        ClientError::Headers(error) => map_headers_error(error, required),
        ClientError::Data(error) => map_data_error(error, required),
        ClientError::Control(error) => match error {
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
        ClientError::Request(_) => VylResult::InvalidArgument,
        ClientError::SettingsNotSent | ClientError::NewStreamsClosed => VylResult::BadState,
        ClientError::ConcurrencyExhausted | ClientError::MaxStreamIdExhausted => {
            VylResult::Capacity
        }
        ClientError::UnknownStream => VylResult::InvalidArgument,
        ClientError::StreamClosed => VylResult::BadState,
        ClientError::WrongRole | ClientError::Stream(_) | ClientError::Flow(_) => {
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
    let handle = unsafe { &mut *handle };
    if let Err(error) = ensure_writable(handle) {
        return error;
    }
    if handle.responses.len() >= super::MAX_RESPONSES {
        return VylResult::Capacity;
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
    let mut method: &[u8] = &[];
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
        if name == b":method" {
            method = unsafe { slice::from_raw_parts(value.as_ptr(), value.len()) };
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
    let started = match driver.start_request(
        &encode_fields[..field_count],
        &mut handle.block,
        output,
        end_stream != 0,
    ) {
        Ok(started) => started,
        Err(error) => {
            let result = map_client_error(error, &mut required);
            unsafe {
                out_required.write(required);
            }
            return result;
        }
    };
    handle.pending = PendingWrite {
        kind: PendingKind::Headers,
        remaining: started.frame.output_length,
        ..PendingWrite::default()
    };
    handle.responses.push((
        started.stream_id,
        veylor_http2::ClientResponseHeaders::new_with_method(method),
    ));
    unsafe {
        out_written.write(started.frame.output_length);
        out_stream_id.write(started.stream_id);
        out_required.write(started.frame.output_length);
    }
    VylResult::Ok
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
            let result = map_client_error(error, &mut required);
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
            ErrorCode::Cancel,
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
            ErrorCode::from(error_code),
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
    error_code: ErrorCode,
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
        out_required.write(FRAME_HEADER_LEN + RST_STREAM_PAYLOAD_LEN);
    }
    if let ErrorCode::Unknown(value) = error_code {
        if value <= 13 {
            return VylResult::InvalidArgument;
        }
    }
    let handle = unsafe { &mut *handle };
    if let Err(error) = ensure_writable(handle) {
        return error;
    }
    let driver = match handle.driver.as_mut() {
        Some(driver) => driver,
        None => return VylResult::InternalError,
    };
    if stream_id == 0 || stream_id > veylor_http2::MAX_STREAM_ID {
        return VylResult::InvalidArgument;
    }
    match driver.driver().streams().find(stream_id) {
        Some(stream) if stream.state != veylor_http2::StreamState::Closed => {}
        Some(_) => return VylResult::BadState,
        None => return VylResult::InvalidArgument,
    }
    if output_capacity < FRAME_HEADER_LEN + RST_STREAM_PAYLOAD_LEN {
        return VylResult::BufferTooSmall;
    }
    let max_frame_size = driver.driver().streams().remote_settings().max_frame_size;
    driver.driver_mut().streams_mut().close_stream(stream_id);
    let output = unsafe { slice::from_raw_parts_mut(output, output_capacity) };
    let mut writer = veylor_http2::ControlFrameWriter::new(output, max_frame_size);
    let mut required = 0;
    let written = match writer.queue_action(
        veylor_http2::H2FrameAction::SendReset {
            stream_id,
            error_code,
        },
        0,
    ) {
        Ok(written) => written,
        Err(error) => {
            let result = match error {
                veylor_http2::FrameWriteError::OutputTooSmall { required: need } => {
                    required = need;
                    VylResult::BufferTooSmall
                }
                veylor_http2::FrameWriteError::InvalidHeader(_)
                | veylor_http2::FrameWriteError::InvalidPayload(_) => VylResult::InvalidArgument,
                veylor_http2::FrameWriteError::PendingFrame => VylResult::WouldBlock,
                veylor_http2::FrameWriteError::OverAcknowledged => VylResult::WriteOverrun,
                veylor_http2::FrameWriteError::Terminal => VylResult::BadState,
                veylor_http2::FrameWriteError::UnsupportedAction => VylResult::InternalError,
            };
            unsafe {
                out_required.write(required);
            }
            return result;
        }
    };
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
    let writer = match driver.goaway(output) {
        Ok(writer) => writer,
        Err(error) => {
            let result = map_client_error(error, &mut required);
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
