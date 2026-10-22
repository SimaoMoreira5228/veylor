use std::slice;

use veylor_http2::{FeedStatus, H2FrameAction, HeaderRange};

use super::VylH2Server;
use crate::abi::{
    VYL_H2_END_STREAM, VylH2Action, VylH2ActionKind, VylH2Field, VylResult, catch_ffi,
    slice_length_valid,
};

pub(crate) fn error_code_value(code: veylor_http2::ErrorCode) -> u32 {
    match code {
        veylor_http2::ErrorCode::NoError => 0,
        veylor_http2::ErrorCode::ProtocolError => 1,
        veylor_http2::ErrorCode::InternalError => 2,
        veylor_http2::ErrorCode::FlowControlError => 3,
        veylor_http2::ErrorCode::SettingsTimeout => 4,
        veylor_http2::ErrorCode::StreamClosed => 5,
        veylor_http2::ErrorCode::FrameSizeError => 6,
        veylor_http2::ErrorCode::RefusedStream => 7,
        veylor_http2::ErrorCode::Cancel => 8,
        veylor_http2::ErrorCode::CompressionError => 9,
        veylor_http2::ErrorCode::ConnectError => 10,
        veylor_http2::ErrorCode::EnhanceYourCalm => 11,
        veylor_http2::ErrorCode::InadequateSecurity => 12,
        veylor_http2::ErrorCode::Http11Required => 13,
        veylor_http2::ErrorCode::Unknown(value) => value,
    }
}

struct PollArguments {
    input: *const u8,
    input_length: usize,
    out_consumed: *mut usize,
    actions: *mut VylH2Action,
    action_capacity: usize,
    fields: *mut VylH2Field,
    field_capacity: usize,
    data: *mut u8,
    data_capacity: usize,
    out_actions: *mut usize,
    out_fields: *mut usize,
    out_actions_required: *mut usize,
    out_fields_required: *mut usize,
    out_data_required: *mut usize,
    out_connection_error: *mut u32,
}












#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h2_server_poll_feed(
    handle: *mut VylH2Server,
    input: *const u8,
    input_length: usize,
    out_consumed: *mut usize,
    actions: *mut VylH2Action,
    action_capacity: usize,
    fields: *mut VylH2Field,
    field_capacity: usize,
    data: *mut u8,
    data_capacity: usize,
    out_actions: *mut usize,
    out_fields: *mut usize,
    out_actions_required: *mut usize,
    out_fields_required: *mut usize,
    out_data_required: *mut usize,
    out_connection_error: *mut u32,
) -> VylResult {
    let arguments = PollArguments {
        input,
        input_length,
        out_consumed,
        actions,
        action_capacity,
        fields,
        field_capacity,
        data,
        data_capacity,
        out_actions,
        out_fields,
        out_actions_required,
        out_fields_required,
        out_data_required,
        out_connection_error,
    };
    catch_ffi(|| unsafe { poll_feed(handle, arguments) })
}

unsafe fn poll_feed(handle: *mut VylH2Server, arguments: PollArguments) -> VylResult {
    let PollArguments {
        input,
        input_length,
        out_consumed,
        actions,
        action_capacity,
        fields,
        field_capacity,
        data,
        data_capacity,
        out_actions,
        out_fields,
        out_actions_required,
        out_fields_required,
        out_data_required,
        out_connection_error,
    } = arguments;
    if handle.is_null()
        || (input.is_null() && input_length != 0)
        || (actions.is_null() && action_capacity != 0)
        || (fields.is_null() && field_capacity != 0)
        || (data.is_null() && data_capacity != 0)
        || out_consumed.is_null()
        || out_actions.is_null()
        || out_fields.is_null()
        || out_actions_required.is_null()
        || out_fields_required.is_null()
        || out_data_required.is_null()
        || out_connection_error.is_null()
        || !slice_length_valid::<u8>(input_length)
        || !slice_length_valid::<VylH2Action>(action_capacity)
        || !slice_length_valid::<VylH2Field>(field_capacity)
        || !slice_length_valid::<u8>(data_capacity)
    {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_consumed.write(0);
        out_actions.write(0);
        out_fields.write(0);
        out_actions_required.write(0);
        out_fields_required.write(0);
        out_data_required.write(0);
        out_connection_error.write(0);
    }
    let handle = unsafe { &mut *handle };
    if handle.cancelled {
        return VylResult::Cancelled;
    }
    if handle.driver.is_none() {
        return VylResult::InternalError;
    }
    let fresh = !handle.feed_pending;
    if fresh {
        let input = if input_length == 0 {
            &[]
        } else {
            unsafe { slice::from_raw_parts(input, input_length) }
        };
        let driver = handle.driver.as_mut().expect("driver checked above");
        let actions_scratch: &mut [Option<H2FrameAction<'_>>] =
            unsafe { std::mem::transmute(&mut handle.feed_actions as &mut [_]) };
        let result = driver
            .driver_mut()
            .feed(input, actions_scratch, &mut handle.feed_headers);
        handle.feed_action_count = result.actions;
        handle.feed_header_count = result.headers;
        handle.feed_consumed = result.consumed;
        handle.feed_failed = result.status == FeedStatus::ConnectionFailed;
        handle.feed_error = result.error.map(error_code_value);
        handle.feed_pending = result.actions > 0;
        unsafe {
            out_consumed.write(result.consumed);
        }
        if !handle.feed_pending {
            if handle.feed_failed {
                unsafe {
                    out_connection_error.write(handle.feed_error.unwrap_or(1));
                }
                return VylResult::ProtocolError;
            }
            return VylResult::NeedMore;
        }
    }
    unsafe {
        translate_pending(
            handle,
            TranslateOutputs {
                actions,
                action_capacity,
                fields,
                field_capacity,
                data,
                data_capacity,
                out_actions,
                out_fields,
                out_actions_required,
                out_fields_required,
                out_data_required,
                out_connection_error,
            },
        )
    }
}

struct TranslateOutputs {
    actions: *mut VylH2Action,
    action_capacity: usize,
    fields: *mut VylH2Field,
    field_capacity: usize,
    data: *mut u8,
    data_capacity: usize,
    out_actions: *mut usize,
    out_fields: *mut usize,
    out_actions_required: *mut usize,
    out_fields_required: *mut usize,
    out_data_required: *mut usize,
    out_connection_error: *mut u32,
}

unsafe fn translate_pending(handle: &mut VylH2Server, outputs: TranslateOutputs) -> VylResult {
    let driver = match handle.driver.as_ref() {
        Some(driver) => driver,
        None => return VylResult::InternalError,
    };
    let mut needed_fields: usize = 0;
    let mut needed_data: usize = 0;
    for slot in handle.feed_actions[..handle.feed_action_count].iter() {
        let Some(action) = slot else { continue };
        match action {
            H2FrameAction::Headers { headers, .. } => {
                needed_fields += headers.count;
                for header in
                    handle.feed_headers[headers.start..headers.start + headers.count].iter()
                {
                    let Some((name, value)) = driver.driver().resolve_header(*header) else {
                        return VylResult::InternalError;
                    };
                    needed_data = needed_data
                        .saturating_add(name.len())
                        .saturating_add(value.len());
                }
            }
            H2FrameAction::Data { data, .. } => {
                needed_data = needed_data.saturating_add(data.len());
            }
            H2FrameAction::GoAway { debug_data, .. } => {
                needed_data = needed_data.saturating_add(debug_data.len());
            }
            _ => {}
        }
    }
    if handle.feed_action_count > outputs.action_capacity {
        unsafe {
            outputs.out_actions_required.write(handle.feed_action_count);
            outputs.out_fields_required.write(needed_fields);
            outputs.out_data_required.write(needed_data);
        }
        return VylResult::BufferTooSmall;
    }
    if needed_fields > outputs.field_capacity || needed_data > outputs.data_capacity {
        unsafe {
            outputs.out_actions_required.write(handle.feed_action_count);
            outputs.out_fields_required.write(needed_fields);
            outputs.out_data_required.write(needed_data);
        }
        return VylResult::BufferTooSmall;
    }
    let actions = if outputs.action_capacity == 0 {
        &mut []
    } else {
        unsafe { slice::from_raw_parts_mut(outputs.actions, outputs.action_capacity) }
    };
    let fields = if outputs.field_capacity == 0 {
        &mut []
    } else {
        unsafe { slice::from_raw_parts_mut(outputs.fields, outputs.field_capacity) }
    };
    let data = if outputs.data_capacity == 0 {
        &mut []
    } else {
        unsafe { slice::from_raw_parts_mut(outputs.data, outputs.data_capacity) }
    };
    let mut field_next = 0;
    let mut data_next = 0;
    let mut action_next = 0;
    for slot in handle.feed_actions[..handle.feed_action_count].iter() {
        let Some(action) = slot else { continue };
        let translated = match action {
            H2FrameAction::Headers {
                stream_id,
                headers: HeaderRange { start, count },
                end_stream,
            } => {
                let field_start = field_next;
                for header in handle.feed_headers[*start..*start + *count].iter() {
                    let Some((name, value)) = driver.driver().resolve_header(*header) else {
                        return VylResult::InternalError;
                    };
                    data[data_next..data_next + name.len()].copy_from_slice(name);
                    let name_offset = data_next;
                    data_next += name.len();
                    data[data_next..data_next + value.len()].copy_from_slice(value);
                    let value_offset = data_next;
                    data_next += value.len();
                    fields[field_next] = VylH2Field {
                        name_offset,
                        name_length: name.len(),
                        value_offset,
                        value_length: value.len(),
                    };
                    field_next += 1;
                }
                VylH2Action {
                    kind: VylH2ActionKind::Headers,
                    stream_id: *stream_id,
                    flags: u32::from(*end_stream) & VYL_H2_END_STREAM,
                    field_start,
                    field_count: *count,
                    ..VylH2Action::default()
                }
            }
            H2FrameAction::Data {
                stream_id,
                data: payload,
                end_stream,
            } => {
                let data_offset = data_next;
                data[data_next..data_next + payload.len()].copy_from_slice(payload);
                data_next += payload.len();
                VylH2Action {
                    kind: VylH2ActionKind::Data,
                    stream_id: *stream_id,
                    flags: u32::from(*end_stream) & VYL_H2_END_STREAM,
                    data_offset,
                    data_length: payload.len(),
                    ..VylH2Action::default()
                }
            }
            H2FrameAction::StreamReset {
                stream_id,
                error_code,
            } => VylH2Action {
                kind: VylH2ActionKind::StreamReset,
                stream_id: *stream_id,
                error_code: error_code_value(*error_code),
                ..VylH2Action::default()
            },
            H2FrameAction::GoAway {
                last_stream_id,
                error_code,
                debug_data,
            } => {
                let data_offset = data_next;
                data[data_next..data_next + debug_data.len()].copy_from_slice(debug_data);
                data_next += debug_data.len();
                VylH2Action {
                    kind: VylH2ActionKind::GoAway,
                    stream_id: *last_stream_id,
                    error_code: error_code_value(*error_code),
                    data_offset,
                    data_length: debug_data.len(),
                    ..VylH2Action::default()
                }
            }
            H2FrameAction::SettingsAckRequired => VylH2Action {
                kind: VylH2ActionKind::SettingsAckRequired,
                ..VylH2Action::default()
            },
            H2FrameAction::PingAck { opaque_data } => VylH2Action {
                kind: VylH2ActionKind::PingAck,
                ping: *opaque_data,
                ..VylH2Action::default()
            },
            H2FrameAction::SendReset {
                stream_id,
                error_code,
            } => VylH2Action {
                kind: VylH2ActionKind::SendReset,
                stream_id: *stream_id,
                error_code: error_code_value(*error_code),
                ..VylH2Action::default()
            },
            H2FrameAction::ConnectionError { error_code } => VylH2Action {
                kind: VylH2ActionKind::ConnectionError,
                error_code: error_code_value(*error_code),
                ..VylH2Action::default()
            },
        };
        actions[action_next] = translated;
        action_next += 1;
    }
    unsafe {
        outputs.out_actions.write(action_next);
        outputs.out_fields.write(field_next);
        outputs
            .out_connection_error
            .write(handle.feed_error.unwrap_or(0));
    }
    handle.feed_actions.fill(None);
    handle.feed_action_count = 0;
    handle.feed_pending = false;
    if handle.feed_failed {
        VylResult::ProtocolError
    } else {
        VylResult::Ok
    }
}
