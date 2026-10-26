use std::boxed::Box;
use std::ptr;
use std::slice;

use veylor_http2::{
    ControlFrameWriter, H2ClientDriver, H2FrameAction, HpackEntrySlot, HpackHeader, HpackTable,
    MAX_WINDOW_INCREMENT, SettingsParameter, StreamSlot, StreamTable, settings_parameter_valid,
};

use super::{PendingKind, PendingWrite, VylH2Client};
use crate::abi::{VylH2Setting, VylResult, catch_ffi, slice_length_valid};

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

fn control_writer_max_frame(handle: &VylH2Client) -> u32 {
    handle
        .driver
        .as_ref()
        .map(|driver| driver.driver().streams().remote_settings().max_frame_size)
        .unwrap_or(veylor_http2::DEFAULT_MAX_FRAME_SIZE)
}

fn map_control_error(error: veylor_http2::FrameWriteError, required: &mut usize) -> VylResult {
    match error {
        veylor_http2::FrameWriteError::OutputTooSmall { required: need } => {
            *required = need;
            VylResult::BufferTooSmall
        }
        veylor_http2::FrameWriteError::PendingFrame => VylResult::WouldBlock,
        veylor_http2::FrameWriteError::Terminal => VylResult::BadState,
        veylor_http2::FrameWriteError::UnsupportedAction => VylResult::InternalError,
        veylor_http2::FrameWriteError::InvalidHeader(_)
        | veylor_http2::FrameWriteError::InvalidPayload(_) => VylResult::InvalidArgument,
        veylor_http2::FrameWriteError::OverAcknowledged => VylResult::WriteOverrun,
    }
}





#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h2_client_new(out_handle: *mut *mut VylH2Client) -> VylResult {
    catch_ffi(|| unsafe { create_handle(out_handle) })
}

unsafe fn create_handle(out_handle: *mut *mut VylH2Client) -> VylResult {
    if out_handle.is_null() {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_handle.write(ptr::null_mut());
    }
    let mut handle = Box::new(VylH2Client {
        slots: [StreamSlot::EMPTY; super::STREAM_SLOTS],
        header_block: [0; super::HEADER_BLOCK],
        decoder_entries: [HpackEntrySlot::EMPTY; super::HPACK_ENTRIES],
        decoder_scratch: [0; super::HPACK_SCRATCH],
        encoder_entries: [HpackEntrySlot::EMPTY; super::HPACK_ENTRIES],
        encoder_scratch: [0; super::HPACK_SCRATCH],
        settings_ack_limits: [0; super::SETTINGS_ACKS],
        streams: None,
        decoder: None,
        encoder: None,
        driver: None,
        feed_actions: [None; crate::abi::VYL_H2_MAX_ACTIONS],
        feed_headers: [HpackHeader::EMPTY; crate::abi::VYL_H2_MAX_FIELDS],
        feed_action_count: 0,
        feed_header_count: 0,
        feed_pending: false,
        feed_consumed: 0,
        feed_error: None,
        feed_failed: false,
        block: [0; super::HEADER_BLOCK_SCRATCH],
        pending: PendingWrite::default(),
        responses: Vec::new(),
        cancelled: false,
    });
    let handle_ptr: *mut VylH2Client = &mut *handle;
    let (streams, decoder, encoder, driver) = unsafe { build_driver(handle_ptr) };
    handle.streams = Some(streams);
    handle.decoder = Some(decoder);
    handle.encoder = Some(encoder);
    match driver {
        Ok(driver) => handle.driver = Some(driver),
        Err(_) => return VylResult::InternalError,
    }
    unsafe {
        out_handle.write(Box::into_raw(handle));
    }
    VylResult::Ok
}

unsafe fn build_driver(
    handle: *mut VylH2Client,
) -> (
    StreamTable<'static>,
    HpackTable<'static>,
    HpackTable<'static>,
    Result<H2ClientDriver<'static, 'static, 'static, 'static>, ()>,
) {
    let slots = unsafe { &mut *ptr::addr_of_mut!((*handle).slots) };
    let header_block = unsafe { &mut *ptr::addr_of_mut!((*handle).header_block) };
    let decoder_entries = unsafe { &mut *ptr::addr_of_mut!((*handle).decoder_entries) };
    let decoder_scratch = unsafe { &mut *ptr::addr_of_mut!((*handle).decoder_scratch) };
    let encoder_entries = unsafe { &mut *ptr::addr_of_mut!((*handle).encoder_entries) };
    let encoder_scratch = unsafe { &mut *ptr::addr_of_mut!((*handle).encoder_scratch) };
    let ack_limits = unsafe { &mut *ptr::addr_of_mut!((*handle).settings_ack_limits) };
    let slots: &'static mut [StreamSlot] =
        unsafe { std::mem::transmute(slots as &mut [StreamSlot]) };
    let header_block: &'static mut [u8] = unsafe { std::mem::transmute(header_block as &mut [u8]) };
    let decoder_entries: &'static mut [HpackEntrySlot] =
        unsafe { std::mem::transmute(decoder_entries as &mut [HpackEntrySlot]) };
    let decoder_scratch: &'static mut [u8] =
        unsafe { std::mem::transmute(decoder_scratch as &mut [u8]) };
    let encoder_entries: &'static mut [HpackEntrySlot] =
        unsafe { std::mem::transmute(encoder_entries as &mut [HpackEntrySlot]) };
    let encoder_scratch: &'static mut [u8] =
        unsafe { std::mem::transmute(encoder_scratch as &mut [u8]) };
    let ack_limits: &'static mut [u32] = unsafe { std::mem::transmute(ack_limits as &mut [u32]) };
    let streams = StreamTable::new(
        slots,
        veylor_http2::H2Role::Client,
        header_block,
        super::HEADER_BLOCK as u32,
    );
    let streams_ptr = unsafe { &mut *ptr::addr_of_mut!((*handle).streams) };
    *streams_ptr = Some(streams);
    let streams: &'static mut StreamTable<'static> = unsafe {
        std::mem::transmute(
            streams_ptr.as_mut().expect("streams assigned above") as &mut StreamTable<'_>
        )
    };
    let decoder = HpackTable::new(decoder_entries, decoder_scratch, 4_096);
    let encoder = HpackTable::new(encoder_entries, encoder_scratch, 4_096);
    let decoder_ptr = unsafe { &mut *ptr::addr_of_mut!((*handle).decoder) };
    let encoder_ptr = unsafe { &mut *ptr::addr_of_mut!((*handle).encoder) };
    *decoder_ptr = Some(decoder);
    *encoder_ptr = Some(encoder);
    let (decoder, encoder) = unsafe {
        (
            std::mem::transmute::<&mut HpackTable<'_>, &'static mut HpackTable<'static>>(
                decoder_ptr.as_mut().expect("decoder assigned above") as &mut HpackTable<'_>,
            ),
            std::mem::transmute::<&mut HpackTable<'_>, &'static mut HpackTable<'static>>(
                encoder_ptr.as_mut().expect("encoder assigned above") as &mut HpackTable<'_>,
            ),
        )
    };
    let driver = H2ClientDriver::new(streams, decoder, encoder, ack_limits).map_err(|_| ());
    let streams = unsafe { (*ptr::addr_of_mut!((*handle).streams)).take() };
    let decoder = unsafe { (*ptr::addr_of_mut!((*handle).decoder)).take() };
    let encoder = unsafe { (*ptr::addr_of_mut!((*handle).encoder)).take() };
    match (streams, decoder, encoder) {
        (Some(streams), Some(decoder), Some(encoder)) => (streams, decoder, encoder, driver),
        _ => unreachable_cold(),
    }
}

fn unreachable_cold() -> ! {
    unreachable!("storage assigned immediately above")
}






#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h2_client_cancel(handle: *mut VylH2Client) -> VylResult {
    catch_ffi(|| unsafe { cancel_handle(handle) })
}

unsafe fn cancel_handle(handle: *mut VylH2Client) -> VylResult {
    if handle.is_null() {
        return VylResult::InvalidArgument;
    }
    let handle = unsafe { &mut *handle };
    handle.cancelled = true;
    handle.pending = PendingWrite::default();
    handle.responses.clear();
    VylResult::Ok
}






#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h2_client_destroy(handle: *mut VylH2Client) -> VylResult {
    catch_ffi(|| unsafe { destroy_handle(handle) })
}

unsafe fn destroy_handle(handle: *mut VylH2Client) -> VylResult {
    if !handle.is_null() {
        unsafe {
            drop(Box::from_raw(handle));
        }
    }
    VylResult::Ok
}







#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h2_client_connection_preface(
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_required: *mut usize,
) -> VylResult {
    catch_ffi(|| unsafe { write_preface(output, output_capacity, out_written, out_required) })
}

unsafe fn write_preface(
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_required: *mut usize,
) -> VylResult {
    if (output.is_null() && output_capacity != 0)
        || out_written.is_null()
        || out_required.is_null()
        || !slice_length_valid::<u8>(output_capacity)
    {
        return VylResult::InvalidArgument;
    }
    let preface = veylor_http2::CLIENT_CONNECTION_PREFACE;
    unsafe {
        out_written.write(0);
        out_required.write(preface.len());
    }
    if output_capacity < preface.len() {
        return VylResult::BufferTooSmall;
    }
    let output = unsafe { slice::from_raw_parts_mut(output, preface.len()) };
    output.copy_from_slice(preface);
    unsafe {
        out_written.write(preface.len());
    }
    VylResult::Ok
}








#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h2_client_send_settings(
    handle: *mut VylH2Client,
    parameters: *const VylH2Setting,
    parameter_count: usize,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_required: *mut usize,
) -> VylResult {
    catch_ffi(|| unsafe {
        send_settings(
            handle,
            parameters,
            parameter_count,
            output,
            output_capacity,
            out_written,
            out_required,
        )
    })
}

unsafe fn send_settings(
    handle: *mut VylH2Client,
    parameters: *const VylH2Setting,
    parameter_count: usize,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_required: *mut usize,
) -> VylResult {
    if handle.is_null()
        || (parameters.is_null() && parameter_count != 0)
        || (output.is_null() && output_capacity != 0)
        || out_written.is_null()
        || out_required.is_null()
        || !slice_length_valid::<VylH2Setting>(parameter_count)
        || !slice_length_valid::<u8>(output_capacity)
    {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_written.write(0);
        out_required.write(0);
    }
    if parameter_count > crate::abi::VYL_H2_MAX_SETTINGS {
        return VylResult::Capacity;
    }
    let handle = unsafe { &mut *handle };
    if let Err(error) = ensure_writable(handle) {
        return error;
    }
    let parameters = if parameter_count == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(parameters, parameter_count) }
    };
    let mut settings = [SettingsParameter { id: 0, value: 0 }; crate::abi::VYL_H2_MAX_SETTINGS];
    for (destination, parameter) in settings.iter_mut().zip(parameters.iter()) {
        if parameter.id == 0 || parameter.id > u16::MAX as u32 {
            return VylResult::InvalidArgument;
        }
        let id = parameter.id as u16;
        if !settings_parameter_valid(id, parameter.value) {
            return VylResult::InvalidArgument;
        }
        *destination = SettingsParameter {
            id,
            value: parameter.value,
        };
    }
    let max_frame_size = control_writer_max_frame(handle);
    let output = if output_capacity == 0 {
        &mut []
    } else {
        unsafe { slice::from_raw_parts_mut(output, output_capacity) }
    };
    let mut writer = ControlFrameWriter::new(output, max_frame_size);
    let mut required = 0;
    let written = match writer.queue_settings(&settings[..parameter_count]) {
        Ok(written) => written,
        Err(error) => {
            let result = map_control_error(error, &mut required);
            unsafe {
                out_required.write(required);
            }
            return result;
        }
    };
    let mut stored = [(0, 0); crate::abi::VYL_H2_MAX_SETTINGS];
    for (destination, setting) in stored.iter_mut().zip(settings.iter()) {
        *destination = (setting.id, setting.value);
    }
    handle.pending = PendingWrite {
        kind: PendingKind::Settings,
        remaining: written,
        setting_count: parameter_count,
        settings: stored,
    };
    unsafe {
        out_written.write(written);
        out_required.write(written);
    }
    VylResult::Ok
}






#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h2_client_acknowledge(
    handle: *mut VylH2Client,
    transferred_bytes: usize,
    out_pending: *mut usize,
) -> VylResult {
    catch_ffi(|| unsafe { acknowledge(handle, transferred_bytes, out_pending) })
}

unsafe fn acknowledge(
    handle: *mut VylH2Client,
    transferred_bytes: usize,
    out_pending: *mut usize,
) -> VylResult {
    if handle.is_null() || out_pending.is_null() {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_pending.write(0);
    }
    let handle = unsafe { &mut *handle };
    if let Err(error) = ensure_live(handle) {
        return error;
    }
    if handle.pending.kind == PendingKind::None {
        return VylResult::BadState;
    }
    if transferred_bytes > handle.pending.remaining {
        return VylResult::WriteOverrun;
    }
    handle.pending.remaining -= transferred_bytes;
    if handle.pending.remaining != 0 {
        unsafe {
            out_pending.write(handle.pending.remaining);
        }
        return VylResult::Ok;
    }
    if handle.pending.kind == PendingKind::Settings {
        let driver = match handle.driver.as_mut() {
            Some(driver) => driver,
            None => return VylResult::InternalError,
        };
        let mut parameters =
            [SettingsParameter { id: 0, value: 0 }; crate::abi::VYL_H2_MAX_SETTINGS];
        for (destination, stored) in parameters
            .iter_mut()
            .zip(handle.pending.settings.iter())
            .take(handle.pending.setting_count)
        {
            *destination = SettingsParameter {
                id: stored.0,
                value: stored.1,
            };
        }
        if driver
            .driver_mut()
            .register_fully_written_local_settings(&parameters[..handle.pending.setting_count])
            .is_err()
        {
            return VylResult::InternalError;
        }
    }
    handle.pending = PendingWrite::default();
    VylResult::Ok
}







#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h2_client_send_settings_ack(
    handle: *mut VylH2Client,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_required: *mut usize,
) -> VylResult {
    catch_ffi(|| unsafe {
        send_control(
            handle,
            ControlKind::SettingsAck,
            ControlArguments {
                output,
                output_capacity,
                out_written,
                out_required,
                opaque_data: None,
            },
        )
    })
}







#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h2_client_send_ping_ack(
    handle: *mut VylH2Client,
    opaque_data: *const u8,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_required: *mut usize,
) -> VylResult {
    catch_ffi(|| unsafe {
        send_control(
            handle,
            ControlKind::PingAck,
            ControlArguments {
                output,
                output_capacity,
                out_written,
                out_required,
                opaque_data: Some(opaque_data),
            },
        )
    })
}







#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h2_client_send_window_update(
    handle: *mut VylH2Client,
    stream_id: u32,
    increment: u32,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_required: *mut usize,
) -> VylResult {
    catch_ffi(|| unsafe {
        send_window_update(
            handle,
            stream_id,
            increment,
            output,
            output_capacity,
            out_written,
            out_required,
        )
    })
}

#[derive(Clone, Copy)]
enum ControlKind {
    SettingsAck,
    PingAck,
}

struct ControlArguments {
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_required: *mut usize,
    opaque_data: Option<*const u8>,
}

unsafe fn send_control(
    handle: *mut VylH2Client,
    kind: ControlKind,
    arguments: ControlArguments,
) -> VylResult {
    let ControlArguments {
        output,
        output_capacity,
        out_written,
        out_required,
        opaque_data,
    } = arguments;
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
    let opaque = match (kind, opaque_data) {
        (ControlKind::PingAck, Some(pointer)) => {
            if pointer.is_null() {
                return VylResult::InvalidArgument;
            }
            let mut bytes = [0; 8];
            bytes.copy_from_slice(unsafe { slice::from_raw_parts(pointer, 8) });
            bytes
        }
        (ControlKind::PingAck, None) => return VylResult::InvalidArgument,
        (_, _) => [0; 8],
    };
    let handle = unsafe { &mut *handle };
    if let Err(error) = ensure_writable(handle) {
        return error;
    }
    let max_frame_size = control_writer_max_frame(handle);
    let output = if output_capacity == 0 {
        &mut []
    } else {
        unsafe { slice::from_raw_parts_mut(output, output_capacity) }
    };
    let mut writer = ControlFrameWriter::new(output, max_frame_size);
    let action = match kind {
        ControlKind::SettingsAck => H2FrameAction::SettingsAckRequired,
        ControlKind::PingAck => H2FrameAction::PingAck {
            opaque_data: opaque,
        },
    };
    let mut required = 0;
    let written = match writer.queue_action(action, 0) {
        Ok(written) => written,
        Err(error) => {
            let result = map_control_error(error, &mut required);
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

unsafe fn send_window_update(
    handle: *mut VylH2Client,
    stream_id: u32,
    increment: u32,
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
    if increment == 0 || increment > MAX_WINDOW_INCREMENT {
        return VylResult::InvalidArgument;
    }
    if stream_id > veylor_http2::MAX_STREAM_ID {
        return VylResult::InvalidArgument;
    }
    let handle = unsafe { &mut *handle };
    if let Err(error) = ensure_writable(handle) {
        return error;
    }
    let driver = match handle.driver.as_mut() {
        Some(driver) => driver,
        None => return VylResult::InternalError,
    };
    let available = if stream_id == 0 {
        veylor_http2::MAX_FLOW_WINDOW_SIZE - driver.driver().connection_receive_window().value()
    } else {
        match driver.driver().streams().find(stream_id) {
            Some(stream) => veylor_http2::MAX_FLOW_WINDOW_SIZE - stream.receive_window.value(),
            None => return VylResult::InvalidArgument,
        }
    };
    if i64::from(increment) > available {
        return VylResult::InvalidArgument;
    }
    let max_frame_size = control_writer_max_frame(handle);
    let output = if output_capacity == 0 {
        &mut []
    } else {
        unsafe { slice::from_raw_parts_mut(output, output_capacity) }
    };
    let mut writer = ControlFrameWriter::new(output, max_frame_size);
    let mut required = 0;
    let written = match writer.queue_window_update(stream_id, increment) {
        Ok(written) => written,
        Err(error) => {
            let result = map_control_error(error, &mut required);
            unsafe {
                out_required.write(required);
            }
            return result;
        }
    };
    let driver = match handle.driver.as_mut() {
        Some(driver) => driver,
        None => return VylResult::InternalError,
    };
    let credited = if stream_id == 0 {
        driver
            .driver_mut()
            .add_connection_receive_credit(increment)
            .is_ok()
    } else {
        driver
            .driver_mut()
            .add_stream_receive_credit(stream_id, increment)
            .is_ok()
    };
    if !credited {
        return VylResult::InternalError;
    }
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
