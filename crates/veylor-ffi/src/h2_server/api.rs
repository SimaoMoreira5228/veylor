use std::boxed::Box;
use std::ptr;
use std::slice;

use veylor_server_conn::h2::{
    FeedScratch, H2ServerConnection, H2ServerTables, HEADER_BLOCK, HPACK_ENTRIES, HPACK_SCRATCH,
    HpackEntrySlot, SETTINGS_ACKS, STREAM_SLOTS, StreamSlot, WriteOut,
};

use super::{VylH2Server, split_error};
use crate::abi::{VylH2Setting, VylResult, catch_ffi, slice_length_valid};

fn write_out(written: usize, required: usize, out_written: *mut usize, out_required: *mut usize) {
    unsafe {
        out_written.write(written);
        out_required.write(required);
    }
}

fn report_write(
    result: Result<WriteOut, veylor_server_conn::h2::H2ServerError>,
    out_written: *mut usize,
    out_required: *mut usize,
) -> VylResult {
    match result {
        Ok(out) => {
            write_out(out.written, out.required, out_written, out_required);
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
pub unsafe extern "C" fn vyl_h2_server_new(out_handle: *mut *mut VylH2Server) -> VylResult {
    catch_ffi(|| unsafe { create_handle(out_handle) })
}

unsafe fn create_handle(out_handle: *mut *mut VylH2Server) -> VylResult {
    if out_handle.is_null() {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_handle.write(ptr::null_mut());
    }
    let mut handle = Box::new(VylH2Server {
        slots: [StreamSlot::EMPTY; STREAM_SLOTS],
        header_block: [0; HEADER_BLOCK],
        decoder_entries: [HpackEntrySlot::EMPTY; HPACK_ENTRIES],
        decoder_scratch: [0; HPACK_SCRATCH],
        encoder_entries: [HpackEntrySlot::EMPTY; HPACK_ENTRIES],
        encoder_scratch: [0; HPACK_SCRATCH],
        settings_ack_limits: [0; SETTINGS_ACKS],
        tables: None,
        connection: None,
        feed_scratch: FeedScratch::new(),
    });
    let handle_ptr: *mut VylH2Server = &mut *handle;
    let tables = unsafe { build_tables(handle_ptr) };
    unsafe {
        (*handle_ptr).tables = Some(tables);
    }
    let connection = unsafe {
        let tables = (*ptr::addr_of_mut!((*handle_ptr).tables))
            .as_mut()
            .expect("tables stored above");
        match tables.connect() {
            Ok(connection) => std::mem::transmute::<
                H2ServerConnection<'_, 'static>,
                H2ServerConnection<'static, 'static>,
            >(connection),
            Err(_) => return VylResult::InternalError,
        }
    };
    unsafe {
        (*handle_ptr).connection = Some(connection);
    }
    unsafe {
        out_handle.write(Box::into_raw(handle));
    }
    VylResult::Ok
}

// SECURITY: the 'static borrows alias fixed-size arrays inside the same heap-pinned handle:
// the Box address is stable from into_raw to the single from_raw at destroy, the fields
// never reallocate, and each FFI call takes exclusive access under the host's single-owner
// contract, so no live alias ever escapes. The connection borrow is additionally tied to the
// tables field it borrows; both are destroyed together without further use.
unsafe fn build_tables(handle: *mut VylH2Server) -> H2ServerTables<'static> {
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
    H2ServerTables::new(
        slots,
        header_block,
        decoder_entries,
        decoder_scratch,
        encoder_entries,
        encoder_scratch,
        ack_limits,
    )
}






#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h2_server_cancel(handle: *mut VylH2Server) -> VylResult {
    catch_ffi(|| unsafe { cancel_handle(handle) })
}

unsafe fn cancel_handle(handle: *mut VylH2Server) -> VylResult {
    if handle.is_null() {
        return VylResult::InvalidArgument;
    }
    let handle = unsafe { &mut *handle };
    let Some(connection) = handle.connection.as_mut() else {
        return VylResult::InternalError;
    };
    connection.cancel();
    VylResult::Ok
}






#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h2_server_destroy(handle: *mut VylH2Server) -> VylResult {
    catch_ffi(|| unsafe { destroy_handle(handle) })
}

unsafe fn destroy_handle(handle: *mut VylH2Server) -> VylResult {
    if !handle.is_null() {
        unsafe {
            drop(Box::from_raw(handle));
        }
    }
    VylResult::Ok
}








#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h2_server_send_settings(
    handle: *mut VylH2Server,
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
    handle: *mut VylH2Server,
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
    let parameters = if parameter_count == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(parameters, parameter_count) }
    };
    let mut settings = [veylor_server_conn::h2::SettingsParameter { id: 0, value: 0 };
        crate::abi::VYL_H2_MAX_SETTINGS];
    // NOTE: out-of-range ids are staged as zero (always rejected) instead of returned here,
    // so state errors reported by the core keep their historical precedence over id errors.
    for (destination, parameter) in settings.iter_mut().zip(parameters.iter()) {
        if parameter.id == 0 || parameter.id > u16::MAX as u32 {
            *destination = veylor_server_conn::h2::SettingsParameter { id: 0, value: 0 };
        } else {
            *destination = veylor_server_conn::h2::SettingsParameter {
                id: parameter.id as u16,
                value: parameter.value,
            };
        }
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
    report_write(
        connection.send_settings(&settings[..parameter_count], output),
        out_written,
        out_required,
    )
}






#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h2_server_acknowledge(
    handle: *mut VylH2Server,
    transferred_bytes: usize,
    out_pending: *mut usize,
) -> VylResult {
    catch_ffi(|| unsafe { acknowledge(handle, transferred_bytes, out_pending) })
}

unsafe fn acknowledge(
    handle: *mut VylH2Server,
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
    let Some(connection) = handle.connection.as_mut() else {
        return VylResult::InternalError;
    };
    match connection.acknowledge(transferred_bytes) {
        Ok(pending) => {
            unsafe {
                out_pending.write(pending);
            }
            VylResult::Ok
        }
        Err(error) => {
            let (result, _) = split_error(error);
            result
        }
    }
}







#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h2_server_send_settings_ack(
    handle: *mut VylH2Server,
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
pub unsafe extern "C" fn vyl_h2_server_send_ping_ack(
    handle: *mut VylH2Server,
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
pub unsafe extern "C" fn vyl_h2_server_send_window_update(
    handle: *mut VylH2Server,
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
    handle: *mut VylH2Server,
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
    let Some(connection) = handle.connection.as_mut() else {
        return VylResult::InternalError;
    };
    let output = if output_capacity == 0 {
        &mut []
    } else {
        unsafe { slice::from_raw_parts_mut(output, output_capacity) }
    };
    let result = match kind {
        ControlKind::SettingsAck => connection.send_settings_ack(output),
        ControlKind::PingAck => connection.send_ping_ack(&opaque, output),
    };
    report_write(result, out_written, out_required)
}

unsafe fn send_window_update(
    handle: *mut VylH2Server,
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
    if increment == 0 || increment > veylor_http2::MAX_WINDOW_INCREMENT {
        return VylResult::InvalidArgument;
    }
    if stream_id > veylor_http2::MAX_STREAM_ID {
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
    report_write(
        connection.send_window_update(stream_id, increment, output),
        out_written,
        out_required,
    )
}
