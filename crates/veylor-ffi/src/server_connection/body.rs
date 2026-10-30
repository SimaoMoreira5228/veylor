use std::slice;

use veylor_server_conn::{Admission, BodyKind, ServerError};

use crate::abi::{VylHeader, VylResult, VylSpan, catch_ffi, slice_length_valid};

use super::api::VylServerConnection;
use super::map_error;

pub const VYL_HTTP1_ADMISSION_READY: u32 = 0;
pub const VYL_HTTP1_ADMISSION_SEND_CONTINUE: u32 = 1;
pub const VYL_HTTP1_ADMISSION_REJECTED: u32 = 2;

pub const VYL_HTTP1_BODY_NEED_MORE: u32 = 0;
pub const VYL_HTTP1_BODY_DATA: u32 = 1;
pub const VYL_HTTP1_BODY_TRAILERS: u32 = 2;
pub const VYL_HTTP1_BODY_COMPLETE: u32 = 3;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VylServerBodyEvent {
    pub kind: u32,
    pub consumed: usize,
    pub data: VylSpan,
    pub trailer_count: usize,
}

struct AdmissionArguments {
    input: *const u8,
    input_length: usize,
    request_headers: *const VylHeader,
    trailer_name_data: *const u8,
    trailer_name_data_length: usize,
    permitted_trailers: *const VylSpan,
    permitted_trailer_count: usize,
    allowed: u32,
    out_admission: *mut u32,
}

struct BodyReadArguments {
    input: *const u8,
    input_length: usize,
    eof: u32,
    trailers: *mut VylHeader,
    trailer_capacity: usize,
    out_event: *mut VylServerBodyEvent,
    out_trailers_required: *mut usize,
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_server_connection_admit_body(
    connection: *mut VylServerConnection,
    input: *const u8,
    input_length: usize,
    request_headers: *const VylHeader,
    trailer_name_data: *const u8,
    trailer_name_data_length: usize,
    permitted_trailers: *const VylSpan,
    permitted_trailer_count: usize,
    allowed: u32,
    out_admission: *mut u32,
) -> VylResult {
    let arguments = AdmissionArguments {
        input,
        input_length,
        request_headers,
        trailer_name_data,
        trailer_name_data_length,
        permitted_trailers,
        permitted_trailer_count,
        allowed,
        out_admission,
    };
    catch_ffi(|| unsafe { admit_body(connection, arguments) })
}

unsafe fn admit_body(
    connection: *mut VylServerConnection,
    arguments: AdmissionArguments,
) -> VylResult {
    let AdmissionArguments {
        input,
        input_length,
        request_headers,
        trailer_name_data,
        trailer_name_data_length,
        permitted_trailers,
        permitted_trailer_count,
        allowed,
        out_admission,
    } = arguments;
    if connection.is_null()
        || (input.is_null() && input_length != 0)
        || (trailer_name_data.is_null() && trailer_name_data_length != 0)
        || (permitted_trailers.is_null() && permitted_trailer_count != 0)
        || allowed > 1
        || out_admission.is_null()
        || !slice_length_valid::<u8>(input_length)
        || !slice_length_valid::<u8>(trailer_name_data_length)
        || !slice_length_valid::<VylSpan>(permitted_trailer_count)
    {
        return VylResult::InvalidArgument;
    }

    unsafe {
        out_admission.write(VYL_HTTP1_ADMISSION_READY);
    }

    let connection = unsafe { &mut *connection };
    if connection.inner.is_cancelled() {
        return VylResult::Cancelled;
    }
    if request_headers != connection.headers_base {
        return VylResult::BadState;
    }
    let Some(request) = connection.inner.request_head() else {
        return VylResult::BadState;
    };
    let input = if input_length == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(input, input_length) }
    };
    // SECURITY: the trusted slices below alias the stable parse-time buffers (the header base
    // equality is checked above) and are erased to 'static; they stay valid until reset or
    // destruction under the documented caller contract.
    let request_headers = if request.header_count == 0 {
        &[]
    } else {
        unsafe {
            std::mem::transmute::<&[VylHeader], &'static [VylHeader]>(slice::from_raw_parts(
                connection.headers_base,
                request.header_count,
            ))
        }
    };
    let trailer_name_data = if trailer_name_data_length == 0 {
        &[]
    } else {
        unsafe {
            std::mem::transmute::<&[u8], &'static [u8]>(slice::from_raw_parts(
                trailer_name_data,
                trailer_name_data_length,
            ))
        }
    };
    let permitted_trailers = if permitted_trailer_count == 0 {
        &[]
    } else {
        unsafe {
            std::mem::transmute::<&[VylSpan], &'static [VylSpan]>(slice::from_raw_parts(
                permitted_trailers,
                permitted_trailer_count,
            ))
        }
    };
    match connection.inner.admit_body(
        input,
        request_headers,
        trailer_name_data,
        permitted_trailers,
        allowed != 0,
    ) {
        Ok(Admission::Ready) => {
            unsafe {
                out_admission.write(VYL_HTTP1_ADMISSION_READY);
            }
            VylResult::Ok
        }
        Ok(Admission::SendContinue) => {
            unsafe {
                out_admission.write(VYL_HTTP1_ADMISSION_SEND_CONTINUE);
            }
            VylResult::Ok
        }
        Ok(Admission::Rejected) => {
            unsafe {
                out_admission.write(VYL_HTTP1_ADMISSION_REJECTED);
            }
            VylResult::Ok
        }
        Err(error) => map_error(error),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_server_connection_continue_sent(
    connection: *mut VylServerConnection,
) -> VylResult {
    catch_ffi(|| unsafe { mark_continue_sent(connection) })
}

unsafe fn mark_continue_sent(connection: *mut VylServerConnection) -> VylResult {
    if connection.is_null() {
        return VylResult::InvalidArgument;
    }

    let connection = unsafe { &mut *connection };
    match connection.inner.mark_continue_sent() {
        Ok(()) => VylResult::Ok,
        Err(error) => map_error(error),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_server_connection_read_body(
    connection: *mut VylServerConnection,
    input: *const u8,
    input_length: usize,
    eof: u32,
    trailers: *mut VylHeader,
    trailer_capacity: usize,
    out_event: *mut VylServerBodyEvent,
    out_trailers_required: *mut usize,
) -> VylResult {
    let arguments = BodyReadArguments {
        input,
        input_length,
        eof,
        trailers,
        trailer_capacity,
        out_event,
        out_trailers_required,
    };
    catch_ffi(|| unsafe { read_body(connection, arguments) })
}

unsafe fn read_body(
    connection: *mut VylServerConnection,
    arguments: BodyReadArguments,
) -> VylResult {
    let BodyReadArguments {
        input,
        input_length,
        eof,
        trailers,
        trailer_capacity,
        out_event,
        out_trailers_required,
    } = arguments;
    if connection.is_null()
        || (input.is_null() && input_length != 0)
        || (trailers.is_null() && trailer_capacity != 0)
        || eof > 1
        || !slice_length_valid::<u8>(input_length)
        || !slice_length_valid::<VylHeader>(trailer_capacity)
        || out_event.is_null()
        || out_trailers_required.is_null()
    {
        return VylResult::InvalidArgument;
    }

    unsafe {
        out_event.write(VylServerBodyEvent::default());
        out_trailers_required.write(0);
    }

    let connection = unsafe { &mut *connection };
    let input = if input_length == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(input, input_length) }
    };
    let trailers = if trailer_capacity == 0 {
        &mut []
    } else {
        unsafe { slice::from_raw_parts_mut(trailers, trailer_capacity) }
    };
    match connection.inner.read_body(input, eof != 0, trailers) {
        Ok(event) => {
            let mut out = VylServerBodyEvent {
                consumed: event.consumed,
                ..VylServerBodyEvent::default()
            };
            match event.kind {
                BodyKind::NeedMore => out.kind = VYL_HTTP1_BODY_NEED_MORE,
                BodyKind::Data => {
                    out.kind = VYL_HTTP1_BODY_DATA;
                    out.data = event.data;
                }
                BodyKind::Trailers => {
                    out.kind = VYL_HTTP1_BODY_TRAILERS;
                    out.trailer_count = event.trailer_count;
                }
                BodyKind::Complete => out.kind = VYL_HTTP1_BODY_COMPLETE,
            }
            unsafe {
                out_event.write(out);
            }
            VylResult::Ok
        }
        Err(ServerError::HeaderCapacity {
            trailers_required, ..
        }) => {
            unsafe {
                out_trailers_required.write(trailers_required);
            }
            VylResult::BufferTooSmall
        }
        Err(error) => map_error(error),
    }
}
