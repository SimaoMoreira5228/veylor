use std::slice;

use veylor_http1::{BodyEvent, BodyLimits, Limits, RequestBodyAdmission, RequestBodyDecoder};

use crate::abi::{VylHeader, VylResult, VylSpan, catch_ffi, slice_length_valid};

use super::api::VylServerConnection;

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
    let Some(request) = connection.request else {
        return VylResult::BadState;
    };
    if connection.body.is_some()
        || connection.response_started
        || input != connection.input_base
        || input_length < request.length
        || request_headers != connection.headers_base
    {
        return VylResult::BadState;
    }

    let request_input = if connection.input_length == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(connection.input_base, connection.input_length) }
    };
    let request_headers = if request.header_count == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(connection.headers_base, request.header_count) }
    };
    let trailer_name_data = if trailer_name_data_length == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(trailer_name_data, trailer_name_data_length) }
    };
    let permitted_trailers = if permitted_trailer_count == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(permitted_trailers, permitted_trailer_count) }
    };

    let decoder = match RequestBodyDecoder::new_with_trailer_spans(
        request,
        request_input,
        request_headers,
        trailer_name_data,
        permitted_trailers,
        BodyLimits::default(),
    ) {
        Ok(decoder) => decoder,
        Err(error) => return map_parse_error(error),
    };
    // SECURITY: C callers retain these borrowed buffers until reset or destruction.
    let mut decoder: RequestBodyDecoder<'static> = unsafe { std::mem::transmute(decoder) };
    let admission = match decoder.admit_body(allowed != 0) {
        Ok(admission) => admission,
        Err(error) => return map_parse_error(error),
    };
    let admission = match admission {
        RequestBodyAdmission::ReadyToRead => VYL_HTTP1_ADMISSION_READY,
        RequestBodyAdmission::SendContinue => VYL_HTTP1_ADMISSION_SEND_CONTINUE,
        RequestBodyAdmission::Rejected => VYL_HTTP1_ADMISSION_REJECTED,
    };
    connection.body = Some(decoder);
    unsafe {
        out_admission.write(admission);
    }
    VylResult::Ok
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
    if connection.response_started {
        return VylResult::BadState;
    }
    if !connection.response_writer.can_flush_continue() {
        return VylResult::BadState;
    }
    let Some(body) = connection.body.as_mut() else {
        return VylResult::BadState;
    };
    match body.continue_sent() {
        Ok(()) => {
            if connection.response_writer.continue_flushed() {
                VylResult::Ok
            } else {
                VylResult::BadState
            }
        }
        Err(error) => map_parse_error(error),
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
    let connection = unsafe { &mut *connection };
    if connection.response_started {
        return VylResult::BadState;
    }
    let Some(body) = connection.body.as_mut() else {
        return VylResult::BadState;
    };
    let progress = match body.next(input, eof != 0, trailers, Limits::default()) {
        Ok(progress) => progress,
        Err(veylor_http1::ParseError::HeaderCapacity) => {
            unsafe {
                out_trailers_required.write(body.required_trailer_capacity());
            }
            return VylResult::BufferTooSmall;
        }
        Err(error) => return map_parse_error(error),
    };

    let mut event = VylServerBodyEvent {
        consumed: progress.consumed,
        ..VylServerBodyEvent::default()
    };
    match progress.event {
        Some(BodyEvent::Data(span)) => {
            event.kind = VYL_HTTP1_BODY_DATA;
            event.data = span;
        }
        Some(BodyEvent::Trailers { count }) => {
            event.kind = VYL_HTTP1_BODY_TRAILERS;
            event.trailer_count = count;
        }
        Some(BodyEvent::Complete) => event.kind = VYL_HTTP1_BODY_COMPLETE,
        None => event.kind = VYL_HTTP1_BODY_NEED_MORE,
    }
    unsafe {
        out_event.write(event);
    }
    VylResult::Ok
}

fn map_parse_error(error: veylor_http1::ParseError) -> VylResult {
    match error {
        veylor_http1::ParseError::BodyTooLarge => VylResult::BodyTooLong,
        veylor_http1::ParseError::AdmissionPending
        | veylor_http1::ParseError::BodyRejected
        | veylor_http1::ParseError::Cancelled => VylResult::BadState,
        veylor_http1::ParseError::HeaderCapacity => VylResult::BufferTooSmall,
        _ => VylResult::ProtocolError,
    }
}
