use std::slice;

use veylor_http1::{
    BodyEvent, ClientResponseEvent, Limits, ResponseBody, ResponseHead, ResponseKind,
};

use crate::abi::{VylHeader, VylResult, catch_ffi, slice_length_valid};

use super::{
    VYL_HTTP1_RESPONSE_CONNECTION_CLOSE, VYL_HTTP1_RESPONSE_CONNECTION_KEEP_ALIVE,
    VYL_HTTP1_RESPONSE_CONNECTION_UPGRADE, VYL_HTTP1_RESPONSE_REUSABLE, VylClientConnection,
    VylClientReadEvent, VylClientReadKind, VylResponseBodyKind, VylResponseHead, VylResponseKind,
};

struct ClientResponseReadArguments {
    input: *const u8,
    input_length: usize,
    eof: u32,
    headers: *mut VylHeader,
    header_capacity: usize,
    trailers: *mut VylHeader,
    trailer_capacity: usize,
    out_event: *mut VylClientReadEvent,
    out_headers_required: *mut usize,
    out_trailers_required: *mut usize,
}











#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_client_connection_read_response(
    connection: *mut VylClientConnection,
    input: *const u8,
    input_length: usize,
    eof: u32,
    headers: *mut VylHeader,
    header_capacity: usize,
    trailers: *mut VylHeader,
    trailer_capacity: usize,
    out_event: *mut VylClientReadEvent,
    out_headers_required: *mut usize,
    out_trailers_required: *mut usize,
) -> VylResult {
    let arguments = ClientResponseReadArguments {
        input,
        input_length,
        eof,
        headers,
        header_capacity,
        trailers,
        trailer_capacity,
        out_event,
        out_headers_required,
        out_trailers_required,
    };
    catch_ffi(|| unsafe { read_response(connection, arguments) })
}

unsafe fn read_response(
    connection: *mut VylClientConnection,
    arguments: ClientResponseReadArguments,
) -> VylResult {
    let ClientResponseReadArguments {
        input,
        input_length,
        eof,
        headers,
        header_capacity,
        trailers,
        trailer_capacity,
        out_event,
        out_headers_required,
        out_trailers_required,
    } = arguments;
    if connection.is_null()
        || (input.is_null() && input_length != 0)
        || (headers.is_null() && header_capacity != 0)
        || (trailers.is_null() && trailer_capacity != 0)
        || eof > 1
        || !slice_length_valid::<u8>(input_length)
        || !slice_length_valid::<VylHeader>(header_capacity)
        || !slice_length_valid::<VylHeader>(trailer_capacity)
        || out_event.is_null()
        || out_headers_required.is_null()
        || out_trailers_required.is_null()
    {
        return VylResult::InvalidArgument;
    }

    unsafe {
        out_event.write(VylClientReadEvent::default());
        out_headers_required.write(0);
        out_trailers_required.write(0);
    }

    let input = if input_length == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(input, input_length) }
    };
    let headers = if header_capacity == 0 {
        &mut []
    } else {
        unsafe { slice::from_raw_parts_mut(headers, header_capacity) }
    };
    let trailers = if trailer_capacity == 0 {
        &mut []
    } else {
        unsafe { slice::from_raw_parts_mut(trailers, trailer_capacity) }
    };
    let connection = unsafe { &mut *connection };
    connection.response_started |= input_length != 0 || eof != 0;

    let progress =
        match connection
            .response
            .next(input, eof != 0, headers, trailers, Limits::default())
        {
            Ok(progress) => progress,
            Err(veylor_http1::ParseError::Cancelled) => return VylResult::Cancelled,
            Err(veylor_http1::ParseError::HeaderCapacity) => {
                unsafe {
                    out_headers_required.write(connection.response.required_header_capacity());
                    out_trailers_required.write(connection.response.required_trailer_capacity());
                }
                return VylResult::BufferTooSmall;
            }
            Err(_) => return VylResult::ProtocolError,
        };

    let mut event = VylClientReadEvent {
        consumed: progress.consumed,
        can_reuse: u32::from(connection.response.can_reuse()),
        ..VylClientReadEvent::default()
    };
    let has_event = progress.event.is_some();
    match progress.event {
        Some(ClientResponseEvent::Informational(head)) => {
            event.kind = VylClientReadKind::Informational;
            event.response = to_ffi_head(head);
        }
        Some(ClientResponseEvent::Head(head)) => {
            event.kind = if head.kind == ResponseKind::Upgrade {
                VylClientReadKind::Upgrade
            } else {
                VylClientReadKind::Head
            };
            event.response = to_ffi_head(head);
        }
        Some(ClientResponseEvent::Body(BodyEvent::Data(span))) => {
            event.kind = VylClientReadKind::BodyData;
            event.body_data = span;
        }
        Some(ClientResponseEvent::Body(BodyEvent::Trailers { count })) => {
            event.kind = VylClientReadKind::Trailers;
            event.trailer_count = count;
        }
        Some(ClientResponseEvent::Body(BodyEvent::Complete)) => {
            event.kind = VylClientReadKind::Complete;
        }
        None if connection.response.is_upgraded() => {
            event.kind = VylClientReadKind::Upgrade;
        }
        None => {}
    }

    unsafe {
        out_event.write(event);
    }
    if has_event || connection.response.is_upgraded() {
        VylResult::Ok
    } else {
        VylResult::NeedMore
    }
}

fn to_ffi_head(head: ResponseHead) -> VylResponseHead {
    let (body_kind, body_length) = match head.body {
        ResponseBody::None => (VylResponseBodyKind::None, 0),
        ResponseBody::Fixed(length) => (VylResponseBodyKind::Fixed, length),
        ResponseBody::Chunked => (VylResponseBodyKind::Chunked, 0),
        ResponseBody::CloseDelimited => (VylResponseBodyKind::CloseDelimited, 0),
    };
    let kind = match head.kind {
        ResponseKind::Informational => VylResponseKind::Informational,
        ResponseKind::Final => VylResponseKind::Final,
        ResponseKind::Upgrade => VylResponseKind::Upgrade,
    };
    let mut flags = 0;
    if head.reusable {
        flags |= VYL_HTTP1_RESPONSE_REUSABLE;
    }
    if head.connection.close {
        flags |= VYL_HTTP1_RESPONSE_CONNECTION_CLOSE;
    }
    if head.connection.keep_alive {
        flags |= VYL_HTTP1_RESPONSE_CONNECTION_KEEP_ALIVE;
    }
    if head.connection.upgrade {
        flags |= VYL_HTTP1_RESPONSE_CONNECTION_UPGRADE;
    }

    VylResponseHead {
        version: head.version,
        reason: head.reason,
        status: head.status,
        body_kind,
        kind,
        header_count: head.header_count,
        length: head.length,
        end_offset: head.end_offset,
        body_length,
        flags,
    }
}
