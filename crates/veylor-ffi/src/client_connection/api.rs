use std::boxed::Box;
use std::ptr;
use std::slice;

use veylor_http1::{
    Limits, ResponseBody, ResponseHead, ResponseKind, ResponseParser,
    ResponseRequestMethod as RequestMethod,
};

use crate::abi::{VylHeader, VylResult, VylSpan, catch_ffi, slice_length_valid};

pub const VYL_HTTP1_RESPONSE_REUSABLE: u32 = 1;
pub const VYL_HTTP1_RESPONSE_CONNECTION_CLOSE: u32 = 2;
pub const VYL_HTTP1_RESPONSE_CONNECTION_KEEP_ALIVE: u32 = 4;
pub const VYL_HTTP1_RESPONSE_CONNECTION_UPGRADE: u32 = 8;

#[repr(u32)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum VylRequestMethod {
    Head = 0,
    Connect = 1,
    #[default]
    Other = 2,
}

#[repr(u32)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum VylResponseBodyKind {
    #[default]
    None = 0,
    Fixed = 1,
    Chunked = 2,
    CloseDelimited = 3,
}

#[repr(u32)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum VylResponseKind {
    Informational = 0,
    #[default]
    Final = 1,
    Upgrade = 2,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VylResponseHead {
    pub version: VylSpan,
    pub reason: VylSpan,
    pub status: u16,
    pub body_kind: VylResponseBodyKind,
    pub kind: VylResponseKind,
    pub header_count: usize,
    pub length: usize,
    pub end_offset: usize,
    pub body_length: u64,
    pub flags: u32,
}

pub struct VylClientConnection {
    parser: ResponseParser<'static>,
}







#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_client_connection_new(
    request_method: u32,
    requested_upgrade: *const u8,
    requested_upgrade_length: usize,
    out_connection: *mut *mut VylClientConnection,
) -> VylResult {
    catch_ffi(|| unsafe {
        create_connection(
            request_method,
            requested_upgrade,
            requested_upgrade_length,
            out_connection,
        )
    })
}

unsafe fn create_connection(
    request_method: u32,
    requested_upgrade: *const u8,
    requested_upgrade_length: usize,
    out_connection: *mut *mut VylClientConnection,
) -> VylResult {
    if out_connection.is_null() {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_connection.write(ptr::null_mut());
    }
    if (requested_upgrade.is_null() && requested_upgrade_length != 0)
        || !slice_length_valid::<u8>(requested_upgrade_length)
    {
        return VylResult::InvalidArgument;
    }

    let Some(request_method) = VylRequestMethod::try_from(request_method).ok() else {
        return VylResult::InvalidArgument;
    };

    let requested_upgrade = if requested_upgrade_length == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(requested_upgrade, requested_upgrade_length) }
    };
    let requested_upgrade =
        unsafe { std::mem::transmute::<&[u8], &'static [u8]>(requested_upgrade) };
    let connection = Box::new(VylClientConnection {
        parser: ResponseParser::new(request_method.into(), requested_upgrade),
    });

    unsafe {
        out_connection.write(Box::into_raw(connection));
    }
    VylResult::Ok
}









#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_client_connection_parse_head(
    connection: *mut VylClientConnection,
    input: *const u8,
    input_length: usize,
    headers: *mut VylHeader,
    header_capacity: usize,
    out_head: *mut VylResponseHead,
    out_headers_required: *mut usize,
) -> VylResult {
    catch_ffi(|| unsafe {
        parse_response_head(
            connection,
            input,
            input_length,
            headers,
            header_capacity,
            out_head,
            out_headers_required,
        )
    })
}

unsafe fn parse_response_head(
    connection: *mut VylClientConnection,
    input: *const u8,
    input_length: usize,
    headers: *mut VylHeader,
    header_capacity: usize,
    out_head: *mut VylResponseHead,
    out_headers_required: *mut usize,
) -> VylResult {
    if connection.is_null()
        || (input.is_null() && input_length != 0)
        || (headers.is_null() && header_capacity != 0)
        || !slice_length_valid::<u8>(input_length)
        || !slice_length_valid::<VylHeader>(header_capacity)
        || out_head.is_null()
        || out_headers_required.is_null()
    {
        return VylResult::InvalidArgument;
    }

    unsafe {
        out_head.write(VylResponseHead::default());
        out_headers_required.write(0);
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
    let connection = unsafe { &mut *connection };

    let parsed = match connection.parser.parse(input, headers, Limits::default()) {
        Ok(Some(head)) => head,
        Ok(None) => return VylResult::NeedMore,
        Err(veylor_http1::ParseError::HeaderCapacity) => {
            unsafe {
                out_headers_required.write(connection.parser.required_header_capacity());
            }
            return VylResult::BufferTooSmall;
        }
        Err(_) => return VylResult::ProtocolError,
    };

    unsafe {
        out_headers_required.write(parsed.header_count);
        out_head.write(to_ffi_head(parsed));
    }
    VylResult::Ok
}







#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_client_connection_advance(
    connection: *mut VylClientConnection,
    input: *const u8,
    input_length: usize,
    next_offset: usize,
) -> VylResult {
    catch_ffi(|| unsafe { advance_connection(connection, input, input_length, next_offset) })
}

unsafe fn advance_connection(
    connection: *mut VylClientConnection,
    input: *const u8,
    input_length: usize,
    next_offset: usize,
) -> VylResult {
    if connection.is_null()
        || (input.is_null() && input_length != 0)
        || !slice_length_valid::<u8>(input_length)
    {
        return VylResult::InvalidArgument;
    }

    let input = if input_length == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(input, input_length) }
    };
    let connection = unsafe { &mut *connection };
    match connection.parser.advance(input, next_offset) {
        Ok(()) => VylResult::Ok,
        Err(_) => VylResult::ProtocolError,
    }
}





#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_client_connection_reset(
    connection: *mut VylClientConnection,
) -> VylResult {
    catch_ffi(|| unsafe { reset_connection(connection) })
}

unsafe fn reset_connection(connection: *mut VylClientConnection) -> VylResult {
    if connection.is_null() {
        return VylResult::InvalidArgument;
    }

    unsafe { &mut *connection }.parser.reset();
    VylResult::Ok
}






#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_client_connection_destroy(
    connection: *mut VylClientConnection,
) -> VylResult {
    catch_ffi(|| unsafe { destroy_connection(connection) })
}

unsafe fn destroy_connection(connection: *mut VylClientConnection) -> VylResult {
    if !connection.is_null() {
        unsafe {
            drop(Box::from_raw(connection));
        }
    }
    VylResult::Ok
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

impl From<VylRequestMethod> for RequestMethod {
    fn from(method: VylRequestMethod) -> Self {
        match method {
            VylRequestMethod::Head => Self::Head,
            VylRequestMethod::Connect => Self::Connect,
            VylRequestMethod::Other => Self::Other,
        }
    }
}

impl TryFrom<u32> for VylRequestMethod {
    type Error = ();

    fn try_from(method: u32) -> Result<Self, Self::Error> {
        match method {
            0 => Ok(Self::Head),
            1 => Ok(Self::Connect),
            2 => Ok(Self::Other),
            _ => Err(()),
        }
    }
}
