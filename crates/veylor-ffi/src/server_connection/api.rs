use std::boxed::Box;
use std::ptr;

use crate::abi::{VylHeader, VylResult, VylSpan, catch_ffi, slice_length_valid};
use veylor_http1::{Limits, Parser, RequestHead, TargetForm};

pub const VYL_HTTP1_HAS_CONTENT_LENGTH: u32 = 1;
pub const VYL_HTTP1_CHUNKED: u32 = 2;
pub const VYL_HTTP1_EXPECT_CONTINUE: u32 = 4;
pub const VYL_HTTP1_CONNECTION_CLOSE: u32 = 8;
pub const VYL_HTTP1_CONNECTION_KEEP_ALIVE: u32 = 16;
pub const VYL_HTTP1_CONNECTION_UPGRADE: u32 = 32;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VylRequestHead {
    pub method: VylSpan,
    pub target: VylSpan,
    pub authority: VylSpan,
    pub version: VylSpan,
    pub target_form: VylTargetForm,
    pub header_count: usize,
    pub length: usize,
    pub content_length: u64,
    pub flags: u32,
}

#[repr(u32)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum VylTargetForm {
    #[default]
    Origin = 0,
    Absolute = 1,
    Authority = 2,
    Asterisk = 3,
}

pub struct VylServerConnection {
    parser: Parser,
}

impl VylServerConnection {
    fn new() -> Self {
        Self {
            parser: Parser::new(),
        }
    }
}





#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_server_connection_new(
    out_connection: *mut *mut VylServerConnection,
) -> VylResult {
    catch_ffi(|| unsafe { create_connection(out_connection) })
}

unsafe fn create_connection(out_connection: *mut *mut VylServerConnection) -> VylResult {
    if out_connection.is_null() {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_connection.write(ptr::null_mut());
    }

    let connection = Box::new(VylServerConnection::new());
    unsafe {
        out_connection.write(Box::into_raw(connection));
    }
    VylResult::Ok
}









#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_server_connection_parse_head(
    connection: *mut VylServerConnection,
    input: *const u8,
    input_length: usize,
    headers: *mut VylHeader,
    header_capacity: usize,
    out_head: *mut VylRequestHead,
    out_headers_required: *mut usize,
) -> VylResult {
    catch_ffi(|| unsafe {
        parse_request_head(
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

unsafe fn parse_request_head(
    connection: *mut VylServerConnection,
    input: *const u8,
    input_length: usize,
    headers: *mut VylHeader,
    header_capacity: usize,
    out_head: *mut VylRequestHead,
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
        out_head.write(VylRequestHead::default());
        out_headers_required.write(0);
    }

    let input = if input_length == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(input, input_length) }
    };
    let headers = if header_capacity == 0 {
        &mut []
    } else {
        unsafe { std::slice::from_raw_parts_mut(headers, header_capacity) }
    };
    let connection = unsafe { &mut *connection };

    let parsed = match connection.parser.parse(input, headers, Limits::default()) {
        Ok(Some(head)) => head,
        Ok(None) => return VylResult::NeedMore,
        Err(veylor_http1::ParseError::HeaderCapacity) => {
            let required = connection.parser.required_header_capacity();
            unsafe {
                out_headers_required.write(required);
            }
            return VylResult::BufferTooSmall;
        }
        Err(_) => return VylResult::ProtocolError,
    };

    unsafe {
        out_headers_required.write(parsed.header_count);
    }
    if header_capacity < parsed.header_count {
        return VylResult::BufferTooSmall;
    }

    unsafe {
        out_head.write(to_ffi_head(parsed));
    }
    VylResult::Ok
}





#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_server_connection_reset(
    connection: *mut VylServerConnection,
) -> VylResult {
    catch_ffi(|| unsafe { reset_connection(connection) })
}

unsafe fn reset_connection(connection: *mut VylServerConnection) -> VylResult {
    if connection.is_null() {
        return VylResult::InvalidArgument;
    }

    let connection = unsafe { &mut *connection };
    connection.parser.reset();
    VylResult::Ok
}






#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_server_connection_destroy(
    connection: *mut VylServerConnection,
) -> VylResult {
    catch_ffi(|| unsafe { destroy_connection(connection) })
}

unsafe fn destroy_connection(connection: *mut VylServerConnection) -> VylResult {
    if !connection.is_null() {
        unsafe {
            drop(Box::from_raw(connection));
        }
    }
    VylResult::Ok
}

fn to_ffi_head(head: RequestHead) -> VylRequestHead {
    let mut flags = 0;
    if head.content_length.is_some() {
        flags |= VYL_HTTP1_HAS_CONTENT_LENGTH;
    }
    if head.is_chunked {
        flags |= VYL_HTTP1_CHUNKED;
    }
    if head.expect_continue {
        flags |= VYL_HTTP1_EXPECT_CONTINUE;
    }
    if head.connection.close {
        flags |= VYL_HTTP1_CONNECTION_CLOSE;
    }
    if head.connection.keep_alive {
        flags |= VYL_HTTP1_CONNECTION_KEEP_ALIVE;
    }
    if head.connection.upgrade {
        flags |= VYL_HTTP1_CONNECTION_UPGRADE;
    }

    VylRequestHead {
        method: head.method,
        target: head.target,
        authority: head.authority,
        version: head.version,
        target_form: match head.target_form {
            TargetForm::Origin => VylTargetForm::Origin,
            TargetForm::Absolute => VylTargetForm::Absolute,
            TargetForm::Authority => VylTargetForm::Authority,
            TargetForm::Asterisk => VylTargetForm::Asterisk,
        },
        header_count: head.header_count,
        length: head.length,
        content_length: head.content_length.unwrap_or_default(),
        flags,
    }
}

const _: () = assert!(std::mem::size_of::<VylRequestHead>() > 0);
