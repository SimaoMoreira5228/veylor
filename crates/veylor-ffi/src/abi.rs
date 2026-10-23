use std::panic::{AssertUnwindSafe, catch_unwind};

use veylor_http1::{Header, Span};

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct VylBytes {
    pub data: *const u8,
    pub length: usize,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct VylField {
    pub name: VylBytes,
    pub value: VylBytes,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct VylH2FieldToEncode {
    pub name: VylBytes,
    pub value: VylBytes,
    pub representation: u32,
    pub huffman_name: u32,
    pub huffman_value: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VylH2Setting {
    pub id: u32,
    pub value: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VylH2Field {
    pub name_offset: usize,
    pub name_length: usize,
    pub value_offset: usize,
    pub value_length: usize,
}

#[repr(u32)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum VylH2ActionKind {
    #[default]
    Headers = 0,
    Data = 1,
    StreamReset = 2,
    GoAway = 3,
    SettingsAckRequired = 4,
    PingAck = 5,
    SendReset = 6,
    ConnectionError = 7,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VylH2Action {
    pub kind: VylH2ActionKind,
    pub stream_id: u32,
    pub flags: u32,
    pub error_code: u32,
    pub data_offset: usize,
    pub data_length: usize,
    pub field_start: usize,
    pub field_count: usize,
    pub ping: [u8; 8],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VylH2ResponseClass {
    pub kind: u32,
    pub status: u16,
    pub phase: u16,
    pub content_length: u64,
    pub has_content_length: u32,
    pub end_stream: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VylH2ValidatedRequest {
    pub method_field: usize,
    pub scheme_field: usize,
    pub authority_field: usize,
    pub path_field: usize,
    pub content_length: u64,
    pub has_content_length: u32,
}

pub const VYL_H2_END_STREAM: u32 = 1;
pub const VYL_H2_NO_FIELD: usize = usize::MAX;
pub const VYL_H2_MAX_ACTIONS: usize = 16;
pub const VYL_H2_MAX_FIELDS: usize = 64;
pub const VYL_H2_MAX_SETTINGS: usize = 8;

pub type VylSpan = Span;
pub type VylHeader = Header;

#[repr(i32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VylResult {
    Ok = 0,
    NeedMore = 1,
    InvalidArgument = 2,
    ProtocolError = 3,
    BufferTooSmall = 4,
    InternalError = 5,
    Capacity = 6,
    WouldBlock = 7,
    BodyTooLong = 8,
    BadState = 9,
    WriteOverrun = 10,
    Cancelled = 11,
    StaleToken = 12,
    Duplicate = 13,
    Gone = 14,
    Busy = 15,
}

pub fn catch_ffi(call: impl FnOnce() -> VylResult) -> VylResult {
    catch_unwind(AssertUnwindSafe(call)).unwrap_or(VylResult::InternalError)
}

pub(crate) fn slice_length_valid<T>(length: usize) -> bool {
    length <= isize::MAX as usize / std::mem::size_of::<T>()
}

const _: () = assert!(std::mem::size_of::<VylSpan>() == std::mem::size_of::<usize>() * 2);
const _: () = assert!(std::mem::size_of::<VylHeader>() == std::mem::size_of::<VylSpan>() * 2);
const _: () = assert!(std::mem::size_of::<VylH2Action>() > 0);
const _: () = assert!(std::mem::size_of::<VylH2ResponseClass>() > 0);
const _: () = assert!(std::mem::size_of::<VylH2ValidatedRequest>() > 0);
