use std::panic::{AssertUnwindSafe, catch_unwind};

use veylor_http1::{Header, Span};

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
}

pub fn catch_ffi(call: impl FnOnce() -> VylResult) -> VylResult {
    catch_unwind(AssertUnwindSafe(call)).unwrap_or(VylResult::InternalError)
}

pub(crate) fn slice_length_valid<T>(length: usize) -> bool {
    length <= isize::MAX as usize / std::mem::size_of::<T>()
}

const _: () = assert!(std::mem::size_of::<VylSpan>() == std::mem::size_of::<usize>() * 2);
const _: () = assert!(std::mem::size_of::<VylHeader>() == std::mem::size_of::<VylSpan>() * 2);
