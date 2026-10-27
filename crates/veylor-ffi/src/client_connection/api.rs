use std::boxed::Box;
use std::ptr;
use std::slice;

use veylor_client::{ClientError, Limits};

use super::{VylClientConnection, VylRequestMethod};
use crate::abi::{VylResult, catch_ffi, slice_length_valid};

pub const VYL_HTTP1_RESPONSE_REUSABLE: u32 = 1;
pub const VYL_HTTP1_RESPONSE_CONNECTION_CLOSE: u32 = 2;
pub const VYL_HTTP1_RESPONSE_CONNECTION_KEEP_ALIVE: u32 = 4;
pub const VYL_HTTP1_RESPONSE_CONNECTION_UPGRADE: u32 = 8;
pub const VYL_HTTP1_MAX_REQUEST_FIELDS: usize = Limits::DEFAULT_HEADER_COUNT;







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
    // NOTE: the borrow is honest on the Rust side (`ClientConnection<'a>`); the FFI handle
    // erases it to `'static`, which is sound only under this function's documented caller
    // contract that the upgrade bytes outlive the handle.
    let requested_upgrade =
        unsafe { std::mem::transmute::<&[u8], &'static [u8]>(requested_upgrade) };
    let connection = Box::new(VylClientConnection {
        inner: veylor_client::ClientConnection::new(request_method.into(), requested_upgrade),
    });

    unsafe {
        out_connection.write(Box::into_raw(connection));
    }
    VylResult::Ok
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

    let connection = unsafe { &mut *connection };
    match connection.inner.reset() {
        Ok(()) => VylResult::Ok,
        Err(ClientError::Cancelled) => VylResult::Cancelled,
        Err(_) => VylResult::BadState,
    }
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

impl From<VylRequestMethod> for veylor_client::RequestMethod {
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
