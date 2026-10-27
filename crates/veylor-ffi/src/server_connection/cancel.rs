use crate::abi::{VylResult, catch_ffi};

use super::api::VylServerConnection;






#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_server_connection_cancel(
    connection: *mut VylServerConnection,
) -> VylResult {
    catch_ffi(|| unsafe { cancel_connection(connection) })
}

unsafe fn cancel_connection(connection: *mut VylServerConnection) -> VylResult {
    if connection.is_null() {
        return VylResult::InvalidArgument;
    }

    let connection = unsafe { &mut *connection };
    if connection.inner.is_cancelled() {
        return VylResult::Ok;
    }

    connection.inner.cancel();
    connection.headers_base = core::ptr::null();
    VylResult::Ok
}
