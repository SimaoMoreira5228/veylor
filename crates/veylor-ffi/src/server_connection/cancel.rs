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
    if connection.cancelled {
        return VylResult::Ok;
    }

    connection.cancelled = true;
    connection.body = None;
    connection.parser.reset();
    connection.request = None;
    connection.input_base = core::ptr::null();
    connection.input_length = 0;
    connection.headers_base = core::ptr::null();
    connection.response_started = true;
    connection.response_writer.cancel();
    VylResult::Ok
}
