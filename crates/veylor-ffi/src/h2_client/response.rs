use super::{VylH2Client, split_error};
use crate::abi::{VylH2ResponseClass, VylResult, catch_ffi};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h2_client_classify_headers(
    handle: *mut VylH2Client,
    stream_id: u32,
    header_start: usize,
    header_count: usize,
    end_stream: u32,
    out_class: *mut VylH2ResponseClass,
) -> VylResult {
    catch_ffi(|| unsafe {
        classify_headers(
            handle,
            stream_id,
            header_start,
            header_count,
            end_stream,
            out_class,
        )
    })
}

unsafe fn classify_headers(
    handle: *mut VylH2Client,
    stream_id: u32,
    header_start: usize,
    header_count: usize,
    end_stream: u32,
    out_class: *mut VylH2ResponseClass,
) -> VylResult {
    if handle.is_null() || end_stream > 1 || out_class.is_null() {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_class.write(VylH2ResponseClass::default());
    }
    let handle = unsafe { &mut *handle };
    let Some(connection) = handle.connection.as_mut() else {
        return VylResult::InternalError;
    };
    if connection.is_cancelled() {
        return VylResult::Cancelled;
    }
    let Some(end) = header_start.checked_add(header_count) else {
        return VylResult::InvalidArgument;
    };
    if end > connection.feed_header_count() {
        return VylResult::InvalidArgument;
    }
    let headers = &handle.feed_scratch.headers()[header_start..end];
    match connection.classify_headers(stream_id, headers, end_stream != 0) {
        Ok(class) => {
            let kind = match class.kind {
                veylor_http2::ResponseSectionKind::Informational => 0,
                veylor_http2::ResponseSectionKind::Final => 1,
                veylor_http2::ResponseSectionKind::Trailers => 2,
            };
            let phase = match class.phase {
                veylor_http2::ClientResponsePhase::AwaitingFinalHeaders => 0,
                veylor_http2::ClientResponsePhase::Content => 1,
                veylor_http2::ClientResponsePhase::Complete => 2,
            };
            let (content_length, has_content_length) = match class.content_length {
                Some(length) => (length, 1),
                None => (0, 0),
            };
            unsafe {
                out_class.write(VylH2ResponseClass {
                    kind,
                    status: class.status,
                    phase,
                    content_length,
                    has_content_length,
                    end_stream,
                });
            }
            VylResult::Ok
        }
        Err(error) => split_error(error).0,
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_h2_client_account_data(
    handle: *mut VylH2Client,
    stream_id: u32,
    data_length: usize,
    end_stream: u32,
    out_phase: *mut u32,
) -> VylResult {
    catch_ffi(|| unsafe { account_data(handle, stream_id, data_length, end_stream, out_phase) })
}

unsafe fn account_data(
    handle: *mut VylH2Client,
    stream_id: u32,
    data_length: usize,
    end_stream: u32,
    out_phase: *mut u32,
) -> VylResult {
    if handle.is_null() || end_stream > 1 || out_phase.is_null() {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_phase.write(0);
    }
    let handle = unsafe { &mut *handle };
    let Some(connection) = handle.connection.as_mut() else {
        return VylResult::InternalError;
    };
    match connection.account_data(stream_id, data_length, end_stream != 0) {
        Ok(accounted) => {
            unsafe {
                out_phase.write(match accounted.phase {
                    veylor_http2::ClientResponsePhase::AwaitingFinalHeaders => 0,
                    veylor_http2::ClientResponsePhase::Content => 1,
                    veylor_http2::ClientResponsePhase::Complete => 2,
                });
            }
            VylResult::Ok
        }
        Err(error) => split_error(error).0,
    }
}
