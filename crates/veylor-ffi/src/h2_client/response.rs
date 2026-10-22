use super::VylH2Client;
use crate::abi::{VylH2ResponseClass, VylResult, catch_ffi};

fn ensure_live(handle: &VylH2Client) -> Result<(), VylResult> {
    if handle.cancelled {
        return Err(VylResult::Cancelled);
    }
    Ok(())
}







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
    if let Err(error) = ensure_live(handle) {
        return error;
    }
    let Some(end) = header_start.checked_add(header_count) else {
        return VylResult::InvalidArgument;
    };
    if end > handle.feed_header_count {
        return VylResult::InvalidArgument;
    }
    let position = match handle.responses.iter().position(|(id, _)| *id == stream_id) {
        Some(position) => position,
        None => return VylResult::InvalidArgument,
    };
    let driver = match handle.driver.as_ref() {
        Some(driver) => driver,
        None => return VylResult::InternalError,
    };
    let section = match handle.responses[position].1.classify(
        driver.driver(),
        &handle.feed_headers[header_start..end],
        end_stream != 0,
    ) {
        Ok(section) => section,
        Err(_) => return VylResult::ProtocolError,
    };
    let kind = match section.kind {
        veylor_http2::ResponseSectionKind::Informational => 0,
        veylor_http2::ResponseSectionKind::Final => 1,
        veylor_http2::ResponseSectionKind::Trailers => 2,
    };
    let phase = match handle.responses[position].1.phase() {
        veylor_http2::ClientResponsePhase::AwaitingFinalHeaders => 0,
        veylor_http2::ClientResponsePhase::Content => 1,
        veylor_http2::ClientResponsePhase::Complete => 2,
    };
    let (content_length, has_content_length) = match handle.responses[position].1.content_length() {
        Some(length) => (length, 1),
        None => (0, 0),
    };
    unsafe {
        out_class.write(VylH2ResponseClass {
            kind,
            status: section.status.unwrap_or(0),
            phase,
            content_length,
            has_content_length,
            end_stream,
        });
    }
    VylResult::Ok
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
    if let Err(error) = ensure_live(handle) {
        return error;
    }
    let position = match handle.responses.iter().position(|(id, _)| *id == stream_id) {
        Some(position) => position,
        None => return VylResult::InvalidArgument,
    };
    let phase = match handle.responses[position]
        .1
        .data_received(data_length, end_stream != 0)
    {
        Ok(phase) => phase,
        Err(_) => return VylResult::ProtocolError,
    };
    unsafe {
        out_phase.write(match phase {
            veylor_http2::ClientResponsePhase::AwaitingFinalHeaders => 0,
            veylor_http2::ClientResponsePhase::Content => 1,
            veylor_http2::ClientResponsePhase::Complete => 2,
        });
    }
    VylResult::Ok
}
