// NOTE: native-invocation third of the static_routes seam: registry owns route storage
// (super), response assembly owns static/file encoding (respond), this module owns handler
// invocation plus the borrowed request view over already-matched native routes.
use std::panic::{AssertUnwindSafe, catch_unwind};

use veylor_http1::{
    ConnectionHeader, RequestHead, ResponseFraming, ResponseToWrite,
    connection_header_for_response, encode_response_head,
};

use crate::host::HOST_MAX_REASON_BYTES;

use super::{EncodedStaticResponse, encode_continue, reject_payload};
use super::{
    IN_NATIVE, MAX_RESPONSE_BODY, MAX_STATIC_BODY_BYTES, NativeHandler, NativeRequestView,
    NativeResponseOut, RouteBuildError, valid_reason,
};

// NOTE: native dispatch: framing mirrors the static path (chunked and over-cap bodies
// reject, expect-continue interims, drain-before-respond) while status/reason/body come
// from the handler's out-params instead of the registry. Reason/body copy into bounded
// stack scratch (the host reason bound and the static body cap); bad return codes, invalid
// status/reason, or oversize output fail closed as overflow, a panicking handler as a
// protocol close, never as a partial write. Bodies ride the view when fully buffered:
// invoke runs at head-parse time, so a declared fixed-length body already past head.length
// borrows input bytes for the call, while a not-yet-arrived tail invokes framing-only
// (null body with has_body/body_len intact, the v1 contract) and the pump still drains
// declared bytes exactly like a static route. The handler views, the pump owns: drain
// accounting below is untouched by what the handler saw.
pub(super) fn respond_native(
    handler: NativeHandler,
    user_data: usize,
    head: RequestHead,
    method: &[u8],
    path: &[u8],
    input: &[u8],
    output: &mut [u8],
) -> Result<EncodedStaticResponse, RouteBuildError> {
    if head.is_chunked {
        return reject_payload(output, method);
    }
    let declared = head.content_length.unwrap_or(0);
    let drain_total = match head.content_length {
        Some(length) if length > MAX_STATIC_BODY_BYTES => {
            return reject_payload(output, method);
        }
        Some(length) => length,
        None => 0,
    };
    let interim_len = if head.expect_continue {
        encode_continue(output)?
    } else {
        0
    };
    let buffered = input.len().saturating_sub(head.length) as u64;
    let staged = declared > 0 && buffered >= declared;
    let body = input
        .get(head.length..head.length + declared as usize)
        .filter(|_| staged)
        .map(|slice| slice.as_ptr())
        .unwrap_or(std::ptr::null());
    let request = NativeRequestView {
        method: method.as_ptr(),
        method_len: method.len(),
        path: path.as_ptr(),
        path_len: path.len(),
        has_body: u8::from(declared > 0),
        body_len: declared,
        body,
    };
    let mut response = NativeResponseOut {
        status: 0,
        reason: std::ptr::null(),
        reason_len: 0,
        body: std::ptr::null(),
        body_len: 0,
    };
    let mut reason_buf = [0u8; HOST_MAX_REASON_BYTES];
    let mut body_buf = [0u8; MAX_RESPONSE_BODY];
    // NOTE: the flag trips nested registry/dispatch entries on this worker. Unwinding through
    // an extern "C" frame aborts the process (nounwind: verified SIGABRT, uncatchable from
    // here), so containment is the handler's contract — a Rust handler must catch its own
    // panics and return nonzero; catch_unwind below covers only the boundary closure itself.
    // The flag clears on every path below since a returning handler cannot skip it.
    IN_NATIVE.set(true);
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        invoke_native(handler, user_data, &request, &mut response)
    }));
    IN_NATIVE.set(false);
    let code = match outcome {
        Ok(code) => code,
        Err(_) => return Err(RouteBuildError::NativePanic),
    };
    if code != 0 {
        return Err(RouteBuildError::Invalid);
    }
    if !(200..=599).contains(&response.status) {
        return Err(RouteBuildError::Invalid);
    }
    if !copy_handler_bytes(response.reason, response.reason_len, &mut reason_buf) {
        return Err(RouteBuildError::Invalid);
    }
    if !copy_handler_bytes(response.body, response.body_len, &mut body_buf) {
        return Err(RouteBuildError::Invalid);
    }
    let reason = &reason_buf[..response.reason_len];
    let body = &body_buf[..response.body_len];
    if !valid_reason(reason) {
        return Err(RouteBuildError::Invalid);
    }
    let status = response.status as u16;
    let response_close = status == 404;
    let connection = connection_header_for_response(
        head,
        input,
        status,
        ResponseFraming::FixedLength,
        true,
        response_close,
    );
    let response = ResponseToWrite {
        request_method: method,
        requested_upgrade: b"",
        status,
        reason,
        fields: &[],
        framing: ResponseFraming::FixedLength,
        content_length: body.len() as u64,
        connection,
    };
    let final_buf = output
        .get_mut(interim_len..)
        .ok_or(RouteBuildError::Invalid)?;
    let encoded =
        encode_response_head(&response, final_buf).map_err(|_| RouteBuildError::Invalid)?;
    let mut length = interim_len + encoded.length;
    if encoded.body_allowed && !body.is_empty() {
        let end = length + body.len();
        output
            .get_mut(length..end)
            .ok_or(RouteBuildError::Invalid)?
            .copy_from_slice(body);
        length = end;
    }
    Ok(EncodedStaticResponse {
        length,
        keep_open: connection != ConnectionHeader::Close,
        drain_total,
        interim_len,
        file: None,
    })
}

// NOTE: single unsafe funnel for the boundary call: the engine holds the request views and
// the out-param alive across it, and registration already rejected null pointers.
#[allow(unsafe_code)]
fn invoke_native(
    handler: NativeHandler,
    user_data: usize,
    request: &NativeRequestView,
    response: &mut NativeResponseOut,
) -> i32 {
    


    unsafe { handler(user_data as *mut std::ffi::c_void, request, response) }
}

// NOTE: single unsafe funnel for handler-owned out bytes: empty outputs skip the slice so
// null+0 never forms one, and bounded lengths copy before the handler's memory can lapse.
#[allow(unsafe_code)]
fn copy_handler_bytes(src: *const u8, len: usize, dst: &mut [u8]) -> bool {
    if len == 0 {
        return true;
    }
    if len > dst.len() || src.is_null() {
        return false;
    }
    

    unsafe {
        dst[..len].copy_from_slice(std::slice::from_raw_parts(src, len));
    }
    true
}

#[cfg(test)]
mod tests {
    use std::ffi::c_void;
    use std::sync::Mutex;

    use veylor_http1::{Header, Limits, Parser};
    use veylor_router::RouteSlot;

    use super::super::{EncodedStaticResponse, respond_to_head};
    use super::super::{MAX_STATIC_BODY_BYTES, NativeRequestView, NativeResponseOut, StaticRoutes};

    struct Observed {
        has_body: u8,
        body_len: u64,
        null_body: bool,
        body: Vec<u8>,
    }

    static OBSERVED: Mutex<Option<Observed>> = Mutex::new(None);
    static SERIAL: Mutex<()> = Mutex::new(());

    #[allow(unsafe_code)]
    unsafe extern "C" fn record_handler(
        _user_data: *mut c_void,
        request: *const NativeRequestView,
        response: *mut NativeResponseOut,
    ) -> i32 {
        if request.is_null() || response.is_null() {
            return 1;
        }
        
        let view = unsafe { &*request };
        let out = unsafe { &mut *response };
        let body = if view.body.is_null() || view.body_len == 0 {
            Vec::new()
        } else {
            unsafe { std::slice::from_raw_parts(view.body, view.body_len as usize) }.to_vec()
        };
        *OBSERVED.lock().expect("observed lock") = Some(Observed {
            has_body: view.has_body,
            body_len: view.body_len,
            null_body: view.body.is_null(),
            body,
        });
        out.status = 200;
        out.reason = b"OK".as_ptr();
        out.reason_len = 2;
        out.body = std::ptr::null();
        out.body_len = 0;
        0
    }

    fn drive(input: &[u8], output: &mut [u8]) -> EncodedStaticResponse {
        *OBSERVED.lock().expect("observed lock") = None;
        let mut routes = StaticRoutes::new();
        routes
            .register_native(b"POST", b"/echo", Some(record_handler), 0)
            .expect("register echo");
        let mut slots = [RouteSlot::EMPTY; 8];
        let table = routes.build_table(&mut slots).expect("build table");
        let mut headers = [Header::default(); 100];
        let mut parser = Parser::new();
        let head = parser
            .parse(input, &mut headers, Limits::default())
            .expect("parse head")
            .expect("head complete");
        respond_to_head(&routes, &table, head, &headers, input, output).expect("respond")
    }

    fn observed() -> Option<Observed> {
        OBSERVED.lock().expect("observed lock").take()
    }

    fn head_with_length(body: &[u8]) -> Vec<u8> {
        let mut input = format!(
            "POST /echo HTTP/1.1\r\nHost: test\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .into_bytes();
        input.extend_from_slice(body);
        input
    }

    #[test]
    fn staged_body_borrows_fully_present_bytes() {
        let _serial = SERIAL.lock().expect("serial lock");
        let input = head_with_length(b"hello");
        let mut output = [0u8; 1024];
        let encoded = drive(&input, &mut output);
        assert_eq!(encoded.drain_total, 5);
        let seen = observed().expect("handler invoked");
        assert_eq!(seen.has_body, 1);
        assert_eq!(seen.body_len, 5);
        assert!(!seen.null_body);
        assert_eq!(seen.body, b"hello");
    }

    #[test]
    fn partial_tail_invokes_framing_only() {
        let _serial = SERIAL.lock().expect("serial lock");
        let mut input = b"POST /echo HTTP/1.1\r\nHost: test\r\nContent-Length: 5\r\n\r\n".to_vec();
        input.extend_from_slice(b"he");
        let mut output = [0u8; 1024];
        let encoded = drive(&input, &mut output);
        assert_eq!(encoded.drain_total, 5);
        let seen = observed().expect("handler invoked");
        assert_eq!(seen.has_body, 1);
        assert_eq!(seen.body_len, 5);
        assert!(seen.null_body);
        assert!(seen.body.is_empty());
    }

    #[test]
    fn zero_length_body_passes_null() {
        let _serial = SERIAL.lock().expect("serial lock");
        let input = head_with_length(b"");
        let mut output = [0u8; 1024];
        let encoded = drive(&input, &mut output);
        assert_eq!(encoded.drain_total, 0);
        let seen = observed().expect("handler invoked");
        assert_eq!(seen.has_body, 0);
        assert_eq!(seen.body_len, 0);
        assert!(seen.null_body);
    }

    #[test]
    fn over_cap_rejects_before_invoke() {
        let _serial = SERIAL.lock().expect("serial lock");
        let input = format!(
            "POST /echo HTTP/1.1\r\nHost: test\r\nContent-Length: {}\r\n\r\n",
            MAX_STATIC_BODY_BYTES + 1
        );
        let mut output = [0u8; 1024];
        let encoded = drive(input.as_bytes(), &mut output);
        assert_eq!(encoded.drain_total, 0);
        assert!(observed().is_none());
        assert!(output[..encoded.length].starts_with(b"HTTP/1.1 413"));
    }

    #[test]
    fn chunked_rejects_before_invoke() {
        let _serial = SERIAL.lock().expect("serial lock");
        let input = b"POST /echo HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n\r\n";
        let mut output = [0u8; 1024];
        let encoded = drive(input, &mut output);
        assert_eq!(encoded.drain_total, 0);
        assert!(observed().is_none());
        assert!(output[..encoded.length].starts_with(b"HTTP/1.1 413"));
    }
}
