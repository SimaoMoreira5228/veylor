use std::slice;

use veylor_http1::{
    Field, Limits, ResponseEncodeError, ResponseFraming,
    encode_response_chunk_with_required_length, encode_response_final_chunk_with_required_length,
};

use crate::abi::{VylBytes, VylField, VylResult, catch_ffi, slice_length_valid};

use super::api::VylServerConnection;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResponseBody {
    Fixed { remaining: u64 },
    Chunked,
    CloseDelimited,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResponseWriteState {
    Idle,
    HeadPending {
        remaining: usize,
        status: u16,
        body: Option<ResponseBody>,
    },
    Ready(ResponseBody),
    BodyPending {
        remaining: usize,
        body: ResponseBody,
        finish: bool,
    },
    Complete,
    Failed(VylResult),
}

pub(super) struct ResponseWriter {
    state: ResponseWriteState,
    continue_ready: bool,
}

struct EncodeBodyArguments {
    input: VylBytes,
    output: *mut u8,
    output_capacity: usize,
    out_consumed: *mut usize,
    out_written: *mut usize,
    out_required: *mut usize,
}

struct FinishBodyArguments {
    trailers: *const VylField,
    trailer_count: usize,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_required: *mut usize,
}

impl ResponseWriter {
    pub(super) const fn new() -> Self {
        Self {
            state: ResponseWriteState::Idle,
            continue_ready: false,
        }
    }

    pub(super) fn can_start_head(&self) -> bool {
        self.state == ResponseWriteState::Idle
    }

    pub(super) fn cancel(&mut self) {
        self.state = ResponseWriteState::Failed(VylResult::Cancelled);
        self.continue_ready = false;
    }

    pub(super) fn start_head(
        &mut self,
        status: u16,
        framing: ResponseFraming,
        content_length: u64,
        body_allowed: bool,
        written: usize,
    ) {
        if status == 100 {
            self.continue_ready = false;
        }
        let body = body_allowed.then_some(match framing {
            ResponseFraming::FixedLength => ResponseBody::Fixed {
                remaining: content_length,
            },
            ResponseFraming::Chunked => ResponseBody::Chunked,
            ResponseFraming::CloseDelimited => ResponseBody::CloseDelimited,
        });
        self.state = ResponseWriteState::HeadPending {
            remaining: written,
            status,
            body,
        };
    }

    pub(super) fn can_flush_continue(&self) -> bool {
        self.continue_ready
    }

    pub(super) fn continue_flushed(&mut self) -> bool {
        let was_flushed = self.continue_ready;
        self.continue_ready = false;
        was_flushed
    }

    pub(super) fn advance(&mut self, transferred: usize) -> Result<usize, VylResult> {
        let (remaining, next) = match self.state {
            ResponseWriteState::HeadPending {
                remaining,
                status,
                body,
            } => {
                let remaining = pending_after(remaining, transferred)?;
                if remaining != 0 {
                    (
                        remaining,
                        ResponseWriteState::HeadPending {
                            remaining,
                            status,
                            body,
                        },
                    )
                } else if status < 200 && status != 101 {
                    if status == 100 {
                        self.continue_ready = true;
                    }
                    (0, ResponseWriteState::Idle)
                } else {
                    match body {
                        Some(body) => (0, ResponseWriteState::Ready(body)),
                        None => (0, ResponseWriteState::Complete),
                    }
                }
            }
            ResponseWriteState::BodyPending {
                remaining,
                body,
                finish,
            } => {
                let remaining = pending_after(remaining, transferred)?;
                if remaining != 0 {
                    (
                        remaining,
                        ResponseWriteState::BodyPending {
                            remaining,
                            body,
                            finish,
                        },
                    )
                } else if finish {
                    (0, ResponseWriteState::Complete)
                } else {
                    (0, ResponseWriteState::Ready(body))
                }
            }
            ResponseWriteState::Idle
            | ResponseWriteState::Ready(_)
            | ResponseWriteState::Complete => return Err(VylResult::BadState),
            ResponseWriteState::Failed(error) => return Err(error),
        };
        self.state = next;
        Ok(remaining)
    }

    fn encode_body(&mut self, arguments: EncodeBodyArguments) -> VylResult {
        let EncodeBodyArguments {
            input,
            output,
            output_capacity,
            out_consumed,
            out_written,
            out_required,
        } = arguments;
        if (output.is_null() && output_capacity != 0)
            || out_consumed.is_null()
            || out_written.is_null()
            || out_required.is_null()
            || !slice_length_valid::<u8>(output_capacity)
        {
            return VylResult::InvalidArgument;
        }
        unsafe {
            out_consumed.write(0);
            out_written.write(0);
            out_required.write(0);
        }

        let ResponseWriteState::Ready(body) = self.state else {
            return match self.state {
                ResponseWriteState::HeadPending { .. } | ResponseWriteState::BodyPending { .. } => {
                    VylResult::WouldBlock
                }
                ResponseWriteState::Failed(error) => error,
                _ => VylResult::BadState,
            };
        };

        let input = match unsafe { read_bytes(input) } {
            Ok(input) => input,
            Err(error) => return error,
        };
        let output = if output_capacity == 0 {
            &mut []
        } else {
            unsafe { slice::from_raw_parts_mut(output, output_capacity) }
        };
        let (consumed, written, next_body) = match body {
            ResponseBody::Fixed { remaining } => {
                let Ok(input_length) = u64::try_from(input.len()) else {
                    return VylResult::BodyTooLong;
                };
                if input_length > remaining {
                    return VylResult::BodyTooLong;
                }
                if input.is_empty() {
                    return VylResult::Ok;
                }
                if output.is_empty() {
                    unsafe {
                        out_required.write(input.len());
                    }
                    return VylResult::BufferTooSmall;
                }
                let written = input.len().min(output.len());
                output[..written].copy_from_slice(&input[..written]);
                let Ok(written_u64) = u64::try_from(written) else {
                    return VylResult::BodyTooLong;
                };
                (
                    written,
                    written,
                    ResponseBody::Fixed {
                        remaining: remaining - written_u64,
                    },
                )
            }
            ResponseBody::Chunked => {
                if input.is_empty() {
                    return VylResult::InvalidArgument;
                }
                let mut required = 0;
                match encode_response_chunk_with_required_length(input, output, &mut required) {
                    Ok(written) => (input.len(), written, ResponseBody::Chunked),
                    Err(ResponseEncodeError::BufferTooSmall) => {
                        unsafe {
                            out_required.write(required);
                        }
                        return VylResult::BufferTooSmall;
                    }
                    Err(_) => return VylResult::InvalidArgument,
                }
            }
            ResponseBody::CloseDelimited => {
                if input.is_empty() {
                    return VylResult::Ok;
                }
                if output.is_empty() {
                    unsafe {
                        out_required.write(input.len());
                    }
                    return VylResult::BufferTooSmall;
                }
                let written = input.len().min(output.len());
                output[..written].copy_from_slice(&input[..written]);
                (written, written, ResponseBody::CloseDelimited)
            }
        };

        self.state = ResponseWriteState::BodyPending {
            remaining: written,
            body: next_body,
            finish: false,
        };
        unsafe {
            out_consumed.write(consumed);
            out_written.write(written);
            out_required.write(written);
        }
        VylResult::Ok
    }

    fn finish_body(&mut self, arguments: FinishBodyArguments) -> VylResult {
        let FinishBodyArguments {
            trailers,
            trailer_count,
            output,
            output_capacity,
            out_written,
            out_required,
        } = arguments;
        if (trailers.is_null() && trailer_count != 0)
            || (output.is_null() && output_capacity != 0)
            || out_written.is_null()
            || out_required.is_null()
            || !slice_length_valid::<VylField>(trailer_count)
            || !slice_length_valid::<u8>(output_capacity)
        {
            return VylResult::InvalidArgument;
        }
        unsafe {
            out_written.write(0);
            out_required.write(0);
        }
        let ResponseWriteState::Ready(body) = self.state else {
            return match self.state {
                ResponseWriteState::HeadPending { .. } | ResponseWriteState::BodyPending { .. } => {
                    VylResult::WouldBlock
                }
                ResponseWriteState::Failed(error) => error,
                _ => VylResult::BadState,
            };
        };
        if trailer_count > Limits::DEFAULT_HEADER_COUNT {
            return VylResult::Capacity;
        }

        match body {
            ResponseBody::Fixed { remaining: 0 } | ResponseBody::CloseDelimited => {
                if trailer_count != 0 {
                    return VylResult::InvalidArgument;
                }
                self.state = ResponseWriteState::Complete;
                VylResult::Ok
            }
            ResponseBody::Fixed { remaining: _ } => {
                self.state = ResponseWriteState::Failed(VylResult::ProtocolError);
                VylResult::ProtocolError
            }
            ResponseBody::Chunked => {
                let trailers = if trailer_count == 0 {
                    &[]
                } else {
                    unsafe { slice::from_raw_parts(trailers, trailer_count) }
                };
                let empty_field = Field {
                    name: &[],
                    value: &[],
                };
                let mut fields = [empty_field; Limits::DEFAULT_HEADER_COUNT];
                for (destination, field) in fields.iter_mut().zip(trailers) {
                    let name = match unsafe { read_bytes(field.name) } {
                        Ok(name) => name,
                        Err(error) => return error,
                    };
                    let value = match unsafe { read_bytes(field.value) } {
                        Ok(value) => value,
                        Err(error) => return error,
                    };
                    *destination = Field { name, value };
                }
                let output = if output_capacity == 0 {
                    &mut []
                } else {
                    unsafe { slice::from_raw_parts_mut(output, output_capacity) }
                };
                let mut required = 0;
                match encode_response_final_chunk_with_required_length(
                    &fields[..trailer_count],
                    output,
                    &mut required,
                ) {
                    Ok(written) => {
                        self.state = ResponseWriteState::BodyPending {
                            remaining: written,
                            body: ResponseBody::Chunked,
                            finish: true,
                        };
                        unsafe {
                            out_written.write(written);
                            out_required.write(required);
                        }
                        VylResult::Ok
                    }
                    Err(ResponseEncodeError::BufferTooSmall) => {
                        unsafe {
                            out_required.write(required);
                        }
                        VylResult::BufferTooSmall
                    }
                    Err(_) => VylResult::InvalidArgument,
                }
            }
        }
    }
}






#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_server_connection_advance_response_write(
    connection: *mut VylServerConnection,
    transferred: usize,
    out_pending: *mut usize,
) -> VylResult {
    catch_ffi(|| unsafe { advance_response_write(connection, transferred, out_pending) })
}

unsafe fn advance_response_write(
    connection: *mut VylServerConnection,
    transferred: usize,
    out_pending: *mut usize,
) -> VylResult {
    if out_pending.is_null() {
        return VylResult::InvalidArgument;
    }
    unsafe {
        out_pending.write(0);
    }
    if connection.is_null() {
        return VylResult::InvalidArgument;
    }
    let connection = unsafe { &mut *connection };
    match connection.response_writer.advance(transferred) {
        Ok(pending) => {
            unsafe {
                out_pending.write(pending);
            }
            VylResult::Ok
        }
        Err(error) => {
            if error == VylResult::WriteOverrun {
                connection.response_writer.state =
                    ResponseWriteState::Failed(VylResult::WriteOverrun);
            }
            error
        }
    }
}







#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_server_connection_encode_response_body(
    connection: *mut VylServerConnection,
    input: VylBytes,
    output: *mut u8,
    output_capacity: usize,
    out_consumed: *mut usize,
    out_written: *mut usize,
    out_required: *mut usize,
) -> VylResult {
    let arguments = EncodeBodyArguments {
        input,
        output,
        output_capacity,
        out_consumed,
        out_written,
        out_required,
    };
    catch_ffi(|| unsafe { encode_response_body(connection, arguments) })
}

unsafe fn encode_response_body(
    connection: *mut VylServerConnection,
    arguments: EncodeBodyArguments,
) -> VylResult {
    if connection.is_null() {
        return VylResult::InvalidArgument;
    }
    let connection = unsafe { &mut *connection };
    connection.response_writer.encode_body(arguments)
}







#[unsafe(no_mangle)]
pub unsafe extern "C" fn vyl_http1_server_connection_finish_response_body(
    connection: *mut VylServerConnection,
    trailers: *const VylField,
    trailer_count: usize,
    output: *mut u8,
    output_capacity: usize,
    out_written: *mut usize,
    out_required: *mut usize,
) -> VylResult {
    let arguments = FinishBodyArguments {
        trailers,
        trailer_count,
        output,
        output_capacity,
        out_written,
        out_required,
    };
    catch_ffi(|| unsafe { finish_response_body(connection, arguments) })
}

unsafe fn finish_response_body(
    connection: *mut VylServerConnection,
    arguments: FinishBodyArguments,
) -> VylResult {
    if connection.is_null() {
        return VylResult::InvalidArgument;
    }
    let connection = unsafe { &mut *connection };
    connection.response_writer.finish_body(arguments)
}

fn pending_after(pending: usize, transferred: usize) -> Result<usize, VylResult> {
    if transferred > pending {
        return Err(VylResult::WriteOverrun);
    }
    Ok(pending - transferred)
}

unsafe fn read_bytes<'a>(bytes: VylBytes) -> Result<&'a [u8], VylResult> {
    if (bytes.data.is_null() && bytes.length != 0) || !slice_length_valid::<u8>(bytes.length) {
        return Err(VylResult::InvalidArgument);
    }
    if bytes.length == 0 {
        return Ok(&[]);
    }
    Ok(unsafe { slice::from_raw_parts(bytes.data, bytes.length) })
}
