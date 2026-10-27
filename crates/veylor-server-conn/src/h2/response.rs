use veylor_http2::{DataWriteError, HeadersWriteError, ServerError};

use super::H2ServerError;

pub(crate) fn map_server_error(error: ServerError, required: &mut usize) -> H2ServerError {
    match error {
        ServerError::OutputTooSmall { required: need } => {
            *required = need;
            H2ServerError::BufferTooSmall { required: need }
        }
        ServerError::Headers(error) => map_headers_error(error, required),
        ServerError::Data(error) => map_data_error(error, required),
        ServerError::Control(error) => match error {
            veylor_http2::FrameWriteError::OutputTooSmall { required: need } => {
                *required = need;
                H2ServerError::BufferTooSmall { required: need }
            }
            veylor_http2::FrameWriteError::InvalidHeader(_)
            | veylor_http2::FrameWriteError::InvalidPayload(_) => H2ServerError::InvalidArgument,
            veylor_http2::FrameWriteError::PendingFrame => H2ServerError::WouldBlock,
            veylor_http2::FrameWriteError::OverAcknowledged => H2ServerError::WriteOverrun,
            veylor_http2::FrameWriteError::Terminal => H2ServerError::BadState,
            veylor_http2::FrameWriteError::UnsupportedAction => H2ServerError::InternalError,
        },
        ServerError::Request(_) => H2ServerError::ProtocolError,
        ServerError::Response(_) => H2ServerError::InvalidArgument,
        ServerError::SettingsNotSent | ServerError::NewStreamsClosed => H2ServerError::BadState,
        ServerError::RefusedStream => H2ServerError::Capacity,
        ServerError::UnknownStream => H2ServerError::InvalidArgument,
        ServerError::StreamClosed => H2ServerError::BadState,
        ServerError::WrongRole | ServerError::Stream(_) | ServerError::Flow(_) => {
            H2ServerError::InternalError
        }
    }
}

pub(crate) fn map_headers_error(error: HeadersWriteError, required: &mut usize) -> H2ServerError {
    match error {
        HeadersWriteError::OutputTooSmall { required: need } => {
            *required = need;
            H2ServerError::BufferTooSmall { required: need }
        }
        HeadersWriteError::SettingsNotSent => H2ServerError::BadState,
        HeadersWriteError::PendingFrame => H2ServerError::WouldBlock,
        HeadersWriteError::Terminal | HeadersWriteError::OverAcknowledged => {
            H2ServerError::WriteOverrun
        }
        HeadersWriteError::StreamRejected(_) => H2ServerError::ProtocolError,
        HeadersWriteError::InvalidStreamId
        | HeadersWriteError::LengthOverflow
        | HeadersWriteError::InvalidHeader(_) => H2ServerError::InternalError,
    }
}

pub(crate) fn map_data_error(error: DataWriteError, required: &mut usize) -> H2ServerError {
    match error {
        DataWriteError::OutputTooSmall { required: need } => {
            *required = need;
            H2ServerError::BufferTooSmall { required: need }
        }
        DataWriteError::InvalidStreamId | DataWriteError::UnknownStream => {
            H2ServerError::InvalidArgument
        }
        DataWriteError::StreamClosed => H2ServerError::BadState,
        DataWriteError::SettingsNotSent => H2ServerError::BadState,
        DataWriteError::NoSendCredit | DataWriteError::PendingFrame => H2ServerError::WouldBlock,
        DataWriteError::OverAcknowledged | DataWriteError::Terminal => H2ServerError::WriteOverrun,
        DataWriteError::InvalidHeader(_) => H2ServerError::InternalError,
    }
}
