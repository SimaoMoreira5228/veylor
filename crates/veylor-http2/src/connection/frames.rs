use super::{
    FeedResult, H2FrameAction, H2FrameDriver, HeaderDisposition, HeaderRange, PendingHeaders,
};
use crate::frame::{
    DATA_FLAG_END_STREAM, ErrorCode, FrameHeader, FrameType, HEADERS_FLAG_END_HEADERS,
    HEADERS_FLAG_END_STREAM, PING_FLAG_ACK, SETTINGS_ACK, decode_data_payload,
    decode_goaway_payload, decode_headers_payload, decode_ping_payload, decode_priority_payload,
    decode_rst_stream_payload, decode_window_update_payload,
};
use crate::hpack::{HpackError, HpackHeader, HpackTable, decode_header_block};
use crate::stream::{FrameDirection, StreamAction, StreamActionKind, StreamTable};

struct FrameOutput<'output, 'input> {
    actions: &'output mut [Option<H2FrameAction<'input>>],
    result: &'output mut FeedResult,
    headers: &'output mut [HpackHeader],
}

impl<'stream, 'streams, 'hpack> H2FrameDriver<'stream, 'streams, 'hpack> {
    pub(super) fn process_frame<'input>(
        &mut self,
        header: FrameHeader,
        payload: &'input [u8],
        actions: &mut [Option<H2FrameAction<'input>>],
        result: &mut FeedResult,
        headers: &mut [HpackHeader],
    ) -> Result<(), ErrorCode> {
        if self.streams.is_assembling_headers() && header.frame_type != FrameType::Continuation {
            let action = self.streams.on_frame(header, FrameDirection::Received, &[]);
            return match action.kind {
                StreamActionKind::ConnectionError => Err(action.code),
                _ => Err(ErrorCode::ProtocolError),
            };
        }

        match header.frame_type {
            FrameType::Data => self.process_data(header, payload, actions, result),
            FrameType::Headers => self.process_headers(header, payload, actions, result, headers),
            FrameType::Continuation => {
                self.process_continuation(header, payload, actions, result, headers)
            }
            FrameType::Settings => self.process_settings(header, payload, actions, result),
            FrameType::WindowUpdate => self.process_window_update(header, payload, actions, result),
            FrameType::RstStream => self.process_rst_stream(header, payload, actions, result),
            FrameType::GoAway => self.process_goaway(header, payload, actions, result),
            FrameType::Ping => self.process_ping(header, payload, actions, result),
            FrameType::Priority => self.process_priority(header, payload),
            FrameType::PushPromise => {
                let action = self
                    .streams
                    .on_frame(header, FrameDirection::Received, payload);
                self.apply_stream_action(action, header.stream_id, actions, result)
            }
            FrameType::Unknown(_) => {
                let action = self
                    .streams
                    .on_frame(header, FrameDirection::Received, payload);
                self.apply_stream_action(action, header.stream_id, actions, result)
            }
        }
    }

    fn process_data<'input>(
        &mut self,
        header: FrameHeader,
        payload: &'input [u8],
        actions: &mut [Option<H2FrameAction<'input>>],
        result: &mut FeedResult,
    ) -> Result<(), ErrorCode> {
        let data = decode_data_payload(header, payload).map_err(payload_error_code)?;
        let action = self.streams.on_frame(header, FrameDirection::Received, &[]);
        if action.kind == StreamActionKind::ConnectionError {
            return Err(action.code);
        }

        let amount = i64::from(header.payload_length);
        if self.connection_receive_window.value() < amount {
            return Err(ErrorCode::FlowControlError);
        }

        if action.kind == StreamActionKind::Accept {
            let stream_window = self
                .streams
                .find(header.stream_id)
                .map(|stream| stream.receive_window.value())
                .ok_or(ErrorCode::InternalError)?;
            let empty_end_stream = amount == 0 && header.flags & DATA_FLAG_END_STREAM != 0;
            if stream_window < amount && !empty_end_stream {
                self.connection_receive_window
                    .take(amount)
                    .map_err(|_| ErrorCode::InternalError)?;
                self.queue_reset(
                    header.stream_id,
                    ErrorCode::FlowControlError,
                    actions,
                    result,
                );
                return Ok(());
            }
            self.connection_receive_window
                .take(amount)
                .map_err(|_| ErrorCode::InternalError)?;
            self.streams
                .receive_window_mut(header.stream_id)
                .ok_or(ErrorCode::InternalError)?
                .take(amount)
                .map_err(|_| ErrorCode::InternalError)?;
            Self::push_action(
                actions,
                result,
                H2FrameAction::Data {
                    stream_id: header.stream_id,
                    data: data.data,
                    end_stream: header.flags & DATA_FLAG_END_STREAM != 0,
                },
            );
            return Ok(());
        }

        self.connection_receive_window
            .take(amount)
            .map_err(|_| ErrorCode::InternalError)?;
        match action.kind {
            StreamActionKind::SendRst | StreamActionKind::RejectProtocolError => {
                let error_code = if action.kind == StreamActionKind::RejectProtocolError {
                    ErrorCode::RefusedStream
                } else {
                    action.code
                };
                self.queue_reset(header.stream_id, error_code, actions, result);
            }
            StreamActionKind::Ignore | StreamActionKind::Accept => {}
            StreamActionKind::ConnectionError => return Err(action.code),
        }
        Ok(())
    }

    fn process_headers<'input>(
        &mut self,
        header: FrameHeader,
        payload: &'input [u8],
        actions: &mut [Option<H2FrameAction<'input>>],
        result: &mut FeedResult,
        headers: &mut [HpackHeader],
    ) -> Result<(), ErrorCode> {
        let decoded = decode_headers_payload(header, payload).map_err(payload_error_code)?;
        let action = self
            .streams
            .on_frame(header, FrameDirection::Received, decoded.fragment);
        if action.kind == StreamActionKind::ConnectionError {
            return Err(action.code);
        }
        let disposition = header_disposition(action);
        let end_stream = header.flags & HEADERS_FLAG_END_STREAM != 0;
        if header.flags & HEADERS_FLAG_END_HEADERS == 0 {
            self.pending_headers = Some(PendingHeaders {
                stream_id: header.stream_id,
                end_stream,
                disposition,
            });
            return Ok(());
        }
        let reset = {
            let mut output = FrameOutput {
                actions,
                result,
                headers,
            };
            Self::decode_completed_headers(
                self.streams,
                self.hpack,
                header.stream_id,
                decoded.fragment,
                end_stream,
                disposition,
                &mut output,
            )?
        };
        if let Some(error_code) = reset {
            self.queue_reset(header.stream_id, error_code, actions, result);
        }
        Ok(())
    }

    fn process_continuation<'input>(
        &mut self,
        header: FrameHeader,
        payload: &'input [u8],
        actions: &mut [Option<H2FrameAction<'input>>],
        result: &mut FeedResult,
        headers: &mut [HpackHeader],
    ) -> Result<(), ErrorCode> {
        let action = self
            .streams
            .on_frame(header, FrameDirection::Received, payload);
        if action.kind == StreamActionKind::ConnectionError {
            return Err(action.code);
        }
        if self.streams.is_assembling_headers() {
            return Ok(());
        }
        let pending = self
            .pending_headers
            .take()
            .ok_or(ErrorCode::ProtocolError)?;
        let streams = &*self.streams;
        let (stream_id, block) = streams
            .assembled_header_block()
            .ok_or(ErrorCode::InternalError)?;
        if stream_id != pending.stream_id {
            return Err(ErrorCode::ProtocolError);
        }
        let reset = {
            let mut output = FrameOutput {
                actions,
                result,
                headers,
            };
            Self::decode_completed_headers(
                streams,
                self.hpack,
                stream_id,
                block,
                pending.end_stream,
                pending.disposition,
                &mut output,
            )?
        };
        if let Some(error_code) = reset {
            self.queue_reset(stream_id, error_code, actions, result);
        }
        Ok(())
    }

    fn decode_completed_headers<'input>(
        streams: &StreamTable<'_>,
        hpack: &mut HpackTable<'_>,
        stream_id: u32,
        block: &[u8],
        end_stream: bool,
        disposition: HeaderDisposition,
        output: &mut FrameOutput<'_, 'input>,
    ) -> Result<Option<ErrorCode>, ErrorCode> {
        let start = output.result.headers;
        let header_output = output
            .headers
            .get_mut(start..)
            .ok_or(ErrorCode::InternalError)?;
        let (count, consumed) =
            decode_header_block(block, hpack, header_output).map_err(hpack_error_code)?;
        if consumed != block.len() {
            return Err(ErrorCode::CompressionError);
        }
        let mut header_list_size = 0_u64;
        for header in &header_output[..count] {
            let name = hpack
                .resolve(header.field.name)
                .ok_or(ErrorCode::InternalError)?;
            let value = hpack
                .resolve(header.field.value)
                .ok_or(ErrorCode::InternalError)?;
            let field_size = u64::try_from(name.len())
                .ok()
                .and_then(|name_length| {
                    u64::try_from(value.len())
                        .ok()
                        .and_then(|value_length| name_length.checked_add(value_length))
                })
                .and_then(|size| size.checked_add(32))
                .ok_or(ErrorCode::EnhanceYourCalm)?;
            header_list_size = header_list_size
                .checked_add(field_size)
                .ok_or(ErrorCode::EnhanceYourCalm)?;
            if header_list_size > u64::from(streams.local_settings().max_header_list_size) {
                return Err(ErrorCode::EnhanceYourCalm);
            }
        }
        output.result.headers += count;

        match disposition {
            HeaderDisposition::Deliver => Self::push_action(
                output.actions,
                output.result,
                H2FrameAction::Headers {
                    stream_id,
                    headers: HeaderRange { start, count },
                    end_stream,
                },
            ),
            HeaderDisposition::Reset(error_code) => return Ok(Some(error_code)),
            HeaderDisposition::Ignore => return Ok(None),
        }
        Ok(None)
    }

    fn process_settings<'input>(
        &mut self,
        header: FrameHeader,
        payload: &[u8],
        actions: &mut [Option<H2FrameAction<'input>>],
        result: &mut FeedResult,
    ) -> Result<(), ErrorCode> {
        let action = self
            .streams
            .on_frame(header, FrameDirection::Received, payload);
        if action.kind == StreamActionKind::ConnectionError {
            return Err(action.code);
        }
        if header.flags & SETTINGS_ACK == 0 {
            Self::push_action(actions, result, H2FrameAction::SettingsAckRequired);
        }
        Ok(())
    }

    fn process_window_update<'input>(
        &mut self,
        header: FrameHeader,
        payload: &[u8],
        actions: &mut [Option<H2FrameAction<'input>>],
        result: &mut FeedResult,
    ) -> Result<(), ErrorCode> {
        let action = self.streams.on_frame(header, FrameDirection::Received, &[]);
        if action.kind == StreamActionKind::ConnectionError {
            return Err(action.code);
        }
        let update = match decode_window_update_payload(header, payload) {
            Ok(update) => update,
            Err(crate::frame::FramePayloadError::InvalidWindowIncrement) => {
                if header.stream_id == 0 {
                    return Err(ErrorCode::ProtocolError);
                }
                self.queue_reset(header.stream_id, ErrorCode::ProtocolError, actions, result);
                return Ok(());
            }
            Err(error) => return Err(payload_error_code(error)),
        };
        if action.kind == StreamActionKind::Ignore {
            return Ok(());
        }
        let window = if header.stream_id == 0 {
            &mut self.connection_send_window
        } else {
            let Some(window) = self.streams.send_window_mut(header.stream_id) else {
                return Err(ErrorCode::InternalError);
            };
            window
        };
        if window.add(update.increment).is_err() {
            if header.stream_id == 0 {
                return Err(ErrorCode::FlowControlError);
            }
            self.queue_reset(
                header.stream_id,
                ErrorCode::FlowControlError,
                actions,
                result,
            );
        }
        Ok(())
    }

    fn process_rst_stream<'input>(
        &mut self,
        header: FrameHeader,
        payload: &[u8],
        actions: &mut [Option<H2FrameAction<'input>>],
        result: &mut FeedResult,
    ) -> Result<(), ErrorCode> {
        let action = self.streams.on_frame(header, FrameDirection::Received, &[]);
        if action.kind == StreamActionKind::ConnectionError {
            return Err(action.code);
        }
        if action.kind == StreamActionKind::Ignore {
            return Ok(());
        }
        let error_code = decode_rst_stream_payload(header, payload).map_err(payload_error_code)?;
        Self::push_action(
            actions,
            result,
            H2FrameAction::StreamReset {
                stream_id: header.stream_id,
                error_code,
            },
        );
        Ok(())
    }

    fn process_goaway<'input>(
        &mut self,
        header: FrameHeader,
        payload: &'input [u8],
        actions: &mut [Option<H2FrameAction<'input>>],
        result: &mut FeedResult,
    ) -> Result<(), ErrorCode> {
        let action = self.streams.on_frame(header, FrameDirection::Received, &[]);
        if action.kind == StreamActionKind::ConnectionError {
            return Err(action.code);
        }
        let goaway = decode_goaway_payload(header, payload).map_err(payload_error_code)?;
        let action = self.streams.on_goaway_received(header, payload);
        if action.kind == StreamActionKind::ConnectionError {
            return Err(action.code);
        }
        Self::push_action(
            actions,
            result,
            H2FrameAction::GoAway {
                last_stream_id: goaway.last_stream_id,
                error_code: goaway.error_code,
                debug_data: goaway.debug_data,
            },
        );
        Ok(())
    }

    fn process_ping<'input>(
        &mut self,
        header: FrameHeader,
        payload: &[u8],
        actions: &mut [Option<H2FrameAction<'input>>],
        result: &mut FeedResult,
    ) -> Result<(), ErrorCode> {
        let action = self.streams.on_frame(header, FrameDirection::Received, &[]);
        if action.kind == StreamActionKind::ConnectionError {
            return Err(action.code);
        }
        let opaque_data = decode_ping_payload(header, payload).map_err(payload_error_code)?;
        if header.flags & PING_FLAG_ACK == 0 {
            Self::push_action(actions, result, H2FrameAction::PingAck { opaque_data });
        }
        Ok(())
    }

    fn process_priority(&mut self, header: FrameHeader, payload: &[u8]) -> Result<(), ErrorCode> {
        let action = self.streams.on_frame(header, FrameDirection::Received, &[]);
        if action.kind == StreamActionKind::ConnectionError {
            return Err(action.code);
        }
        let _priority = decode_priority_payload(header, payload).map_err(payload_error_code)?;
        Ok(())
    }

    fn apply_stream_action<'input>(
        &mut self,
        action: StreamAction,
        stream_id: u32,
        actions: &mut [Option<H2FrameAction<'input>>],
        result: &mut FeedResult,
    ) -> Result<(), ErrorCode> {
        match action.kind {
            StreamActionKind::Accept | StreamActionKind::Ignore => Ok(()),
            StreamActionKind::SendRst => {
                self.queue_reset(stream_id, action.code, actions, result);
                Ok(())
            }
            StreamActionKind::RejectProtocolError => {
                self.queue_reset(stream_id, ErrorCode::RefusedStream, actions, result);
                Ok(())
            }
            StreamActionKind::ConnectionError => Err(action.code),
        }
    }
}

fn header_disposition(action: StreamAction) -> HeaderDisposition {
    match action.kind {
        StreamActionKind::Accept => HeaderDisposition::Deliver,
        StreamActionKind::Ignore => HeaderDisposition::Ignore,
        StreamActionKind::SendRst => HeaderDisposition::Reset(action.code),
        StreamActionKind::RejectProtocolError => HeaderDisposition::Reset(ErrorCode::RefusedStream),
        StreamActionKind::ConnectionError => HeaderDisposition::Reset(action.code),
    }
}

fn payload_error_code(error: crate::frame::FramePayloadError) -> ErrorCode {
    use crate::frame::FramePayloadError;
    match error {
        FramePayloadError::PayloadLengthMismatch
        | FramePayloadError::InvalidPayloadLength
        | FramePayloadError::BufferTooSmall { .. } => ErrorCode::FrameSizeError,
        FramePayloadError::WrongFrameType
        | FramePayloadError::InvalidStreamId
        | FramePayloadError::InvalidErrorCode
        | FramePayloadError::InvalidPadding
        | FramePayloadError::InvalidSetting
        | FramePayloadError::InvalidWindowIncrement => ErrorCode::ProtocolError,
    }
}

fn hpack_error_code(error: HpackError) -> ErrorCode {
    match error {
        HpackError::HeaderCapacity { .. } => ErrorCode::InternalError,
        _ => ErrorCode::CompressionError,
    }
}
