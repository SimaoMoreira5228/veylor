use veylor_http2::{ErrorCode, FeedStatus, H2FrameAction, HpackHeader};

use super::{H2ClientConnection, H2ClientError, MAX_ACTIONS, MAX_FIELDS};

pub struct FeedScratch<'i> {
    actions: [Option<H2FrameAction<'i>>; MAX_ACTIONS],
    headers: [HpackHeader; MAX_FIELDS],
}

impl<'i> FeedScratch<'i> {
    pub fn new() -> Self {
        Self {
            actions: [None; MAX_ACTIONS],
            headers: [HpackHeader::EMPTY; MAX_FIELDS],
        }
    }

    pub fn headers(&self) -> &[HpackHeader] {
        &self.headers
    }
}

impl Default for FeedScratch<'_> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BatchStatus {
    NeedMore,
    Ready,
}

pub struct FeedBatch<'s, 'i> {
    pub consumed: usize,
    pub actions: &'s [Option<H2FrameAction<'i>>],
    pub action_count: usize,
    pub headers: &'s [HpackHeader],
    pub header_count: usize,
    pub status: BatchStatus,
    pub connection_failed: bool,
    pub connection_error: Option<ErrorCode>,
}

pub fn error_code_value(code: ErrorCode) -> u32 {
    match code {
        ErrorCode::NoError => 0,
        ErrorCode::ProtocolError => 1,
        ErrorCode::InternalError => 2,
        ErrorCode::FlowControlError => 3,
        ErrorCode::SettingsTimeout => 4,
        ErrorCode::StreamClosed => 5,
        ErrorCode::FrameSizeError => 6,
        ErrorCode::RefusedStream => 7,
        ErrorCode::Cancel => 8,
        ErrorCode::CompressionError => 9,
        ErrorCode::ConnectError => 10,
        ErrorCode::EnhanceYourCalm => 11,
        ErrorCode::InadequateSecurity => 12,
        ErrorCode::Http11Required => 13,
        ErrorCode::Unknown(value) => value,
    }
}

impl<'c, 'b> H2ClientConnection<'c, 'b> {
    fn batch_from<'s, 'i>(&self, scratch: &'s FeedScratch<'i>) -> FeedBatch<'s, 'i> {
        FeedBatch {
            consumed: self.feed_consumed,
            actions: &scratch.actions,
            action_count: self.feed_action_count,
            headers: &scratch.headers,
            header_count: self.feed_header_count,
            status: if self.feed_pending {
                BatchStatus::Ready
            } else {
                BatchStatus::NeedMore
            },
            connection_failed: self.feed_failed,
            connection_error: self.feed_error,
        }
    }

    // NOTE: the driver consumes whole frames only and reports the count: advance past
    // `consumed` and re-feed the remainder (unlike the H1 client's re-parse, which
    // replays from the head start). NeedMore with consumed == len means wait for more
    // bytes; finish_batch() before acting, then poll again.
    pub fn poll_feed<'i, 's>(
        &mut self,
        input: &'i [u8],
        scratch: &'s mut FeedScratch<'i>,
    ) -> Result<FeedBatch<'s, 'i>, H2ClientError> {
        self.ensure_live()?;
        let result =
            self.driver
                .driver_mut()
                .feed(input, &mut scratch.actions, &mut scratch.headers);
        self.feed_action_count = result.actions;
        self.feed_header_count = result.headers;
        self.feed_consumed = result.consumed;
        self.feed_failed = result.status == FeedStatus::ConnectionFailed;
        self.feed_error = result.error;
        self.feed_pending = result.actions > 0;
        Ok(self.batch_from(scratch))
    }

    pub fn poll_retry<'i, 's>(
        &mut self,
        scratch: &'s mut FeedScratch<'i>,
    ) -> Result<FeedBatch<'s, 'i>, H2ClientError> {
        self.ensure_live()?;
        if !self.feed_pending {
            return Err(H2ClientError::BadState);
        }
        Ok(self.batch_from(scratch))
    }

    pub fn finish_batch(&mut self) {
        // NOTE: header counts and bytes stay readable for classification until the next poll,
        // mirroring the previous FFI retention contract.
        self.feed_action_count = 0;
        self.feed_pending = false;
    }

    pub fn has_pending_batch(&self) -> bool {
        self.feed_pending
    }

    pub fn feed_header_count(&self) -> usize {
        self.feed_header_count
    }
}
