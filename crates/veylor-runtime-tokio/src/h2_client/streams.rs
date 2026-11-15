use std::sync::Arc;

use tokio::time::Instant;
use veylor_client::h2::ErrorCode;

use super::headers::HeaderList;
use super::shared::StreamShared;
use super::upload::Upload;

pub(super) enum Source {
    Full {
        data: Vec<u8>,
        sent: usize,
        trailers: HeaderList,
    },
    Stream(Arc<Upload>),
    Trailers(HeaderList),
    Done,
}

pub(super) struct ActiveStream {
    pub(super) id: u32,
    pub(super) shared: Arc<StreamShared>,
    pub(super) source: Source,
    pub(super) deadline: Option<Instant>,
    pub(super) remote_ended: bool,
}

impl ActiveStream {
    pub(super) fn local_ended(&self) -> bool {
        matches!(self.source, Source::Done)
    }
}

pub(super) const MAX_PINGS: usize = 8;

pub(super) struct Outbox {
    pub(super) settings_acks: u32,
    pub(super) pings: [[u8; 8]; MAX_PINGS],
    pub(super) ping_count: usize,
    pub(super) resets: Vec<(u32, ErrorCode)>,
    pub(super) connection_error: Option<ErrorCode>,
    pub(super) goaway: bool,
}

impl Outbox {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            settings_acks: 0,
            pings: [[0; 8]; MAX_PINGS],
            ping_count: 0,
            resets: Vec::with_capacity(capacity),
            connection_error: None,
            goaway: false,
        }
    }

    pub(super) fn push_ping(&mut self, data: [u8; 8]) {
        if self.ping_count < MAX_PINGS {
            self.pings[self.ping_count] = data;
            self.ping_count += 1;
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.settings_acks == 0
            && self.ping_count == 0
            && self.resets.is_empty()
            && self.connection_error.is_none()
            && !self.goaway
    }
}
