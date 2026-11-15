use std::time::Duration;

use veylor_client::h2::{
    DEFAULT_MAX_FIELDS, DEFAULT_MAX_HEADER_LIST_SIZE, DEFAULT_REQUEST_BLOCK, MAX_STREAM_SLOTS,
};

// NOTE: hard maxima keep per-connection memory bounded whatever the caller configures. Two
// stream slots stay spare for closed streams that are not reclaimed yet.
pub const MAX_CONCURRENT_STREAMS: usize = MAX_STREAM_SLOTS - 2;
pub const MAX_HEADER_LIST_SIZE: u32 = 1024 * 1024;
pub const MAX_HEADER_FIELDS: usize = 512;

#[derive(Clone, Debug)]
pub struct H2Config {
    pub max_concurrent_streams: usize,
    pub max_pending_requests: usize,
    pub max_header_list_size: u32,
    pub max_header_fields: usize,
    pub max_request_header_bytes: usize,
    pub stream_window: u32,
    pub connection_window: u32,
    pub read_buffer: usize,
    pub write_buffer: usize,
    pub handshake_timeout: Duration,
    pub stream_timeout: Option<Duration>,
    pub idle_timeout: Option<Duration>,
    pub close_timeout: Duration,
}

impl Default for H2Config {
    fn default() -> Self {
        Self {
            max_concurrent_streams: 100,
            max_pending_requests: 128,
            max_header_list_size: DEFAULT_MAX_HEADER_LIST_SIZE,
            max_header_fields: DEFAULT_MAX_FIELDS,
            max_request_header_bytes: DEFAULT_REQUEST_BLOCK,
            stream_window: 128 * 1024,
            connection_window: 1024 * 1024,
            read_buffer: 64 * 1024,
            write_buffer: 64 * 1024,
            handshake_timeout: Duration::from_secs(10),
            stream_timeout: Some(Duration::from_secs(30)),
            idle_timeout: Some(Duration::from_secs(60)),
            close_timeout: Duration::from_secs(5),
        }
    }
}

impl H2Config {
    pub(crate) fn stream_slots(&self) -> usize {
        self.max_concurrent_streams + 2
    }

    pub(crate) fn normalized(mut self) -> Self {
        self.max_concurrent_streams = self.max_concurrent_streams.clamp(1, MAX_CONCURRENT_STREAMS);
        self.max_header_list_size = self
            .max_header_list_size
            .clamp(4 * 1024, MAX_HEADER_LIST_SIZE);
        self.max_header_fields = self.max_header_fields.clamp(8, MAX_HEADER_FIELDS);
        self.max_request_header_bytes = self
            .max_request_header_bytes
            .clamp(1024, MAX_HEADER_LIST_SIZE as usize);
        self.max_pending_requests = self.max_pending_requests.max(1);
        self.stream_window = self.stream_window.clamp(16 * 1024, 0x7FFF_FFFF);
        self.connection_window = self
            .connection_window
            .clamp(self.stream_window, 0x7FFF_FFFF);
        self.read_buffer = self.read_buffer.max(32 * 1024);
        self.write_buffer = self
            .write_buffer
            .max(32 * 1024)
            .max(self.max_request_header_bytes + 4 * 1024);
        self
    }
}
