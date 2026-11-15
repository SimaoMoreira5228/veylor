use std::time::Duration;

use veylor_client::h2::STREAM_SLOTS;

#[derive(Clone, Debug)]
pub struct H2Config {
    pub max_concurrent_streams: usize,
    pub max_pending_requests: usize,
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
            max_concurrent_streams: STREAM_SLOTS,
            max_pending_requests: 128,
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
    pub(crate) fn normalized(mut self) -> Self {
        self.max_concurrent_streams = self.max_concurrent_streams.clamp(1, STREAM_SLOTS - 2);
        self.max_pending_requests = self.max_pending_requests.max(1);
        self.stream_window = self.stream_window.clamp(16 * 1024, 0x7FFF_FFFF);
        self.connection_window = self
            .connection_window
            .clamp(self.stream_window, 0x7FFF_FFFF);
        self.read_buffer = self.read_buffer.max(32 * 1024);
        self.write_buffer = self.write_buffer.max(32 * 1024);
        self
    }
}
