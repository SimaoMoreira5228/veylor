use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::config::H2Config;
use super::error::H2Error;
use super::request::H2Request;
use super::response::{H2Body, H2Response};
use super::shared::{DriverShared, STATE_CLOSED};

pub struct H2Client {
    shared: Arc<DriverShared>,
}

pub struct H2Start {
    pub(super) config: H2Config,
    pub(super) shared: Arc<DriverShared>,
}

impl H2Client {
    pub fn new(config: H2Config) -> (Self, H2Start) {
        let config = config.normalized();
        let shared = DriverShared::new(
            config.max_pending_requests,
            config.stream_timeout,
            config.max_concurrent_streams,
        );
        let client = Self {
            shared: shared.clone(),
        };

        (client, H2Start { config, shared })
    }

    pub async fn send(&self, request: H2Request) -> Result<H2Response, H2Error> {
        let stream = self.shared.submit(request)?;
        let mut body = H2Body::new(stream);
        let (status, headers) = body.head().await?;
        Ok(H2Response {
            status,
            headers,
            body,
        })
    }

    pub fn stream_limit(&self) -> usize {
        self.shared.stream_limit.load(Ordering::Acquire)
    }

    pub fn is_reusable(&self) -> bool {
        self.shared.is_reusable()
    }

    pub async fn close(&self) {
        self.shared.close_requested.store(true, Ordering::Release);
        self.shared.notify.notify(1);
        loop {
            let exited = self.shared.exited.listen();
            if self.shared.state.load(Ordering::Acquire) == STATE_CLOSED {
                return;
            }
            exited.await;
        }
    }
}

impl Clone for H2Client {
    fn clone(&self) -> Self {
        self.shared.handles.fetch_add(1, Ordering::AcqRel);
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl Drop for H2Client {
    fn drop(&mut self) {
        if self.shared.handles.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.shared.notify.notify(1);
        }
    }
}
