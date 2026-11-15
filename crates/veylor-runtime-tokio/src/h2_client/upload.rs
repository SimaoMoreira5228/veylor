use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;
use tokio::sync::Notify;

use super::error::H2Error;
use super::shared::DriverShared;

struct UploadBuffer {
    data: Vec<u8>,
    start: usize,
    finished: bool,
    aborted: bool,
}

pub(crate) struct Upload {
    buffer: Mutex<UploadBuffer>,
    space: Notify,
    driver: OnceLock<Arc<DriverShared>>,
    capacity: usize,
}

pub(crate) enum UploadState {
    Aborted,
    Open,
    Finished,
}

impl Upload {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            buffer: Mutex::new(UploadBuffer {
                data: Vec::new(),
                start: 0,
                finished: false,
                aborted: false,
            }),
            space: Notify::new(),
            driver: OnceLock::new(),
            capacity,
        }
    }

    pub(crate) fn pair(capacity: usize) -> (Arc<Self>, UploadSender) {
        let upload = Arc::new(Self::new(capacity.max(1)));
        let sender = UploadSender::new(upload.clone());

        (upload, sender)
    }

    pub(crate) fn attach(&self, driver: &Arc<DriverShared>) {
        let _ = self.driver.set(driver.clone());
    }

    fn wake_driver(&self) {
        if let Some(driver) = self.driver.get() {
            driver.notify.notify_one();
        }
    }

    pub(crate) fn drain(&self, send: impl FnOnce(&[u8], bool) -> usize) -> UploadState {
        let mut buffer = self.buffer.lock();
        if buffer.aborted {
            return UploadState::Aborted;
        }
        let finished = buffer.finished;
        let taken = send(&buffer.data[buffer.start..], finished);
        if taken > 0 {
            buffer.start += taken;
            if buffer.start == buffer.data.len() {
                buffer.data.clear();
                buffer.start = 0;
            }
            self.space.notify_one();
        }
        if finished && buffer.start == buffer.data.len() {
            return UploadState::Finished;
        }
        UploadState::Open
    }

    pub(crate) fn has_data(&self) -> bool {
        let buffer = self.buffer.lock();
        buffer.start < buffer.data.len() || (buffer.finished && !buffer.aborted)
    }

    pub(crate) fn abort(&self) {
        self.buffer.lock().aborted = true;
        self.space.notify_one();
    }
}

pub struct UploadSender {
    upload: Option<Arc<Upload>>,
}

impl UploadSender {
    pub(crate) fn new(upload: Arc<Upload>) -> Self {
        Self {
            upload: Some(upload),
        }
    }

    pub async fn write_all(&mut self, mut data: &[u8]) -> Result<(), H2Error> {
        let Some(upload) = self.upload.as_ref() else {
            return Err(H2Error::Closed);
        };
        while !data.is_empty() {
            let waiting = upload.space.notified();
            let taken = {
                let mut buffer = upload.buffer.lock();
                if buffer.aborted {
                    return Err(H2Error::Cancelled);
                }
                if buffer.start > 0 {
                    let start = buffer.start;
                    buffer.data.drain(..start);
                    buffer.start = 0;
                }
                let free = upload.capacity - (buffer.data.len() - buffer.start);
                let taken = free.min(data.len());
                buffer.data.extend_from_slice(&data[..taken]);
                taken
            };
            if taken > 0 {
                upload.wake_driver();
                data = &data[taken..];
                continue;
            }
            waiting.await;
        }
        Ok(())
    }

    pub fn finish(mut self) {
        let Some(upload) = self.upload.take() else {
            return;
        };
        upload.buffer.lock().finished = true;
        upload.wake_driver();
    }
}

impl Drop for UploadSender {
    fn drop(&mut self) {
        let Some(upload) = self.upload.take() else {
            return;
        };
        upload.buffer.lock().aborted = true;
        upload.wake_driver();
    }
}
