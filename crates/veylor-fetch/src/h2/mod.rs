mod driver;
mod fields;
mod inbound;
mod outbound;
mod reader;
mod shared;

use std::io;
use std::sync::Arc;
use std::time::Duration;

use crate::budget::Budget;
use crate::error::Error;
use crate::headers::Headers;
use crate::outgoing::Outgoing;
use crate::wire::Wire;

use shared::{DriverShared, StreamShared};

pub(crate) const MAX_STREAMS: usize = 100;

#[derive(Clone, Copy)]
pub(crate) struct H2Settings {
    pub(crate) max_head: usize,
    pub(crate) idle_timeout: Duration,
    pub(crate) handshake_timeout: Duration,
    pub(crate) write_timeout: Option<Duration>,
}

pub(super) struct Command {
    pub(super) method: String,
    pub(super) authority: String,
    pub(super) path: String,
    pub(super) headers: Vec<(String, String)>,
    pub(super) body: Vec<u8>,
    pub(super) expects_body: bool,
}

pub(crate) struct Refused {
    pub(crate) error: io::Error,
    pub(crate) retryable: bool,
    pub(crate) unprocessed: bool,
}

pub(crate) struct H2Handle {
    shared: Arc<DriverShared>,
}

pub(crate) struct H2Stream {
    shared: Arc<StreamShared>,
}

impl H2Handle {
    pub(crate) fn spawn(wire: Wire, settings: H2Settings) -> Result<Arc<Self>, Error> {
        let reader = wire.reader().map_err(Error::Io)?;
        let shared = DriverShared::new(MAX_STREAMS);
        let worker = Arc::clone(&shared);
        let feeder = Arc::clone(&shared);

        std::thread::Builder::new()
            .name("veylor-fetch-h2".to_owned())
            .spawn(move || driver::run(wire, worker, settings))
            .map_err(Error::Io)?;

        let spawned = std::thread::Builder::new()
            .name("veylor-fetch-h2-read".to_owned())
            .spawn(move || reader::run(reader, feeder));

        if let Err(error) = spawned {
            shared.core.lock().closing = true;
            shared.wake.notify_one();

            return Err(Error::Io(error));
        }

        Ok(Arc::new(Self { shared }))
    }

    pub(crate) fn is_dead(&self) -> bool {
        !self.shared.is_open()
    }

    pub(crate) fn load(&self) -> usize {
        self.shared
            .inflight
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn reserve(&self, force: bool) -> bool {
        self.shared.reserve(force)
    }

    pub(crate) fn request(
        &self,
        request: &Outgoing<'_>,
        authority: &str,
        budget: &Budget,
    ) -> Result<(u16, Headers, H2Stream), Refused> {
        let command = Command {
            method: request.method.as_str().to_owned(),
            authority: authority.to_owned(),
            path: request.target.to_owned(),
            headers: fields::lowered(request.headers),
            body: request.body.to_vec(),
            expects_body: request.expects_body(),
        };
        let shared = self.shared.submit(command, budget.overall)?;
        let stream = H2Stream { shared };
        let (status, headers) = stream.shared.wait_head(budget)?;

        Ok((status, headers, stream))
    }
}

impl Drop for H2Handle {
    fn drop(&mut self) {
        self.shared.core.lock().closing = true;
        self.shared.wake.notify_one();
    }
}

impl H2Stream {
    pub(crate) fn read(&self, buf: &mut [u8], budget: &Budget) -> io::Result<usize> {
        self.shared.read(buf, budget)
    }

    pub(crate) fn cancel(&self) {
        self.shared.cancel();
    }
}

impl Drop for H2Stream {
    fn drop(&mut self) {
        if !self.shared.is_terminal() {
            self.shared.cancel();
        }
    }
}
