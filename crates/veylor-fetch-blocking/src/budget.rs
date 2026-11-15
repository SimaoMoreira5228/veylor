use std::io;
use std::time::{Duration, Instant};

const MIN_TIMEOUT: Duration = Duration::from_millis(1);

#[derive(Clone, Copy, Debug)]
pub(crate) struct Budget {
    pub(crate) read: Option<Duration>,
    pub(crate) write: Option<Duration>,
    pub(crate) overall: Option<Instant>,
}

impl Budget {
    pub(crate) fn expired(&self) -> bool {
        self.overall
            .is_some_and(|deadline| Instant::now() >= deadline)
    }

    pub(crate) fn read_timeout(&self) -> io::Result<Option<Duration>> {
        self.clamp(self.read)
    }

    pub(crate) fn write_timeout(&self) -> io::Result<Option<Duration>> {
        self.clamp(self.write)
    }

    pub(crate) fn clamp(&self, limit: Option<Duration>) -> io::Result<Option<Duration>> {
        let Some(deadline) = self.overall else {
            return Ok(limit.map(|limit| limit.max(MIN_TIMEOUT)));
        };

        let now = Instant::now();

        if now >= deadline {
            return Err(io::ErrorKind::TimedOut.into());
        }

        let remaining = (deadline - now).max(MIN_TIMEOUT);

        match limit {
            Some(limit) => Ok(Some(limit.max(MIN_TIMEOUT).min(remaining))),
            None => Ok(Some(remaining)),
        }
    }
}
