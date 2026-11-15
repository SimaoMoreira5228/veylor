use std::future::Future;
use std::io;
use std::time::{Duration, Instant};

use veylor_client_driver::{
    ClientConfig, ClientRequest, HeaderIter, HeaderList, PoolStats, Protocol,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Runtime {
    #[cfg(feature = "tokio")]
    Tokio,
    #[cfg(feature = "smol")]
    Smol,
}

impl Default for Runtime {
    fn default() -> Self {
        #[cfg(feature = "tokio")]
        return Self::Tokio;

        #[cfg(all(feature = "smol", not(feature = "tokio")))]
        return Self::Smol;
    }
}

pub(crate) enum Transport {
    #[cfg(feature = "tokio")]
    Tokio(veylor_runtime_tokio::Client),
    #[cfg(feature = "smol")]
    Smol(veylor_runtime_smol::Client),
}

pub(crate) enum Stream {
    #[cfg(feature = "tokio")]
    Tokio(veylor_runtime_tokio::ResponseStream),
    #[cfg(feature = "smol")]
    Smol(veylor_runtime_smol::ResponseStream),
}

impl Transport {
    pub fn new(runtime: Runtime, config: ClientConfig) -> io::Result<Self> {
        match runtime {
            #[cfg(feature = "tokio")]
            Runtime::Tokio => Ok(Self::Tokio(veylor_runtime_tokio::Client::new(config)?)),
            #[cfg(feature = "smol")]
            Runtime::Smol => Ok(Self::Smol(veylor_runtime_smol::Client::new(config)?)),
        }
    }

    pub fn runtime(&self) -> Runtime {
        match self {
            #[cfg(feature = "tokio")]
            Self::Tokio(_) => Runtime::Tokio,
            #[cfg(feature = "smol")]
            Self::Smol(_) => Runtime::Smol,
        }
    }

    pub async fn send(&self, request: &ClientRequest<'_>) -> io::Result<Stream> {
        match self {
            #[cfg(feature = "tokio")]
            Self::Tokio(client) => client.send(request).await.map(Stream::Tokio),
            #[cfg(feature = "smol")]
            Self::Smol(client) => client.send(request).await.map(Stream::Smol),
        }
    }

    pub fn stats(&self) -> PoolStats {
        match self {
            #[cfg(feature = "tokio")]
            Self::Tokio(client) => client.pool_stats(),
            #[cfg(feature = "smol")]
            Self::Smol(client) => client.pool_stats(),
        }
    }

    pub fn close_idle(&self) {
        match self {
            #[cfg(feature = "tokio")]
            Self::Tokio(client) => client.close_idle(),
            #[cfg(feature = "smol")]
            Self::Smol(client) => client.close_idle(),
        }
    }
}

impl Stream {
    pub fn status(&self) -> u16 {
        match self {
            #[cfg(feature = "tokio")]
            Self::Tokio(stream) => stream.status(),
            #[cfg(feature = "smol")]
            Self::Smol(stream) => stream.status(),
        }
    }

    pub fn headers(&self) -> HeaderIter<'_> {
        match self {
            #[cfg(feature = "tokio")]
            Self::Tokio(stream) => stream.headers(),
            #[cfg(feature = "smol")]
            Self::Smol(stream) => stream.headers(),
        }
    }

    pub fn header(&self, name: &str) -> Option<&[u8]> {
        match self {
            #[cfg(feature = "tokio")]
            Self::Tokio(stream) => stream.header(name),
            #[cfg(feature = "smol")]
            Self::Smol(stream) => stream.header(name),
        }
    }

    pub fn protocol(&self) -> Protocol {
        match self {
            #[cfg(feature = "tokio")]
            Self::Tokio(stream) => stream.protocol(),
            #[cfg(feature = "smol")]
            Self::Smol(stream) => stream.protocol(),
        }
    }

    pub async fn chunk(&mut self) -> io::Result<Option<&[u8]>> {
        match self {
            #[cfg(feature = "tokio")]
            Self::Tokio(stream) => stream.chunk().await,
            #[cfg(feature = "smol")]
            Self::Smol(stream) => stream.chunk().await,
        }
    }

    pub fn trailers(&mut self) -> Option<HeaderList> {
        match self {
            #[cfg(feature = "tokio")]
            Self::Tokio(stream) => stream.trailers(),
            #[cfg(feature = "smol")]
            Self::Smol(stream) => stream.trailers(),
        }
    }

    pub async fn collect(self, max: usize) -> io::Result<Vec<u8>> {
        match self {
            #[cfg(feature = "tokio")]
            Self::Tokio(stream) => stream.collect(max).await,
            #[cfg(feature = "smol")]
            Self::Smol(stream) => stream.collect(max).await,
        }
    }
}

pub(crate) async fn bounded<T>(
    runtime: Runtime,
    deadline: Option<Instant>,
    work: impl Future<Output = io::Result<T>>,
) -> io::Result<T> {
    let Some(deadline) = deadline else {
        return work.await;
    };
    let remaining = deadline.saturating_duration_since(Instant::now());

    match runtime {
        #[cfg(feature = "tokio")]
        Runtime::Tokio => match tokio::time::timeout(remaining, work).await {
            Ok(done) => done,
            Err(_) => Err(expired()),
        },
        #[cfg(feature = "smol")]
        Runtime::Smol => {
            let finished = async { Some(work.await) };
            let elapsed = async {
                smol::Timer::after(remaining).await;

                None
            };

            match smol::future::or(finished, elapsed).await {
                Some(done) => done,
                None => Err(expired()),
            }
        }
    }
}

fn expired() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "request timed out")
}

pub(crate) fn deadline_after(timeout: Option<Duration>) -> Option<Instant> {
    timeout.and_then(|timeout| Instant::now().checked_add(timeout))
}
