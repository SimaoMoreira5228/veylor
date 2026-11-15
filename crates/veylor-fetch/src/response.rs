use std::io;
use std::time::Instant;

use veylor_client_driver::{HeaderIter, HeaderList, Protocol};

use crate::decode::Gunzip;
use crate::error::{Error, Result};
use crate::runtime::{Runtime, Stream, bounded};
use crate::url::Url;

pub struct Response {
    stream: Stream,
    url: Url,
    decoder: Option<Gunzip>,
    finished: bool,
    deadline: Option<Instant>,
    runtime: Runtime,
    max_body: usize,
}

impl std::fmt::Debug for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Response")
            .field("status", &self.status())
            .field("url", &self.url.as_str())
            .field("protocol", &self.protocol())
            .finish()
    }
}

impl Response {
    pub(crate) fn new(
        stream: Stream,
        url: Url,
        head_request: bool,
        deadline: Option<Instant>,
        runtime: Runtime,
        max_body: usize,
    ) -> Self {
        let bodiless = head_request || matches!(stream.status(), 204 | 304);
        let decoder = Gunzip::start(stream.header("content-encoding"), bodiless);

        Self {
            stream,
            url,
            decoder,
            finished: false,
            deadline,
            runtime,
            max_body,
        }
    }

    pub fn status(&self) -> u16 {
        self.stream.status()
    }

    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status())
    }

    pub fn headers(&self) -> HeaderIter<'_> {
        self.stream.headers()
    }

    pub fn header(&self, name: &str) -> Option<&[u8]> {
        self.stream.header(name)
    }

    pub fn header_str(&self, name: &str) -> Option<&str> {
        std::str::from_utf8(self.header(name)?).ok()
    }

    pub fn protocol(&self) -> Protocol {
        self.stream.protocol()
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    pub fn error_for_status(self) -> Result<Self> {
        self.error_for_status_ref()?;

        Ok(self)
    }

    pub fn error_for_status_ref(&self) -> Result<&Self> {
        let status = self.status();

        if status < 400 {
            return Ok(self);
        }

        Err(Error::Status {
            status,
            url: self.url.to_string(),
        })
    }

    pub async fn chunk(&mut self) -> Result<Option<&[u8]>> {
        let Self {
            stream,
            decoder,
            finished,
            deadline,
            runtime,
            ..
        } = self;

        if *finished {
            return Ok(None);
        }

        let Some(decoder) = decoder else {
            let next = bounded(*runtime, *deadline, stream.chunk())
                .await
                .map_err(Error::from_body)?;

            *finished = next.is_none();

            return Ok(next);
        };

        loop {
            let next = bounded(*runtime, *deadline, stream.chunk())
                .await
                .map_err(Error::from_body)?;

            let Some(raw) = next else {
                *finished = true;

                decoder.finish()?;

                if decoder.output().is_empty() {
                    return Ok(None);
                }

                return Ok(Some(decoder.output()));
            };

            decoder.feed(raw)?;

            if !decoder.output().is_empty() {
                return Ok(Some(decoder.output()));
            }
        }
    }

    pub fn trailers(&mut self) -> Option<HeaderList> {
        self.stream.trailers()
    }

    pub async fn bytes(self) -> Result<Vec<u8>> {
        if self.decoder.is_some() {
            return Ok(self.bytes_and_trailers().await?.0);
        }

        let Self {
            stream,
            deadline,
            runtime,
            max_body,
            ..
        } = self;

        bounded(runtime, deadline, stream.collect(max_body))
            .await
            .map_err(Error::from_body)
    }

    pub async fn bytes_and_trailers(mut self) -> Result<(Vec<u8>, Option<HeaderList>)> {
        let mut body = Vec::new();
        let max = self.max_body;

        while let Some(chunk) = self.chunk().await? {
            if body.len() + chunk.len() > max {
                return Err(Error::Body(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "response body exceeds the limit",
                )));
            }

            body.extend_from_slice(chunk);
        }

        Ok((body, self.stream.trailers()))
    }

    pub async fn text(self) -> Result<String> {
        let bytes = self.bytes().await?;

        Ok(match String::from_utf8(bytes) {
            Ok(text) => text,
            Err(error) => String::from_utf8_lossy(error.as_bytes()).into_owned(),
        })
    }

    #[cfg(feature = "json")]
    pub async fn json<T: serde::de::DeserializeOwned>(self) -> Result<T> {
        let bytes = self.bytes().await?;

        serde_json::from_slice(&bytes).map_err(|e| Error::Decode(e.to_string()))
    }
}
