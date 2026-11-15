use std::io::{self, Read};

use crate::agent::Agent;
use crate::budget::Budget;
use crate::encoding;
use crate::error::Error;
use crate::h1::H1Conn;
use crate::h2::H2Stream;
use crate::headers::{HeaderBytesIter, HeaderIter, Headers};
use crate::pool::Route;
use crate::protocol::Protocol;
use crate::status;

const REDIRECT_DRAIN_LIMIT: usize = 64 * 1024;

pub(crate) struct H1Body {
    pub(crate) conn: Option<Box<H1Conn>>,
    pub(crate) agent: Agent,
    pub(crate) route: Route,
    pub(crate) budget: Budget,
}

pub(crate) struct H2Body {
    pub(crate) stream: H2Stream,
    pub(crate) budget: Budget,
}

pub(crate) enum Body {
    H1(H1Body),
    H2(H2Body),
}

impl H1Body {
    fn give_back(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.agent.pool().put_h1(&self.route, conn);
        }
    }

    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let Some(conn) = self.conn.as_mut() else {
            return Ok(0);
        };

        if buf.is_empty() {
            return Ok(0);
        }

        let data = match conn.fill_buf(&self.budget) {
            Ok(data) => data,
            Err(error) => {
                self.conn = None;

                return Err(error);
            }
        };

        if data.is_empty() {
            self.give_back();

            return Ok(0);
        }

        let count = data.len().min(buf.len());

        buf[..count].copy_from_slice(&data[..count]);
        conn.consume(count);

        Ok(count)
    }

    fn discard(&mut self, limit: usize) {
        let Some(conn) = self.conn.as_mut() else {
            return;
        };

        if conn.drain(&self.budget, limit) {
            self.give_back();
        } else {
            self.conn = None;
        }
    }
}

impl Drop for H1Body {
    fn drop(&mut self) {
        let Some(conn) = self.conn.as_mut() else {
            return;
        };

        if conn.drain_buffered() {
            self.give_back();
        }
    }
}

impl H2Body {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let result = self.stream.read(buf, &self.budget);

        if result.is_err() {
            self.stream.cancel();
        }

        result
    }
}

struct RawReader {
    body: Body,
}

impl Read for RawReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match &mut self.body {
            Body::H1(body) => body.read(buf),
            Body::H2(body) => body.read(buf),
        }
    }
}

enum Source {
    Plain(RawReader),
    #[cfg(feature = "gzip")]
    Gzip(flate2::read::GzDecoder<RawReader>),
}

pub struct BodyReader {
    source: Source,
}

impl Read for BodyReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match &mut self.source {
            Source::Plain(reader) => reader.read(buf),
            #[cfg(feature = "gzip")]
            Source::Gzip(reader) => reader.read(buf),
        }
    }
}

pub struct Response {
    status: u16,
    protocol: Protocol,
    headers: Headers,
    url: String,
    max_body: usize,
    gzip: bool,
    body: Body,
}

impl Response {
    pub(crate) fn new(
        status: u16,
        protocol: Protocol,
        headers: Headers,
        url: String,
        max_body: usize,
        head_request: bool,
        body: Body,
    ) -> Self {
        let bodiless = head_request || matches!(status, 204 | 304);
        let gzip = cfg!(feature = "gzip")
            && !bodiless
            && headers
                .get("content-encoding")
                .is_some_and(encoding::is_gzip);

        Self {
            status,
            protocol,
            headers,
            url,
            max_body,
            gzip,
            body,
        }
    }

    pub fn status(&self) -> u16 {
        self.status
    }

    pub fn protocol(&self) -> Protocol {
        self.protocol
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        std::str::from_utf8(self.header_bytes(name)?).ok()
    }

    pub fn header_bytes(&self, name: &str) -> Option<&[u8]> {
        self.headers.get(name)
    }

    pub fn headers(&self) -> HeaderIter<'_> {
        self.headers.iter()
    }

    pub fn headers_bytes(&self) -> HeaderBytesIter<'_> {
        self.headers.iter_bytes()
    }

    pub fn is_success(&self) -> bool {
        status::is_success(self.status)
    }

    pub fn into_reader(self) -> BodyReader {
        let reader = RawReader { body: self.body };

        #[cfg(feature = "gzip")]
        if self.gzip {
            return BodyReader {
                source: Source::Gzip(flate2::read::GzDecoder::new(reader)),
            };
        }

        BodyReader {
            source: Source::Plain(reader),
        }
    }

    pub fn bytes(self) -> Result<Vec<u8>, Error> {
        let limit = self.max_body;
        let declared = self
            .header("content-length")
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|_| !self.gzip);

        if declared.is_some_and(|length| length > limit) {
            return Err(Error::BodyTooLarge { limit });
        }

        let mut bytes = Vec::with_capacity(declared.unwrap_or(0).min(limit));
        let mut reader = self.into_reader().take(limit as u64 + 1);

        reader
            .read_to_end(&mut bytes)
            .map_err(|error| Error::from_io(error, crate::error::TimeoutKind::Read, false))?;

        if bytes.len() > limit {
            return Err(Error::BodyTooLarge { limit });
        }

        Ok(bytes)
    }

    pub fn text(self) -> Result<String, Error> {
        let bytes = self.bytes()?;

        Ok(match String::from_utf8(bytes) {
            Ok(text) => text,
            Err(error) => String::from_utf8_lossy(error.as_bytes()).into_owned(),
        })
    }

    #[cfg(feature = "json")]
    pub fn json<T: serde::de::DeserializeOwned>(self) -> Result<T, Error> {
        let bytes = self.bytes()?;

        serde_json::from_slice(&bytes).map_err(|error| Error::Json(error.to_string()))
    }

    pub(crate) fn discard(&mut self) {
        match &mut self.body {
            Body::H1(body) => body.discard(REDIRECT_DRAIN_LIMIT),
            Body::H2(_) => {}
        }
    }
}

impl std::fmt::Debug for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Response")
            .field("status", &self.status)
            .field("protocol", &self.protocol)
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}
