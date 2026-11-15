use std::io::{self, Read};
use std::sync::Arc;

use crate::agent::Agent;
use crate::budget::Budget;
use crate::error::Error;
use crate::h1::H1Conn;
use crate::h2::H2Handle;
use crate::headers::{HeaderIter, Headers};
use crate::url::Origin;

const REDIRECT_DRAIN_LIMIT: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Protocol {
    Http11,
    Http2,
}

pub(crate) struct H1Body {
    pub(crate) conn: Option<Box<H1Conn>>,
    pub(crate) agent: Agent,
    pub(crate) origin: Origin,
    pub(crate) budget: Budget,
}

pub(crate) struct H2Body {
    pub(crate) handle: Arc<H2Handle>,
    pub(crate) budget: Budget,
    pub(crate) released: bool,
}

pub(crate) enum Body {
    H1(H1Body),
    H2(H2Body),
}

impl H1Body {
    fn give_back(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.agent.pool().put_h1(&self.origin, conn);
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
    fn release(&mut self) {
        if !self.released {
            self.released = true;
            self.handle.release();
        }
    }

    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.released || buf.is_empty() {
            return Ok(0);
        }

        match self.handle.read(buf, &self.budget) {
            Ok(0) => {
                self.release();

                Ok(0)
            }
            Ok(count) => Ok(count),
            Err(error) => {
                self.handle.abandon();
                self.release();

                Err(error)
            }
        }
    }
}

impl Drop for H2Body {
    fn drop(&mut self) {
        if !self.handle.is_finished() {
            self.handle.abandon();
        }

        self.release();
    }
}

pub struct BodyReader {
    body: Body,
}

impl Read for BodyReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match &mut self.body {
            Body::H1(body) => body.read(buf),
            Body::H2(body) => body.read(buf),
        }
    }
}

pub struct Response {
    status: u16,
    protocol: Protocol,
    headers: Headers,
    url: String,
    max_body: usize,
    body: Body,
}

impl Response {
    pub(crate) fn new(
        status: u16,
        protocol: Protocol,
        headers: Headers,
        url: String,
        max_body: usize,
        body: Body,
    ) -> Self {
        Self {
            status,
            protocol,
            headers,
            url,
            max_body,
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
        self.headers.get(name)
    }

    pub fn headers(&self) -> HeaderIter<'_> {
        self.headers.iter()
    }

    pub fn into_reader(self) -> BodyReader {
        BodyReader { body: self.body }
    }

    pub fn into_bytes(self) -> Result<Vec<u8>, Error> {
        let limit = self.max_body;
        let declared = self
            .header("content-length")
            .and_then(|value| value.trim().parse::<usize>().ok());

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

    pub fn into_string(self) -> Result<String, Error> {
        let bytes = self.into_bytes()?;

        String::from_utf8(bytes).map_err(|error| {
            Error::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                error.to_string(),
            ))
        })
    }

    #[cfg(feature = "json")]
    pub fn into_json<T: serde::de::DeserializeOwned>(self) -> Result<T, Error> {
        let bytes = self.into_bytes()?;

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
