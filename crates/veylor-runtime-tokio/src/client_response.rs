use std::io;

use crate::client_conn::protocol as protocol_error;
use crate::client_pool::{H2Ticket, Lease, Protocol};
use crate::h2_client::{H2Body, H2Error, HeaderIter, HeaderList};

const CHUNK: usize = 16 * 1024;

enum Body {
    Http1(Lease),
    Http2 {
        body: H2Body,
        scratch: Vec<u8>,
        _ticket: H2Ticket,
    },
}

pub struct ResponseStream {
    status: u16,
    headers: HeaderList,
    body: Body,
}

pub struct ClientResponse {
    pub status: u16,
    pub headers: HeaderList,
    pub trailers: Option<HeaderList>,
    pub protocol: Protocol,
    pub body: Vec<u8>,
}

impl ResponseStream {
    pub(crate) fn http1(status: u16, headers: HeaderList, lease: Lease) -> Self {
        Self {
            status,
            headers,
            body: Body::Http1(lease),
        }
    }

    pub(crate) fn http2(status: u16, headers: HeaderList, body: H2Body, ticket: H2Ticket) -> Self {
        Self {
            status,
            headers,
            body: Body::Http2 {
                body,
                scratch: Vec::new(),
                _ticket: ticket,
            },
        }
    }

    pub fn status(&self) -> u16 {
        self.status
    }

    pub fn headers(&self) -> HeaderIter<'_> {
        self.headers.iter()
    }

    pub fn header(&self, name: &str) -> Option<&[u8]> {
        self.headers.get(name.as_bytes())
    }

    pub fn protocol(&self) -> Protocol {
        match self.body {
            Body::Http1(_) => Protocol::Http1,
            Body::Http2 { .. } => Protocol::Http2,
        }
    }

    pub fn alpn(&self) -> Option<&[u8]> {
        match &self.body {
            Body::Http1(lease) => lease.conn_ref().alpn(),
            Body::Http2 { .. } => Some(b"h2"),
        }
    }

    pub async fn chunk(&mut self) -> io::Result<Option<&[u8]>> {
        match &mut self.body {
            Body::Http1(lease) => lease.conn().next_chunk().await,
            Body::Http2 { body, scratch, .. } => {
                if scratch.is_empty() {
                    scratch.resize(CHUNK, 0);
                }

                let read = body.read(scratch).await?;

                if read == 0 {
                    return Ok(None);
                }

                Ok(Some(&scratch[..read]))
            }
        }
    }

    pub fn trailers(&mut self) -> Option<HeaderList> {
        match &mut self.body {
            Body::Http1(lease) => lease.conn().take_trailers(),
            Body::Http2 { body, .. } => body.trailers(),
        }
    }

    pub async fn collect(mut self, max: usize) -> io::Result<Vec<u8>> {
        self.collect_body(max).await
    }

    async fn collect_body(&mut self, max: usize) -> io::Result<Vec<u8>> {
        if let Body::Http2 { body, .. } = &mut self.body {
            return body.read_to_end(max).await.map_err(|error| match error {
                H2Error::BodyOverflow => limit_error(),
                other => other.into(),
            });
        }

        let mut body = Vec::new();

        while let Some(chunk) = self.chunk().await? {
            if body.len() + chunk.len() > max {
                return Err(limit_error());
            }

            body.extend_from_slice(chunk);
        }

        Ok(body)
    }

    pub async fn into_buffered(mut self, max: usize) -> io::Result<ClientResponse> {
        let body = self.collect_body(max).await?;
        let trailers = self.trailers();

        Ok(ClientResponse {
            status: self.status,
            protocol: self.protocol(),
            headers: self.headers,
            trailers,
            body,
        })
    }
}

fn limit_error() -> io::Error {
    protocol_error("response body exceeds the limit")
}
