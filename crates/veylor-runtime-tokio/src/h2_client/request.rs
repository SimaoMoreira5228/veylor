use std::sync::Arc;
use std::time::Duration;

use super::error::H2Error;
use super::headers::HeaderList;
use super::upload::{Upload, UploadSender};

pub(crate) enum RequestBody {
    Empty,
    Full(Vec<u8>),
    Stream(Arc<Upload>),
}

pub struct H2Request {
    pub(crate) method: Vec<u8>,
    pub(crate) scheme: Vec<u8>,
    pub(crate) authority: Vec<u8>,
    pub(crate) path: Vec<u8>,
    pub(crate) headers: HeaderList,
    pub(crate) body: RequestBody,
    pub(crate) timeout: Option<Option<Duration>>,
}

impl H2Request {
    pub fn new(method: &[u8], authority: &[u8], path: &[u8]) -> Self {
        Self {
            method: method.to_vec(),
            scheme: b"http".to_vec(),
            authority: authority.to_vec(),
            path: path.to_vec(),
            headers: HeaderList::default(),
            body: RequestBody::Empty,
            timeout: None,
        }
    }

    pub fn get(authority: &[u8], path: &[u8]) -> Self {
        Self::new(b"GET", authority, path)
    }

    pub fn header(mut self, name: &[u8], value: &[u8]) -> Self {
        self.headers.push(name, value);
        self
    }

    pub fn scheme(mut self, scheme: &[u8]) -> Self {
        self.scheme = scheme.to_vec();
        self
    }

    pub fn body(mut self, body: Vec<u8>) -> Self {
        self.body = if body.is_empty() {
            RequestBody::Empty
        } else {
            RequestBody::Full(body)
        };
        self
    }

    pub fn timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn streaming_body(mut self, capacity: usize) -> (Self, UploadSender) {
        let upload = Arc::new(Upload::new(capacity.max(1)));
        self.body = RequestBody::Stream(upload.clone());
        (self, UploadSender::new(upload))
    }

    pub(crate) fn with_upload(mut self, upload: Arc<Upload>) -> Self {
        self.body = RequestBody::Stream(upload);
        self
    }

    pub(crate) fn validate(&self) -> Result<(), H2Error> {
        if self.method.is_empty() || self.path.is_empty() || self.authority.is_empty() {
            return Err(H2Error::InvalidRequest);
        }
        Ok(())
    }
}
