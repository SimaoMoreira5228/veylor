#[derive(Debug, Default)]
pub enum Body {
    #[default]
    Empty,
    Static(&'static [u8]),
    Owned(Vec<u8>),
}

impl Body {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Body::Empty => &[],
            Body::Static(bytes) => bytes,
            Body::Owned(bytes) => bytes,
        }
    }
}

impl From<&'static [u8]> for Body {
    fn from(bytes: &'static [u8]) -> Self {
        Body::Static(bytes)
    }
}

impl From<&'static str> for Body {
    fn from(text: &'static str) -> Self {
        Body::Static(text.as_bytes())
    }
}

impl From<Vec<u8>> for Body {
    fn from(bytes: Vec<u8>) -> Self {
        Body::Owned(bytes)
    }
}

impl From<String> for Body {
    fn from(text: String) -> Self {
        Body::Owned(text.into_bytes())
    }
}

pub struct Response {
    status: u16,
    headers: Vec<(Vec<u8>, Vec<u8>)>,
    body: Body,
}

impl Response {
    pub fn new(status: u16, body: impl Into<Body>) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: body.into(),
        }
    }

    pub fn ok(body: impl Into<Body>) -> Self {
        Self::new(200, body)
    }

    pub fn text(body: impl Into<Body>) -> Self {
        Self::ok(body).header("content-type", "text/plain; charset=utf-8")
    }

    pub fn not_found() -> Self {
        Self::new(404, &b"Not Found"[..])
    }

    pub fn status(mut self, status: u16) -> Self {
        self.status = status;
        self
    }

    pub fn header(mut self, name: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    pub fn status_code(&self) -> u16 {
        self.status
    }

    pub fn headers(&self) -> &[(Vec<u8>, Vec<u8>)] {
        &self.headers
    }

    pub fn body(&self) -> &Body {
        &self.body
    }

    pub fn into_body(self) -> Body {
        self.body
    }
}

impl From<&'static str> for Response {
    fn from(text: &'static str) -> Self {
        Response::ok(text)
    }
}

impl From<String> for Response {
    fn from(text: String) -> Self {
        Response::ok(text)
    }
}

impl From<&'static [u8]> for Response {
    fn from(bytes: &'static [u8]) -> Self {
        Response::ok(bytes)
    }
}

impl From<Vec<u8>> for Response {
    fn from(bytes: Vec<u8>) -> Self {
        Response::ok(bytes)
    }
}

impl From<(u16, &'static str)> for Response {
    fn from((status, text): (u16, &'static str)) -> Self {
        Response::new(status, text)
    }
}

impl From<(u16, String)> for Response {
    fn from((status, text): (u16, String)) -> Self {
        Response::new(status, text)
    }
}

impl From<(u16, Vec<u8>)> for Response {
    fn from((status, bytes): (u16, Vec<u8>)) -> Self {
        Response::new(status, bytes)
    }
}

pub trait IntoResponse {
    fn into_response(self) -> Response;
}

impl<T: Into<Response>> IntoResponse for T {
    fn into_response(self) -> Response {
        self.into()
    }
}

// NOTE: reason phrases exist only for the H1 status line; unknown codes fall back to a
// bare status line, which is legal because the reason phrase is advisory.
pub(crate) fn reason(status: u16) -> &'static [u8] {
    match status {
        200 => b"OK",
        201 => b"Created",
        202 => b"Accepted",
        204 => b"No Content",
        206 => b"Partial Content",
        301 => b"Moved Permanently",
        302 => b"Found",
        304 => b"Not Modified",
        307 => b"Temporary Redirect",
        308 => b"Permanent Redirect",
        400 => b"Bad Request",
        401 => b"Unauthorized",
        403 => b"Forbidden",
        404 => b"Not Found",
        405 => b"Method Not Allowed",
        409 => b"Conflict",
        413 => b"Content Too Large",
        415 => b"Unsupported Media Type",
        422 => b"Unprocessable Content",
        429 => b"Too Many Requests",
        500 => b"Internal Server Error",
        501 => b"Not Implemented",
        502 => b"Bad Gateway",
        503 => b"Service Unavailable",
        504 => b"Gateway Timeout",
        _ => b"",
    }
}
