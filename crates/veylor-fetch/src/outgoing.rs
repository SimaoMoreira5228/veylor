use crate::method::Method;

pub(crate) struct Outgoing<'a> {
    pub(crate) method: &'a Method,
    pub(crate) host: &'a str,
    pub(crate) target: &'a str,
    pub(crate) headers: &'a [(String, String)],
    pub(crate) body: &'a [u8],
}

impl Outgoing<'_> {
    pub(crate) fn expects_body(&self) -> bool {
        !self.body.is_empty() || matches!(self.method, Method::Post | Method::Put | Method::Patch)
    }
}
