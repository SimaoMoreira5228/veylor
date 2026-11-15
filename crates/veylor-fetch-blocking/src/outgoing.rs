use crate::error::Error;

pub(crate) struct Outgoing<'a> {
    pub(crate) method: &'a str,
    pub(crate) host: &'a str,
    pub(crate) target: &'a str,
    pub(crate) headers: &'a [(String, String)],
    pub(crate) body: &'a [u8],
}

impl Outgoing<'_> {
    pub(crate) fn is_idempotent(&self) -> bool {
        matches!(
            self.method,
            "GET" | "HEAD" | "PUT" | "DELETE" | "OPTIONS" | "TRACE"
        )
    }

    pub(crate) fn expects_body(&self) -> bool {
        !self.body.is_empty() || matches!(self.method, "POST" | "PUT" | "PATCH")
    }
}

pub(crate) fn check_header(name: &str, value: &str) -> Result<(), Error> {
    let name_ok = !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte));
    let value_ok = value
        .bytes()
        .all(|byte| (byte >= 0x20 && byte != 0x7f) || byte == b'\t');

    if !name_ok {
        return Err(Error::InvalidRequest(format!("bad header name {name:?}")));
    }

    if !value_ok {
        return Err(Error::InvalidRequest(format!(
            "bad value for header {name}"
        )));
    }

    Ok(())
}
