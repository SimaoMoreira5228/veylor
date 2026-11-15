use std::io::Read;
use std::time::Duration;

use crate::agent::Agent;
use crate::auth;
use crate::error::Error;
use crate::headers::check as check_header;
use crate::method::Method;
use crate::outgoing::Payload;
use crate::percent;
use crate::response::Response;
use crate::url::Url;

pub struct Request {
    agent: Agent,
    method: Method,
    url: String,
    query: String,
    headers: Vec<(String, String)>,
    timeout: Option<Duration>,
    error: Option<Error>,
}

impl Request {
    pub(crate) fn new(agent: Agent, method: Method, url: &str) -> Self {
        Self {
            agent,
            method,
            url: url.to_owned(),
            query: String::new(),
            headers: Vec::new(),
            timeout: None,
            error: None,
        }
    }

    pub fn set(mut self, name: &str, value: &str) -> Self {
        if self.check(name, value) {
            self.headers
                .retain(|(existing, _)| !existing.eq_ignore_ascii_case(name));
            self.headers.push((name.to_owned(), value.to_owned()));
        }

        self
    }

    pub fn header(mut self, name: &str, value: &str) -> Self {
        if self.check(name, value) {
            self.headers.push((name.to_owned(), value.to_owned()));
        }

        self
    }

    pub fn query<I, K, V>(mut self, pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        for (name, value) in pairs {
            percent::push_pair(&mut self.query, name.as_ref(), value.as_ref(), false);
        }

        self
    }

    pub fn bearer_auth(self, token: impl AsRef<str>) -> Self {
        self.set("authorization", &auth::bearer(token.as_ref()))
    }

    pub fn basic_auth(self, user: impl AsRef<str>, password: Option<&str>) -> Self {
        self.set("authorization", &auth::basic(user.as_ref(), password))
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn call(self) -> Result<Response, Error> {
        self.dispatch(&[])
    }

    pub fn send(self, body: &[u8]) -> Result<Response, Error> {
        self.dispatch(body)
    }

    pub fn send_stream<R: Read>(self, reader: &mut R) -> Result<Response, Error> {
        self.dispatch_stream(reader)
    }

    pub fn send_string(self, body: &str) -> Result<Response, Error> {
        self.default_type("text/plain; charset=utf-8")
            .dispatch(body.as_bytes())
    }

    pub fn send_form(self, fields: &[(&str, &str)]) -> Result<Response, Error> {
        let mut body = String::new();

        for (name, value) in fields {
            percent::push_pair(&mut body, name, value, true);
        }

        self.default_type("application/x-www-form-urlencoded")
            .dispatch(body.as_bytes())
    }

    #[cfg(feature = "json")]
    pub fn send_json<T: serde::Serialize>(self, value: &T) -> Result<Response, Error> {
        let body = serde_json::to_vec(value).map_err(|error| Error::Json(error.to_string()))?;

        self.default_type("application/json").dispatch(&body)
    }

    fn default_type(self, content_type: &str) -> Self {
        if self
            .headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("content-type"))
        {
            return self;
        }

        self.set("content-type", content_type)
    }

    fn check(&mut self, name: &str, value: &str) -> bool {
        match check_header(name, value) {
            Ok(()) => true,
            Err(error) => {
                self.error.get_or_insert(error);

                false
            }
        }
    }

    fn dispatch(self, body: &[u8]) -> Result<Response, Error> {
        let prepared = self.prepare()?;

        prepared.agent.execute(
            prepared.method,
            prepared.url,
            prepared.headers,
            Payload::Buffered(body),
            prepared.timeout,
        )
    }

    fn dispatch_stream<R: Read>(self, reader: &mut R) -> Result<Response, Error> {
        let prepared = self.prepare()?;
        let declared = declared_length(&prepared.headers)?;

        prepared.agent.execute(
            prepared.method,
            prepared.url,
            prepared.headers,
            Payload::Stream { reader, declared },
            prepared.timeout,
        )
    }

    fn prepare(self) -> Result<Prepared, Error> {
        if let Some(error) = self.error {
            return Err(error);
        }

        let mut url =
            Url::parse(&self.url).map_err(|message| Error::InvalidUrl(message.to_owned()))?;

        url.append_query(&self.query);

        Ok(Prepared {
            agent: self.agent,
            method: self.method,
            url,
            headers: self.headers,
            timeout: self.timeout,
        })
    }
}

struct Prepared {
    agent: Agent,
    method: Method,
    url: Url,
    headers: Vec<(String, String)>,
    timeout: Option<Duration>,
}

fn declared_length(headers: &[(String, String)]) -> Result<Option<u64>, Error> {
    let mut declared = None;

    for (name, value) in headers {
        if !name.eq_ignore_ascii_case("content-length") {
            continue;
        }

        let length = value.trim().parse::<u64>().map_err(|_| {
            Error::InvalidRequest("content-length must be a decimal byte count".to_owned())
        })?;

        if declared.is_some_and(|existing| existing != length) {
            return Err(Error::InvalidRequest(
                "conflicting content-length headers".to_owned(),
            ));
        }

        declared = Some(length);
    }

    Ok(declared)
}
