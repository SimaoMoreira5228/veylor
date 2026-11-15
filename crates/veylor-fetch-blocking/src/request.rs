use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;

use crate::agent::Agent;
use crate::encode;
use crate::error::Error;
use crate::outgoing::check_header;
use crate::response::Response;
use crate::url::Url;

pub struct Request {
    agent: Agent,
    method: String,
    url: String,
    query: Vec<(String, String)>,
    headers: Vec<(String, String)>,
    timeout: Option<Duration>,
    error: Option<Error>,
}

impl Request {
    pub(crate) fn new(agent: Agent, method: &str, url: &str) -> Self {
        Self {
            agent,
            method: method.to_ascii_uppercase(),
            url: url.to_owned(),
            query: Vec::new(),
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

    pub fn query(mut self, name: &str, value: &str) -> Self {
        self.query.push((name.to_owned(), value.to_owned()));
        self
    }

    pub fn bearer_auth(self, token: &str) -> Self {
        self.set("authorization", &format!("Bearer {token}"))
    }

    pub fn basic_auth(self, user: &str, password: &str) -> Self {
        let credentials = format!("{user}:{password}");
        let value = format!("Basic {}", STANDARD.encode(credentials.as_bytes()));

        self.set("authorization", &value)
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

    pub fn send_string(self, body: &str) -> Result<Response, Error> {
        self.default_type("text/plain; charset=utf-8")
            .dispatch(body.as_bytes())
    }

    pub fn send_form(self, fields: &[(&str, &str)]) -> Result<Response, Error> {
        let mut body = String::new();

        for (index, (name, value)) in fields.iter().enumerate() {
            if index > 0 {
                body.push('&');
            }

            encode::percent(name, true, &mut body);
            body.push('=');
            encode::percent(value, true, &mut body);
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
        if let Some(error) = self.error {
            return Err(error);
        }

        let mut url = Url::parse(&self.url)?;

        url.push_query(&self.query);

        self.agent
            .execute(&self.method, url, self.headers, body, self.timeout)
    }
}
