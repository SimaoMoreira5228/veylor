use std::time::Duration;

use crate::body::Body;
use crate::client::Client;
use crate::encode::{base64, push_pair};
use crate::error::{Error, Result};
use crate::headers::Headers;
use crate::method::Method;
use crate::response::Response;
use crate::send::{Plan, execute};
use crate::url::Url;

pub struct RequestBuilder {
    client: Client,
    method: Method,
    url: Option<Url>,
    headers: Headers,
    query: String,
    body: Body,
    timeout: Option<Duration>,
    error: Option<Error>,
}

impl RequestBuilder {
    pub(crate) fn new(client: Client, method: Method, url: Result<Url>) -> Self {
        let (url, error) = match url {
            Ok(url) => (Some(url), None),
            Err(error) => (None, Some(error)),
        };

        Self {
            client,
            method,
            url,
            headers: Headers::default(),
            query: String::new(),
            body: Body::Empty,
            timeout: None,
            error,
        }
    }

    pub fn header(mut self, name: impl AsRef<str>, value: impl AsRef<[u8]>) -> Self {
        self.add_header(name.as_ref(), value.as_ref());

        self
    }

    pub fn headers<I, K, V>(mut self, headers: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<[u8]>,
    {
        for (name, value) in headers {
            self.add_header(name.as_ref(), value.as_ref());
        }

        self
    }

    pub fn query<I, K, V>(mut self, pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        for (key, value) in pairs {
            push_pair(&mut self.query, key, value);
        }

        self
    }

    pub fn bearer_auth(mut self, token: impl AsRef<str>) -> Self {
        let value = format!("Bearer {}", token.as_ref());

        self.add_header("authorization", value.as_bytes());

        self
    }

    pub fn basic_auth(mut self, user: impl AsRef<str>, password: Option<&str>) -> Self {
        let mut credentials = user.as_ref().to_owned();

        credentials.push(':');

        if let Some(password) = password {
            credentials.push_str(password.as_ref());
        }

        let value = format!("Basic {}", base64(credentials.as_bytes()));

        self.add_header("authorization", value.as_bytes());

        self
    }

    pub fn body(mut self, body: impl Into<Body>) -> Self {
        self.body = body.into();

        self
    }

    pub fn form<I, K, V>(mut self, pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let mut encoded = String::new();

        for (key, value) in pairs {
            push_pair(&mut encoded, key, value);
        }

        self.body = Body::from(encoded);
        self.default_content_type("application/x-www-form-urlencoded");

        self
    }

    #[cfg(feature = "json")]
    pub fn json<T: serde::Serialize + ?Sized>(mut self, value: &T) -> Self {
        match serde_json::to_vec(value) {
            Ok(bytes) => {
                self.body = Body::from(bytes);
                self.default_content_type("application/json");
            }
            Err(error) => {
                self.error.get_or_insert(Error::builder(error.to_string()));
            }
        }

        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);

        self
    }

    pub async fn send(self) -> Result<Response> {
        if let Some(error) = self.error {
            return Err(error);
        }

        let Some(mut url) = self.url else {
            return Err(Error::builder("missing url"));
        };

        url.append_query(&self.query);

        let plan = Plan {
            method: self.method,
            url,
            headers: self.headers,
            body: self.body,
            timeout: self.timeout,
        };

        execute(&self.client, plan).await
    }

    fn add_header(&mut self, name: &str, value: &[u8]) {
        if let Err(error) = self.headers.push(name, value) {
            self.error.get_or_insert(error);
        }
    }

    fn default_content_type(&mut self, value: &str) {
        if !self.headers.contains("content-type") {
            self.add_header("content-type", value.as_bytes());
        }
    }
}
