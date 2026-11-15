use std::fmt;

use super::error::{Error, Result};
use crate::url::Url as Parsed;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Url {
    inner: Parsed,
}

impl Url {
    pub fn parse(input: &str) -> Result<Self> {
        Parsed::parse(input)
            .map(|inner| Self { inner })
            .map_err(Error::builder)
    }

    pub fn as_str(&self) -> &str {
        self.inner.as_str()
    }

    pub fn scheme(&self) -> &str {
        self.inner.origin().scheme()
    }

    pub fn host(&self) -> &str {
        &self.inner.origin().host
    }

    pub fn port(&self) -> u16 {
        self.inner.origin().port
    }

    pub fn path(&self) -> &str {
        self.inner.path()
    }

    pub fn query(&self) -> Option<&str> {
        self.inner.target().split_once('?').map(|(_, query)| query)
    }

    pub fn is_same_origin(&self, other: &Url) -> bool {
        self.inner.origin() == other.inner.origin()
    }

    pub(crate) fn append_query(&mut self, encoded: &str) {
        self.inner.append_query(encoded);
    }

    pub(crate) fn join(&self, location: &str) -> Result<Url> {
        self.inner
            .resolve(location)
            .map(|inner| Self { inner })
            .map_err(Error::builder)
    }
}

impl fmt::Display for Url {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub trait IntoUrl {
    fn into_url(self) -> Result<Url>;
}

impl IntoUrl for Url {
    fn into_url(self) -> Result<Url> {
        Ok(self)
    }
}

impl IntoUrl for &Url {
    fn into_url(self) -> Result<Url> {
        Ok(self.clone())
    }
}

impl IntoUrl for &str {
    fn into_url(self) -> Result<Url> {
        Url::parse(self)
    }
}

impl IntoUrl for &String {
    fn into_url(self) -> Result<Url> {
        Url::parse(self)
    }
}

impl IntoUrl for String {
    fn into_url(self) -> Result<Url> {
        Url::parse(&self)
    }
}

#[cfg(test)]
mod tests {
    use super::Url;

    #[test]
    fn exposes_the_parsed_parts() {
        let url = Url::parse("HTTP://Example.com?q=1#frag").expect("url");

        assert_eq!(url.as_str(), "http://example.com/?q=1");
        assert_eq!(url.scheme(), "http");
        assert_eq!(url.host(), "example.com");
        assert_eq!(url.port(), 80);
        assert_eq!(url.path(), "/");
        assert_eq!(url.query(), Some("q=1"));
        assert!(Url::parse("ftp://x/").is_err());
    }

    #[test]
    fn origins_compare_by_scheme_host_and_port() {
        let a = Url::parse("http://h/x").expect("url");

        assert!(a.is_same_origin(&Url::parse("http://H:80/y").expect("url")));
        assert!(!a.is_same_origin(&Url::parse("https://h/x").expect("url")));
        assert!(!a.is_same_origin(&Url::parse("http://h:81/x").expect("url")));
    }

    #[test]
    fn join_resolves_against_the_base() {
        let base = Url::parse("http://h:8080/a/b/c?x=1").expect("url");

        assert_eq!(
            base.join("../d").expect("join").as_str(),
            "http://h:8080/a/d"
        );
    }
}
