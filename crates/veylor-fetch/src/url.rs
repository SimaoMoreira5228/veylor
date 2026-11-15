use std::fmt;

use veylor_client_driver::{Origin, parse_url};

use crate::error::{Error, Result};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Url {
    text: String,
    origin: Origin,
    target_start: usize,
}

impl Url {
    pub fn parse(input: &str) -> Result<Self> {
        let parsed = parse_url(input).map_err(|e| Error::builder(e.to_string()))?;
        let without_fragment = input.split('#').next().unwrap_or("");
        let scheme_len = if parsed.origin.tls { 8 } else { 7 };
        let rest = &without_fragment[scheme_len..];
        let split = rest.find(['/', '?']).unwrap_or(rest.len());
        let (authority, tail) = rest.split_at(split);
        let scheme = if parsed.origin.tls { "https" } else { "http" };
        let mut text = String::with_capacity(input.len() + 2);

        text.push_str(scheme);
        text.push_str("://");
        text.push_str(authority);

        let target_start = text.len();

        match tail {
            "" => text.push('/'),
            t if t.starts_with('?') => {
                text.push('/');
                text.push_str(t);
            }
            t => text.push_str(t),
        }

        Ok(Self {
            text,
            origin: parsed.origin,
            target_start,
        })
    }

    pub fn as_str(&self) -> &str {
        &self.text
    }

    pub fn scheme(&self) -> &str {
        if self.origin.tls { "https" } else { "http" }
    }

    pub fn host(&self) -> &str {
        &self.origin.host
    }

    pub fn port(&self) -> u16 {
        self.origin.port
    }

    pub fn path(&self) -> &str {
        let target = self.target();

        target.split('?').next().unwrap_or(target)
    }

    pub fn query(&self) -> Option<&str> {
        self.target().split_once('?').map(|(_, query)| query)
    }

    pub fn is_same_origin(&self, other: &Url) -> bool {
        self.origin == other.origin
    }

    pub(crate) fn append_query(&mut self, encoded: &str) {
        if encoded.is_empty() {
            return;
        }

        let separator = if self.text.contains('?') { '&' } else { '?' };

        self.text.push(separator);
        self.text.push_str(encoded);
    }

    pub(crate) fn join(&self, location: &str) -> Result<Url> {
        let location = location.trim();

        if has_scheme(location) {
            return Url::parse(location);
        }

        let authority = &self.text[self.scheme().len() + 3..self.target_start];
        let mut next = String::with_capacity(self.text.len() + location.len());

        if let Some(rest) = location.strip_prefix("//") {
            next.push_str(self.scheme());
            next.push_str("://");
            next.push_str(rest);

            return Url::parse(&next);
        }

        next.push_str(self.scheme());
        next.push_str("://");
        next.push_str(authority);

        if location.starts_with('/') {
            next.push_str(&normalize_path(location));
        } else if location.starts_with('?') {
            next.push_str(self.path());
            next.push_str(location);
        } else {
            let path = self.path();
            let directory = &path[..path.rfind('/').map_or(0, |i| i + 1)];
            let joined = format!("{directory}{location}");

            next.push_str(&normalize_path(&joined));
        }

        Url::parse(&next)
    }

    fn target(&self) -> &str {
        &self.text[self.target_start..]
    }
}

impl fmt::Display for Url {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
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

fn has_scheme(location: &str) -> bool {
    let lower = |prefix: &str| {
        location
            .get(..prefix.len())
            .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
    };

    lower("http://") || lower("https://")
}

fn normalize_path(path: &str) -> String {
    let (path, query) = match path.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (path, None),
    };
    let mut segments: Vec<&str> = Vec::new();
    let mut trailing = false;

    for segment in path.split('/').skip(1) {
        trailing = false;

        match segment {
            "." => trailing = true,
            ".." => {
                segments.pop();

                trailing = true;
            }
            other => segments.push(other),
        }
    }

    let mut out = String::with_capacity(path.len() + 1);

    for segment in &segments {
        out.push('/');
        out.push_str(segment);
    }

    if segments.is_empty() || trailing {
        out.push('/');
    }

    if let Some(query) = query {
        out.push('?');
        out.push_str(query);
    }

    out
}

#[cfg(test)]
mod tests {
    use super::Url;

    #[test]
    fn parse_normalizes_the_target() {
        let url = Url::parse("HTTP://Example.com?q=1#frag").expect("url");

        assert_eq!(url.as_str(), "http://Example.com/?q=1");
        assert_eq!(url.host(), "example.com");
        assert_eq!(url.path(), "/");
        assert_eq!(url.query(), Some("q=1"));
        assert!(Url::parse("ftp://x/").is_err());
    }

    #[test]
    fn join_resolves_references() {
        let base = Url::parse("http://h:8080/a/b/c?x=1").expect("url");
        let cases = [
            ("/z", "http://h:8080/z"),
            ("d", "http://h:8080/a/b/d"),
            ("../d", "http://h:8080/a/d"),
            ("./d/", "http://h:8080/a/b/d/"),
            ("?y=2", "http://h:8080/a/b/c?y=2"),
            ("//other/p", "http://other/p"),
            ("https://s/q#f", "https://s/q"),
            ("../../../../up", "http://h:8080/up"),
        ];

        for (location, expected) in cases {
            assert_eq!(base.join(location).expect(location).as_str(), expected);
        }
    }

    #[test]
    fn origins_compare_by_scheme_host_and_port() {
        let a = Url::parse("http://h/x").expect("url");

        assert!(a.is_same_origin(&Url::parse("http://H:80/y").expect("url")));
        assert!(!a.is_same_origin(&Url::parse("https://h/x").expect("url")));
        assert!(!a.is_same_origin(&Url::parse("http://h:81/x").expect("url")));
    }
}
