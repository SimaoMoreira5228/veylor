use crate::error::Error;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct Origin {
    pub(crate) tls: bool,
    pub(crate) host: String,
    pub(crate) port: u16,
}

impl Origin {
    pub(crate) fn host_header(&self) -> String {
        let default = if self.tls { 443 } else { 80 };
        let host = if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };

        if self.port == default {
            host
        } else {
            format!("{host}:{}", self.port)
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Url {
    pub(crate) origin: Origin,
    pub(crate) target: String,
}

impl Url {
    pub(crate) fn parse(url: &str) -> Result<Self, Error> {
        let (tls, rest) = if let Some(rest) = strip_scheme(url, "https://") {
            (true, rest)
        } else if let Some(rest) = strip_scheme(url, "http://") {
            (false, rest)
        } else {
            return Err(invalid("scheme must be http or https"));
        };

        Self::from_parts(tls, rest)
    }

    fn from_parts(tls: bool, rest: &str) -> Result<Self, Error> {
        let rest = rest.split('#').next().unwrap_or("");
        let split = rest.find(['/', '?']).unwrap_or(rest.len());
        let (authority, tail) = rest.split_at(split);

        if authority.contains('@') {
            return Err(invalid("userinfo is not supported"));
        }

        let (host, port) = split_authority(authority, if tls { 443 } else { 80 })?;

        if host.is_empty() {
            return Err(invalid("no host"));
        }

        let target = match tail {
            "" => "/".to_owned(),
            tail if tail.starts_with('?') => format!("/{tail}"),
            tail => tail.to_owned(),
        };

        if target.bytes().any(|byte| byte <= 0x20 || byte == 0x7f) {
            return Err(invalid("target contains control or space characters"));
        }

        Ok(Self {
            origin: Origin {
                tls,
                host: host.to_ascii_lowercase(),
                port,
            },
            target,
        })
    }

    pub(crate) fn resolve(&self, location: &str) -> Result<Self, Error> {
        let location = location.trim();

        if let Some(rest) = location.strip_prefix("//") {
            return Self::from_parts(self.origin.tls, rest);
        }

        if strip_scheme(location, "http://").is_some()
            || strip_scheme(location, "https://").is_some()
        {
            return Self::parse(location);
        }

        if location.contains("://") {
            return Err(invalid("redirect to an unsupported scheme"));
        }

        let location = location.split('#').next().unwrap_or("");
        let target = if location.is_empty() {
            self.target.clone()
        } else if location.starts_with('/') {
            normalize(location)
        } else if location.starts_with('?') {
            let path = self.target.split('?').next().unwrap_or("/");

            format!("{path}{location}")
        } else {
            let path = self.target.split('?').next().unwrap_or("/");
            let directory = &path[..path.rfind('/').map_or(0, |index| index + 1)];

            normalize(&format!("{directory}{location}"))
        };

        if target.bytes().any(|byte| byte <= 0x20 || byte == 0x7f) {
            return Err(invalid("target contains control or space characters"));
        }

        Ok(Self {
            origin: self.origin.clone(),
            target,
        })
    }

    pub(crate) fn push_query(&mut self, pairs: &[(String, String)]) {
        for (name, value) in pairs {
            self.target
                .push(if self.target.contains('?') { '&' } else { '?' });
            crate::encode::percent(name, false, &mut self.target);
            self.target.push('=');
            crate::encode::percent(value, false, &mut self.target);
        }
    }

    pub(crate) fn display(&self) -> String {
        let scheme = if self.origin.tls { "https" } else { "http" };

        format!("{scheme}://{}{}", self.origin.host_header(), self.target)
    }
}

fn normalize(path_and_query: &str) -> String {
    let (path, query) = match path_and_query.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (path_and_query, None),
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
            segment => segments.push(segment),
        }
    }

    let mut out = String::with_capacity(path_and_query.len());

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

fn strip_scheme<'a>(url: &'a str, scheme: &str) -> Option<&'a str> {
    let head = url.get(..scheme.len())?;

    if head.eq_ignore_ascii_case(scheme) {
        return url.get(scheme.len()..);
    }

    None
}

fn split_authority(authority: &str, default: u16) -> Result<(&str, u16), Error> {
    if let Some(inner) = authority.strip_prefix('[') {
        let (host, after) = inner
            .split_once(']')
            .ok_or_else(|| invalid("bad ipv6 host"))?;

        return match after {
            "" => Ok((host, default)),
            _ => Ok((host, parse_port(after.strip_prefix(':'))?)),
        };
    }

    match authority.rsplit_once(':') {
        Some((host, port)) => Ok((host, parse_port(Some(port))?)),
        None => Ok((authority, default)),
    }
}

fn parse_port(port: Option<&str>) -> Result<u16, Error> {
    match port.and_then(|port| port.parse::<u16>().ok()) {
        Some(port) if port != 0 => Ok(port),
        _ => Err(invalid("bad port")),
    }
}

fn invalid(message: &str) -> Error {
    Error::InvalidUrl(message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::Url;

    #[test]
    fn parses_defaults_and_ports() {
        let url = Url::parse("HTTPS://Example.COM/a?b=1#frag").expect("url");

        assert!(url.origin.tls);
        assert_eq!(url.origin.host, "example.com");
        assert_eq!(url.origin.port, 443);
        assert_eq!(url.target, "/a?b=1");
        assert_eq!(url.origin.host_header(), "example.com");

        let url = Url::parse("http://[::1]:81?x").expect("url");

        assert_eq!(url.origin.host, "::1");
        assert_eq!(url.target, "/?x");
        assert_eq!(url.origin.host_header(), "[::1]:81");
    }

    #[test]
    fn rejects_bad_urls() {
        for bad in [
            "ftp://x/",
            "http://",
            "http://u@h/",
            "http://h:0/",
            "http://h:99999/",
            "http://h/a b",
            "http://[::1/",
        ] {
            assert!(Url::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn resolves_locations() {
        let base = Url::parse("http://h:8080/a/b/c?q=1").expect("url");

        assert_eq!(base.resolve("/x/y").expect("abs").target, "/x/y");
        assert_eq!(base.resolve("d").expect("rel").target, "/a/b/d");
        assert_eq!(base.resolve("../d?z=2").expect("up").target, "/a/d?z=2");
        assert_eq!(base.resolve("../../../d").expect("clamp").target, "/d");
        assert_eq!(base.resolve("?n=2").expect("query").target, "/a/b/c?n=2");
        assert_eq!(base.resolve("./").expect("dot").target, "/a/b/");

        let other = base.resolve("https://other.test/z").expect("full");

        assert!(other.origin.tls);
        assert_eq!(other.origin.host, "other.test");

        let scheme_relative = base.resolve("//cdn.test:9/p").expect("scheme relative");

        assert!(!scheme_relative.origin.tls);
        assert_eq!(scheme_relative.origin.port, 9);
        assert!(base.resolve("ftp://x/").is_err());
    }

    #[test]
    fn appends_query_pairs() {
        let mut url = Url::parse("http://h/p").expect("url");

        url.push_query(&[
            ("a b".to_owned(), "c&d".to_owned()),
            ("e".to_owned(), "f".to_owned()),
        ]);

        assert_eq!(url.target, "/p?a%20b=c%26d&e=f");
    }
}
