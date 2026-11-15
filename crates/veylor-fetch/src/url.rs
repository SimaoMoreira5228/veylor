pub(crate) type Invalid = &'static str;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct Origin {
    pub(crate) tls: bool,
    pub(crate) host: String,
    pub(crate) port: u16,
}

impl Origin {
    pub(crate) fn scheme(&self) -> &'static str {
        if self.tls { "https" } else { "http" }
    }

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Url {
    text: String,
    origin: Origin,
    target_start: usize,
}

impl Url {
    pub(crate) fn parse(url: &str) -> Result<Self, Invalid> {
        let (tls, rest) = if let Some(rest) = strip_scheme(url, "https://") {
            (true, rest)
        } else if let Some(rest) = strip_scheme(url, "http://") {
            (false, rest)
        } else {
            return Err("scheme must be http or https");
        };

        Self::from_authority(tls, rest)
    }

    pub(crate) fn resolve(&self, location: &str) -> Result<Self, Invalid> {
        let location = location.trim();

        if let Some(rest) = location.strip_prefix("//") {
            return Self::from_authority(self.origin.tls, rest);
        }

        if strip_scheme(location, "http://").is_some()
            || strip_scheme(location, "https://").is_some()
        {
            return Self::parse(location);
        }

        if location.contains("://") {
            return Err("redirect to an unsupported scheme");
        }

        let location = location.split('#').next().unwrap_or("");
        let target = if location.is_empty() {
            self.target().to_owned()
        } else if location.starts_with('/') {
            normalize(location)
        } else if location.starts_with('?') {
            format!("{}{location}", self.path())
        } else {
            let path = self.path();
            let directory = &path[..path.rfind('/').map_or(0, |index| index + 1)];

            normalize(&format!("{directory}{location}"))
        };

        Self::from_parts(self.origin.clone(), &target)
    }

    pub(crate) fn append_query(&mut self, encoded: &str) {
        if encoded.is_empty() {
            return;
        }

        let separator = if self.target().contains('?') {
            '&'
        } else {
            '?'
        };

        self.text.push(separator);
        self.text.push_str(encoded);
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.text
    }

    pub(crate) fn origin(&self) -> &Origin {
        &self.origin
    }

    pub(crate) fn target(&self) -> &str {
        &self.text[self.target_start..]
    }

    pub(crate) fn path(&self) -> &str {
        let target = self.target();

        target.split('?').next().unwrap_or(target)
    }

    fn from_authority(tls: bool, rest: &str) -> Result<Self, Invalid> {
        let rest = rest.split('#').next().unwrap_or("");
        let split = rest.find(['/', '?']).unwrap_or(rest.len());
        let (authority, tail) = rest.split_at(split);

        if authority.contains('@') {
            return Err("userinfo is not supported");
        }

        let (host, port) = split_authority(authority, if tls { 443 } else { 80 })?;

        if host.is_empty() {
            return Err("no host");
        }

        let target = match tail {
            "" => "/".to_owned(),
            tail if tail.starts_with('?') => format!("/{tail}"),
            tail => tail.to_owned(),
        };
        let origin = Origin {
            tls,
            host: host.to_ascii_lowercase(),
            port,
        };

        Self::from_parts(origin, &target)
    }

    fn from_parts(origin: Origin, target: &str) -> Result<Self, Invalid> {
        if target.bytes().any(|byte| byte <= 0x20 || byte == 0x7f) {
            return Err("target contains control or space characters");
        }

        let mut text = String::with_capacity(target.len() + 32);

        text.push_str(origin.scheme());
        text.push_str("://");
        text.push_str(&origin.host_header());

        let target_start = text.len();

        text.push_str(target);

        Ok(Self {
            text,
            origin,
            target_start,
        })
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

fn split_authority(authority: &str, default: u16) -> Result<(&str, u16), Invalid> {
    if let Some(inner) = authority.strip_prefix('[') {
        let (host, after) = inner.split_once(']').ok_or("bad ipv6 host")?;

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

fn parse_port(port: Option<&str>) -> Result<u16, Invalid> {
    match port.and_then(|port| port.parse::<u16>().ok()) {
        Some(port) if port != 0 => Ok(port),
        _ => Err("bad port"),
    }
}

#[cfg(test)]
mod tests {
    use super::Url;

    #[test]
    fn parses_defaults_and_ports() {
        let url = Url::parse("HTTPS://Example.COM/a?b=1#frag").expect("url");

        assert!(url.origin().tls);
        assert_eq!(url.origin().host, "example.com");
        assert_eq!(url.origin().port, 443);
        assert_eq!(url.target(), "/a?b=1");
        assert_eq!(url.as_str(), "https://example.com/a?b=1");
        assert_eq!(url.origin().host_header(), "example.com");

        let url = Url::parse("http://[::1]:81?x").expect("url");

        assert_eq!(url.origin().host, "::1");
        assert_eq!(url.target(), "/?x");
        assert_eq!(url.path(), "/");
        assert_eq!(url.origin().host_header(), "[::1]:81");
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
        let cases = [
            ("/x/y", "http://h:8080/x/y"),
            ("d", "http://h:8080/a/b/d"),
            ("../d?z=2", "http://h:8080/a/d?z=2"),
            ("../../../d", "http://h:8080/d"),
            ("?n=2", "http://h:8080/a/b/c?n=2"),
            ("./", "http://h:8080/a/b/"),
            ("./d/", "http://h:8080/a/b/d/"),
            ("//cdn.test:9/p", "http://cdn.test:9/p"),
            ("https://s/q#f", "https://s/q"),
            ("../../../../up", "http://h:8080/up"),
        ];

        for (location, expected) in cases {
            assert_eq!(base.resolve(location).expect(location).as_str(), expected);
        }

        assert!(base.resolve("ftp://x/").is_err());
    }

    #[test]
    fn appends_a_query_string() {
        let mut url = Url::parse("http://h/p").expect("url");

        url.append_query("a%20b=c%26d");
        url.append_query("e=f");
        url.append_query("");

        assert_eq!(url.as_str(), "http://h/p?a%20b=c%26d&e=f");
    }

    #[test]
    fn origins_compare_by_scheme_host_and_port() {
        let a = Url::parse("http://h/x").expect("url");

        assert_eq!(
            a.origin(),
            Url::parse("http://H:80/y").expect("url").origin()
        );
        assert_ne!(a.origin(), Url::parse("https://h/x").expect("url").origin());
        assert_ne!(
            a.origin(),
            Url::parse("http://h:81/x").expect("url").origin()
        );
    }
}
