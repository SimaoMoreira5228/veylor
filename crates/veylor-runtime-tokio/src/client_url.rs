use std::io;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Origin {
    pub tls: bool,
    pub host: String,
    pub port: u16,
}

impl Origin {
    pub fn host_header(&self) -> String {
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

pub struct ParsedUrl {
    pub origin: Origin,
    pub target: String,
}

pub fn parse_url(url: &str) -> io::Result<ParsedUrl> {
    let (tls, rest) = if let Some(rest) = strip_scheme(url, "https://") {
        (true, rest)
    } else if let Some(rest) = strip_scheme(url, "http://") {
        (false, rest)
    } else {
        return Err(invalid("url scheme must be http or https"));
    };

    let rest = rest.split('#').next().unwrap_or("");
    let split = rest.find(['/', '?']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(split);

    if authority.contains('@') {
        return Err(invalid("url userinfo is not supported"));
    }

    let (host, port) = split_authority(authority, if tls { 443 } else { 80 })?;

    if host.is_empty() {
        return Err(invalid("url has no host"));
    }

    let target = match tail {
        "" => "/".to_owned(),
        t if t.starts_with('?') => format!("/{t}"),
        t => t.to_owned(),
    };

    if target.bytes().any(|b| b <= 0x20 || b == 0x7f) {
        return Err(invalid("url target contains control or space characters"));
    }

    Ok(ParsedUrl {
        origin: Origin {
            tls,
            host: host.to_ascii_lowercase(),
            port,
        },
        target,
    })
}

fn strip_scheme<'a>(url: &'a str, scheme: &str) -> Option<&'a str> {
    let head = url.get(..scheme.len())?;

    if head.eq_ignore_ascii_case(scheme) {
        return url.get(scheme.len()..);
    }

    None
}

fn split_authority(authority: &str, default: u16) -> io::Result<(&str, u16)> {
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

fn parse_port(port: Option<&str>) -> io::Result<u16> {
    match port.and_then(|p| p.parse::<u16>().ok()) {
        Some(port) if port != 0 => Ok(port),
        _ => Err(invalid("bad url port")),
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::parse_url;

    #[test]
    fn parses_defaults_and_ports() {
        let url = parse_url("HTTPS://Example.COM/a?b=1#frag").expect("url");

        assert!(url.origin.tls);
        assert_eq!(url.origin.host, "example.com");
        assert_eq!(url.origin.port, 443);
        assert_eq!(url.target, "/a?b=1");
        assert_eq!(url.origin.host_header(), "example.com");

        let url = parse_url("http://127.0.0.1:8080").expect("url");

        assert_eq!(url.target, "/");
        assert_eq!(url.origin.host_header(), "127.0.0.1:8080");

        let url = parse_url("http://[::1]:81?x").expect("url");

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
            assert!(parse_url(bad).is_err(), "{bad}");
        }
    }
}
