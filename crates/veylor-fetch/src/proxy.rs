use std::io;
use std::time::Duration;

use crate::auth;
use crate::budget::Budget;
use crate::error::{Error, TimeoutKind};
use crate::pool::Route;
use crate::url::{Origin, split_authority, strip_scheme};
use crate::wire::Wire;

const CONNECT_READ: usize = 512;
const DEFAULT_PORT: u16 = 80;

// SECURITY: lowercase names win over their uppercase forms, as in curl, because HTTP_PROXY is
// attacker-controllable in CGI environments (httpoxy).
const HTTPS_NAMES: [&str; 2] = ["https_proxy", "HTTPS_PROXY"];
const HTTP_NAMES: [&str; 2] = ["http_proxy", "HTTP_PROXY"];
const ALL_NAMES: [&str; 2] = ["all_proxy", "ALL_PROXY"];
const NO_PROXY_NAMES: [&str; 2] = ["no_proxy", "NO_PROXY"];

#[derive(Clone, Debug)]
pub(crate) struct Proxy {
    endpoint: Origin,
    authorization: Option<String>,
}

impl Proxy {
    pub(crate) fn parse(url: &str) -> Result<Self, Error> {
        let Some(rest) = strip_scheme(url.trim(), "http://") else {
            return Err(Error::InvalidRequest(
                "proxy url must use the http scheme".to_owned(),
            ));
        };
        let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
        let (userinfo, authority) = match authority.rsplit_once('@') {
            Some((userinfo, authority)) => (Some(userinfo), authority),
            None => (None, authority),
        };
        let (host, port) = split_authority(authority, DEFAULT_PORT)
            .map_err(|message| Error::InvalidRequest(format!("bad proxy url: {message}")))?;

        if host.is_empty() {
            return Err(Error::InvalidRequest("proxy url has no host".to_owned()));
        }

        let authorization = match userinfo {
            Some(userinfo) => Some(credentials(userinfo)?),
            None => None,
        };

        Ok(Self {
            endpoint: Origin {
                tls: false,
                host: host.to_ascii_lowercase().into(),
                port,
            },
            authorization,
        })
    }

    pub(crate) fn endpoint(&self) -> &Origin {
        &self.endpoint
    }

    pub(crate) fn authorization(&self) -> Option<&str> {
        self.authorization.as_deref()
    }

    pub(crate) fn route(&self, target: &Origin) -> Route {
        if target.tls {
            return Route::Tunnel {
                proxy: self.endpoint().clone(),
                target: target.clone(),
            };
        }

        Route::Forward(self.endpoint().clone())
    }

    pub(crate) fn tunnel(
        &self,
        wire: &mut Wire,
        target: &Origin,
        max_head: usize,
        connect_timeout: Option<Duration>,
        budget: &Budget,
    ) -> Result<(), Error> {
        let authority = target.authority();
        let mut head = String::with_capacity(96 + authority.len());

        head.push_str("CONNECT ");
        head.push_str(&authority);
        head.push_str(" HTTP/1.1\r\nhost: ");
        head.push_str(&authority);
        head.push_str("\r\n");

        if let Some(authorization) = &self.authorization {
            head.push_str("proxy-authorization: ");
            head.push_str(authorization);
            head.push_str("\r\n");
        }

        head.push_str("\r\n");

        let failed =
            |error: io::Error| Error::from_io(error, TimeoutKind::Connect, budget.expired());

        wire.arm_timeouts(budget.clamp(connect_timeout), budget.clamp(connect_timeout))
            .map_err(failed)?;
        wire.write_plain(head.as_bytes()).map_err(failed)?;

        self.read_connect(wire, &authority, max_head, budget)
    }

    fn read_connect(
        &self,
        wire: &mut Wire,
        authority: &str,
        max_head: usize,
        budget: &Budget,
    ) -> Result<(), Error> {
        let mut head = Vec::with_capacity(CONNECT_READ);

        loop {
            if let Some(end) = head.windows(4).position(|window| window == b"\r\n\r\n") {
                return self.connect_status(&head[..end], authority);
            }

            if head.len() >= max_head {
                return Err(Error::Protocol(
                    "proxy CONNECT response head is too large".to_owned(),
                ));
            }

            let want = CONNECT_READ.min(max_head - head.len());
            let read = wire
                .read_plain(&mut head, want)
                .map_err(|error| Error::from_io(error, TimeoutKind::Connect, budget.expired()))?;

            if read == 0 {
                return Err(Error::Connect(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("proxy {} closed during CONNECT", self.endpoint.authority()),
                )));
            }
        }
    }

    fn connect_status(&self, head: &[u8], authority: &str) -> Result<(), Error> {
        let text = std::str::from_utf8(head)
            .map_err(|_| Error::Protocol("proxy CONNECT response is not valid text".to_owned()))?;
        let mut parts = text.split_whitespace();
        let version = parts.next().unwrap_or("");

        if !version.starts_with("HTTP/1.") {
            return Err(Error::Protocol(
                "proxy CONNECT response is not http/1.x".to_owned(),
            ));
        }

        let status = parts
            .next()
            .and_then(|code| code.parse::<u16>().ok())
            .ok_or_else(|| {
                Error::Protocol("proxy CONNECT response has no status code".to_owned())
            })?;

        match status {
            200..=299 => Ok(()),
            407 => Err(auth_required(&self.endpoint)),
            _ => Err(connect_refused(&self.endpoint, authority, status)),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct NoProxy {
    rules: Vec<Rule>,
}

#[derive(Clone, Debug)]
enum Rule {
    All,
    Host { host: String, port: Option<u16> },
}

impl NoProxy {
    pub(crate) fn parse(spec: &str) -> Self {
        let mut rules = Vec::new();

        for token in spec.split(',') {
            let token = token.trim();

            if token.is_empty() {
                continue;
            }

            if token == "*" {
                rules.push(Rule::All);

                continue;
            }

            let (host, port) = split_rule(token);

            if host.is_empty() {
                continue;
            }

            rules.push(Rule::Host { host, port });
        }

        Self { rules }
    }

    pub(crate) fn matches(&self, host: &str, port: u16) -> bool {
        self.rules.iter().any(|rule| rule.matches(host, port))
    }
}

impl Rule {
    fn matches(&self, host: &str, port: u16) -> bool {
        match self {
            Self::All => true,
            Self::Host {
                host: rule,
                port: rule_port,
            } => rule_port.is_none_or(|rule_port| rule_port == port) && host_matches(rule, host),
        }
    }
}

fn host_matches(rule: &str, host: &str) -> bool {
    if host == rule {
        return true;
    }

    host.len() > rule.len()
        && host.ends_with(rule)
        && host.as_bytes()[host.len() - rule.len() - 1] == b'.'
}

fn split_rule(token: &str) -> (String, Option<u16>) {
    let token = token.trim_start_matches('*');

    if let Some(inner) = token.strip_prefix('[')
        && let Some((host, after)) = inner.split_once(']')
    {
        let port = after
            .strip_prefix(':')
            .and_then(|port| port.parse::<u16>().ok());

        return (host.to_ascii_lowercase(), port);
    }

    let (host, port) = match token.rsplit_once(':') {
        Some((host, port)) => match port.parse::<u16>() {
            Ok(port) => (host, Some(port)),
            Err(_) => (token, None),
        },
        None => (token, None),
    };

    (host.trim_start_matches('.').to_ascii_lowercase(), port)
}

pub(crate) fn auth_required(endpoint: &Origin) -> Error {
    Error::Connect(io::Error::new(
        io::ErrorKind::PermissionDenied,
        format!(
            "proxy {} requires authentication (407)",
            endpoint.authority()
        ),
    ))
}

fn connect_refused(endpoint: &Origin, authority: &str, status: u16) -> Error {
    Error::Connect(io::Error::new(
        io::ErrorKind::ConnectionRefused,
        format!(
            "proxy {} refused CONNECT to {authority} with status {status}",
            endpoint.authority()
        ),
    ))
}

fn credentials(userinfo: &str) -> Result<String, Error> {
    let (user, password) = match userinfo.split_once(':') {
        Some((user, password)) => (decode(user)?, Some(decode(password)?)),
        None => (decode(userinfo)?, None),
    };

    Ok(auth::basic(&user, password.as_deref()))
}

fn decode(input: &str) -> Result<String, Error> {
    let bad = || Error::InvalidRequest("bad percent escape in proxy credentials".to_owned());

    if !input.contains('%') {
        return Ok(input.to_owned());
    }

    let raw = input.as_bytes();
    let mut bytes = Vec::with_capacity(raw.len());
    let mut index = 0;

    while index < raw.len() {
        if raw[index] != b'%' {
            bytes.push(raw[index]);
            index += 1;

            continue;
        }

        let digits = raw.get(index + 1..index + 3).ok_or_else(bad)?;
        let digits = std::str::from_utf8(digits).map_err(|_| bad())?;
        let byte = u8::from_str_radix(digits, 16).map_err(|_| bad())?;

        bytes.push(byte);
        index += 3;
    }

    String::from_utf8(bytes).map_err(|_| bad())
}

pub(crate) struct FromEnv {
    pub(crate) https: Option<Proxy>,
    pub(crate) http: Option<Proxy>,
    pub(crate) bypass: Option<NoProxy>,
}

impl FromEnv {
    pub(crate) fn read() -> Result<Self, Error> {
        from_lookup(|name| std::env::var(name).ok())
    }
}

fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<FromEnv, Error> {
    let parse = |names: &[&str]| -> Result<Option<Proxy>, Error> {
        env_value(&lookup, names)
            .map(|value| Proxy::parse(&value))
            .transpose()
    };
    let all = parse(&ALL_NAMES)?;
    let https = parse(&HTTPS_NAMES)?.or_else(|| all.clone());
    let http = parse(&HTTP_NAMES)?.or_else(|| all.clone());
    let bypass = env_value(&lookup, &NO_PROXY_NAMES).map(|value| NoProxy::parse(&value));

    Ok(FromEnv {
        https,
        http,
        bypass,
    })
}

fn env_value(lookup: &impl Fn(&str) -> Option<String>, names: &[&str]) -> Option<String> {
    for name in names {
        let Some(value) = lookup(name) else {
            continue;
        };
        let value = value.trim();

        if !value.is_empty() {
            return Some(value.to_owned());
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::{NoProxy, Proxy, from_lookup};

    fn bypassed(rule: &str, host: &str, port: u16) -> bool {
        NoProxy::parse(rule).matches(host, port)
    }

    #[test]
    fn no_proxy_matches_curl_style_tokens() {
        assert!(bypassed("*", "anything.test", 80));
        assert!(bypassed("example.com", "example.com", 80));
        assert!(bypassed("example.com", "www.example.com", 443));
        assert!(bypassed("example.com", "example.com", 81));
        assert!(bypassed("EXAMPLE.COM", "api.example.com", 80));
        assert!(bypassed(".example.com", "example.com", 80));
        assert!(bypassed(".example.com", "api.example.com", 80));
        assert!(bypassed("*.example.com", "api.example.com", 80));
        assert!(bypassed("example.com:8080", "api.example.com", 8080));
        assert!(bypassed(" example.com , other.test ", "other.test", 80));

        assert!(!bypassed("example.com", "badexample.com", 80));
        assert!(!bypassed("example.com", "example.com.evil", 80));
        assert!(!bypassed("example.com:8080", "example.com", 80));
        assert!(!bypassed("", "example.com", 80));
        assert!(!bypassed("other.test", "example.com", 80));
    }

    #[test]
    fn parses_proxy_urls_and_credentials() {
        let proxy = Proxy::parse("http://User:p%40ss@127.0.0.1:3128/").expect("proxy");

        assert_eq!(proxy.endpoint().host.as_ref(), "127.0.0.1");
        assert_eq!(proxy.endpoint().port, 3128);
        assert!(!proxy.endpoint().tls);
        assert_eq!(proxy.authorization(), Some("Basic VXNlcjpwQHNz"));

        let plain = Proxy::parse("http://proxy.test").expect("proxy");

        assert_eq!(plain.endpoint().port, 80);
        assert_eq!(plain.authorization(), None);
    }

    #[test]
    fn rejects_non_http_proxy_urls() {
        for bad in [
            "https://proxy.test:443",
            "socks5://proxy.test:1080",
            "proxy.test:3128",
            "",
            "http://",
            "http://user@:3128",
        ] {
            let error = Proxy::parse(bad).expect_err(bad);

            assert!(error.is_builder(), "{bad}: {error}");
        }
    }

    #[test]
    fn reads_the_environment_per_scheme() {
        let both = |name: &str| match name {
            "https_proxy" | "HTTPS_PROXY" => Some("http://secure.test:443".to_owned()),
            "http_proxy" | "HTTP_PROXY" => Some("http://plain.test:8080".to_owned()),
            "all_proxy" => Some("http://all.test:1080".to_owned()),
            "no_proxy" => Some("localhost, .internal".to_owned()),
            _ => None,
        };
        let env = from_lookup(both).expect("env");

        assert_eq!(
            env.https.expect("https").endpoint().host.as_ref(),
            "secure.test"
        );
        assert_eq!(
            env.http.expect("http").endpoint().host.as_ref(),
            "plain.test"
        );

        let bypass = env.bypass.expect("bypass");

        assert!(bypass.matches("internal", 80));
        assert!(bypass.matches("localhost", 80));

        let mixed = |name: &str| match name {
            "http_proxy" => Some("http://plain.test:8080".to_owned()),
            "all_proxy" => Some("http://all.test:1080".to_owned()),
            _ => None,
        };
        let env = from_lookup(mixed).expect("env");

        assert_eq!(
            env.http.expect("http").endpoint().host.as_ref(),
            "plain.test"
        );
        assert_eq!(
            env.https.expect("https").endpoint().host.as_ref(),
            "all.test"
        );

        let all = |name: &str| match name {
            "all_proxy" => Some("http://all.test:1080".to_owned()),
            _ => None,
        };
        let env = from_lookup(all).expect("env");

        assert_eq!(env.http.expect("http").endpoint().host.as_ref(), "all.test");
        assert_eq!(
            env.https.expect("https").endpoint().host.as_ref(),
            "all.test"
        );

        let lower = |name: &str| match name {
            "http_proxy" => Some("http://lower.test:1".to_owned()),
            "HTTP_PROXY" => Some("http://upper.test:2".to_owned()),
            _ => None,
        };
        let env = from_lookup(lower).expect("env");

        assert_eq!(
            env.http.expect("http").endpoint().host.as_ref(),
            "lower.test"
        );
        assert!(env.https.is_none());

        let empty = |_: &str| Some(String::new());
        let env = from_lookup(empty).expect("env");

        assert!(env.http.is_none());
        assert!(env.https.is_none());
    }
}
