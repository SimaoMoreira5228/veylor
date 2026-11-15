use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;

use parking_lot::Mutex;
use veylor_fetch::{Agent, AgentBuilder, Error, Protocol};
use veylor_server::{MAX_STATIC_BODY_BYTES, RunningServer, Server, ServerConfig, TlsConfig};
use veylor_tls::{Certificate, PrivateKey};

use crate::support::{TestServer, respond, serve_loop, spawn};

const CERT: &[u8] = include_bytes!("../../../veylor-tls/tests/testdata/leaf_cert.der");
const KEY: &[u8] = include_bytes!("../../../veylor-tls/tests/testdata/leaf_key.der");

#[derive(Clone)]
struct Recorded {
    line: String,
    host: String,
    proxy_authorization: String,
}

type Seen = Arc<Mutex<Vec<Recorded>>>;

fn proxy_server<F>(answer: F) -> (TestServer, Seen)
where
    F: Fn(&Recorded, &mut TcpStream) + Send + Sync + 'static,
{
    let seen: Seen = Arc::default();
    let recorder = Arc::clone(&seen);
    let answer = Arc::new(answer);
    let server = spawn(move |_, stream| {
        let answer = Arc::clone(&answer);
        let recorder = Arc::clone(&recorder);

        serve_loop(stream, move |request, out| {
            let recorded = Recorded {
                line: request.request_line().to_owned(),
                host: request.header("host").unwrap_or("-").to_owned(),
                proxy_authorization: request
                    .header("proxy-authorization")
                    .unwrap_or("-")
                    .to_owned(),
            };

            recorder.lock().push(recorded.clone());
            answer(&recorded, out);
        });
    });

    (server, seen)
}

fn forwarding_proxy() -> (TestServer, Seen) {
    proxy_server(|recorded, out| {
        respond(out, "200 OK", &[], recorded.line.as_bytes());
    })
}

fn trap_proxy() -> TestServer {
    spawn(|_, stream| drop(stream))
}

fn origin(body: &'static [u8]) -> TestServer {
    spawn(move |_, stream| {
        serve_loop(stream, move |_, out| respond(out, "200 OK", &[], body));
    })
}

fn splice(client: &mut TcpStream, authority: &str) {
    let Ok(mut upstream) = TcpStream::connect(authority) else {
        let _ = client.write_all(b"HTTP/1.1 502 Bad Gateway\r\ncontent-length: 0\r\n\r\n");

        return;
    };

    let _ = client.write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n");

    let mut client_read = client.try_clone().expect("client clone");
    let mut upstream_write = upstream.try_clone().expect("upstream clone");

    std::thread::spawn(move || {
        let _ = std::io::copy(&mut client_read, &mut upstream_write);
    });

    let _ = std::io::copy(&mut upstream, client);
}

fn trusting() -> AgentBuilder {
    Agent::builder()
        .builtin_root_certificates(false)
        .add_root_certificate(CERT)
}

fn managed_server() -> RunningServer {
    let config = ServerConfig {
        tls: Some(TlsConfig {
            cert_chain: vec![Certificate::from(CERT.to_vec())],
            key: PrivateKey::Pkcs8(KEY.to_vec()),
            max_handshakes: 64,
        }),
        ..ServerConfig::default()
    };
    let mut server = Server::bind(SocketAddr::from(([127, 0, 0, 1], 0)), config).expect("bind");

    server
        .register_route(
            b"GET",
            b"/hello",
            200,
            b"OK",
            b"hello",
            MAX_STATIC_BODY_BYTES,
        )
        .expect("route");

    server.start().expect("start")
}

fn stop(server: RunningServer) {
    server.stop_accepting();
    server.join().expect("join");
}

#[test]
fn a_forward_proxy_gets_absolute_targets_on_one_shared_connection() {
    let (proxy, seen) = forwarding_proxy();
    let agent = Agent::builder()
        .proxy(&format!("http://user:secret@{}", proxy.addr))
        .build()
        .expect("agent");
    let first = agent.get("http://a.test/one?x=1").call().expect("first");

    assert_eq!(first.protocol(), Protocol::Http1);
    assert_eq!(
        first.text().expect("body"),
        "GET http://a.test/one?x=1 HTTP/1.1"
    );

    let second = agent.get("http://b.test:8080/two").call().expect("second");

    assert_eq!(
        second.text().expect("body"),
        "GET http://b.test:8080/two HTTP/1.1"
    );

    let seen = seen.lock();

    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].line, "GET http://a.test/one?x=1 HTTP/1.1");
    assert_eq!(seen[0].host, "a.test");
    assert_eq!(seen[0].proxy_authorization, "Basic dXNlcjpzZWNyZXQ=");
    assert_eq!(seen[1].line, "GET http://b.test:8080/two HTTP/1.1");
    assert_eq!(seen[1].host, "b.test:8080");
    assert_eq!(seen[1].proxy_authorization, "Basic dXNlcjpzZWNyZXQ=");
    drop(seen);

    assert_eq!(proxy.accepted(), 1);
    assert_eq!(agent.pool_stats().idle_h1, 1);
}

#[test]
fn a_connect_tunnel_reaches_a_tls_origin_and_keeps_alpn_h2() {
    let server = managed_server();
    let (proxy, seen) = proxy_server(|recorded, out| {
        let authority = recorded.line.split_whitespace().nth(1).unwrap_or("");

        splice(out, authority);
    });
    let proxy_url = format!("http://user:secret@{}", proxy.addr);
    let target = format!("https://localhost:{}/hello", server.addr().port());
    let agent = trusting().proxy(&proxy_url).build().expect("agent");

    for _ in 0..3 {
        let response = agent.get(&target).call().expect("response");

        assert_eq!(response.protocol(), Protocol::Http2);
        assert_eq!(response.text().expect("body"), "hello");
    }

    assert_eq!(agent.pool_stats().h2, 1);

    let plain = trusting()
        .http1_only(true)
        .proxy(&proxy_url)
        .build()
        .expect("plain agent");
    let response = plain.get(&target).call().expect("h1 response");

    assert_eq!(response.protocol(), Protocol::Http1);
    assert_eq!(response.text().expect("body"), "hello");

    let authority = format!("localhost:{}", server.addr().port());
    let seen = seen.lock();

    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].line, format!("CONNECT {authority} HTTP/1.1"));
    assert_eq!(seen[0].host, authority);
    assert_eq!(seen[0].proxy_authorization, "Basic dXNlcjpzZWNyZXQ=");
    assert_eq!(seen[1].line, format!("CONNECT {authority} HTTP/1.1"));
    assert_eq!(seen[1].host, authority);
    drop(seen);

    stop(server);
}

#[test]
fn no_proxy_bypasses_star_host_suffix_and_port_forms() {
    let origin = origin(b"direct");
    let trap = trap_proxy();
    let proxy_url = format!("http://{}", trap.addr);
    let port = origin.addr.port();
    let by_ip = format!("http://127.0.0.1:{port}/");
    let by_name = format!("http://localhost:{port}/");
    let cases = [
        ("*".to_owned(), by_ip.clone()),
        ("127.0.0.1".to_owned(), by_ip.clone()),
        (format!("127.0.0.1:{port}"), by_ip.clone()),
        (".0.0.1".to_owned(), by_ip),
        ("LOCALHOST".to_owned(), by_name),
    ];

    for (rule, url) in cases {
        let agent = Agent::builder()
            .proxy(&proxy_url)
            .no_proxy(&rule)
            .build()
            .expect("agent");
        let body = agent.get(&url).call().expect(&rule).text().expect("body");

        assert_eq!(body, "direct", "{rule}");
    }

    assert_eq!(trap.accepted(), 0);
}

#[test]
fn a_non_matching_no_proxy_rule_still_uses_the_proxy() {
    let origin = origin(b"direct");
    let trap = trap_proxy();
    let agent = Agent::builder()
        .proxy(&format!("http://{}", trap.addr))
        .no_proxy(".example.com,other.test:8080")
        .build()
        .expect("agent");
    let error = agent.get(&origin.url("/")).call().expect_err("dead proxy");

    assert!(!error.is_timeout(), "{error}");
    assert_eq!(trap.accepted(), 1);
}

#[test]
fn bypassed_requests_drop_proxy_credentials() {
    let seen: Arc<Mutex<Vec<String>>> = Arc::default();
    let recorder = Arc::clone(&seen);
    let origin = spawn(move |_, stream| {
        let recorder = Arc::clone(&recorder);

        serve_loop(stream, move |request, out| {
            recorder.lock().push(
                request
                    .header("proxy-authorization")
                    .unwrap_or("-")
                    .to_owned(),
            );
            respond(out, "200 OK", &[], b"direct");
        });
    });
    let agent = Agent::builder()
        .proxy("http://user:secret@127.0.0.1:9")
        .no_proxy("*")
        .build()
        .expect("agent");
    let body = agent
        .get(&origin.url("/"))
        .set("proxy-authorization", "Basic d3Jvbmc=")
        .call()
        .expect("response")
        .text()
        .expect("body");

    assert_eq!(body, "direct");
    assert_eq!(seen.lock()[0], "-");
}

#[test]
fn configured_credentials_replace_the_caller_proxy_authorization() {
    let (proxy, seen) = forwarding_proxy();
    let agent = Agent::builder()
        .proxy(&format!("http://user:secret@{}", proxy.addr))
        .build()
        .expect("agent");
    let response = agent
        .get("http://a.test/")
        .set("proxy-authorization", "Basic d3Jvbmc=")
        .call()
        .expect("response");

    assert_eq!(response.status(), 200);
    assert_eq!(seen.lock()[0].proxy_authorization, "Basic dXNlcjpzZWNyZXQ=");
}

#[test]
fn proxy_auth_errors_and_connect_failures_are_connect_errors() {
    let (proxy, _) =
        proxy_server(|_, out| respond(out, "407 Proxy Authentication Required", &[], b""));
    let agent = Agent::builder()
        .proxy(&format!("http://{}", proxy.addr))
        .build()
        .expect("agent");
    let error = agent.get("http://a.test/").call().expect_err("407");

    assert!(error.is_connect(), "{error}");

    for status in ["407 Proxy Authentication Required", "403 Forbidden"] {
        let (proxy, _) = proxy_server(move |_, out| respond(out, status, &[], b""));
        let agent = trusting()
            .proxy(&format!("http://{}", proxy.addr))
            .build()
            .expect("agent");
        let error = agent.get("https://localhost:9/").call().expect_err(status);

        assert!(error.is_connect(), "{status}: {error}");
    }
}

#[test]
fn malformed_proxy_responses_are_protocol_errors() {
    let (proxy, _) = proxy_server(|_, out| {
        let _ = out.write_all(b"TUNNEL READY\r\n\r\n");
    });
    let agent = trusting()
        .proxy(&format!("http://{}", proxy.addr))
        .build()
        .expect("agent");
    let error = agent
        .get("https://localhost:9/")
        .call()
        .expect_err("malformed");

    assert!(matches!(error, Error::Protocol(_)), "{error:?}");
}

#[test]
fn redirects_enter_and_leave_the_proxy_per_hop() {
    let origin = spawn(|_, stream| {
        serve_loop(stream, |request, out| {
            if request.request_line().contains("/next") {
                respond(out, "302 Found", &["location: http://a.test/again"], b"");
            } else {
                respond(out, "200 OK", &[], b"direct");
            }
        });
    });
    let origin_port = origin.addr.port();
    let (proxy, seen) = proxy_server(move |recorded, out| {
        if recorded.line.contains("/start") {
            let location = format!("location: http://127.0.0.1:{origin_port}/next");

            respond(out, "302 Found", &[&location], b"");
        } else {
            respond(out, "200 OK", &[], b"proxied");
        }
    });
    let agent = Agent::builder()
        .proxy(&format!("http://{}", proxy.addr))
        .no_proxy("127.0.0.1")
        .build()
        .expect("agent");
    let response = agent.get("http://a.test/start").call().expect("response");

    assert_eq!(response.url(), "http://a.test/again");
    assert_eq!(response.text().expect("body"), "proxied");

    let seen = seen.lock();

    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].line, "GET http://a.test/start HTTP/1.1");
    assert_eq!(seen[0].host, "a.test");
    assert_eq!(seen[1].line, "GET http://a.test/again HTTP/1.1");
    assert_eq!(seen[1].host, "a.test");
    drop(seen);

    assert_eq!(proxy.accepted(), 1);
    assert_eq!(origin.accepted(), 1);
}

#[test]
fn a_redirect_from_http_to_https_enters_the_tunnel() {
    let server = managed_server();
    let port = server.addr().port();
    let target = format!("https://localhost:{port}/hello");
    let (proxy, seen) = proxy_server(move |recorded, out| {
        if recorded.line.starts_with("CONNECT") {
            let authority = recorded.line.split_whitespace().nth(1).unwrap_or("");

            splice(out, authority);
        } else {
            let location = format!("location: {target}");

            respond(out, "302 Found", &[&location], b"");
        }
    });
    let agent = trusting()
        .proxy(&format!("http://{}", proxy.addr))
        .build()
        .expect("agent");
    let response = agent.get("http://a.test/upgrade").call().expect("response");

    assert_eq!(response.protocol(), Protocol::Http2);
    assert_eq!(response.text().expect("body"), "hello");

    let seen = seen.lock();

    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].line, "GET http://a.test/upgrade HTTP/1.1");
    assert_eq!(seen[1].line, format!("CONNECT localhost:{port} HTTP/1.1"));
    drop(seen);

    stop(server);
}

#[test]
fn only_http_proxy_urls_are_accepted() {
    for url in [
        "https://127.0.0.1:3128",
        "socks5://127.0.0.1:1080",
        "127.0.0.1:3128",
    ] {
        let error = Agent::builder().proxy(url).build().err().expect(url);

        assert!(error.is_builder(), "{url}: {error}");
    }
}

#[test]
fn the_connect_budget_honours_the_connect_timeout() {
    let (proxy, _) = proxy_server(|_, out| {
        std::thread::sleep(std::time::Duration::from_millis(400));

        let _ = out.write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n");
    });
    let agent = trusting()
        .proxy(&format!("http://{}", proxy.addr))
        .connect_timeout(std::time::Duration::from_millis(100))
        .build()
        .expect("agent");
    let error = agent
        .get("https://localhost:9/")
        .call()
        .expect_err("timeout");

    assert!(error.is_timeout(), "{error}");
    assert_eq!(proxy.accepted(), 1);
}
