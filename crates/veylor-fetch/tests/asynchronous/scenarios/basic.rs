use std::io::Write;
use std::sync::atomic::Ordering;
use std::time::Duration;

use veylor_fetch::asynchronous::{Client, RedirectPolicy, Protocol};

use crate::support::{Req, TestServer, pause, respond, serve};

fn client() -> Client {
    Client::builder().runtime(RUNTIME).build().expect("client")
}

fn echo(request: &Req, socket: &mut std::net::TcpStream) {
    let summary = format!(
        "{} {}\ncontent-type: {}\nauthorization: {}\nuser-agent: {}\nx-extra: {}\n\n{}",
        request.method,
        request.target,
        request.header("content-type").unwrap_or("-"),
        request.header("authorization").unwrap_or("-"),
        request.header("user-agent").unwrap_or("-"),
        request.header("x-extra").unwrap_or("-"),
        String::from_utf8_lossy(&request.body),
    );

    respond(socket, "200 OK", &[("x-seen", "yes")], summary.as_bytes());
}

fn echo_server() -> TestServer {
    serve(echo)
}

#[test]
fn get_returns_status_headers_and_text() {
    run(async {
        let server = serve(|_, socket| respond(socket, "200 OK", &[("x-k", "v")], b"hello"));
        let response = client().get(server.url("/a")).send().await.expect("send");

        assert_eq!(response.status(), 200);
        assert!(response.is_success());
        assert_eq!(response.header("X-K"), Some("v"));
        assert_eq!(response.protocol(), Protocol::Http1);
        assert_eq!(veylor_fetch::asynchronous::Url::parse(response.url()).expect("url").path(), "/a");
        assert_eq!(response.text().await.expect("text"), "hello");
    });
}

#[test]
fn every_method_reaches_the_server() {
    run(async {
        let server = echo_server();
        let client = client();
        let url = server.url("/m");

        for (builder, name) in [
            (client.get(&url), "GET"),
            (client.post(&url), "POST"),
            (client.put(&url), "PUT"),
            (client.patch(&url), "PATCH"),
            (client.delete(&url), "DELETE"),
        ] {
            let text = builder.send().await.expect("send").text().await.expect("text");

            assert!(text.starts_with(&format!("{name} /m\n")), "{text}");
        }

        let head = client.head(&url).send().await.expect("head");

        assert_eq!(head.status(), 200);
        assert!(head.bytes().await.expect("bytes").is_empty());
    });
}

#[test]
fn query_pairs_are_encoded_and_appended() {
    run(async {
        let server = echo_server();
        let text = client()
            .get(server.url("/q?a=1"))
            .query([("k y", "v&1"), ("é", "x")])
            .send()
            .await
            .expect("send")
            .text()
            .await
            .expect("text");

        assert!(text.starts_with("GET /q?a=1&k%20y=v%261&%C3%A9=x\n"), "{text}");
    });
}

#[test]
fn form_sets_content_type_and_encodes() {
    run(async {
        let server = echo_server();
        let text = client()
            .post(server.url("/f"))
            .form([("name", "a b"), ("sym", "+&=")])
            .send()
            .await
            .expect("send")
            .text()
            .await
            .expect("text");

        assert!(text.contains("content-type: application/x-www-form-urlencoded"), "{text}");
        assert!(text.ends_with("\n\nname=a+b&sym=%2B%26%3D"), "{text}");
    });
}

#[test]
fn json_round_trips() {
    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    struct Item {
        id: u32,
        name: String,
    }

    run(async {
        let server = serve(|request, socket| {
            respond(socket, "200 OK", &[("content-type", "application/json")], &request.body);
        });
        let sent = Item {
            id: 7,
            name: "x".to_owned(),
        };
        let response = client()
            .post(server.url("/j"))
            .json(&sent)
            .send()
            .await
            .expect("send");
        let back: Item = response.json().await.expect("json");

        assert_eq!(back, sent);

        let bad = serve(|_, socket| respond(socket, "200 OK", &[], b"not json"));
        let error = client()
            .get(bad.url("/"))
            .send()
            .await
            .expect("send")
            .json::<Item>()
            .await
            .expect_err("decode");

        assert!(error.is_decode());
    });
}

#[test]
fn body_variants_are_sent() {
    run(async {
        let server = echo_server();
        let client = client();
        let url = server.url("/b");

        for text in ["static", "owned"] {
            let builder = client.post(&url);
            let builder = if text == "static" {
                builder.body("static")
            } else {
                builder.body(String::from("owned"))
            };
            let echoed = builder.send().await.expect("send").text().await.expect("text");

            assert!(echoed.ends_with(&format!("\n\n{text}")), "{echoed}");
        }

        let echoed = client
            .post(&url)
            .body(vec![b'v', b'e', b'c'])
            .send()
            .await
            .expect("send")
            .text()
            .await
            .expect("text");

        assert!(echoed.ends_with("\n\nvec"));

        let echoed = client
            .post(&url)
            .body(b"slice".as_slice())
            .send()
            .await
            .expect("send")
            .text()
            .await
            .expect("text");

        assert!(echoed.ends_with("\n\nslice"));
    });
}

#[test]
fn auth_headers_are_built() {
    run(async {
        let server = echo_server();
        let client = client();
        let bearer = client
            .get(server.url("/"))
            .bearer_auth("tok")
            .send()
            .await
            .expect("send")
            .text()
            .await
            .expect("text");
        let basic = client
            .get(server.url("/"))
            .basic_auth("user", Some("pass"))
            .send()
            .await
            .expect("send")
            .text()
            .await
            .expect("text");
        let bare = client
            .get(server.url("/"))
            .basic_auth("user", None)
            .send()
            .await
            .expect("send")
            .text()
            .await
            .expect("text");

        assert!(bearer.contains("authorization: Bearer tok\n"), "{bearer}");
        assert!(basic.contains("authorization: Basic dXNlcjpwYXNz\n"), "{basic}");
        assert!(bare.contains("authorization: Basic dXNlcjo=\n"), "{bare}");
    });
}

#[test]
fn default_headers_and_user_agent_apply_and_can_be_overridden() {
    run(async {
        let server = echo_server();
        let client = Client::builder()
            .runtime(RUNTIME)
            .user_agent("agent/1")
            .default_headers([("x-extra", "default")])
            .build()
            .expect("client");
        let plain = client
            .get(server.url("/"))
            .send()
            .await
            .expect("send")
            .text()
            .await
            .expect("text");
        let over = client
            .get(server.url("/"))
            .headers([("X-Extra", "mine")])
            .send()
            .await
            .expect("send")
            .text()
            .await
            .expect("text");

        assert!(plain.contains("user-agent: agent/1\n"), "{plain}");
        assert!(plain.contains("x-extra: default\n"), "{plain}");
        assert!(over.contains("x-extra: mine\n"), "{over}");
        assert!(!over.contains("x-extra: default"), "{over}");
    });
}

#[test]
fn bad_input_is_a_builder_error() {
    run(async {
        let client = client();
        let error = client
            .get("http://127.0.0.1:1/")
            .header("x-bad", "a\r\nb: c")
            .send()
            .await
            .expect_err("header");

        assert!(error.is_builder());
        assert!(client.get("ftp://nope/").send().await.expect_err("scheme").is_builder());
        assert!(client.get("http://h/").header("bad name", "v").send().await.is_err());
        assert!(Client::builder().default_header("x\n", "v").build().is_err());
    });
}

#[test]
fn connection_refused_is_a_connect_error() {
    run(async {
        let port = {
            let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("bind");

            listener.local_addr().expect("addr").port()
        };
        let error = client()
            .get(format!("http://127.0.0.1:{port}/"))
            .send()
            .await
            .expect_err("refused");

        assert!(error.is_connect(), "{error}");
    });
}

#[test]
fn requests_reuse_pooled_connections() {
    run(async {
        let server = serve(|_, socket| respond(socket, "200 OK", &[], b"ok"));
        let client = client();

        for _ in 0..4 {
            let text = client.get(server.url("/")).send().await.expect("send");

            assert_eq!(text.text().await.expect("text"), "ok");
        }

        assert_eq!(server.connections.load(Ordering::SeqCst), 1);
        assert_eq!(client.pool_stats().open, 1);
    });
}

#[test]
fn request_timeout_covers_the_head() {
    run(async {
        let server = serve(|_, socket| {
            pause(600);
            respond(socket, "200 OK", &[], b"late");
        });
        let error = client()
            .get(server.url("/"))
            .timeout(Duration::from_millis(100))
            .send()
            .await
            .expect_err("timeout");

        assert!(error.is_timeout(), "{error}");

        let configured = Client::builder()
            .runtime(RUNTIME)
            .timeout(Duration::from_millis(100))
            .build()
            .expect("client");
        let error = configured.get(server.url("/")).send().await.expect_err("timeout");

        assert!(error.is_timeout(), "{error}");
    });
}

#[test]
fn request_timeout_covers_the_body() {
    run(async {
        let server = serve(|_, socket| {
            let _ = socket.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 10\r\n\r\nabc");
            let _ = socket.flush();

            pause(800);
        });
        let response = client()
            .get(server.url("/"))
            .timeout(Duration::from_millis(200))
            .send()
            .await
            .expect("head arrives in time");
        let error = response.text().await.expect_err("body stalls");

        assert!(error.is_timeout(), "{error}");
    });
}

#[test]
fn body_limit_is_enforced() {
    run(async {
        let server = serve(|_, socket| respond(socket, "200 OK", &[], &[b'x'; 4096]));
        let client = Client::builder()
            .runtime(RUNTIME)
            .max_body(100)
            .build()
            .expect("client");
        let error = client
            .get(server.url("/"))
            .send()
            .await
            .expect("send")
            .bytes()
            .await
            .expect_err("limit");

        assert!(error.is_body(), "{error}");
    });
}

#[test]
fn error_for_status_flags_client_and_server_errors() {
    run(async {
        let server = serve(|request, socket| match request.target.as_str() {
            "/missing" => respond(socket, "404 Not Found", &[], b"nope"),
            "/boom" => respond(socket, "500 Internal Server Error", &[], b""),
            _ => respond(socket, "200 OK", &[], b"fine"),
        });
        let client = client();
        let error = client
            .get(server.url("/missing"))
            .send()
            .await
            .expect("send")
            .error_for_status()
            .expect_err("404");

        assert!(error.is_status());
        assert_eq!(error.status(), Some(404));
        assert!(error.url().expect("url").ends_with("/missing"));

        let boom = client.get(server.url("/boom")).send().await.expect("send");

        assert_eq!(boom.error_for_status_ref().err().and_then(|e| e.status()), Some(500));

        let fine = client.get(server.url("/ok")).send().await.expect("send");
        let fine = fine.error_for_status().expect("2xx passes");

        assert_eq!(fine.text().await.expect("text"), "fine");
    });
}

#[test]
fn gzip_responses_are_decoded() {
    use flate2::Compression;
    use flate2::write::GzEncoder;

    let payload: Vec<u8> = (0..20_000u32).flat_map(|n| n.to_string().into_bytes()).collect();
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());

    encoder.write_all(&payload).expect("encode");

    let packed = encoder.finish().expect("finish");
    let wire = packed.clone();

    run(async move {
        let server = serve(move |request, socket| {
            let accepts = request.header("accept-encoding").unwrap_or("");

            if accepts.contains("gzip") {
                respond(socket, "200 OK", &[("content-encoding", "gzip")], &wire);
            } else {
                respond(socket, "400 Bad Request", &[], b"no gzip offered");
            }
        });
        let client = client();
        let buffered = client.get(server.url("/")).send().await.expect("send");

        assert_eq!(buffered.bytes().await.expect("bytes"), payload);

        let mut streamed = client.get(server.url("/")).send().await.expect("send");
        let mut joined = Vec::new();

        while let Some(chunk) = streamed.chunk().await.expect("chunk") {
            joined.extend_from_slice(chunk);
        }

        assert_eq!(joined, payload);

        let tiny = Client::builder()
            .runtime(RUNTIME)
            .max_body(1000)
            .build()
            .expect("client");
        let error = tiny
            .get(server.url("/"))
            .send()
            .await
            .expect("send")
            .bytes()
            .await
            .expect_err("decoded size is capped");

        assert!(error.is_body(), "{error}");
    });
}

#[test]
fn gzip_garbage_is_a_decode_error() {
    run(async {
        let server = serve(|_, socket| {
            respond(socket, "200 OK", &[("content-encoding", "gzip")], b"definitely not gzip");
        });
        let error = client()
            .get(server.url("/"))
            .send()
            .await
            .expect("send")
            .bytes()
            .await
            .expect_err("garbage");

        assert!(error.is_decode(), "{error}");
    });
}

#[test]
fn redirect_policy_is_configurable() {
    assert_eq!(RedirectPolicy::default().limit(), 10);
    assert_eq!(RedirectPolicy::none().limit(), 0);
    assert_eq!(RedirectPolicy::limited(3).limit(), 3);
}
