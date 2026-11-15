use std::io::{Read, Write};
use std::time::{Duration, Instant};

use crate::support::{pause, read_request, respond, serve_loop, spawn};
use veylor_fetch::{Agent, Error, Protocol};

#[test]
fn keep_alive_reuses_one_connection() {
    let server = spawn(|_, stream| {
        serve_loop(stream, |_, out| respond(out, "200 OK", &[], b"hello"));
    });
    let agent = Agent::new();

    for _ in 0..4 {
        let response = agent.get(&server.url("/")).call().expect("response");

        assert_eq!(response.status(), 200);
        assert_eq!(response.protocol(), Protocol::Http1);
        assert_eq!(response.text().expect("body"), "hello");
    }

    assert_eq!(server.accepted(), 1);
    assert_eq!(agent.pool_stats().idle_h1, 1);
}

#[test]
fn chunked_bodies_decode_and_keep_alive() {
    let server = spawn(|_, mut stream| {
        while read_request(&mut stream).is_some() {
            let reply = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";

            let _ = stream.write_all(reply);
        }
    });
    let agent = Agent::new();

    for _ in 0..2 {
        let text = agent
            .get(&server.url("/c"))
            .call()
            .expect("response")
            .text()
            .expect("body");

        assert_eq!(text, "hello world");
    }

    assert_eq!(server.accepted(), 1);
}

#[test]
fn headers_status_and_request_parts_arrive() {
    let server = spawn(|_, stream| {
        serve_loop(stream, |request, out| {
            let line = request.request_line().to_owned();
            let auth = request.header("authorization").unwrap_or("-").to_owned();
            let agent = request.header("user-agent").unwrap_or("-").to_owned();
            let custom = request.header("x-custom").unwrap_or("-").to_owned();
            let body = format!("{line}|{auth}|{agent}|{custom}");

            respond(
                out,
                "201 Created",
                &["x-reply: yes", "X-Multi: a"],
                body.as_bytes(),
            );
        });
    });
    let agent = Agent::builder()
        .user_agent("probe/1")
        .default_header("x-custom", "default")
        .build()
        .expect("agent");
    let response = agent
        .get(&server.url("/p?z=1"))
        .query([("a b", "c&d")])
        .basic_auth("user", Some("pass"))
        .call()
        .expect("response");

    assert_eq!(response.status(), 201);
    assert_eq!(response.header("X-Reply"), Some("yes"));
    assert!(
        response
            .headers()
            .any(|(name, _)| name.eq_ignore_ascii_case("x-multi"))
    );
    assert_eq!(
        response.text().expect("body"),
        "GET /p?z=1&a%20b=c%26d HTTP/1.1|Basic dXNlcjpwYXNz|probe/1|default"
    );

    let overridden = agent
        .get(&server.url("/"))
        .set("x-custom", "mine")
        .bearer_auth("tok")
        .call()
        .expect("response")
        .text()
        .expect("body");

    assert_eq!(overridden, "GET / HTTP/1.1|Bearer tok|probe/1|mine");
}

#[test]
fn request_bodies_reach_the_server() {
    let server = spawn(|_, stream| {
        serve_loop(stream, |request, out| {
            let kind = request.header("content-type").unwrap_or("-").to_owned();
            let length = request.header("content-length").unwrap_or("-").to_owned();
            let mut reply = format!("{}|{kind}|{length}|", request.request_line()).into_bytes();

            reply.extend_from_slice(&request.body);
            respond(out, "200 OK", &[], &reply);
        });
    });
    let agent = Agent::new();
    let big = vec![b'x'; 100_000];

    let raw = agent.post(&server.url("/r")).send(b"raw").expect("raw");

    assert_eq!(raw.text().expect("body"), "POST /r HTTP/1.1|-|3|raw");

    let text = agent
        .put(&server.url("/t"))
        .send_string("hi")
        .expect("text");

    assert_eq!(
        text.text().expect("body"),
        "PUT /t HTTP/1.1|text/plain; charset=utf-8|2|hi"
    );

    let form = agent
        .post(&server.url("/f"))
        .send_form(&[("a", "b c"), ("d", "e&f")])
        .expect("form");

    assert_eq!(
        form.text().expect("body"),
        "POST /f HTTP/1.1|application/x-www-form-urlencoded|13|a=b+c&d=e%26f"
    );

    let empty = agent.post(&server.url("/e")).call().expect("empty");

    assert_eq!(empty.text().expect("body"), "POST /e HTTP/1.1|-|0|");

    let large = agent
        .patch(&server.url("/big"))
        .send(&big)
        .expect("large")
        .bytes()
        .expect("body");

    assert!(large.ends_with(&big));
    assert_eq!(server.accepted(), 1);
}

#[test]
fn streaming_reader_yields_the_body_in_pieces_and_pools() {
    let total = 300_000usize;
    let server = spawn(move |_, mut stream| {
        while read_request(&mut stream).is_some() {
            let head = format!("HTTP/1.1 200 OK\r\ncontent-length: {total}\r\n\r\n");

            let _ = stream.write_all(head.as_bytes());

            let piece = vec![b'z'; 10_000];

            for _ in 0..total / piece.len() {
                let _ = stream.write_all(&piece);
                pause(1);
            }
        }
    });
    let agent = Agent::new();

    for _ in 0..2 {
        let mut reader = agent
            .get(&server.url("/stream"))
            .call()
            .expect("response")
            .into_reader();
        let mut buf = [0u8; 777];
        let mut seen = 0;

        loop {
            let count = reader.read(&mut buf).expect("read");

            if count == 0 {
                break;
            }

            assert!(buf[..count].iter().all(|byte| *byte == b'z'));
            seen += count;
        }

        assert_eq!(seen, total);
    }

    assert_eq!(server.accepted(), 1);
    assert_eq!(agent.pool_stats().idle_h1, 1);
}

#[test]
fn dropping_a_partly_read_body_closes_the_connection() {
    let server = spawn(|_, mut stream| {
        while read_request(&mut stream).is_some() {
            let head = "HTTP/1.1 200 OK\r\ncontent-length: 1000000\r\n\r\n";

            let _ = stream.write_all(head.as_bytes());
            let _ = stream.write_all(&[b'q'; 5000]);
            pause(100);
        }
    });
    let agent = Agent::new();
    let mut reader = agent
        .get(&server.url("/"))
        .call()
        .expect("response")
        .into_reader();
    let mut buf = [0u8; 10];

    reader.read_exact(&mut buf).expect("some bytes");
    drop(reader);

    assert_eq!(agent.pool_stats().idle_h1, 0);

    let _ = agent.get(&server.url("/")).call().expect("second");

    assert_eq!(server.accepted(), 2);
}

#[test]
fn exactly_read_then_dropped_body_is_pooled() {
    let server = spawn(|_, stream| {
        serve_loop(stream, |_, out| respond(out, "200 OK", &[], b"12345"));
    });
    let agent = Agent::new();
    let mut reader = agent
        .get(&server.url("/"))
        .call()
        .expect("response")
        .into_reader();
    let mut buf = [0u8; 5];

    reader.read_exact(&mut buf).expect("body");
    drop(reader);

    agent.get(&server.url("/")).call().expect("second");

    assert_eq!(server.accepted(), 1);
}

#[test]
fn unread_empty_body_responses_are_pooled() {
    let server = spawn(|_, stream| {
        serve_loop(stream, |request, out| {
            if request.request_line().starts_with("HEAD") {
                let _ = out.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 50\r\n\r\n");
            } else {
                let _ = out.write_all(b"HTTP/1.1 204 No Content\r\n\r\n");
            }
        });
    });
    let agent = Agent::new();

    agent.head(&server.url("/")).call().expect("head");
    agent.delete(&server.url("/")).call().expect("delete");
    agent.head(&server.url("/")).call().expect("head again");

    assert_eq!(server.accepted(), 1);
}

#[test]
fn server_closing_a_pooled_connection_is_survived() {
    let server = spawn(|_, mut stream| {
        if read_request(&mut stream).is_some() {
            respond(&mut stream, "200 OK", &[], b"once");
        }
    });
    let agent = Agent::new();

    for round in 0..3 {
        let text = agent
            .get(&server.url("/"))
            .call()
            .unwrap_or_else(|error| panic!("round {round}: {error}"))
            .text()
            .expect("body");

        assert_eq!(text, "once");
        pause(30);
    }

    assert_eq!(server.accepted(), 3);
}

#[test]
fn stale_connection_is_retried_for_idempotent_requests_only() {
    let server = spawn(|index, mut stream| {
        if index == 0 {
            if read_request(&mut stream).is_some() {
                respond(&mut stream, "200 OK", &[], b"first");
            }

            let _ = read_request(&mut stream);

            return;
        }

        serve_loop(stream, |_, out| respond(out, "200 OK", &[], b"again"));
    });
    let agent = Agent::new();

    agent.get(&server.url("/")).call().expect("first");

    let again = agent.get(&server.url("/")).call().expect("retried");

    assert_eq!(again.text().expect("body"), "again");
    assert_eq!(server.accepted(), 2);
}

#[test]
fn read_timeout_is_reported() {
    let server = spawn(|_, mut stream| {
        let _ = read_request(&mut stream);

        pause(2000);
    });
    let agent = Agent::builder()
        .read_timeout(Duration::from_millis(150))
        .build()
        .expect("agent");
    let started = Instant::now();
    let error = agent.get(&server.url("/")).call().expect_err("timeout");

    assert!(error.is_timeout(), "{error}");
    assert!(started.elapsed() < Duration::from_millis(1500));
}

#[test]
fn overall_timeout_cuts_a_dripping_body() {
    let server = spawn(|_, mut stream| {
        let _ = read_request(&mut stream);
        let _ = stream.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 100\r\n\r\n");

        for _ in 0..100 {
            if stream.write_all(b"x").is_err() {
                return;
            }

            pause(40);
        }
    });
    let agent = Agent::new();
    let response = agent
        .get(&server.url("/"))
        .timeout(Duration::from_millis(300))
        .call()
        .expect("head arrives in time");
    let error = response.bytes().expect_err("body is cut");

    assert!(error.is_timeout(), "{error}");
}

#[test]
fn connect_failure_is_classified() {
    let port = {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("bind");

        listener.local_addr().expect("addr").port()
    };
    let error = Agent::new()
        .get(&format!("http://127.0.0.1:{port}/"))
        .call()
        .expect_err("refused");

    assert!(error.is_connect(), "{error}");
    assert!(!error.is_timeout());
}

#[test]
fn error_statuses_turn_into_errors_unless_disabled() {
    let server = spawn(|_, stream| {
        serve_loop(stream, |_, out| respond(out, "404 Not Found", &[], b"nope"));
    });
    let error = Agent::new().get(&server.url("/")).call().expect_err("404");

    assert_eq!(error.status(), Some(404));
    assert!(matches!(error, Error::Status(404, _)));

    let body = error
        .into_response()
        .expect("response")
        .text()
        .expect("body");

    assert_eq!(body, "nope");

    let lenient = Agent::builder()
        .http_status_as_error(false)
        .build()
        .expect("agent");
    let response = lenient.get(&server.url("/")).call().expect("ok");

    assert_eq!(response.status(), 404);
}

#[test]
fn body_size_limit_is_enforced() {
    let server = spawn(|_, stream| {
        serve_loop(stream, |_, out| respond(out, "200 OK", &[], &[b'a'; 1000]));
    });
    let agent = Agent::builder().max_body(100).build().expect("agent");
    let error = agent
        .get(&server.url("/"))
        .call()
        .expect("response")
        .bytes()
        .expect_err("too large");

    assert!(matches!(error, Error::BodyTooLarge { limit: 100 }));
}

#[test]
fn bad_input_is_rejected_before_sending() {
    let agent = Agent::new();

    assert!(matches!(
        agent.get("ftp://x/").call(),
        Err(Error::InvalidUrl(_))
    ));
    assert!(matches!(
        agent.get("http://127.0.0.1:1/").set("bad name", "v").call(),
        Err(Error::InvalidRequest(_))
    ));
    assert!(matches!(
        agent
            .get("http://127.0.0.1:1/")
            .set("x", "a\r\nb: c")
            .call(),
        Err(Error::InvalidRequest(_))
    ));
}

#[test]
fn pool_caps_idle_connections() {
    let server = spawn(|_, stream| {
        serve_loop(stream, |_, out| respond(out, "200 OK", &[], b"ok"));
    });
    let agent = Agent::builder()
        .pool_max_idle_per_host(2)
        .build()
        .expect("agent");
    let url = server.url("/");
    let first: Vec<_> = (0..4)
        .map(|_| agent.get(&url).call().expect("response"))
        .collect();

    for response in first {
        response.text().expect("body");
    }

    assert_eq!(agent.pool_stats().idle_h1, 2);
}
