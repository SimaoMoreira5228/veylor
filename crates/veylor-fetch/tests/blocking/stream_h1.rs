use std::io::{self, Read, Write};

use crate::support::{read_request, respond, serve_loop, spawn};
use veylor_fetch::{Agent, Error};

// NOTE: the peer must decode the framing itself, so the assertions cover the wire format the
// client actually produced rather than a body the server reassembled for us.
fn summary(request: &crate::support::Raw) -> String {
    let chunked = request
        .header("transfer-encoding")
        .is_some_and(|value| value.eq_ignore_ascii_case("chunked"));
    let declared = request.header("content-length").unwrap_or("-");
    let all = request.body.iter().all(|byte| *byte == b'x');

    format!("{chunked}|{declared}|{}|{all}", request.body.len())
}

struct Paced {
    remaining: usize,
    step: usize,
}

impl Read for Paced {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.remaining == 0 || buf.is_empty() {
            return Ok(0);
        }

        let count = self.remaining.min(self.step).min(buf.len());

        buf[..count].fill(b'x');
        self.remaining -= count;

        Ok(count)
    }
}

struct Failing {
    remaining: usize,
}

impl Read for Failing {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            return Err(io::Error::other("source surrendered"));
        }

        if buf.is_empty() {
            return Ok(0);
        }

        let count = self.remaining.min(buf.len());

        buf[..count].fill(b'e');
        self.remaining -= count;

        Ok(count)
    }
}

#[test]
fn chunked_stream_reaches_a_raw_peer_and_keeps_the_connection() {
    let server = spawn(|_, stream| {
        serve_loop(stream, |request, out| {
            respond(out, "200 OK", &[], summary(request).as_bytes());
        });
    });
    let agent = Agent::new();
    let total = 2 * 1024 * 1024;
    let mut body = Paced {
        remaining: total,
        step: 997,
    };
    let text = agent
        .post(&server.url("/up"))
        .send_stream(&mut body)
        .expect("stream")
        .text()
        .expect("body");

    assert_eq!(text, format!("true|-|{total}|true"));
    assert_eq!(body.remaining, 0);

    let second = agent
        .post(&server.url("/again"))
        .send_stream(&mut Paced {
            remaining: 10,
            step: 3,
        })
        .expect("second")
        .text()
        .expect("body");

    assert_eq!(second, "true|-|10|true");

    let empty = agent
        .get(&server.url("/empty"))
        .send_stream(&mut Paced {
            remaining: 0,
            step: 8,
        })
        .expect("empty")
        .text()
        .expect("body");

    assert_eq!(empty, "true|-|0|true");
    assert_eq!(server.accepted(), 1);
}

#[test]
fn a_reader_error_aborts_the_upload_and_drops_the_connection() {
    let server = spawn(|index, mut stream| {
        if index == 0 {
            let _ = read_request(&mut stream);

            return;
        }

        serve_loop(stream, |_, out| respond(out, "200 OK", &[], b"ok"));
    });
    let agent = Agent::new();
    let mut body = Failing { remaining: 100_000 };
    let error = agent
        .post(&server.url("/down"))
        .send_stream(&mut body)
        .expect_err("reader failed");

    assert!(matches!(error, Error::Io(_)), "{error}");
    assert!(error.to_string().contains("source surrendered"), "{error}");
    assert_eq!(agent.pool_stats().idle_h1, 0);

    let text = agent
        .get(&server.url("/after"))
        .call()
        .expect("after")
        .text()
        .expect("body");

    assert_eq!(text, "ok");
    assert_eq!(server.accepted(), 2);
}

#[test]
fn declared_content_length_uses_fixed_framing() {
    let server = spawn(|_, stream| {
        serve_loop(stream, |request, out| {
            respond(out, "200 OK", &[], summary(request).as_bytes());
        });
    });
    let agent = Agent::new();
    let mut body = Paced {
        remaining: 5,
        step: 5,
    };
    let text = agent
        .put(&server.url("/fixed"))
        .set("content-length", "5")
        .send_stream(&mut body)
        .expect("exact")
        .text()
        .expect("body");

    assert_eq!(text, "false|5|5|true");
    assert_eq!(server.accepted(), 1);
}

#[test]
fn a_short_stream_against_a_declared_length_is_an_error() {
    let server = spawn(|_, mut stream| {
        let _ = read_request(&mut stream);
    });
    let agent = Agent::new();
    let mut body = Paced {
        remaining: 3,
        step: 3,
    };
    let error = agent
        .post(&server.url("/short"))
        .set("content-length", "5")
        .send_stream(&mut body)
        .expect_err("short");

    assert!(matches!(error, Error::Protocol(_)), "{error}");
    assert!(
        error.to_string().contains("declared content-length"),
        "{error}"
    );
    assert_eq!(agent.pool_stats().idle_h1, 0);
}

#[test]
fn a_long_stream_against_a_declared_length_is_an_error() {
    let server = spawn(|_, mut stream| {
        let _ = read_request(&mut stream);
        let _ = stream.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n");
    });
    let agent = Agent::new();
    let mut body = Paced {
        remaining: 9,
        step: 4,
    };
    let error = agent
        .post(&server.url("/long"))
        .set("content-length", "3")
        .send_stream(&mut body)
        .expect_err("long");

    assert!(matches!(error, Error::Protocol(_)), "{error}");
    assert!(
        error.to_string().contains("declared content-length"),
        "{error}"
    );
    assert_eq!(agent.pool_stats().idle_h1, 0);
}
