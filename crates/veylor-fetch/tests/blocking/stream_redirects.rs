use std::io::{self, Read};
use std::sync::Arc;

use crate::support::{Raw, TestServer, respond, serve_loop, spawn};
use parking_lot::Mutex;
use veylor_fetch::{Agent, Error};

type Hits = Arc<Mutex<usize>>;

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

        buf[..count].fill(b'r');
        self.remaining -= count;

        Ok(count)
    }
}

fn summary(request: &Raw) -> String {
    let declared = request.header("content-length").unwrap_or("-");

    format!(
        "{}|{}|{declared}",
        request.request_line(),
        request.body.len()
    )
}

fn redirecting(status: &'static str) -> (TestServer, Hits) {
    let hits: Hits = Arc::new(Mutex::new(0));
    let seen = Arc::clone(&hits);
    let server = spawn(move |_, stream| {
        let seen = Arc::clone(&seen);

        serve_loop(stream, move |request, out| {
            if request.request_line().contains("/final") {
                *seen.lock() += 1;
                respond(out, "200 OK", &[], summary(request).as_bytes());
            } else {
                let location = "location: /final";

                respond(out, status, &[location], b"");
            }
        });
    });

    (server, hits)
}

#[test]
fn a_303_turns_a_streamed_body_into_a_bodyless_get() {
    let (server, hits) = redirecting("303 See Other");
    let agent = Agent::new();
    let mut body = Paced {
        remaining: 4096,
        step: 500,
    };
    let text = agent
        .post(&server.url("/start"))
        .send_stream(&mut body)
        .expect("redirect")
        .text()
        .expect("body");

    assert_eq!(text, "GET /final HTTP/1.1|0|-");
    assert_eq!(*hits.lock(), 1);
    assert_eq!(body.remaining, 0);
    assert_eq!(server.accepted(), 1);
}

#[test]
fn a_307_cannot_replay_a_streamed_body() {
    let (server, hits) = redirecting("307 Temporary Redirect");
    let agent = Agent::new();
    let mut body = Paced {
        remaining: 4096,
        step: 500,
    };
    let error = agent
        .post(&server.url("/start"))
        .send_stream(&mut body)
        .expect_err("307");

    assert!(matches!(error, Error::InvalidRequest(_)), "{error}");
    assert!(error.to_string().contains("streamed"), "{error}");
    assert_eq!(*hits.lock(), 0);
    assert_eq!(body.remaining, 0);
}
