use std::io::{self, Read};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use veylor_fetch::Agent;

use super::mux_peer::{CERT, Peer, Plan, Script, spawn};

fn agent() -> Agent {
    Agent::builder()
        .builtin_root_certificates(false)
        .add_root_certificate(CERT)
        .timeout(Duration::from_secs(60))
        .build()
        .expect("agent")
}

fn url(peer: &Peer, path: &str) -> String {
    format!("https://localhost:{}{path}", peer.port)
}

fn wait_for(what: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);

    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
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

        buf[..count].fill(b'u');
        self.remaining -= count;

        Ok(count)
    }
}

#[test]
fn a_streamed_upload_larger_than_the_window_completes() {
    let peer = spawn(Script::new(|_| Plan::Echo));
    let agent = agent();
    let total = 1_500_000;
    let mut body = Paced {
        remaining: total,
        step: 7_919,
    };
    let text = agent
        .post(&url(&peer, "/up"))
        .send_stream(&mut body)
        .expect("upload")
        .text()
        .expect("body");

    assert_eq!(text, format!("got:{total}"));
    assert_eq!(body.remaining, 0);
    assert_eq!(peer.accepted.load(Ordering::SeqCst), 1);
}

#[test]
fn concurrent_streamed_uploads_share_one_connection() {
    let mut script = Script::new(|_| Plan::Echo);

    script.release_at = 4;

    let peer = spawn(script);
    let agent = agent();
    let target = url(&peer, "/up");
    let workers: Vec<_> = (0..4)
        .map(|worker| {
            let agent = agent.clone();
            let target = target.clone();

            std::thread::spawn(move || {
                let total = 300_000 + worker;
                let mut body = Paced {
                    remaining: total,
                    step: 4_093,
                };
                let text = agent
                    .post(&target)
                    .send_stream(&mut body)
                    .expect("upload")
                    .text()
                    .expect("body");

                assert_eq!(text, format!("got:{total}"));
            })
        })
        .collect();

    for worker in workers {
        worker.join().expect("worker");
    }

    assert_eq!(peer.accepted.load(Ordering::SeqCst), 1);
    assert!(peer.max_open.load(Ordering::SeqCst) >= 2);
}

#[test]
fn an_early_response_stops_the_upload() {
    let mut script = Script::new(|_| Plan::Silent);

    script.respond_at = Some(1);

    let peer = spawn(script);
    let agent = agent();
    let mut body = Paced {
        remaining: 64 * 1024 * 1024,
        step: 16 * 1024,
    };
    let response = agent
        .post(&url(&peer, "/up"))
        .send_stream(&mut body)
        .expect("early response");

    assert_eq!(response.status(), 200);
    assert_eq!(response.text().expect("body"), "early");
    assert!(body.remaining > 0);
    assert_eq!(peer.accepted.load(Ordering::SeqCst), 1);
}

#[test]
fn a_short_stream_against_a_declared_length_resets_the_stream() {
    let peer = spawn(Script::new(|_| Plan::Echo));
    let agent = agent();
    let mut body = Paced {
        remaining: 200_000,
        step: 16 * 1024,
    };
    let error = agent
        .post(&url(&peer, "/up"))
        .set("content-length", "1000000")
        .send_stream(&mut body)
        .expect_err("short");

    assert!(
        error.to_string().contains("declared content-length"),
        "{error}"
    );

    wait_for("the cancel", || !peer.resets.lock().is_empty());

    assert_eq!(peer.resets.lock()[0], (1, 8));
    assert_eq!(peer.accepted.load(Ordering::SeqCst), 1);
}

#[test]
fn a_long_stream_against_a_declared_length_is_an_error() {
    let peer = spawn(Script::new(|_| Plan::Echo));
    let agent = agent();
    let mut body = Paced {
        remaining: 300_000,
        step: 16 * 1024,
    };
    let error = agent
        .post(&url(&peer, "/up"))
        .set("content-length", "200000")
        .send_stream(&mut body)
        .expect_err("long");

    assert!(
        error.to_string().contains("declared content-length"),
        "{error}"
    );

    wait_for("the cancel", || !peer.resets.lock().is_empty());

    assert_eq!(peer.resets.lock()[0], (1, 8));
}

#[test]
fn a_reset_upload_fails_promptly_and_the_connection_survives() {
    let mut script = Script::new(|index| if index == 0 { Plan::Silent } else { Plan::Echo });

    script.reset_after = Some(1);

    let peer = spawn(script);
    let agent = agent();
    let mut body = Paced {
        remaining: 64 * 1024 * 1024,
        step: 16 * 1024,
    };
    let started = Instant::now();
    let error = agent
        .post(&url(&peer, "/up"))
        .send_stream(&mut body)
        .expect_err("reset");

    assert!(!error.is_timeout(), "{error}");
    assert!(!error.is_connect(), "{error}");
    assert!(error.to_string().contains("reset"), "{error}");
    assert!(started.elapsed() < Duration::from_secs(10));
    assert!(body.remaining > 0);

    let text = agent
        .get(&url(&peer, "/after"))
        .call()
        .expect("after")
        .text()
        .expect("body");

    assert_eq!(text, "got:0");
    assert_eq!(peer.accepted.load(Ordering::SeqCst), 1);
    assert!(peer.resets.lock().is_empty());
}
