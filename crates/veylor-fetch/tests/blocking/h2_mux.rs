use std::io::Read;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use veylor_fetch::{Agent, Error, Protocol};

use super::mux_peer::{CERT, Peer, Plan, Script, spawn};

const CANCEL: u32 = 8;

fn agent() -> Agent {
    Agent::builder()
        .builtin_root_certificates(false)
        .add_root_certificate(CERT)
        .timeout(Duration::from_secs(20))
        .build()
        .expect("agent")
}

fn url(peer: &Peer, path: &str) -> String {
    format!("https://localhost:{}{path}", peer.port)
}

fn read_all(mut reader: impl Read, chunk: usize) -> Vec<u8> {
    let mut out = Vec::new();
    let mut buf = vec![0u8; chunk];

    loop {
        let count = reader.read(&mut buf).expect("read");

        if count == 0 {
            return out;
        }

        out.extend_from_slice(&buf[..count]);
    }
}

fn wait_for(what: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);

    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn many_threads_share_one_connection() {
    let peer = spawn(Script::new(|_| Plan::Body(1_000)));
    let agent = agent();
    let target = url(&peer, "/");
    let workers: Vec<_> = (0..16)
        .map(|_| {
            let agent = agent.clone();
            let target = target.clone();

            std::thread::spawn(move || {
                for _ in 0..25 {
                    let response = agent.get(&target).call().expect("response");

                    assert_eq!(response.protocol(), Protocol::Http2);
                    assert_eq!(response.bytes().expect("body").len(), 1_000);
                }
            })
        })
        .collect();

    for worker in workers {
        worker.join().expect("worker");
    }

    assert_eq!(peer.accepted.load(Ordering::SeqCst), 1);
    assert_eq!(peer.opened.load(Ordering::SeqCst), 400);
    assert_eq!(agent.pool_stats().h2, 1);
}

#[test]
fn requests_are_in_flight_together_on_one_connection() {
    let mut script = Script::new(|_| Plan::Body(100));

    script.release_at = 8;

    let peer = spawn(script);
    let agent = agent();
    let target = url(&peer, "/");
    let workers: Vec<_> = (0..8)
        .map(|_| {
            let agent = agent.clone();
            let target = target.clone();

            std::thread::spawn(move || agent.get(&target).call().expect("response").bytes())
        })
        .collect();

    for worker in workers {
        assert_eq!(worker.join().expect("worker").expect("body").len(), 100);
    }

    assert_eq!(peer.accepted.load(Ordering::SeqCst), 1);
    assert_eq!(peer.max_open.load(Ordering::SeqCst), 8);
    assert_eq!(agent.pool_stats().h2, 1);
}

#[test]
fn a_slow_reader_does_not_block_a_fast_one() {
    let peer = spawn(Script::new(|index| {
        if index == 0 {
            Plan::Body(2_000_000)
        } else {
            Plan::Body(300_000)
        }
    }));
    let agent = agent();
    let slow = agent.get(&url(&peer, "/slow")).call().expect("slow");
    let started = Instant::now();

    for _ in 0..10 {
        let body = agent
            .get(&url(&peer, "/fast"))
            .call()
            .expect("fast")
            .bytes()
            .expect("fast body");

        assert_eq!(body.len(), 300_000);
    }

    assert!(started.elapsed() < Duration::from_secs(10));

    let body = read_all(slow.into_reader(), 64 * 1024);

    assert_eq!(body.len(), 2_000_000);
    assert!(body.iter().all(|byte| *byte == 0));
    assert_eq!(peer.accepted.load(Ordering::SeqCst), 1);
}

#[test]
fn a_slow_reader_does_not_block_concurrent_fast_readers() {
    let peer = spawn(Script::new(|index| {
        if index < 4 {
            Plan::Body(1_000_000)
        } else {
            Plan::Body(200_000)
        }
    }));
    let agent = agent();
    let slow: Vec<_> = (0..4)
        .map(|_| agent.get(&url(&peer, "/slow")).call().expect("slow"))
        .collect();
    let workers: Vec<_> = (0..4)
        .map(|_| {
            let agent = agent.clone();
            let target = url(&peer, "/fast");

            std::thread::spawn(move || {
                for _ in 0..5 {
                    let body = agent
                        .get(&target)
                        .call()
                        .expect("fast")
                        .bytes()
                        .expect("body");

                    assert_eq!(body.len(), 200_000);
                }
            })
        })
        .collect();

    for worker in workers {
        worker.join().expect("worker");
    }

    for response in slow {
        assert_eq!(read_all(response.into_reader(), 32 * 1024).len(), 1_000_000);
    }

    assert_eq!(peer.accepted.load(Ordering::SeqCst), 1);
}

#[test]
fn dropping_a_body_cancels_only_its_stream() {
    let peer = spawn(Script::new(|index| {
        if index == 0 {
            Plan::Body(5_000_000)
        } else {
            Plan::Body(400_000)
        }
    }));
    let agent = agent();
    let mut cancelled = agent
        .get(&url(&peer, "/a"))
        .call()
        .expect("first")
        .into_reader();
    let mut sibling = agent
        .get(&url(&peer, "/b"))
        .call()
        .expect("sibling")
        .into_reader();
    let mut head = [0u8; 10];

    cancelled.read_exact(&mut head).expect("some bytes");
    drop(cancelled);

    assert_eq!(read_all(&mut sibling, 8 * 1024).len(), 400_000);

    wait_for("the reset", || !peer.resets.lock().is_empty());

    assert_eq!(peer.resets.lock()[0], (1, CANCEL));

    let again = agent.get(&url(&peer, "/c")).call().expect("again");

    assert_eq!(again.bytes().expect("body").len(), 400_000);
    assert_eq!(peer.accepted.load(Ordering::SeqCst), 1);
    assert_eq!(agent.pool_stats().h2, 1);
}

#[test]
fn goaway_finishes_in_flight_streams_and_reconnects() {
    let mut script = Script::new(|_| Plan::Body(1_500_000));

    script.goaway_at = Some(1);

    let peer = spawn(script);
    let agent = agent();
    let first = agent.get(&url(&peer, "/first")).call().expect("first");
    let second = agent.get(&url(&peer, "/second")).call().expect("second");

    assert_eq!(peer.accepted.load(Ordering::SeqCst), 2);
    assert_eq!(second.bytes().expect("second body").len(), 1_500_000);
    assert_eq!(read_all(first.into_reader(), 16 * 1024).len(), 1_500_000);

    let third = agent.get(&url(&peer, "/third")).call().expect("third");

    assert_eq!(third.bytes().expect("third body").len(), 1_500_000);
    assert_eq!(peer.accepted.load(Ordering::SeqCst), 2);
}

#[test]
fn the_peer_stream_limit_holds_under_saturation() {
    let mut script = Script::new(|_| Plan::Body(300_000));

    script.max_concurrent = Some(2);

    let peer = spawn(script);
    let agent = agent();
    let target = url(&peer, "/");
    let workers: Vec<_> = (0..6)
        .map(|_| {
            let agent = agent.clone();
            let target = target.clone();

            std::thread::spawn(move || agent.get(&target).call().expect("response").bytes())
        })
        .collect();

    for worker in workers {
        assert_eq!(worker.join().expect("worker").expect("body").len(), 300_000);
    }

    assert_eq!(peer.opened.load(Ordering::SeqCst), 6);
    assert_eq!(peer.violations.load(Ordering::SeqCst), 0);
    assert!(peer.max_open.load(Ordering::SeqCst) <= 2);
    assert!(peer.accepted.load(Ordering::SeqCst) <= 2);
}

#[test]
fn large_concurrent_bodies_cross_small_windows_intact() {
    let peer = spawn(Script::new(|_| Plan::Body(1_500_000)));
    let agent = agent();
    let target = url(&peer, "/");
    let workers: Vec<_> = (0..8)
        .map(|_| {
            let agent = agent.clone();
            let target = target.clone();

            std::thread::spawn(move || {
                let response = agent.get(&target).call().expect("response");

                read_all(response.into_reader(), 4_096)
            })
        })
        .collect();

    for worker in workers {
        let body = worker.join().expect("worker");

        assert_eq!(body.len(), 1_500_000);
        assert!(body.iter().all(|byte| *byte == body[0]));
    }

    assert_eq!(peer.accepted.load(Ordering::SeqCst), 1);
}

#[test]
fn concurrent_uploads_get_their_own_credit() {
    let peer = spawn(Script::new(|_| Plan::Echo));
    let agent = agent();
    let target = url(&peer, "/up");
    let workers: Vec<_> = (0..4)
        .map(|worker| {
            let agent = agent.clone();
            let target = target.clone();

            std::thread::spawn(move || {
                let body = vec![b'u'; 300_000 + worker];
                let text = agent.post(&target).send(&body).expect("upload").text();

                (text.expect("text"), body.len())
            })
        })
        .collect();

    for worker in workers {
        let (text, length) = worker.join().expect("worker");

        assert_eq!(text, format!("got:{length}"));
    }

    assert_eq!(peer.accepted.load(Ordering::SeqCst), 1);
}

#[test]
fn a_timeout_hits_only_its_own_stream() {
    let peer = spawn(Script::new(|index| {
        if index == 0 {
            Plan::Silent
        } else {
            Plan::Body(10_000)
        }
    }));
    let agent = agent();
    let target = url(&peer, "/");
    let waiting = {
        let agent = agent.clone();
        let target = target.clone();

        std::thread::spawn(move || {
            agent
                .get(&target)
                .timeout(Duration::from_millis(400))
                .call()
                .map(|_| ())
        })
    };

    wait_for("the silent stream", || {
        peer.opened.load(Ordering::SeqCst) == 1
    });

    for _ in 0..5 {
        let body = agent
            .get(&target)
            .call()
            .expect("sibling")
            .bytes()
            .expect("body");

        assert_eq!(body.len(), 10_000);
    }

    let error = waiting.join().expect("waiter").expect_err("timed out");

    assert!(error.is_timeout(), "{error}");

    wait_for("the cancel", || !peer.resets.lock().is_empty());

    assert_eq!(peer.resets.lock()[0], (1, CANCEL));
    assert_eq!(peer.accepted.load(Ordering::SeqCst), 1);
}

#[test]
fn a_slow_body_times_out_without_touching_siblings() {
    let peer = spawn(Script::new(|index| {
        if index == 0 {
            Plan::Body(5_000_000)
        } else {
            Plan::Body(10_000)
        }
    }));
    let agent = Agent::builder()
        .builtin_root_certificates(false)
        .add_root_certificate(CERT)
        .read_timeout(Duration::from_millis(300))
        .build()
        .expect("agent");
    let mut stalled = agent
        .get(&url(&peer, "/a"))
        .call()
        .expect("head")
        .into_reader();
    let mut first = [0u8; 16];

    stalled.read_exact(&mut first).expect("first bytes");
    std::thread::sleep(Duration::from_millis(50));

    let body = agent
        .get(&url(&peer, "/b"))
        .call()
        .expect("sibling")
        .bytes();

    assert_eq!(body.expect("sibling body").len(), 10_000);
    assert_eq!(peer.accepted.load(Ordering::SeqCst), 1);
}

#[test]
fn a_peer_reset_fails_one_stream_and_keeps_the_connection() {
    let peer = spawn(Script::new(|index| {
        if index == 0 {
            Plan::Reset(2)
        } else {
            Plan::Body(100)
        }
    }));
    let agent = agent();
    let error: Error = agent.get(&url(&peer, "/")).call().expect_err("reset");

    assert!(!error.is_timeout(), "{error}");

    let body = agent.get(&url(&peer, "/")).call().expect("again").bytes();

    assert_eq!(body.expect("body").len(), 100);
    assert_eq!(peer.accepted.load(Ordering::SeqCst), 1);
}

#[test]
fn an_idle_connection_closes_after_the_idle_timeout() {
    let peer = spawn(Script::new(|_| Plan::Body(10)));
    let agent = Agent::builder()
        .builtin_root_certificates(false)
        .add_root_certificate(CERT)
        .pool_idle_timeout(Duration::from_millis(200))
        .build()
        .expect("agent");

    agent
        .get(&url(&peer, "/"))
        .call()
        .expect("response")
        .bytes()
        .expect("body");

    wait_for("the idle close", || peer.closed.load(Ordering::SeqCst) == 1);

    assert_eq!(agent.pool_stats().h2, 0);

    agent
        .get(&url(&peer, "/"))
        .call()
        .expect("again")
        .bytes()
        .expect("body");

    assert_eq!(peer.accepted.load(Ordering::SeqCst), 2);
}

#[test]
fn dropping_the_agent_closes_its_connection() {
    let peer = spawn(Script::new(|_| Plan::Body(10)));
    let agent = agent();

    agent
        .get(&url(&peer, "/"))
        .call()
        .expect("response")
        .bytes()
        .expect("body");
    drop(agent);

    wait_for("the abandoned close", || {
        peer.closed.load(Ordering::SeqCst) == 1
    });
}
