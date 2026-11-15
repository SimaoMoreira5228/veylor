#![cfg(feature = "gzip")]

use std::io::Write;

use flate2::Compression;
use flate2::write::GzEncoder;
use veylor_fetch::{Agent, Error};

use crate::support::{respond, serve_loop, spawn};

fn compress(plain: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());

    encoder.write_all(plain).expect("write");
    encoder.finish().expect("finish")
}

#[test]
fn gzip_responses_are_decoded() {
    let plain = "hello gzip ".repeat(500);
    let wire = compress(plain.as_bytes());
    let server = spawn(move |_, stream| {
        serve_loop(stream, |request, out| {
            let accepts = request.header("accept-encoding").unwrap_or("");

            if accepts.contains("gzip") {
                respond(out, "200 OK", &["content-encoding: gzip"], &wire);
            } else {
                respond(out, "400 Bad Request", &[], b"no gzip offered");
            }
        });
    });
    let text = Agent::new()
        .get(&server.url("/"))
        .call()
        .expect("response")
        .text()
        .expect("body");

    assert_eq!(text, plain);
}

#[test]
fn head_requests_skip_the_decoder() {
    let server = spawn(|_, stream| {
        serve_loop(stream, |request, out| {
            assert!(request.header("accept-encoding").is_none());
            respond(out, "200 OK", &["content-encoding: gzip"], b"");
        });
    });
    let bytes = Agent::new()
        .head(&server.url("/"))
        .call()
        .expect("response")
        .bytes()
        .expect("body");

    assert!(bytes.is_empty());
}

#[test]
fn gzip_garbage_is_an_error() {
    let server = spawn(|_, stream| {
        serve_loop(stream, |_, out| {
            respond(
                out,
                "200 OK",
                &["content-encoding: gzip"],
                b"definitely not gzip",
            );
        });
    });
    let error = Agent::new()
        .get(&server.url("/"))
        .call()
        .expect("response")
        .bytes()
        .expect_err("garbage");

    assert!(
        matches!(error, Error::Io(_) | Error::Protocol(_)),
        "{error}"
    );
}
