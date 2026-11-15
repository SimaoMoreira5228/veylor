#![cfg(feature = "json")]

mod support;

use serde::{Deserialize, Serialize};
use support::{respond, serve_loop, spawn};
use veylor_fetch_blocking::Agent;

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct Item {
    name: String,
    count: u32,
}

#[test]
fn json_round_trips_through_an_echo_server() {
    let server = spawn(|_, stream| {
        serve_loop(stream, |request, out| {
            let kind = request.header("content-type").unwrap_or("-");

            assert_eq!(kind, "application/json");
            respond(
                out,
                "200 OK",
                &["content-type: application/json"],
                &request.body,
            );
        });
    });
    let sent = Item {
        name: "bolt".to_owned(),
        count: 7,
    };
    let back: Item = Agent::new()
        .post(&server.url("/echo"))
        .send_json(&sent)
        .expect("response")
        .into_json()
        .expect("json");

    assert_eq!(back, sent);
}

#[test]
fn malformed_json_is_a_json_error() {
    let server = spawn(|_, stream| {
        serve_loop(stream, |_, out| respond(out, "200 OK", &[], b"{nope"));
    });
    let error = Agent::new()
        .get(&server.url("/"))
        .call()
        .expect("response")
        .into_json::<Item>()
        .expect_err("bad json");

    assert!(matches!(error, veylor_fetch_blocking::Error::Json(_)));
}
