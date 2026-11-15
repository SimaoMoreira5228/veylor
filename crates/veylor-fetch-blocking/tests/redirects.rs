mod support;

use std::sync::Arc;

use parking_lot::Mutex;
use support::{Raw, respond, serve_loop, spawn};
use veylor_fetch_blocking::{Agent, Error};

type Seen = Arc<Mutex<Vec<String>>>;

fn summary(request: &Raw) -> String {
    let auth = request.header("authorization").unwrap_or("-");
    let body = String::from_utf8_lossy(&request.body);

    format!("{}|{auth}|{body}", request.request_line())
}

fn redirecting(seen: &Seen, status: &'static str, location: String) -> support::TestServer {
    let seen = Arc::clone(seen);

    spawn(move |_, stream| {
        let seen = Arc::clone(&seen);
        let location = location.clone();

        serve_loop(stream, move |request, out| {
            seen.lock().push(summary(request));

            if request.request_line().contains("/final") {
                respond(out, "200 OK", &[], summary(request).as_bytes());
            } else {
                let header = format!("location: {location}");

                respond(out, status, &[header.as_str()], b"");
            }
        });
    })
}

#[test]
fn follows_a_302_with_get_and_drops_the_body() {
    let seen: Seen = Arc::default();
    let server = redirecting(&seen, "302 Found", "/final".to_owned());
    let agent = Agent::new();
    let response = agent
        .post(&server.url("/start"))
        .send_string("payload")
        .expect("response");

    assert!(response.url().ends_with("/final"));
    assert_eq!(
        response.into_string().expect("body"),
        "GET /final HTTP/1.1|-|"
    );
    assert_eq!(seen.lock().len(), 2);
    assert_eq!(server.accepted(), 1);
}

#[test]
fn a_303_turns_put_into_get_but_keeps_head() {
    let seen: Seen = Arc::default();
    let server = redirecting(&seen, "303 See Other", "/final".to_owned());
    let agent = Agent::new();
    let put = agent
        .put(&server.url("/a"))
        .send(b"body")
        .expect("put")
        .into_string()
        .expect("body");

    assert_eq!(put, "GET /final HTTP/1.1|-|");

    let head = agent.head(&server.url("/a")).call().expect("head");

    assert_eq!(head.status(), 200);
    assert!(
        seen.lock()
            .iter()
            .any(|line| line.starts_with("HEAD /final"))
    );
}

#[test]
fn a_307_keeps_method_and_body() {
    let seen: Seen = Arc::default();
    let server = redirecting(&seen, "307 Temporary Redirect", "/final".to_owned());
    let agent = Agent::new();
    let text = agent
        .post(&server.url("/start"))
        .send_string("again")
        .expect("response")
        .into_string()
        .expect("body");

    assert_eq!(text, "POST /final HTTP/1.1|-|again");
}

#[test]
fn a_308_keeps_put_and_301_turns_post_into_get() {
    let seen: Seen = Arc::default();
    let permanent = redirecting(&seen, "308 Permanent Redirect", "/final".to_owned());
    let moved = redirecting(&seen, "301 Moved Permanently", "/final".to_owned());
    let agent = Agent::new();
    let put = agent
        .put(&permanent.url("/s"))
        .send(b"x")
        .expect("put")
        .into_string()
        .expect("body");
    let post = agent
        .post(&moved.url("/s"))
        .send(b"x")
        .expect("post")
        .into_string()
        .expect("body");

    assert_eq!(put, "PUT /final HTTP/1.1|-|x");
    assert_eq!(post, "GET /final HTTP/1.1|-|");
}

#[test]
fn relative_locations_resolve_against_the_request_path() {
    let seen: Seen = Arc::default();
    let server = redirecting(&seen, "302 Found", "../final".to_owned());
    let agent = Agent::new();

    agent
        .get(&server.url("/a/b/c"))
        .call()
        .expect("response")
        .into_string()
        .expect("body");

    assert_eq!(seen.lock()[1], "GET /a/final HTTP/1.1|-|");
}

#[test]
fn redirect_loops_hit_the_limit() {
    let seen: Seen = Arc::default();
    let server = redirecting(&seen, "302 Found", "/loop".to_owned());
    let error = Agent::new()
        .get(&server.url("/loop"))
        .call()
        .expect_err("loop");

    assert!(matches!(error, Error::TooManyRedirects));
    assert_eq!(seen.lock().len(), 6);

    let tight = Agent::builder().redirects(2).build().expect("agent");

    seen.lock().clear();

    assert!(matches!(
        tight.get(&server.url("/loop")).call(),
        Err(Error::TooManyRedirects)
    ));
    assert_eq!(seen.lock().len(), 3);
}

#[test]
fn redirects_can_be_turned_off() {
    let seen: Seen = Arc::default();
    let server = redirecting(&seen, "302 Found", "/final".to_owned());
    let agent = Agent::builder().redirects(0).build().expect("agent");
    let response = agent.get(&server.url("/start")).call().expect("response");

    assert_eq!(response.status(), 302);
    assert_eq!(response.header("location"), Some("/final"));
}

#[test]
fn authorization_is_dropped_only_across_origins() {
    let seen_target: Seen = Arc::default();
    let target = redirecting(&seen_target, "302 Found", "/unused".to_owned());
    let seen_source: Seen = Arc::default();
    let cross = redirecting(&seen_source, "302 Found", target.url("/final"));
    let agent = Agent::new();

    let crossed = agent
        .get(&cross.url("/start"))
        .bearer_auth("secret")
        .call()
        .expect("cross origin")
        .into_string()
        .expect("body");

    assert_eq!(crossed, "GET /final HTTP/1.1|-|");
    assert_eq!(seen_source.lock()[0], "GET /start HTTP/1.1|Bearer secret|");

    let same: Seen = Arc::default();
    let local = redirecting(&same, "302 Found", "/final".to_owned());
    let kept = agent
        .get(&local.url("/start"))
        .bearer_auth("secret")
        .call()
        .expect("same origin")
        .into_string()
        .expect("body");

    assert_eq!(kept, "GET /final HTTP/1.1|Bearer secret|");
}
