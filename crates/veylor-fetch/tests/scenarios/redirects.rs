fn report(request: &Req) -> String {
    format!(
        "{}|{}|{}|{}|{}",
        request.method,
        request.header("authorization").unwrap_or("-"),
        request.header("content-type").unwrap_or("-"),
        request.header("cookie").unwrap_or("-"),
        String::from_utf8_lossy(&request.body),
    )
}

fn redirector(status: &'static str, location: &'static str) -> TestServer {
    serve(move |request, socket| {
        if request.target == "/start" {
            let body: &[u8] = if request.method == "HEAD" { b"" } else { b"moved" };
            respond(socket, status, &[("location", location)], body);
        } else {
            respond(socket, "200 OK", &[], report(request).as_bytes());
        }
    })
}

#[test]
fn relative_redirects_are_followed() {
    run(async {
        let server = serve(|request, socket| match request.target.as_str() {
            "/a/start" => respond(socket, "302 Found", &[("location", "next")], b""),
            "/a/next" => respond(socket, "301 Moved", &[("location", "../final?x=1")], b""),
            "/final?x=1" => respond(socket, "200 OK", &[], b"done"),
            other => respond(socket, "404 Not Found", &[], other.as_bytes()),
        });
        let response = client().get(server.url("/a/start")).send().await.expect("send");

        assert_eq!(response.status(), 200);
        assert_eq!(response.url().path(), "/final");
        assert_eq!(response.url().query(), Some("x=1"));
        assert_eq!(response.text().await.expect("text"), "done");
    });
}

#[test]
fn post_becomes_get_on_301_302_303() {
    run(async {
        for (status, location) in [
            ("301 Moved Permanently", "/end"),
            ("302 Found", "/end"),
            ("303 See Other", "/end"),
        ] {
            let server = redirector(status, location);
            let text = client()
                .post(server.url("/start"))
                .header("content-type", "text/plain")
                .body("payload")
                .send()
                .await
                .expect("send")
                .text()
                .await
                .expect("text");

            assert_eq!(text, "GET|-|-|-|", "{status}");
        }
    });
}

#[test]
fn put_becomes_get_only_on_303() {
    run(async {
        let see_other = redirector("303 See Other", "/end");
        let text = client()
            .put(see_other.url("/start"))
            .body("p")
            .send()
            .await
            .expect("send")
            .text()
            .await
            .expect("text");

        assert_eq!(text, "GET|-|-|-|");

        let found = redirector("302 Found", "/end");
        let text = client()
            .put(found.url("/start"))
            .body("p")
            .send()
            .await
            .expect("send")
            .text()
            .await
            .expect("text");

        assert_eq!(text, "PUT|-|-|-|p");
    });
}

#[test]
fn temporary_and_permanent_redirects_keep_method_and_body() {
    run(async {
        for status in ["307 Temporary Redirect", "308 Permanent Redirect"] {
            let server = redirector(status, "/end");
            let text = client()
                .post(server.url("/start"))
                .header("content-type", "text/plain")
                .body("payload")
                .send()
                .await
                .expect("send")
                .text()
                .await
                .expect("text");

            assert_eq!(text, "POST|-|text/plain|-|payload", "{status}");
        }
    });
}

#[test]
fn head_stays_head_on_303() {
    run(async {
        let server = redirector("303 See Other", "/end");
        let response = client().head(server.url("/start")).send().await.expect("send");

        assert_eq!(response.status(), 200);
        assert_eq!(response.url().path(), "/end");
    });
}

#[test]
fn same_origin_redirects_keep_credentials() {
    run(async {
        let server = redirector("302 Found", "/end");
        let text = client()
            .get(server.url("/start"))
            .bearer_auth("secret")
            .header("cookie", "a=b")
            .send()
            .await
            .expect("send")
            .text()
            .await
            .expect("text");

        assert_eq!(text, "GET|Bearer secret|-|a=b|");
    });
}

#[test]
fn cross_origin_redirects_drop_credentials() {
    run(async {
        let target = serve(|request, socket| respond(socket, "200 OK", &[], report(request).as_bytes()));
        let location = format!("http://{}/landed", target.addr);
        let origin = serve(move |_, socket| {
            respond(socket, "307 Temporary Redirect", &[("location", &location)], b"");
        });
        let text = client()
            .post(origin.url("/start"))
            .bearer_auth("secret")
            .basic_auth("u", Some("p"))
            .header("cookie", "a=b")
            .header("content-type", "text/plain")
            .body("kept")
            .send()
            .await
            .expect("send")
            .text()
            .await
            .expect("text");

        assert_eq!(text, "POST|-|text/plain|-|kept");
    });
}

#[test]
fn cross_origin_303_also_drops_body_headers() {
    run(async {
        let target = serve(|request, socket| respond(socket, "200 OK", &[], report(request).as_bytes()));
        let location = format!("http://{}/landed", target.addr);
        let origin = serve(move |_, socket| {
            respond(socket, "303 See Other", &[("location", &location)], b"");
        });
        let text = client()
            .post(origin.url("/start"))
            .bearer_auth("secret")
            .header("content-type", "application/json")
            .body("{}")
            .send()
            .await
            .expect("send")
            .text()
            .await
            .expect("text");

        assert_eq!(text, "GET|-|-|-|");
    });
}

#[test]
fn redirect_limit_is_enforced() {
    run(async {
        let server = serve(|request, socket| {
            let hop: usize = request.target.trim_start_matches("/h").parse().unwrap_or(0);
            let next = format!("/h{}", hop + 1);

            respond(socket, "302 Found", &[("location", &next)], b"");
        });
        let error = client().get(server.url("/h0")).send().await.expect_err("loop");

        assert!(error.is_redirect(), "{error}");

        let few = Client::builder()
            .runtime(RUNTIME)
            .redirect(Policy::limited(2))
            .build()
            .expect("client");

        assert!(few.get(server.url("/h0")).send().await.expect_err("limit").is_redirect());

        let exact = serve(|request, socket| match request.target.as_str() {
            "/h0" => respond(socket, "302 Found", &[("location", "/h1")], b""),
            "/h1" => respond(socket, "302 Found", &[("location", "/h2")], b""),
            _ => respond(socket, "200 OK", &[], b"arrived"),
        });
        let response = few.get(exact.url("/h0")).send().await.expect("two hops are allowed");

        assert_eq!(response.text().await.expect("text"), "arrived");
    });
}

#[test]
fn policy_none_returns_the_redirect() {
    run(async {
        let server = redirector("302 Found", "/end");
        let client = Client::builder()
            .runtime(RUNTIME)
            .redirect(Policy::none())
            .build()
            .expect("client");
        let response = client.get(server.url("/start")).send().await.expect("send");

        assert_eq!(response.status(), 302);
        assert_eq!(response.header_str("location"), Some("/end"));
    });
}

#[test]
fn redirect_without_location_is_returned() {
    run(async {
        let server = serve(|_, socket| respond(socket, "302 Found", &[], b"no place"));
        let response = client().get(server.url("/")).send().await.expect("send");

        assert_eq!(response.status(), 302);
        assert_eq!(response.text().await.expect("text"), "no place");
    });
}
