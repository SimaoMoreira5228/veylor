fn chunked_server(trailer: bool) -> TestServer {
    serve(move |_, socket| {
        let head: &[u8] = if trailer {
            b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\ntrailer: x-sum\r\n\r\n"
        } else {
            b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n"
        };

        let _ = socket.write_all(head);
        let _ = socket.write_all(b"5\r\nhello\r\n");
        let _ = socket.flush();

        pause(250);

        let _ = socket.write_all(b"6\r\n world\r\n");
        let _ = socket.flush();

        pause(50);

        let end: &[u8] = if trailer {
            b"0\r\nx-sum: 11\r\n\r\n"
        } else {
            b"0\r\n\r\n"
        };

        let _ = socket.write_all(end);
        let _ = socket.flush();
    })
}

#[test]
fn chunks_stream_as_they_arrive() {
    run(async {
        let server = chunked_server(false);
        let started = std::time::Instant::now();
        let mut response = client().get(server.url("/")).send().await.expect("send");
        let mut chunks: Vec<Vec<u8>> = Vec::new();
        let mut first_at = None;

        while let Some(chunk) = response.chunk().await.expect("chunk") {
            first_at.get_or_insert_with(|| started.elapsed());
            chunks.push(chunk.to_vec());
        }

        assert_eq!(chunks.first().map(Vec::as_slice), Some(b"hello".as_slice()));
        assert!(chunks.len() >= 2, "{chunks:?}");
        assert!(first_at.expect("first") < Duration::from_millis(220), "{first_at:?}");
        assert_eq!(chunks.concat(), b"hello world");
        assert!(response.chunk().await.expect("after end").is_none());
    });
}

#[test]
fn trailers_arrive_after_the_body() {
    run(async {
        let server = chunked_server(true);
        let (body, trailers) = client()
            .get(server.url("/"))
            .send()
            .await
            .expect("send")
            .bytes_and_trailers()
            .await
            .expect("body");
        let trailers = trailers.expect("trailers");

        assert_eq!(body, b"hello world");
        assert_eq!(trailers.get(b"x-sum"), Some(b"11".as_slice()));

        let mut response = client().get(server.url("/")).send().await.expect("send");

        while response.chunk().await.expect("chunk").is_some() {}

        assert!(response.trailers().is_some());
    });
}

#[test]
fn a_streamed_response_can_be_dropped_midway() {
    run(async {
        let server = chunked_server(false);
        let client = client();
        let mut response = client.get(server.url("/")).send().await.expect("send");

        assert_eq!(response.chunk().await.expect("chunk"), Some(b"hello".as_slice()));

        drop(response);

        let again = client.get(server.url("/")).send().await.expect("send");

        assert_eq!(again.text().await.expect("text"), "hello world");
    });
}
