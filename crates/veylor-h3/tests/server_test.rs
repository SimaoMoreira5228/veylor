use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Buf;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use veylor_h3::{Config, Limits, Server, Tls, http, text_response};

fn identity() -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
    let generated = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("generate certificate");
    let cert = generated.cert.der().clone();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        generated.signing_key.serialize_der(),
    ));
    (cert, key)
}

fn client_endpoint(cert: CertificateDer<'static>) -> (quinn::Endpoint, quinn::ClientConfig) {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert).expect("trust fixture");
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .expect("tls 1.3")
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let crypto =
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).expect("quic client config");
    let config = quinn::ClientConfig::new(Arc::new(crypto));
    let endpoint =
        quinn::Endpoint::client(SocketAddr::from(([127, 0, 0, 1], 0))).expect("client endpoint");
    (endpoint, config)
}

#[test]
fn serves_h3_get_and_post() {
    let (cert, key) = identity();
    let server = Server::bind(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        Config {
            tls: Tls::new(vec![cert.clone()], key),
            limits: Limits::default(),
        },
    );
    let running = server
        .start(|request: http::Request<()>, body: bytes::Bytes| {
            let status = if request.method() == http::Method::POST {
                http::StatusCode::CREATED
            } else {
                http::StatusCode::OK
            };
            let path = request.uri().path().as_bytes().to_vec();
            text_response(status, [path, b":".to_vec(), body.to_vec()].concat())
        })
        .expect("start h3 server");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("client runtime");
    runtime.block_on(async {
        let (endpoint, config) = client_endpoint(cert);
        let connection = endpoint
            .connect_with(config, running.address(), "localhost")
            .expect("connect")
            .await
            .expect("handshake");
        let (mut driver, mut client) = h3::client::new(h3_quinn::Connection::new(connection))
            .await
            .expect("h3 client");
        let driver = tokio::spawn(async move { driver.wait_idle().await });

        let request = http::Request::builder()
            .method(http::Method::GET)
            .uri("https://localhost/hello")
            .body(())
            .expect("request");
        let mut stream = client.send_request(request).await.expect("send");
        stream.finish().await.expect("finish");
        let response = stream.recv_response().await.expect("response");
        assert_eq!(response.status(), http::StatusCode::OK);
        let mut body = Vec::new();
        while let Some(chunk) = stream.recv_data().await.expect("data") {
            body.extend_from_slice(chunk.chunk());
        }
        assert_eq!(body, b"/hello:");

        let request = http::Request::builder()
            .method(http::Method::POST)
            .uri("https://localhost/upload")
            .body(())
            .expect("request");
        let mut stream = client.send_request(request).await.expect("send");
        stream
            .send_data(bytes::Bytes::from_static(b"payload"))
            .await
            .expect("body");
        stream.finish().await.expect("finish");
        let response = stream.recv_response().await.expect("response");
        assert_eq!(response.status(), http::StatusCode::CREATED);
        let mut body = Vec::new();
        while let Some(chunk) = stream.recv_data().await.expect("data") {
            body.extend_from_slice(chunk.chunk());
        }
        assert_eq!(body, b"/upload:payload");

        endpoint.close(quinn::VarInt::from_u32(0), b"done");
        let _ = driver.await;
    });

    running.join().expect("join h3 server");
}

struct Chunks {
    chunks: Vec<&'static [u8]>,
    index: usize,
}

impl veylor_h3::BodySource for Chunks {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let Some(chunk) = self.chunks.get(self.index) else {
            return Ok(0);
        };
        self.index += 1;
        out[..chunk.len()].copy_from_slice(chunk);
        Ok(chunk.len())
    }
}

#[test]
fn streams_h3_response_body() {
    let (cert, key) = identity();
    let server = Server::bind(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        Config {
            tls: Tls::new(vec![cert.clone()], key),
            limits: Limits::default(),
        },
    );
    let running = server
        .start(|_request: http::Request<()>, _body: bytes::Bytes| {
            let source = Chunks {
                chunks: vec![b"first-".as_slice(), b"second-", b"third"],
                index: 0,
            };
            let body = veylor_h3::Body::Stream(veylor_h3::StreamBody::new(Some(18), source));
            http::Response::builder()
                .status(http::StatusCode::OK)
                .body(body)
                .expect("stream response")
        })
        .expect("start h3 server");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("client runtime");
    runtime.block_on(async {
        let (endpoint, config) = client_endpoint(cert);
        let connection = endpoint
            .connect_with(config, running.address(), "localhost")
            .expect("connect")
            .await
            .expect("handshake");
        let (mut driver, mut client) = h3::client::new(h3_quinn::Connection::new(connection))
            .await
            .expect("h3 client");
        let driver = tokio::spawn(async move { driver.wait_idle().await });

        let request = http::Request::builder()
            .method(http::Method::GET)
            .uri("https://localhost/stream")
            .body(())
            .expect("request");
        let mut stream = client.send_request(request).await.expect("send");
        stream.finish().await.expect("finish");
        let response = stream.recv_response().await.expect("response");
        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("content-length")
                .and_then(|value| value.to_str().ok()),
            Some("18")
        );
        let mut body = Vec::new();
        while let Some(chunk) = stream.recv_data().await.expect("data") {
            body.extend_from_slice(chunk.chunk());
        }
        assert_eq!(body, b"first-second-third");

        endpoint.close(quinn::VarInt::from_u32(0), b"done");
        let _ = driver.await;
    });

    running.join().expect("join h3 server");
}

struct WriterHandler;

impl veylor_h3::Handler for WriterHandler {
    fn handle(
        &self,
        _request: http::Request<()>,
        _body: bytes::Bytes,
    ) -> http::Response<veylor_h3::Body> {
        text_response(http::StatusCode::OK, "fallback")
    }

    fn managed<'a>(
        &'a self,
        _request: http::Request<()>,
        _body: bytes::Bytes,
        writer: &'a mut veylor_h3::ResponseWriter<'_>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = veylor_h3::ManagedOutcome> + Send + 'a>>
    {
        Box::pin(async move {
            let headers = vec![(b"x-stream".to_vec(), b"yes".to_vec())];
            if writer
                .send_headers(http::StatusCode::OK, &headers)
                .await
                .is_err()
            {
                return veylor_h3::ManagedOutcome::Streamed;
            }
            if writer.write(b"writer-1;").await.is_err() || writer.write(b"writer-2").await.is_err()
            {
                writer.reset(0);
                return veylor_h3::ManagedOutcome::Streamed;
            }
            let _ = writer.finish().await;
            veylor_h3::ManagedOutcome::Streamed
        })
    }
}

#[test]
fn writes_h3_response_through_writer() {
    let (cert, key) = identity();
    let server = Server::bind(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        Config {
            tls: Tls::new(vec![cert.clone()], key),
            limits: Limits::default(),
        },
    );
    let running = server.start(WriterHandler).expect("start h3 server");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("client runtime");
    runtime.block_on(async {
        let (endpoint, config) = client_endpoint(cert);
        let connection = endpoint
            .connect_with(config, running.address(), "localhost")
            .expect("connect")
            .await
            .expect("handshake");
        let (mut driver, mut client) = h3::client::new(h3_quinn::Connection::new(connection))
            .await
            .expect("h3 client");
        let driver = tokio::spawn(async move { driver.wait_idle().await });

        let request = http::Request::builder()
            .method(http::Method::GET)
            .uri("https://localhost/writer")
            .body(())
            .expect("request");
        let mut stream = client.send_request(request).await.expect("send");
        stream.finish().await.expect("finish");
        let response = stream.recv_response().await.expect("response");
        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("x-stream")
                .and_then(|value| value.to_str().ok()),
            Some("yes")
        );
        let mut body = Vec::new();
        while let Some(chunk) = stream.recv_data().await.expect("data") {
            body.extend_from_slice(chunk.chunk());
        }
        assert_eq!(body, b"writer-1;writer-2");

        endpoint.close(quinn::VarInt::from_u32(0), b"done");
        let _ = driver.await;
    });

    running.join().expect("join h3 server");
}
