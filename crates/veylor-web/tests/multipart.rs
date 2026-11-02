use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};

use veylor_server::{Server, ServerConfig};
use veylor_web::{Multipart, Response, Router, ServeExt};

fn upload(multipart: Multipart<'_>) -> Response {
    let mut out = Vec::new();
    for part in multipart.parts() {
        let part = match part {
            Ok(part) => part,
            Err(reject) => return Response::new(reject.status, reject.message),
        };
        out.extend_from_slice(part.name());
        out.push(b'=');
        out.extend_from_slice(part.data());
        out.push(b';');
    }
    Response::ok(out)
}

#[test]
fn multipart_route_reads_fields_and_files() {
    let server = Server::bind(
        ([127, 0, 0, 1], 0).into(),
        ServerConfig {
            worker_count: 1,
            ..ServerConfig::default()
        },
    )
    .expect("bind");
    let app = Router::new().post("/upload", upload);

    let running = server.serve(app).expect("serve");
    let addr = running.addr();

    let body = "--XyZ\r\n\
Content-Disposition: form-data; name=\"title\"\r\n\
\r\n\
Hello\r\n\
--XyZ\r\n\
Content-Disposition: form-data; name=\"file\"; filename=\"a.txt\"\r\n\
Content-Type: text/plain\r\n\
\r\n\
file body\r\n\
--XyZ--\r\n";
    let request = format!(
        "POST /upload HTTP/1.1\r\nHost: t\r\nContent-Type: multipart/form-data; boundary=XyZ\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let response = call(addr, &request);
    assert!(
        response.ends_with("title=Hello;file=file body;"),
        "{response:?}"
    );

    let response = call(
        addr,
        "POST /upload HTTP/1.1\r\nHost: t\r\nContent-Type: multipart/form-data\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    );
    assert!(response.starts_with("HTTP/1.1 415 "), "{response:?}");

    running.join().expect("join");
}

fn call(addr: SocketAddr, raw: &str) -> String {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream.write_all(raw.as_bytes()).expect("write");

    let mut response = Vec::new();
    let _ = stream.read_to_end(&mut response);
    String::from_utf8_lossy(&response).into_owned()
}
