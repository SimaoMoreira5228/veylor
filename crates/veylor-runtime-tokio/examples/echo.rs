use std::net::SocketAddr;

use veylor_runtime_tokio::{Request, Response, serve};

fn echo(request: Request<'_>) -> Response<'_> {
    Response::ok(request.body())
}

fn main() {
    let addr: SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:18080".to_owned())
        .parse()
        .expect("address");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    runtime.block_on(async move {
        let listener = tokio::net::TcpListener::bind(addr).await.expect("bind");
        println!("READY {}", listener.local_addr().expect("addr"));
        serve(listener, echo, Default::default())
            .await
            .expect("serve");
    });
}
