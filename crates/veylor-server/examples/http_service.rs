use std::io;
use std::net::SocketAddr;

use veylor_server::{Server, ServerConfig};

fn main() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let addr: SocketAddr = args
        .next()
        .unwrap_or_else(|| "127.0.0.1:8080".to_owned())
        .parse()
        .map_err(|error| format!("invalid address: {error}"))?;
    let worker_count = args
        .next()
        .unwrap_or_else(|| "1".to_owned())
        .parse()
        .map_err(|error| format!("invalid worker count: {error}"))?;

    if args.next().is_some() {
        return Err("usage: http_service [address] [workers]".to_owned());
    }

    let config = ServerConfig {
        worker_count,
        max_connections: 1024,
        h2c_port: Some(0),
        ..ServerConfig::default()
    };
    let mut server = Server::bind(addr, config).map_err(|error| format!("bind: {error:?}"))?;

    server
        .register_route(b"GET", b"/ready", 200, b"OK", b"ready\n", 0)
        .map_err(|error| format!("register readiness: {error:?}"))?;
    server
        .register_route(b"GET", b"/payload", 200, b"OK", &[b'x'; 4096], 0)
        .map_err(|error| format!("register payload: {error:?}"))?;
    server
        .register_route(b"POST", b"/upload", 204, b"No Content", b"", 65536)
        .map_err(|error| format!("register upload: {error:?}"))?;

    let running = server
        .start()
        .map_err(|error| format!("start: {error:?}"))?;
    println!("HTTP/1: http://{}", running.addr());
    if let Some(addr) = running.h2c_addr() {
        println!("HTTP/2 prior knowledge: http://{addr}");
    }
    println!("Press Enter to drain connections and stop.");

    let input = io::stdin().read_line(&mut String::new());

    running.stop_accepting();
    let result = running.join().map_err(|error| format!("join: {error:?}"));

    input.map_err(|error| format!("stdin: {error}"))?;

    println!("{:#?}", result?);

    Ok(())
}
