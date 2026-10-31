use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use veylor_runtime::ConnectionToken;
use veylor_server::{
    EventSlot, HostEventKind, HostHandle, RespondOutcome, Server, ServerConfig, TlsConfig,
};

struct Options {
    address: SocketAddr,
    h2c_port: u16,
    h3_port: Option<u16>,
    workers: usize,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
    assets: Option<PathBuf>,
}

fn parse() -> Result<Options, String> {
    let mut options = Options {
        address: SocketAddr::from(([127, 0, 0, 1], 0)),
        h2c_port: 0,
        h3_port: None,
        workers: 1,
        tls_cert: None,
        tls_key: None,
        assets: None,
    };
    let mut args = std::env::args_os().skip(1);
    while let Some(flag) = args.next() {
        let flag = flag.to_string_lossy().into_owned();
        let value = args
            .next()
            .ok_or_else(|| format!("missing value for {flag}"))?;
        match flag.as_str() {
            "--address" => {
                options.address = value
                    .to_string_lossy()
                    .parse()
                    .map_err(|_| "invalid address".to_owned())?;
            }
            "--h2c-port" => {
                options.h2c_port = value
                    .to_string_lossy()
                    .parse()
                    .map_err(|_| "invalid h2c port".to_owned())?;
            }
            "--h3-port" => {
                options.h3_port = Some(
                    value
                        .to_string_lossy()
                        .parse()
                        .map_err(|_| "invalid h3 port".to_owned())?,
                );
            }
            "--workers" => {
                options.workers = value
                    .to_string_lossy()
                    .parse()
                    .map_err(|_| "invalid worker count".to_owned())?;
            }
            "--tls-cert" => options.tls_cert = Some(PathBuf::from(value)),
            "--tls-key" => options.tls_key = Some(PathBuf::from(value)),
            "--assets" => options.assets = Some(PathBuf::from(value)),
            _ => return Err(format!("unknown option: {flag}")),
        }
    }
    if options.tls_cert.is_some() != options.tls_key.is_some() {
        return Err("TLS needs both --tls-cert and --tls-key".to_owned());
    }
    if options.h3_port.is_some() && options.tls_cert.is_none() {
        return Err("HTTP/3 needs --tls-cert and --tls-key".to_owned());
    }
    Ok(options)
}

fn main() -> Result<(), String> {
    let options = parse()?;
    let tls = match (&options.tls_cert, &options.tls_key) {
        (Some(cert), Some(key)) => Some(
            TlsConfig::from_files(cert, key, 64)
                .map_err(|error| format!("TLS identity: {error:?}"))?,
        ),
        _ => None,
    };
    let encrypted = tls.is_some();
    let config = ServerConfig {
        worker_count: options.workers,
        h2c_port: Some(options.h2c_port),
        h3_port: options.h3_port,
        tls,
        ..ServerConfig::default()
    };
    let mut server =
        Server::bind(options.address, config).map_err(|error| format!("bind: {error:?}"))?;

    server
        .register_managed(b"GET", b"/", 1024 * 1024)
        .map_err(|error| format!("managed GET /: {error:?}"))?;
    server
        .register_managed(b"GET", b"/stream", 1024 * 1024)
        .map_err(|error| format!("managed GET /stream: {error:?}"))?;
    server
        .register_managed_with_trailers(
            b"POST",
            b"/",
            1024 * 1024,
            &[b"x-test", b"x-trailer", b"trailer", b"digest"],
        )
        .map_err(|error| format!("managed POST /: {error:?}"))?;
    if let Some(assets) = &options.assets {
        server
            .register_file_mount(b"/assets/", assets)
            .map_err(|error| format!("file mount /assets: {error:?}"))?;
    }
    for (path, body) in [
        (b"/hello".as_slice(), b"hello\n".as_slice()),
        (b"/empty".as_slice(), b"".as_slice()),
        (b"/fixed".as_slice(), b"fixed-body-1234".as_slice()),
        (b"/items/:id".as_slice(), b"item\n".as_slice()),
    ] {
        server
            .register_route(b"GET", path, 200, b"OK", body, 1024 * 1024)
            .map_err(|error| format!("route {}: {error:?}", String::from_utf8_lossy(path)))?;
    }

    let running = server
        .start()
        .map_err(|error| format!("start: {error:?}"))?;
    let h3_address = running
        .h3_addr()
        .map(|address| address.to_string())
        .unwrap_or_else(|| "-".to_owned());
    let stop = Arc::new(AtomicBool::new(false));
    let host = running.host();
    let dispatch_stop = Arc::clone(&stop);
    std::thread::Builder::new()
        .name("conformance-dispatch".into())
        .spawn(move || dispatch(host, dispatch_stop))
        .map_err(|error| format!("dispatch: {error}"))?;
    let h2c = running
        .h2c_addr()
        .map(|address| address.to_string())
        .unwrap_or_else(|| "-".to_owned());
    println!(
        "READY h1={} h2c={} h3={} tls={}",
        running.addr(),
        h2c,
        h3_address,
        if encrypted { 1 } else { 0 }
    );
    loop {
        std::thread::park();
    }
}

const PENDING_CAP: usize = 256;
const BODY_CAP: usize = 1024;

struct Pending {
    token: ConnectionToken,
    body: [u8; BODY_CAP],
    used: usize,
    stream: bool,
}

fn dispatch(host: HostHandle, stop: Arc<AtomicBool>) {
    let empty = EventSlot {
        kind: HostEventKind::Shutdown,
        token: ConnectionToken::default(),
        route_id: 0,
        method_len: 0,
        path_len: 0,
        method_offset: 0,
        path_offset: 0,
    };
    let mut events = [empty; 64];
    let mut methods = [0u8; 64 * 16];
    let mut paths = [0u8; 64 * 2048];
    let mut pending = Vec::with_capacity(PENDING_CAP);

    while !stop.load(Ordering::Acquire) {
        let report = host.poll(1_000_000, &mut events, &mut methods, &mut paths);
        for event in &events[..report.copied] {
            if event.kind != HostEventKind::Request {
                continue;
            }
            if pending.len() == PENDING_CAP {
                host.respond(event.token, 503, b"Service Unavailable", b"busy\n");
                continue;
            }
            let path = &paths[event.path_offset..][..event.path_len];
            pending.push(Pending {
                token: event.token,
                body: [0; BODY_CAP],
                used: 0,
                stream: path == b"/stream",
            });
        }

        let mut index = 0;
        while index < pending.len() {
            let request = &mut pending[index];
            let (outcome, count, done) =
                host.pull_body(request.token, &mut request.body[request.used..]);
            if outcome != RespondOutcome::Ok {
                pending.swap_remove(index);
                continue;
            }
            request.used += count;
            if !done {
                if request.used == BODY_CAP {
                    host.respond(request.token, 413, b"Payload Too Large", b"too large\n");
                    pending.swap_remove(index);
                    continue;
                }
                index += 1;
                continue;
            }
            let request = pending.swap_remove(index);
            if request.stream {
                stream_response(&host, request.token);
            } else {
                host.respond(request.token, 200, b"OK", b"ok\n");
            }
        }
    }
}

fn stream_response(host: &HostHandle, token: ConnectionToken) {
    if host.begin_stream(token, 200, b"OK") != RespondOutcome::Ok {
        return;
    }
    for chunk in [b"stream-1;".as_slice(), b"stream-2;", b"stream-3"] {
        wait_writable(host, token);
        if host.write_stream(token, chunk) != RespondOutcome::Ok {
            host.cancel(token);
            return;
        }
    }
    wait_writable(host, token);
    host.end_stream(token);
}

fn wait_writable(host: &HostHandle, token: ConnectionToken) {
    for _ in 0..1000 {
        if host.is_writable(token) {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}
