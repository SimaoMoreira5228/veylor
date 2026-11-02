mod config;
mod handlers;
mod store;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use veylor_server::{Server, ServerConfig, TlsConfig};
use veylor_web::{MethodRouter, Router, ServeExt};

use crate::config::Config;
use crate::handlers::App;
use crate::store::Store;

fn main() -> Result<(), String> {
    let options = Config::parse(std::env::args_os().skip(1))?;
    let tls = match (&options.tls_cert, &options.tls_key) {
        (Some(cert), Some(key)) => {
            let identity = TlsConfig::from_files(cert, key, 32)
                .map_err(|error| format!("TLS identity: {error:?}"))?;

            Some(identity)
        }
        _ => None,
    };
    let encrypted = tls.is_some();
    let config = ServerConfig {
        worker_count: options.workers,
        max_connections: 1024,
        h2c_port: options.h2c_port,
        tls,
        ..ServerConfig::default()
    };
    let store = Store::open(&options.database).map_err(|error| format!("database: {error:?}"))?;
    let app: Router<App> = Router::with_state(Mutex::new(store))
        .get("/ready", handlers::ready)
        .route(
            "/accounts/:account/statement",
            MethodRouter::new().get(handlers::statement).managed(),
        )
        .post("/accounts/:account/transactions", handlers::transaction)
        .body_limit(handlers::BODY_CAP);

    let server =
        Server::bind(options.address, config).map_err(|error| format!("bind: {error:?}"))?;
    let running = server
        .serve(app)
        .map_err(|error| format!("start: {error:?}"))?;

    let shutdown = Arc::new(AtomicBool::new(false));
    let signal_shutdown = Arc::clone(&shutdown);
    let main_thread = std::thread::current();

    ctrlc::set_handler(move || {
        signal_shutdown.store(true, Ordering::Release);
        main_thread.unpark();
    })
    .map_err(|error| format!("signal handler: {error}"))?;

    if encrypted {
        println!("HTTPS: https://{}", running.addr());
    } else {
        println!("HTTP/1: http://{}", running.addr());
    }

    if let Some(address) = running.h2c_addr() {
        println!("HTTP/2: http://{address}");
    }

    println!("Ready; send a termination signal to drain and stop.");

    while !shutdown.load(Ordering::Acquire) {
        std::thread::park();
    }

    println!("Draining active connections.");
    running.stop_accepting();
    let counts = running
        .join()
        .map_err(|error| format!("shutdown: {error:?}"))?;
    println!("{counts:#?}");

    Ok(())
}
