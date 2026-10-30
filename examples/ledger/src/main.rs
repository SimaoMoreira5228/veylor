mod config;
mod dispatch;
mod store;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use veylor_server::{Server, ServerConfig, TlsConfig};

use crate::config::Config;
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
    let mut server =
        Server::bind(options.address, config).map_err(|error| format!("bind: {error:?}"))?;

    server
        .register_route(b"GET", b"/ready", 200, b"OK", b"ready\n", 0)
        .map_err(|error| format!("readiness: {error:?}"))?;

    for id in 1..=5 {
        let transaction_path = format!("/accounts/{id}/transactions");
        server
            .register_managed(
                b"POST",
                transaction_path.as_bytes(),
                dispatch::BODY_CAP as u64,
            )
            .map_err(|error| format!("register transactions: {error:?}"))?;

        let statement_path = format!("/accounts/{id}/statement");
        server
            .register_managed(b"GET", statement_path.as_bytes(), 0)
            .map_err(|error| format!("register statement: {error:?}"))?;
    }

    let shutdown = Arc::new(AtomicBool::new(false));
    let signal_shutdown = Arc::clone(&shutdown);
    let main_thread = std::thread::current();

    ctrlc::set_handler(move || {
        signal_shutdown.store(true, Ordering::Release);
        main_thread.unpark();
    })
    .map_err(|error| format!("signal handler: {error}"))?;

    let running = server
        .start()
        .map_err(|error| format!("start: {error:?}"))?;
    let host = running.host();
    let stopped = Arc::new(AtomicBool::new(false));
    let dispatch_stop = Arc::clone(&stopped);
    let dispatcher = match std::thread::Builder::new()
        .name("ledger-dispatch".into())
        .spawn(move || dispatch::run(host, store, dispatch_stop))
    {
        Ok(dispatcher) => dispatcher,
        Err(error) => {
            running.stop_force();
            let _ = running.join();

            return Err(format!("dispatcher: {error}"));
        }
    };

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
    let joined = running.join();

    stopped.store(true, Ordering::Release);
    dispatcher
        .join()
        .map_err(|_| "dispatcher panicked".to_owned())?;

    let counts = joined.map_err(|error| format!("shutdown: {error:?}"))?;
    println!("{counts:#?}");

    Ok(())
}
