#![cfg(feature = "tokio")]

#[path = "asynchronous/support/mod.rs"]
mod support;

use std::future::Future;

use veylor_fetch::asynchronous::Runtime;

const RUNTIME: Runtime = Runtime::Tokio;

fn run<F: Future>(work: F) -> F::Output {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .expect("runtime")
        .block_on(work)
}

mod scenarios {
    use super::*;

    include!("asynchronous/scenarios/basic.rs");
    include!("asynchronous/scenarios/redirects.rs");
    include!("asynchronous/scenarios/streaming.rs");
    include!("asynchronous/scenarios/tls.rs");
}
