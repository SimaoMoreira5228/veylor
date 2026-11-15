#![cfg(feature = "tokio")]

mod support;

use std::future::Future;

use veylor_fetch::Runtime;

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

    include!("scenarios/basic.rs");
    include!("scenarios/redirects.rs");
    include!("scenarios/streaming.rs");
    include!("scenarios/tls.rs");
}
