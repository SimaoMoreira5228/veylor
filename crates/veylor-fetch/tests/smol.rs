#![cfg(feature = "smol")]

#[path = "asynchronous/support/mod.rs"]
mod support;

use std::future::Future;

use veylor_fetch::asynchronous::Runtime;

const RUNTIME: Runtime = Runtime::Smol;

fn run<F: Future>(work: F) -> F::Output {
    smol::block_on(work)
}

mod scenarios {
    use super::*;

    include!("asynchronous/scenarios/basic.rs");
    include!("asynchronous/scenarios/redirects.rs");
    include!("asynchronous/scenarios/streaming.rs");
    include!("asynchronous/scenarios/tls.rs");
}
