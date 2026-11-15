#![cfg(feature = "smol")]

mod support;

use std::future::Future;

use veylor_fetch::Runtime;

const RUNTIME: Runtime = Runtime::Smol;

fn run<F: Future>(work: F) -> F::Output {
    smol::block_on(work)
}

mod scenarios {
    use super::*;

    include!("scenarios/basic.rs");
    include!("scenarios/redirects.rs");
    include!("scenarios/streaming.rs");
    include!("scenarios/tls.rs");
}
