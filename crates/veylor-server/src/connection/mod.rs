pub(crate) mod chunked;
pub(crate) mod exchange;
pub(crate) mod file;
mod park_request;
pub(crate) mod parked;
mod parked_pull;
mod pump;
mod state;
pub(crate) mod streams;
pub(crate) mod tls;
pub(crate) mod trailers;
mod tunnel;

pub(crate) use exchange::ExchangeOutcome;
#[cfg(target_os = "linux")]
pub use file::{FD_CACHE_ENTRIES, FdCache};
pub(crate) use parked::push_staged_body;
pub(crate) use parked_pull::pull_staged_body;
pub(crate) use pump::reuse_keep_alive;
pub use pump::{PumpOutcome, PumpProgress, pump_connection};
pub(crate) use state::ConnPhase;
pub use state::{ConnClose, ConnState};
pub use trailers::{TrailerOutcome, TrailerReport};

pub const INPUT_CAP: usize = 8192;
// NOTE: bytes drained from the receive queue before closing a rejected exchange, so an
// in-flight body does not make the kernel answer the peer with an RST that swallows the
// response. Bounded because the declared length is untrusted.
pub(crate) const CLOSE_DRAIN_BYTES: u64 = 64 * 1024;
pub const OUTPUT_CAP: usize = 16 * 1024;
// NOTE: H1 request-head slots; the H2 turn has its own HEADER_SLOTS (= 64) because decoded
// HPACK entries carry dynamic-table state and cost more per slot than these plain ranges.
// A peer sending 65-100 headers is served over H1 and refused over H2 — protocol cost,
// not an inconsistency; keep both numbers beside their owner, not unified.
pub const HEADER_SLOTS: usize = 100;
