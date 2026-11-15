mod agent;
mod auth;
mod budget;
mod config;
mod encoding;
mod error;
mod field;
mod h1;
mod h2;
mod headers;
mod limits;
mod method;
mod outgoing;
mod percent;
mod pool;
mod protocol;
mod redirect;
mod request;
mod response;
mod status;
mod url;
mod wire;

#[cfg(any(feature = "tokio", feature = "smol"))]
pub mod asynchronous;

pub use agent::Agent;
pub use config::AgentBuilder;
pub use error::{Error, TimeoutKind};
pub use headers::{HeaderBytesIter, HeaderIter};
pub use method::Method;
pub use pool::PoolStats;
pub use protocol::Protocol;
pub use redirect::RedirectPolicy;
pub use request::Request;
pub use response::{BodyReader, Response};

// NOTE: the free functions build a fresh Agent per call, so nothing is pooled between them and no
// hidden global state exists. Keep an Agent around for keep-alive and h2 reuse.
pub fn get(url: &str) -> Request {
    Agent::new().get(url)
}

pub fn post(url: &str) -> Request {
    Agent::new().post(url)
}

pub fn put(url: &str) -> Request {
    Agent::new().put(url)
}

pub fn patch(url: &str) -> Request {
    Agent::new().patch(url)
}

pub fn delete(url: &str) -> Request {
    Agent::new().delete(url)
}

pub fn head(url: &str) -> Request {
    Agent::new().head(url)
}
