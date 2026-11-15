mod agent;
mod budget;
mod config;
mod encode;
mod error;
mod h1;
mod h2;
mod headers;
mod outgoing;
mod pool;
mod request;
mod response;
mod url;
mod wire;

pub use agent::Agent;
pub use config::AgentBuilder;
pub use error::{Error, TimeoutKind};
pub use headers::HeaderIter;
pub use pool::PoolStats;
pub use request::Request;
pub use response::{BodyReader, Protocol, Response};

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
