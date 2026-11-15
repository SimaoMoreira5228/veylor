#[cfg(not(any(feature = "tokio", feature = "smol")))]
compile_error!("veylor-fetch needs a runtime: enable the `tokio` or the `smol` feature");

mod body;
mod client;
mod decode;
mod encode;
mod error;
mod headers;
mod method;
mod policy;
mod request;
mod response;
mod runtime;
mod send;
mod url;

pub use body::Body;
pub use client::{Client, ClientBuilder};
pub use error::{Error, Result};
pub use method::Method;
pub use policy::Policy;
pub use request::RequestBuilder;
pub use response::Response;
pub use runtime::Runtime;
pub use url::{IntoUrl, Url};
pub use veylor_client_driver::{HeaderIter, HeaderList, PoolStats, Protocol};
