#![allow(unsafe_code)]

mod executor;
mod handler;
mod managed;
mod middleware;
mod native;
mod request;
mod response;
mod router;
mod serve;
mod sse;

pub use handler::{
    AsyncHandler, Handler, HeadersArg, PathQueryArg, PathStateQueryArg, QueryArg, RequestArg,
};
pub use middleware::{Ctx, Middleware, Next};
pub use request::{
    Bytes, Form, FormValues, FromForm, FromParams, FromQuery, FromRequest, Headers, HeadersOwned,
    Param, Path, Query, QueryOwned, QueryParams, QueryValues, Reject, Request, State,
};
pub use response::{Body as ResponseBody, IntoResponse, Response};
pub use router::{Files, MethodRouter, RouteTarget, Router, files};
pub use serve::{RunningApp, ServeError, ServeExt};
pub use sse::{SseArg, SseEvent, SseHandler, SseSender, SseStateArg};

#[cfg(feature = "json")]
pub use request::Json;
