use std::future::Future;

use crate::executor::block_on;
use crate::request::{FromParams, FromRequest, Headers, Path, Query, Reject, Request, State};
use crate::response::{IntoResponse, Response};

pub trait Handler<T, S: 'static>: Send + Sync + 'static {
    fn call(&self, request: &Request<'_, S>, state: &'static S) -> Response;
}

pub(crate) type RouteHandler<S> =
    Box<dyn Fn(&Request<'_, S>, &'static S) -> Response + Send + Sync>;

pub(crate) fn erase<T, S: 'static, H>(handler: H) -> RouteHandler<S>
where
    H: Handler<T, S>,
{
    Box::new(move |request, state| handler.call(request, state))
}

fn rejected(reject: Reject) -> Response {
    Response::new(reject.status, reject.message)
}

macro_rules! impl_handler {
    () => {
        pub struct RequestArg;

impl<S: 'static, F, R> Handler<RequestArg, S> for F
where
    F: for<'a> Fn(Request<'a, S>) -> R + Send + Sync + 'static,
    R: IntoResponse,
{
    fn call(&self, request: &Request<'_, S>, _state: &'static S) -> Response {
        (self)(*request).into_response()
    }
}

pub struct QueryArg;

impl<S: 'static, F, R> Handler<QueryArg, S> for F
where
    F: for<'a> Fn(Query<'a>) -> R + Send + Sync + 'static,
    R: IntoResponse,
{
    fn call(&self, request: &Request<'_, S>, _state: &'static S) -> Response {
        (self)(Query(request.query())).into_response()
    }
}

pub struct HeadersArg;

impl<S: 'static, F, R> Handler<HeadersArg, S> for F
where
    F: for<'a> Fn(Headers<'a>) -> R + Send + Sync + 'static,
    R: IntoResponse,
{
    fn call(&self, request: &Request<'_, S>, _state: &'static S) -> Response {
        (self)(Headers(request.headers())).into_response()
    }
}

pub struct PathQueryArg<T>(std::marker::PhantomData<T>);

impl<S: 'static, F, R, T> Handler<PathQueryArg<T>, S> for F
where
    F: for<'a> Fn(Path<T>, Query<'a>) -> R + Send + Sync + 'static,
    T: FromParams,
    R: IntoResponse,
{
    fn call(&self, request: &Request<'_, S>, _state: &'static S) -> Response {
        let invoked = (|| -> Result<Response, Reject> {
            let path = Path(T::from_params(request.params())?);
            Ok((self)(path, Query(request.query())).into_response())
        })();
        invoked.unwrap_or_else(rejected)
    }
}

pub struct PathStateQueryArg<T>(std::marker::PhantomData<T>);

impl<S: 'static, F, R, T> Handler<PathStateQueryArg<T>, S> for F
where
    F: for<'a> Fn(Path<T>, State<S>, Query<'a>) -> R + Send + Sync + 'static,
    T: FromParams,
    R: IntoResponse,
{
    fn call(&self, request: &Request<'_, S>, state: &'static S) -> Response {
        let invoked = (|| -> Result<Response, Reject> {
            let path = Path(T::from_params(request.params())?);
            Ok((self)(path, State(state), Query(request.query())).into_response())
        })();
        invoked.unwrap_or_else(rejected)
    }
}

impl<S: 'static, F, R> Handler<(), S> for F
        where
            F: Fn() -> R + Send + Sync + 'static,
            R: IntoResponse,
        {
            fn call(&self, _request: &Request<'_, S>, _state: &'static S) -> Response {
                (self)().into_response()
            }
        }
    };
    ($($extractor:ident),*) => {
        #[allow(non_snake_case)]
        impl<S: 'static, F, R, $($extractor),*> Handler<($($extractor,)*), S> for F
        where
            F: Fn($($extractor),*) -> R + Send + Sync + 'static,
            R: IntoResponse,
            $($extractor: FromRequest<S>),*
        {
            fn call(&self, request: &Request<'_, S>, state: &'static S) -> Response {
                let invoked = (|| -> Result<Response, Reject> {
                    $(let $extractor = $extractor::from_request(request, state)?;)*
                    Ok((self)($($extractor),*).into_response())
                })();
                invoked.unwrap_or_else(rejected)
            }
        }
    };
}

impl_handler!();

pub trait AsyncHandler<T, S: 'static>: Send + Sync + 'static {
    fn call(&self, request: &Request<'_, S>, state: &'static S) -> Response;
}

pub(crate) fn erase_async<T, S: 'static, H>(handler: H) -> RouteHandler<S>
where
    H: AsyncHandler<T, S>,
{
    Box::new(move |request, state| handler.call(request, state))
}

macro_rules! impl_async_handler {
    () => {
        impl<S: 'static, F, Fut, R> AsyncHandler<(), S> for F
        where
            F: Fn() -> Fut + Send + Sync + 'static,
            Fut: Future<Output = R>,
            R: IntoResponse,
        {
            fn call(&self, _request: &Request<'_, S>, _state: &'static S) -> Response {
                block_on((self)()).into_response()
            }
        }
    };
    ($($extractor:ident),*) => {
        #[allow(non_snake_case)]
        impl<S: 'static, F, Fut, R, $($extractor),*> AsyncHandler<($($extractor,)*), S> for F
        where
            F: Fn($($extractor),*) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = R>,
            R: IntoResponse,
            $($extractor: FromRequest<S>),*
        {
            fn call(&self, request: &Request<'_, S>, state: &'static S) -> Response {
                let invoked = (|| -> Result<Response, Reject> {
                    $(let $extractor = $extractor::from_request(request, state)?;)*
                    Ok(block_on((self)($($extractor),*)).into_response())
                })();
                invoked.unwrap_or_else(rejected)
            }
        }
    };
}

impl_async_handler!();
impl_async_handler!(A);
impl_async_handler!(A, B);
impl_async_handler!(A, B, C);
impl_async_handler!(A, B, C, D);
impl_async_handler!(A, B, C, D, E);
impl_handler!(A);
impl_handler!(A, B);
impl_handler!(A, B, C);
impl_handler!(A, B, C, D);
impl_handler!(A, B, C, D, E);
