use std::str::{FromStr, from_utf8};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Param<'a> {
    pub name: &'a [u8],
    pub value: &'a [u8],
}

#[derive(Debug)]
pub struct Request<'a, S: 'static> {
    method: &'a [u8],
    target: &'a [u8],
    path: &'a [u8],
    query: &'a [u8],
    headers: &'a [(&'a [u8], &'a [u8])],
    body: &'a [u8],
    params: &'a [Param<'a>],
    state: &'static S,
}

impl<S: 'static> Copy for Request<'_, S> {}

impl<S: 'static> Clone for Request<'_, S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'a, S: 'static> Request<'a, S> {
    pub(crate) fn new(
        method: &'a [u8],
        target: &'a [u8],
        headers: &'a [(&'a [u8], &'a [u8])],
        body: &'a [u8],
        params: &'a [Param<'a>],
        state: &'static S,
    ) -> Self {
        let split = target
            .iter()
            .position(|byte| *byte == b'?')
            .unwrap_or(target.len());
        Self {
            method,
            target,
            path: &target[..split],
            query: target[split..].get(1..).unwrap_or(&[]),
            headers,
            body,
            params,
            state,
        }
    }

    pub fn state(&self) -> &'static S {
        self.state
    }

    pub fn method(&self) -> &'a [u8] {
        self.method
    }

    pub fn target(&self) -> &'a [u8] {
        self.target
    }

    pub fn path(&self) -> &'a [u8] {
        self.path
    }

    pub fn query(&self) -> QueryParams<'a> {
        QueryParams { raw: self.query }
    }

    pub fn headers(&self) -> &'a [(&'a [u8], &'a [u8])] {
        self.headers
    }

    pub fn header(&self, name: &str) -> Option<&'a [u8]> {
        self.headers
            .iter()
            .find(|(found, _)| found.eq_ignore_ascii_case(name.as_bytes()))
            .map(|(_, value)| *value)
    }

    pub fn body(&self) -> &'a [u8] {
        self.body
    }

    pub fn params(&self) -> &'a [Param<'a>] {
        self.params
    }

    pub fn param(&self, name: &str) -> Option<&'a [u8]> {
        self.params
            .iter()
            .find(|param| param.name == name.as_bytes())
            .map(|param| param.value)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct QueryParams<'a> {
    raw: &'a [u8],
}

impl<'a> QueryParams<'a> {
    pub fn get(self, name: &str) -> Option<&'a [u8]> {
        self.iter()
            .find(|(key, _)| *key == name.as_bytes())
            .map(|(_, value)| value)
    }

    pub fn get_str(self, name: &str) -> Option<&'a str> {
        from_utf8(self.get(name)?).ok()
    }

    pub fn parse<T: FromStr>(self, name: &str) -> Option<T> {
        self.get_str(name)?.parse().ok()
    }

    pub fn iter(self) -> impl Iterator<Item = (&'a [u8], &'a [u8])> {
        self.raw
            .split(|byte| *byte == b'&')
            .filter(|pair| !pair.is_empty())
            .map(|pair| match pair.iter().position(|byte| *byte == b'=') {
                Some(split) => (&pair[..split], &pair[split + 1..]),
                None => (pair, &b""[..]),
            })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Reject {
    pub status: u16,
    pub message: &'static str,
}

impl Reject {
    pub const fn new(status: u16, message: &'static str) -> Self {
        Self { status, message }
    }
}

// NOTE: extractors own their values because a handler's parameter type cannot borrow the
// request for its call (Rust has no variadic higher-ranked handler arity). Path/State stay
// allocation-free; Query/Headers/Body allocate only when a handler asks for them.
pub trait FromRequest<S: 'static>: Sized {
    fn from_request(request: &Request<'_, S>, state: &'static S) -> Result<Self, Reject>;
}

impl<S: 'static> FromRequest<S> for () {
    fn from_request(_request: &Request<'_, S>, _state: &'static S) -> Result<Self, Reject> {
        Ok(())
    }
}

pub struct State<S: 'static>(pub &'static S);

impl<S: 'static> FromRequest<S> for State<S> {
    fn from_request(_request: &Request<'_, S>, state: &'static S) -> Result<Self, Reject> {
        Ok(State(state))
    }
}

pub struct Headers(pub Vec<(Vec<u8>, Vec<u8>)>);

impl Headers {
    pub fn get(&self, name: &str) -> Option<&[u8]> {
        self.0
            .iter()
            .find(|(found, _)| found.eq_ignore_ascii_case(name.as_bytes()))
            .map(|(_, value)| value.as_slice())
    }
}

impl<S: 'static> FromRequest<S> for Headers {
    fn from_request(request: &Request<'_, S>, _state: &'static S) -> Result<Self, Reject> {
        Ok(Headers(
            request
                .headers()
                .iter()
                .map(|(name, value)| (name.to_vec(), value.to_vec()))
                .collect(),
        ))
    }
}

pub struct Body(pub Vec<u8>);

impl<S: 'static> FromRequest<S> for Body {
    fn from_request(request: &Request<'_, S>, _state: &'static S) -> Result<Self, Reject> {
        Ok(Body(request.body().to_vec()))
    }
}

pub struct Query<T>(pub T);

impl<S: 'static, T: FromQuery> FromRequest<S> for Query<T> {
    fn from_request(request: &Request<'_, S>, _state: &'static S) -> Result<Self, Reject> {
        T::from_query(request.query()).map(Query)
    }
}

pub trait FromQuery: Sized {
    fn from_query(query: QueryParams<'_>) -> Result<Self, Reject>;
}

#[derive(Default)]
pub struct QueryValues(Vec<(Vec<u8>, Vec<u8>)>);

impl QueryValues {
    pub fn get(&self, name: &str) -> Option<&[u8]> {
        self.0
            .iter()
            .find(|(found, _)| found == name.as_bytes())
            .map(|(_, value)| value.as_slice())
    }

    pub fn parse<T: FromStr>(&self, name: &str) -> Option<T> {
        from_utf8(self.get(name)?).ok()?.parse().ok()
    }
}

impl FromQuery for QueryValues {
    fn from_query(query: QueryParams<'_>) -> Result<Self, Reject> {
        Ok(QueryValues(
            query
                .iter()
                .map(|(name, value)| (name.to_vec(), value.to_vec()))
                .collect(),
        ))
    }
}

impl FromQuery for () {
    fn from_query(_query: QueryParams<'_>) -> Result<Self, Reject> {
        Ok(())
    }
}

pub struct Path<T>(pub T);

impl<S: 'static, T: FromParams> FromRequest<S> for Path<T> {
    fn from_request(request: &Request<'_, S>, _state: &'static S) -> Result<Self, Reject> {
        T::from_params(request.params()).map(Path)
    }
}

pub trait FromParams: Sized {
    fn from_params(params: &[Param<'_>]) -> Result<Self, Reject>;
}

const BAD_PATH: Reject = Reject::new(400, "invalid path parameter");

fn parse_param<T: FromStr>(param: &Param<'_>) -> Result<T, Reject> {
    from_utf8(param.value)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or(BAD_PATH)
}

impl FromParams for String {
    fn from_params(params: &[Param<'_>]) -> Result<Self, Reject> {
        match params {
            [param] => from_utf8(param.value)
                .map(str::to_owned)
                .map_err(|_| BAD_PATH),
            _ => Err(BAD_PATH),
        }
    }
}

macro_rules! impl_from_params_scalar {
    ($($ty:ty),*) => {
        $(
            impl FromParams for $ty {
                fn from_params(params: &[Param<'_>]) -> Result<Self, Reject> {
                    match params {
                        [param] => parse_param(param),
                        _ => Err(BAD_PATH),
                    }
                }
            }
        )*
    };
}

impl_from_params_scalar!(i64, u64, i32, u32);

impl<A: FromParams, B: FromParams> FromParams for (A, B) {
    fn from_params(params: &[Param<'_>]) -> Result<Self, Reject> {
        if params.len() != 2 {
            return Err(BAD_PATH);
        }
        Ok((A::from_params(&params[..1])?, B::from_params(&params[1..])?))
    }
}

impl<A: FromParams, B: FromParams, C: FromParams> FromParams for (A, B, C) {
    fn from_params(params: &[Param<'_>]) -> Result<Self, Reject> {
        if params.len() != 3 {
            return Err(BAD_PATH);
        }
        Ok((
            A::from_params(&params[..1])?,
            B::from_params(&params[1..2])?,
            C::from_params(&params[2..])?,
        ))
    }
}

#[cfg(feature = "json")]
pub struct Json<T>(pub T);

#[cfg(feature = "json")]
impl<T: serde::Serialize> crate::response::IntoResponse for Json<T> {
    fn into_response(self) -> crate::response::Response {
        match serde_json::to_vec(&self.0) {
            Ok(body) => {
                crate::response::Response::new(200, body).header("content-type", "application/json")
            }
            Err(_) => crate::response::Response::new(500, &b"serialization failed"[..]),
        }
    }
}

#[cfg(feature = "json")]
impl<S: 'static, T: serde::de::DeserializeOwned> FromRequest<S> for Json<T> {
    fn from_request(request: &Request<'_, S>, _state: &'static S) -> Result<Self, Reject> {
        serde_json::from_slice(request.body())
            .map(Json)
            .map_err(|_| Reject::new(400, "invalid json body"))
    }
}
