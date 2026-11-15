use std::time::Instant;

use veylor_client::Field;
use veylor_client_driver::ClientRequest;

use crate::body::Body;
use crate::client::{Client, Inner};
use crate::error::{Error, Result};
use crate::headers::Headers;
use crate::method::Method;
use crate::response::Response;
use crate::runtime::{Runtime, Stream, bounded, deadline_after};
use crate::url::Url;

const DRAIN_LIMIT: usize = 64 * 1024;

const MANAGED: [&str; 7] = [
    "content-length",
    "transfer-encoding",
    "connection",
    "keep-alive",
    "upgrade",
    "te",
    "proxy-connection",
];
const SENSITIVE: [&str; 3] = ["authorization", "proxy-authorization", "cookie"];
const ABOUT_BODY: [&str; 5] = [
    "content-type",
    "content-encoding",
    "content-language",
    "content-location",
    "digest",
];

pub(crate) struct Plan {
    pub method: Method,
    pub url: Url,
    pub headers: Headers,
    pub body: Body,
    pub timeout: Option<std::time::Duration>,
}

struct Hop {
    strip_sensitive: bool,
    drop_body: bool,
}

pub(crate) async fn execute(client: &Client, plan: Plan) -> Result<Response> {
    let inner = &*client.inner;
    let runtime = inner.transport.runtime();
    let deadline = deadline_after(plan.timeout.or(inner.timeout));
    let limit = inner.policy.limit();
    let Plan {
        mut method,
        mut url,
        headers,
        body,
        ..
    } = plan;
    let mut hop = Hop {
        strip_sensitive: false,
        drop_body: false,
    };
    let mut followed = 0usize;

    loop {
        let stream = {
            let fields = fields(inner, &headers, &method, &hop);
            let payload: &[u8] = if hop.drop_body { &[] } else { body.as_slice() };
            let request = ClientRequest::new(method.as_str().as_bytes(), url.as_str())
                .headers(&fields)
                .body(payload);

            bounded(runtime, deadline, inner.transport.send(&request))
                .await
                .map_err(Error::from_send)?
        };

        let status = stream.status();

        if limit == 0 || !is_redirect(status) {
            return Ok(finish(inner, stream, url, &method, deadline, runtime));
        }

        let Some(location) = stream.header("location") else {
            return Ok(finish(inner, stream, url, &method, deadline, runtime));
        };

        if followed >= limit {
            return Err(Error::Redirect(format!("more than {limit} redirects")));
        }

        let location = std::str::from_utf8(location)
            .map_err(|_| Error::Redirect("location is not valid text".to_owned()))?;
        let next = url
            .join(location)
            .map_err(|e| Error::Redirect(e.to_string()))?;

        if !url.is_same_origin(&next) {
            hop.strip_sensitive = true;
        }

        if rewrites_to_get(status, &method) {
            if method != Method::Head {
                method = Method::Get;
            }

            hop.drop_body = true;
        }

        drain(runtime, deadline, stream).await;

        url = next;
        followed += 1;
    }
}

fn finish(
    inner: &Inner,
    stream: Stream,
    url: Url,
    method: &Method,
    deadline: Option<Instant>,
    runtime: Runtime,
) -> Response {
    Response::new(
        stream,
        url,
        *method == Method::Head,
        deadline,
        runtime,
        inner.max_body,
    )
}

fn fields<'a>(
    inner: &'a Inner,
    headers: &'a Headers,
    method: &Method,
    hop: &Hop,
) -> Vec<Field<'a>> {
    let mut fields = Vec::with_capacity(8);
    let defaults = inner
        .headers
        .iter()
        .filter(|(name, _)| !headers.contains(name));

    for (name, value) in defaults.chain(headers.iter()) {
        if MANAGED.contains(&name) {
            continue;
        }

        if hop.strip_sensitive && SENSITIVE.contains(&name) {
            continue;
        }

        if hop.drop_body && ABOUT_BODY.contains(&name) {
            continue;
        }

        fields.push(Field {
            name: name.as_bytes(),
            value,
        });
    }

    let wants_encoding = cfg!(feature = "gzip")
        && *method != Method::Head
        && !headers.contains("accept-encoding")
        && !inner.headers.contains("accept-encoding");

    if wants_encoding {
        fields.push(Field {
            name: b"accept-encoding",
            value: b"gzip",
        });
    }

    fields
}

fn is_redirect(status: u16) -> bool {
    matches!(status, 301 | 302 | 303 | 307 | 308)
}

fn rewrites_to_get(status: u16, method: &Method) -> bool {
    match status {
        303 => *method != Method::Head,
        301 | 302 => *method == Method::Post,
        _ => false,
    }
}

async fn drain(runtime: Runtime, deadline: Option<Instant>, mut stream: Stream) {
    let mut seen = 0usize;

    loop {
        match bounded(runtime, deadline, stream.chunk()).await {
            Ok(Some(chunk)) => seen += chunk.len(),
            _ => return,
        }

        if seen > DRAIN_LIMIT {
            return;
        }
    }
}
