use std::io;
use std::path::Path;

use veylor_h3::http::header::{
    ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, DATE, LAST_MODIFIED,
};
use veylor_h3::http::{HeaderMap, HeaderName, HeaderValue, Response, StatusCode};
use veylor_h3::{Body, BodySource, Bytes, StreamBody};
use veylor_http1::{CONTENT_RANGE_BYTES, MULTIPART_CTYPE_BYTES};

use crate::static_files::{
    FileHead, FileMounts, FilePlan, FileReject, HTTP_DATE_BYTES, plan_file_head, read_chunk,
    write_content_range, write_http_date,
};
use crate::static_routes::MAX_STATIC_BODY_BYTES;

use super::H2StaticRoute;

pub(super) fn serve(
    routes: &[H2StaticRoute],
    method: &[u8],
    path: &[u8],
    declared: u64,
    range: Option<&[u8]>,
    since: Option<&[u8]>,
) -> Option<Response<Body>> {
    if declared > MAX_STATIC_BODY_BYTES {
        return Some(text(StatusCode::PAYLOAD_TOO_LARGE, b"Payload Too Large"));
    }
    let plan = match resolve_file(routes, method, path) {
        Ok(plan) => plan,
        Err(FileReject::TooLarge) => {
            return Some(text(StatusCode::PAYLOAD_TOO_LARGE, b"Payload Too Large"));
        }
        Err(_) => {
            if method != b"GET" && method != b"HEAD" {
                let exists = matches!(
                    resolve_file(routes, b"GET", path),
                    Ok(_) | Err(FileReject::TooLarge)
                );
                if exists {
                    let mut response = text(StatusCode::METHOD_NOT_ALLOWED, b"Method Not Allowed");
                    response
                        .headers_mut()
                        .insert("allow", HeaderValue::from_static("GET, HEAD"));
                    return Some(response);
                }
            }
            return None;
        }
    };
    let (plan, head, send_body) = plan_file_head(plan, method, range, since);
    Some(file_response(plan, head, send_body))
}

fn resolve_file(
    routes: &[H2StaticRoute],
    method: &[u8],
    path: &[u8],
) -> Result<FilePlan, FileReject> {
    let (prefix, root) = lookup(routes, path).ok_or(FileReject::NotFound)?;
    let mut mounts = FileMounts::new();
    mounts
        .register(prefix, root)
        .map_err(|_| FileReject::NotFound)?;
    mounts.resolve(method, path)
}

fn lookup<'a>(routes: &'a [H2StaticRoute], path: &[u8]) -> Option<(&'a [u8], &'a Path)> {
    routes
        .iter()
        .filter_map(|route| {
            let root = route.file_root.as_deref()?;
            path.starts_with(&route.path)
                .then_some((route.path.as_slice(), root))
        })
        .max_by_key(|(prefix, _)| prefix.len())
}

fn file_response(plan: FilePlan, head: FileHead, send_body: bool) -> Response<Body> {
    let status = StatusCode::from_u16(head.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut response = Response::new(Body::Bytes(Bytes::new()));
    *response.status_mut() = status;
    let headers = response.headers_mut();

    let mut now = [0u8; HTTP_DATE_BYTES];
    let now_len = crate::static_routes::now_http_date(&mut now);
    insert(headers, DATE, &now[..now_len]);
    if head.status != 304 {
        let length = head.content_length.to_string();
        insert(headers, CONTENT_LENGTH, length.as_bytes());
    }
    if head.multipart {
        let mut content_type = [0u8; MULTIPART_CTYPE_BYTES];
        let len = plan.multipart_content_type(&mut content_type);
        insert(headers, CONTENT_TYPE, &content_type[..len]);
    } else if let Some(content_type) = head.content_type {
        insert(headers, CONTENT_TYPE, content_type.as_bytes());
    }
    if head.accept_ranges {
        insert(headers, ACCEPT_RANGES, b"bytes");
    }
    if let Some(mtime) = head.mtime {
        let mut date = [0u8; HTTP_DATE_BYTES];
        let len = write_http_date(mtime, &mut date);
        insert(headers, LAST_MODIFIED, &date[..len]);
    }
    if head.status == 416 || head.window.is_some() {
        let mut range = [0u8; CONTENT_RANGE_BYTES];
        let len = write_content_range(&mut range, head.window, head.total);
        insert(headers, CONTENT_RANGE, &range[..len]);
    }

    if send_body && plan.size > 0 {
        let length = plan.size;
        *response.body_mut() =
            Body::Stream(StreamBody::new(Some(length), FileBody { plan, offset: 0 }));
    }

    response
}

fn insert(headers: &mut HeaderMap, name: HeaderName, value: &[u8]) {
    if let Ok(value) = HeaderValue::from_bytes(value) {
        headers.insert(name, value);
    }
}

struct FileBody {
    plan: FilePlan,
    offset: u64,
}

impl BodySource for FileBody {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.offset >= self.plan.size {
            return Ok(0);
        }
        let take = (self.plan.size - self.offset).min(out.len() as u64) as usize;
        let read = read_chunk(&self.plan, self.offset, &mut out[..take])?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "file shrank underfoot",
            ));
        }
        self.offset += read as u64;
        Ok(read)
    }
}

fn text(status: StatusCode, body: &'static [u8]) -> Response<Body> {
    let mut response = Response::new(Body::Bytes(Bytes::from_static(body)));
    *response.status_mut() = status;
    let length = body.len().to_string();
    response.headers_mut().insert(
        CONTENT_LENGTH,
        HeaderValue::from_bytes(length.as_bytes()).expect("length"),
    );
    response
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::super::H2StaticRoute;
    use super::*;

    const BODY: &[u8] = b"file-body-1234567890\n";
    static NEXT: AtomicU64 = AtomicU64::new(0);

    struct Fixture {
        root: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let id = NEXT.fetch_add(1, Ordering::Relaxed);
            let root =
                std::env::temp_dir().join(format!("veylor-h3-file-{}-{id}", std::process::id()));
            fs::create_dir_all(&root).expect("temp root");
            fs::write(root.join("hello.txt"), BODY).expect("write fixture");
            Self { root }
        }

        fn routes(&self) -> Vec<H2StaticRoute> {
            vec![route(&self.root)]
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn route(root: &Path) -> H2StaticRoute {
        H2StaticRoute {
            method: b"GET".to_vec(),
            path: b"/assets/".to_vec(),
            status: 200,
            body: Vec::new(),
            gzip_body: None,
            managed: false,
            max_body_bytes: 0,
            native: None,
            file_root: Some(root.to_path_buf()),
            trailers: Vec::new(),
        }
    }

    fn drain(body: Body) -> Vec<u8> {
        let Body::Stream(mut stream) = body else {
            panic!("expected streaming body");
        };
        let mut out = Vec::new();
        let mut scratch = [0u8; 7];
        loop {
            let read = stream.read(&mut scratch).expect("read chunk");
            if read == 0 {
                return out;
            }
            out.extend_from_slice(&scratch[..read]);
        }
    }

    fn header<'a>(response: &'a Response<Body>, name: &str) -> Option<&'a str> {
        response
            .headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
    }

    #[test]
    fn serves_full_file_as_stream() {
        let fixture = Fixture::new();
        let routes = fixture.routes();
        let response =
            serve(&routes, b"GET", b"/assets/hello.txt", 0, None, None).expect("file response");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(header(&response, "content-length"), Some("21"));
        assert_eq!(header(&response, "accept-ranges"), Some("bytes"));
        assert!(header(&response, "last-modified").is_some());
        assert_eq!(drain(response.into_body()), BODY);
    }

    #[test]
    fn serves_head_without_body() {
        let fixture = Fixture::new();
        let routes = fixture.routes();
        let response =
            serve(&routes, b"HEAD", b"/assets/hello.txt", 0, None, None).expect("head response");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(header(&response, "content-length"), Some("21"));
        assert!(matches!(response.into_body(), Body::Bytes(body) if body.is_empty()));
    }

    #[test]
    fn serves_single_range() {
        let fixture = Fixture::new();
        let routes = fixture.routes();
        let response = serve(
            &routes,
            b"GET",
            b"/assets/hello.txt",
            0,
            Some(b"bytes=5-9"),
            None,
        )
        .expect("range response");

        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(header(&response, "content-range"), Some("bytes 5-9/21"));
        assert_eq!(drain(response.into_body()), b"body-");
    }

    #[test]
    fn serves_multipart_ranges() {
        let fixture = Fixture::new();
        let routes = fixture.routes();
        let response = serve(
            &routes,
            b"GET",
            b"/assets/hello.txt",
            0,
            Some(b"bytes=0-1,4-5"),
            None,
        )
        .expect("multipart response");

        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        let content_type = header(&response, "content-type").expect("multipart type");
        assert!(content_type.starts_with("multipart/byteranges; boundary="));
        let body = drain(response.into_body());
        assert!(body.windows(2).any(|window| window == b"fi"));
        assert!(body.windows(2).any(|window| window == b"-b"));
    }

    #[test]
    fn answers_not_modified() {
        let fixture = Fixture::new();
        let routes = fixture.routes();
        let mtime = fs::metadata(fixture.root.join("hello.txt"))
            .expect("metadata")
            .modified()
            .expect("mtime");
        let mut date = [0u8; HTTP_DATE_BYTES];
        let len = write_http_date(
            crate::static_files::system_time_secs(Some(mtime)),
            &mut date,
        );
        let response = serve(
            &routes,
            b"GET",
            b"/assets/hello.txt",
            0,
            None,
            Some(&date[..len]),
        )
        .expect("not modified");

        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert!(header(&response, "content-length").is_none());
        assert!(matches!(response.into_body(), Body::Bytes(body) if body.is_empty()));
    }

    #[test]
    fn rejects_methods_over_existing_files() {
        let fixture = Fixture::new();
        let routes = fixture.routes();
        let response =
            serve(&routes, b"POST", b"/assets/hello.txt", 0, None, None).expect("method response");

        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(header(&response, "allow"), Some("GET, HEAD"));
    }

    #[test]
    fn misses_unknown_files_and_forbids_large_bodies() {
        let fixture = Fixture::new();
        let routes = fixture.routes();
        assert!(serve(&routes, b"GET", b"/assets/missing.txt", 0, None, None).is_none());
        assert!(serve(&routes, b"GET", b"/assets/../hello.txt", 0, None, None).is_none());

        let response = serve(
            &routes,
            b"GET",
            b"/assets/hello.txt",
            MAX_STATIC_BODY_BYTES + 1,
            None,
            None,
        )
        .expect("too large");
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }
}
