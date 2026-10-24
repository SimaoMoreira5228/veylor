use std::path::Path;

use veylor_http1::is_token;
use veylor_http2::{ErrorCode, H2ServerDriver, HpackFieldToEncode, HpackRepresentation};

use crate::static_files::{FileMounts, FilePlan, FileReject, HTTP_DATE_BYTES, write_http_date};
use crate::static_routes::{MAX_RESPONSE_BODY, MAX_ROUTES, NativeHandler, RouteBuildError};

use super::{DECIMAL_CAP, HPACK_BLOCK_CAP};

pub(crate) const NOT_FOUND_BODY: &[u8] = b"Not Found";
// NOTE: "bytes " plus three u64 fields plus separators tops out at 68 bytes; 80 leaves margin.
const CONTENT_RANGE_BYTES: usize = 80;
// NOTE: "multipart/byteranges; boundary=" plus token and hex fields tops out near 91 bytes;
// 128 leaves margin, mirroring the H1 multipart scratch.
const MULTIPART_CTYPE_BYTES: usize = 128;

#[derive(Clone, Copy, Debug)]
pub struct FrozenRoute<'a> {
    pub method: &'a [u8],
    pub path: &'a [u8],
    pub status: u16,
    pub body: &'a [u8],
    pub managed: bool,
    pub file_root: Option<&'a Path>,
}

// NOTE: native projection for the h2c turn: same method+path match shape as FrozenRoute,
// but dispatch invokes the handler synchronously instead of serving a stored body. Status
// and body dummies stay in FrozenRoute (natives never project there); validation and the
// HEAD-to-GET fallback mirror the static view so H1 precedence carries over.
#[derive(Clone, Copy, Debug)]
pub struct FrozenNativeRoute<'a> {
    pub method: &'a [u8],
    pub path: &'a [u8],
    pub handler: NativeHandler,
    pub user_data: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct FrozenRoutesView<'a> {
    routes: &'a [FrozenRoute<'a>],
    natives: &'a [FrozenNativeRoute<'a>],
}

impl<'a> FrozenRoutesView<'a> {
    pub fn new(routes: &'a [FrozenRoute<'a>]) -> Result<Self, RouteBuildError> {
        if routes.len() > MAX_ROUTES {
            return Err(RouteBuildError::Full);
        }
        // NOTE: file entries skip validation and the duplicate scan below: the mount
        // registry already validated the prefix, and a static route may legally share the
        // prefix path (exact matches win over mounts, same as H1).
        for route in routes {
            if route.file_root.is_some() {
                continue;
            }
            if route.method.is_empty()
                || !route.method.iter().copied().all(is_token)
                || route.path.is_empty()
                || !(200..=599).contains(&route.status)
                || route.status == 204
            {
                return Err(RouteBuildError::Invalid);
            }
            if route.body.len() > MAX_RESPONSE_BODY {
                return Err(RouteBuildError::BodyTooLarge);
            }
        }
        if (1..routes.len()).any(|index| {
            routes[index].file_root.is_none()
                && routes[..index].iter().any(|prev| {
                    prev.file_root.is_none()
                        && prev.method == routes[index].method
                        && prev.path == routes[index].path
                })
        }) {
            return Err(RouteBuildError::Duplicate);
        }
        Ok(Self {
            routes,
            natives: &[],
        })
    }

    // NOTE: native attach runs after new so the frozen static table keeps its shape: same
    // method+path rules as the registry (bad method/path Invalid, any overlap Duplicate),
    // and natives overlap nothing since the registry already rejects cross-kind dupes.
    pub fn with_natives(
        mut self,
        natives: &'a [FrozenNativeRoute<'a>],
    ) -> Result<Self, RouteBuildError> {
        for native in natives {
            let bad = native.method.is_empty() || native.path.is_empty();
            let token = !native.method.iter().copied().all(is_token);
            if bad || token {
                return Err(RouteBuildError::Invalid);
            }
        }
        if (1..natives.len()).any(|index| {
            natives[..index].iter().any(|prev| {
                prev.method == natives[index].method && prev.path == natives[index].path
            })
        }) {
            return Err(RouteBuildError::Duplicate);
        }
        let overlap = natives.iter().any(|native| {
            self.routes.iter().any(|route| {
                route.file_root.is_none()
                    && route.method == native.method
                    && route.path == native.path
            })
        });
        if overlap {
            return Err(RouteBuildError::Duplicate);
        }
        self.natives = natives;
        Ok(self)
    }

    pub(crate) fn find(&self, method: &[u8], path: &[u8]) -> Option<usize> {
        let exact = self.routes.iter().position(|route| {
            route.file_root.is_none() && route.method == method && route.path == path
        });
        if exact.is_some() {
            return exact;
        }
        if method != b"HEAD" {
            return None;
        }
        self.routes.iter().position(|route| {
            route.file_root.is_none() && route.method == b"GET" && route.path == path
        })
    }

    pub(crate) fn lookup(&self, method: &[u8], path: &[u8]) -> Option<(u16, &[u8])> {
        self.find(method, path)
            .map(|index| (self.routes[index].status, self.routes[index].body))
    }

    // NOTE: native-first dispatch like H1 (respond_to_head checks natives before statics):
    // same HEAD-to-GET fallback as find, so a HEAD static never shadows a GET native.
    pub(crate) fn lookup_native(
        &self,
        method: &[u8],
        path: &[u8],
    ) -> Option<(NativeHandler, usize)> {
        let direct = self
            .natives
            .iter()
            .find(|native| native.method == method && native.path == path);
        let fallback = direct.is_none() && method == b"HEAD";
        let found = if fallback {
            self.natives
                .iter()
                .find(|native| native.method == b"GET" && native.path == path)
        } else {
            direct
        };
        found.map(|native| (native.handler, native.user_data))
    }

    pub(crate) fn is_managed(&self, method: &[u8], path: &[u8]) -> bool {
        self.find(method, path)
            .is_some_and(|index| self.routes[index].managed)
    }

    // NOTE: longest-prefix file match over the mirrored mount entries: exact routes never
    // land here (find skips file entries), so H1 precedence (statics and natives win, files
    // serve table misses) carries over byte for byte.
    pub(crate) fn lookup_file_mount(&self, path: &[u8]) -> Option<(&'a [u8], &'a Path)> {
        self.routes
            .iter()
            .filter_map(|route| {
                let root = route.file_root?;
                if path.starts_with(route.path) {
                    Some((route.path, root))
                } else {
                    None
                }
            })
            .max_by_key(|(prefix, _)| prefix.len())
    }

    // NOTE: the filesystem walk reuses FileMounts::resolve exactly (no second traversal
    // dialect): a single-mount table is assembled per request from the matched mirror
    // entry, so component decoding, symlink rejection, and method gating stay single-
    // sourced in static_files. A register failure reads as no match (H1 answers 404 for
    // a vanished root the same way).
    pub(crate) fn resolve_file(&self, method: &[u8], path: &[u8]) -> Result<FilePlan, FileReject> {
        let (prefix, root) = self.lookup_file_mount(path).ok_or(FileReject::NotFound)?;
        let mut mounts = FileMounts::new();
        if mounts.register(prefix, root).is_err() {
            return Err(FileReject::NotFound);
        }
        mounts.resolve(method, strip_file_query(path))
    }
}

// NOTE: file HEADERS assembly next to send_full: 200/206 carry content-length, content-
// type, accept-ranges, and last-modified; 304 carries content-length 0 plus last-modified
// (H1 emits both); 416 carries content-length 0 plus content-range */total. Single-range
// 206 adds content-range; multipart 206 carries the dynamic boundary content-type instead
// of the file type and no top-level content-range (H1 parity, body pages via read_chunk).
pub(crate) struct FileHead {
    pub(crate) status: u16,
    pub(crate) content_length: u64,
    pub(crate) content_type: Option<&'static str>,
    pub(crate) mtime: Option<u64>,
    pub(crate) total: u64,
    pub(crate) window: Option<(u64, u64)>,
    pub(crate) accept_ranges: bool,
    pub(crate) multipart: bool,
}

pub(crate) fn send_file_head(
    server: &mut H2ServerDriver<'_, '_, '_, '_>,
    stream: u32,
    plan: &FilePlan,
    head: &FileHead,
    end_stream: bool,
    output: &mut [u8],
    used: &mut usize,
) -> Result<(), ErrorCode> {
    let mut status_buf = [0; DECIMAL_CAP];
    let status_len = push_decimal(&mut status_buf, u64::from(head.status));
    let mut length_buf = [0; DECIMAL_CAP];
    let length_len = push_decimal(&mut length_buf, head.content_length);
    let mut date_buf = [0; HTTP_DATE_BYTES];
    let date_len = head
        .mtime
        .map(|mtime| write_http_date(mtime, &mut date_buf))
        .unwrap_or(0);
    let mut range_buf = [0; CONTENT_RANGE_BYTES];
    let range_len = if head.status == 416 || head.window.is_some() {
        write_file_content_range(&mut range_buf, head.window, head.total)
    } else {
        0
    };
    let mut ctype_buf = [0; MULTIPART_CTYPE_BYTES];
    let ctype_len = if head.multipart {
        plan.multipart_content_type(&mut ctype_buf)
    } else {
        0
    };
    let mut fields = [field(b"", b""); 6];
    fields[0] = field(b":status", &status_buf[..status_len]);
    fields[1] = field(b"content-length", &length_buf[..length_len]);
    let mut count = 2;
    if head.multipart {
        fields[count] = field(b"content-type", &ctype_buf[..ctype_len]);
        count += 1;
    } else if let Some(content_type) = head.content_type {
        fields[count] = field(b"content-type", content_type.as_bytes());
        count += 1;
    }
    if head.accept_ranges {
        fields[count] = field(b"accept-ranges", b"bytes");
        count += 1;
    }
    if head.mtime.is_some() {
        fields[count] = field(b"last-modified", &date_buf[..date_len]);
        count += 1;
    }
    if head.status == 416 || head.window.is_some() {
        fields[count] = field(b"content-range", &range_buf[..range_len]);
        count += 1;
    }
    let mut block = [0; HPACK_BLOCK_CAP];
    let sent = server
        .send_response(
            stream,
            &fields[..count],
            &mut block,
            &mut output[*used..],
            end_stream,
        )
        .map_err(|_| ErrorCode::InternalError)?;
    *used += sent.writer.pending_bytes().len();
    Ok(())
}

fn write_file_content_range(out: &mut [u8], window: Option<(u64, u64)>, total: u64) -> usize {
    use crate::static_files::{push_bytes, push_decimal};
    let mut len = push_bytes(out, 0, b"bytes ");
    match window {
        Some((start, last)) => {
            len = push_decimal(out, len, start);
            len = push_bytes(out, len, b"-");
            len = push_decimal(out, len, last);
            len = push_bytes(out, len, b"/");
        }
        None => len = push_bytes(out, len, b"*/"),
    }
    push_decimal(out, len, total)
}

// NOTE: query strings strip like the H1 file_target so /assets/a.css?x=1 names the file.
fn strip_file_query(path: &[u8]) -> &[u8] {
    let end = path
        .iter()
        .position(|byte| *byte == b'?')
        .unwrap_or(path.len());
    &path[..end]
}

// NOTE: RFC 9113 forbids content-length on 1xx/204 responses and the driver rejects it
// at send_response time, so bodyless statuses emit :status-only HEADERS with END_STREAM
// and never DATA; H1 drops the 204 body the same way (body_allowed gates it out).
pub(crate) fn send_status_only(
    server: &mut H2ServerDriver<'_, '_, '_, '_>,
    stream: u32,
    status: u16,
    output: &mut [u8],
    used: &mut usize,
) -> Result<(), ErrorCode> {
    let mut status_buf = [0; DECIMAL_CAP];
    let status_len = push_decimal(&mut status_buf, u64::from(status));
    let response_fields = [field(b":status", &status_buf[..status_len])];
    let mut block = [0; HPACK_BLOCK_CAP];
    let sent = server
        .send_response(
            stream,
            &response_fields,
            &mut block,
            &mut output[*used..],
            true,
        )
        .map_err(|_| ErrorCode::InternalError)?;
    *used += sent.writer.pending_bytes().len();
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn send_full(
    server: &mut H2ServerDriver<'_, '_, '_, '_>,
    stream: u32,
    status: u16,
    body: &[u8],
    end_stream: bool,
    send_body: bool,
    output: &mut [u8],
    used: &mut usize,
) -> Result<(), ErrorCode> {
    if status == 204 {
        return send_status_only(server, stream, status, output, used);
    }
    let mut status_buf = [0; DECIMAL_CAP];
    let status_len = push_decimal(&mut status_buf, u64::from(status));
    let mut length_buf = [0; DECIMAL_CAP];
    let length_len = push_decimal(&mut length_buf, body.len() as u64);
    let response_fields = [
        field(b":status", &status_buf[..status_len]),
        field(b"content-length", &length_buf[..length_len]),
    ];
    let mut block = [0; HPACK_BLOCK_CAP];
    let sent = server
        .send_response(
            stream,
            &response_fields,
            &mut block,
            &mut output[*used..],
            end_stream,
        )
        .map_err(|_| ErrorCode::InternalError)?;
    *used += sent.writer.pending_bytes().len();
    if end_stream || !send_body {
        return Ok(());
    }
    let mut offset = 0;
    while offset < body.len() {
        let room = output.len() - *used;
        if room < 10 {
            return Err(ErrorCode::EnhanceYourCalm);
        }
        let take = (body.len() - offset).min(room - 9);
        let last = offset + take == body.len();
        let sent = server
            .send_data(
                stream,
                &body[offset..offset + take],
                last,
                &mut output[*used..],
            )
            .map_err(|_| ErrorCode::FlowControlError)?;
        if sent.frame.payload_length == 0 {
            return Err(ErrorCode::FlowControlError);
        }
        *used += sent.writer.pending_bytes().len();
        offset += sent.frame.payload_length;
    }
    Ok(())
}

pub(crate) fn field<'a>(name: &'a [u8], value: &'a [u8]) -> HpackFieldToEncode<'a> {
    HpackFieldToEncode {
        name,
        value,
        representation: HpackRepresentation::Default,
        huffman_name: false,
        huffman_value: false,
    }
}

fn push_decimal(buf: &mut [u8; DECIMAL_CAP], mut value: u64) -> usize {
    let mut end = DECIMAL_CAP;
    loop {
        end -= 1;
        buf[end] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    let len = DECIMAL_CAP - end;
    buf.copy_within(end.., 0);
    len
}
