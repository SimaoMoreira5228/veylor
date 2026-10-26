use std::path::Path;

use veylor_http1::is_token;
use veylor_http2::{
    ErrorCode, H2ServerDriver, HpackFieldToEncode, HpackHeader, HpackRepresentation,
};

use crate::static_files::{FileMounts, FilePlan, FileReject, HTTP_DATE_BYTES, write_http_date};
use crate::static_routes::{
    MAX_RESPONSE_BODY, MAX_ROUTES, NativeHandler, RouteBuildError, accepts_gzip_value,
    now_http_date,
};

use super::{DECIMAL_CAP, HPACK_BLOCK_CAP};

pub(crate) const NOT_FOUND_BODY: &[u8] = b"Not Found";
pub(crate) const METHOD_NOT_ALLOWED_BODY: &[u8] = b"Method Not Allowed";
pub(crate) const URI_TOO_LONG_BODY: &[u8] = b"URI Too Long";
// NOTE: one target limit for both protocols, same 8 KiB number as the H1 request-line cap
// (Limits::default().request_line_bytes): H1 attributes buffered-line overflow minus the
// minimal framing, while H2 checks the decoded :path directly since its length is known
// exactly — no buffered-line attribution needed.
pub(crate) const MAX_REQUEST_TARGET_BYTES: usize = 8 * 1024;
// NOTE: at most this many distinct methods list in Allow; more truncate the tail, so size
// it at the route cap and the 405 below RSTs past the value buffer instead.
const ALLOW_METHODS: usize = 16;
pub(crate) const ALLOW_BYTES: usize = 320;
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
    pub gzip_body: Option<&'a [u8]>,
    pub managed: bool,
    pub file_root: Option<&'a Path>,
    pub trailers: &'a [Vec<u8>],
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
    pub gzip: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct FrozenRoutesView<'a> {
    routes: &'a [FrozenRoute<'a>],
    natives: &'a [FrozenNativeRoute<'a>],
}

pub(crate) type StaticContent<'a> = (u16, &'a [u8], Option<&'a [u8]>);

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

    pub(crate) fn accepts_gzip<'b, 't, 'd, 'e>(
        fields: &[HpackHeader],
        server: &H2ServerDriver<'b, 't, 'd, 'e>,
    ) -> bool {
        for header in fields {
            if let Some((name, value)) = server.driver().resolve_header(*header) {
                if name.eq_ignore_ascii_case(b"accept-encoding") && accepts_gzip_value(value) {
                    return true;
                }
            }
        }

        false
    }

    pub(crate) fn lookup(&self, method: &[u8], path: &[u8]) -> Option<StaticContent<'_>> {
        self.find(method, path).map(|index| {
            (
                self.routes[index].status,
                self.routes[index].body,
                self.routes[index].gzip_body,
            )
        })
    }

    // NOTE: native-first dispatch like H1 (respond_to_head checks natives before statics):
    // same HEAD-to-GET fallback as find, so a HEAD static never shadows a GET native.
    pub(crate) fn lookup_native(
        &self,
        method: &[u8],
        path: &[u8],
    ) -> Option<(NativeHandler, usize, bool)> {
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
        found.map(|native| (native.handler, native.user_data, native.gzip))
    }

    // NOTE: miss-only path scan, so hits return with no extra work: one bounded pass
    // over statics then natives distinguishes a wrong method from a miss. File entries
    // never match here (exact routes win over mounts, same as H1), and HEAD fallback
    // stays dispatch-only (unregistered HEAD never lists, like unregistered OPTIONS).
    pub(crate) fn has_path_match(&self, path: &[u8]) -> bool {
        let statics = self
            .routes
            .iter()
            .any(|route| route.file_root.is_none() && route.path == path);
        if statics {
            return true;
        }
        self.natives.iter().any(|native| native.path == path)
    }

    // NOTE: Allow companion to has_path_match for 405 assembly: same frozen slots and
    // match rules (exact path equality, file entries skipped), no alloc, miss-path only.
    // Fills unique methods in table order (statics then natives) and truncates at out
    // capacity. Exact registered methods only, never synthesized.
    pub(crate) fn allowed_methods(&self, path: &[u8], out: &mut [&'a [u8]]) -> usize {
        let mut count = 0;
        for route in self.routes.iter() {
            if route.file_root.is_some() {
                continue;
            }
            if route.path != path {
                continue;
            }
            if out[..count].contains(&route.method) {
                continue;
            }
            let Some(slot) = out.get_mut(count) else {
                break;
            };
            *slot = route.method;
            count += 1;
        }
        for native in self.natives.iter() {
            if native.path != path {
                continue;
            }
            if out[..count].contains(&native.method) {
                continue;
            }
            let Some(slot) = out.get_mut(count) else {
                break;
            };
            *slot = native.method;
            count += 1;
        }
        count
    }

    // NOTE: comma-joined Allow value into one literal field (H1 respond_method_not_allowed
    // shape): ", " separators, None past the value buffer so the caller RSTs fail-closed.
    pub(crate) fn write_allow(&self, path: &[u8], out: &mut [u8]) -> Option<usize> {
        let mut methods: [&[u8]; ALLOW_METHODS] = [&[]; ALLOW_METHODS];
        let count = self.allowed_methods(path, &mut methods);
        if count == 0 {
            return None;
        }
        let mut len = 0;
        for (index, name) in methods[..count].iter().enumerate() {
            if index > 0 {
                let end = len + 2;
                if end > out.len() {
                    return None;
                }
                out[len..end].copy_from_slice(b", ");
                len = end;
            }
            let end = len + name.len();
            if end > out.len() {
                return None;
            }
            out[len..end].copy_from_slice(name);
            len = end;
        }
        Some(len)
    }

    pub(crate) fn is_managed(&self, method: &[u8], path: &[u8]) -> bool {
        self.find(method, path)
            .is_some_and(|index| self.routes[index].managed)
    }

    // NOTE: read-only allowlist for the park path: same match shape as find (exact then
    // HEAD-to-GET), empty when the route is gone so trailer staging fails closed without
    // disturbing the body path. Mirrors StaticRoutes::managed_trailers field for field.
    pub(crate) fn managed_trailers(&self, method: &[u8], path: &[u8]) -> &[Vec<u8>] {
        let found = self.routes.iter().find(|route| {
            route.file_root.is_none() && route.method == method && route.path == path
        });
        match found {
            Some(route) => route.trailers,
            None if method == b"HEAD" => self
                .routes
                .iter()
                .find(|route| {
                    route.file_root.is_none() && route.method == b"GET" && route.path == path
                })
                .map(|route| route.trailers)
                .unwrap_or(&[]),
            None => &[],
        }
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

// NOTE: file HEADERS assembly next to send_full: every status carries Date first (H1 parity:
// respond_file 200/206, respond_not_modified 304, respond_range_unsatisfiable 416 all emit
// Date first); 200/206 add content-length, content-type, accept-ranges, and last-modified;
// 304 carries content-length 0 plus last-modified (H1 emits both); 416 carries content-length 0 plus content-range */total. Single-range
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
    let mut mtime_buf = [0; HTTP_DATE_BYTES];
    let mtime_len = head
        .mtime
        .map(|mtime| write_http_date(mtime, &mut mtime_buf))
        .unwrap_or(0);
    let mut now_buf = [0; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now_buf);
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
    let mut fields = [field(b"", b""); 7];
    fields[0] = field(b":status", &status_buf[..status_len]);
    fields[1] = field(b"date", &now_buf[..now_len]);
    fields[2] = field(b"content-length", &length_buf[..length_len]);
    let mut count = 3;
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
        fields[count] = field(b"last-modified", &mtime_buf[..mtime_len]);
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
// at send_response time, so 204 emits :status plus Date with END_STREAM and never DATA;
// H1 carries Date on 204 the same way (body_allowed gates the body, never the Date).
pub(crate) fn send_status_only(
    server: &mut H2ServerDriver<'_, '_, '_, '_>,
    stream: u32,
    status: u16,
    output: &mut [u8],
    used: &mut usize,
) -> Result<(), ErrorCode> {
    let mut status_buf = [0; DECIMAL_CAP];
    let status_len = push_decimal(&mut status_buf, u64::from(status));
    let mut now_buf = [0; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now_buf);
    let response_fields = [
        field(b":status", &status_buf[..status_len]),
        field(b"date", &now_buf[..now_len]),
    ];
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

// NOTE: RFC 9113 section 8.2.2 length-match pin: a length-unknown Begin carries :status
// plus Date with END_STREAM clear, so the DATA that follows never contradicts a declared
// length (a content-length: 0 would make the first DATA a client-side PROTOCOL_ERROR).
// H1 Begin mirrors this with chunked framing plus Date (streams.rs Begin arm), and H2
// has no reason phrase, so :status plus Date is the whole head.
pub(crate) fn send_begin(
    server: &mut H2ServerDriver<'_, '_, '_, '_>,
    stream: u32,
    status: u16,
    output: &mut [u8],
    used: &mut usize,
) -> Result<(), ErrorCode> {
    let mut status_buf = [0; DECIMAL_CAP];
    let status_len = push_decimal(&mut status_buf, u64::from(status));
    let mut now_buf = [0; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now_buf);
    let response_fields = [
        field(b":status", &status_buf[..status_len]),
        field(b"date", &now_buf[..now_len]),
    ];
    let mut block = [0; HPACK_BLOCK_CAP];
    let sent = server
        .send_response(
            stream,
            &response_fields,
            &mut block,
            &mut output[*used..],
            false,
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
    content_encoding: Option<&[u8]>,
    vary: bool,
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
    let mut now_buf = [0; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now_buf);
    let mut response_fields = [
        field(b":status", &status_buf[..status_len]),
        field(b"date", &now_buf[..now_len]),
        field(b"content-length", &length_buf[..length_len]),
        field(b"content-encoding", b""),
        field(b"vary", b""),
    ];
    let mut field_count = 3;
    if let Some(encoding) = content_encoding {
        response_fields[field_count] = field(b"content-encoding", encoding);
        field_count += 1;
    }
    if vary {
        response_fields[field_count] = field(b"vary", b"Accept-Encoding");
        field_count += 1;
    }
    let mut block = [0; HPACK_BLOCK_CAP];
    let sent = server
        .send_response(
            stream,
            &response_fields[..field_count],
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

// NOTE: 405 HEADERS assembly beside send_full: :status plus Date plus content-length
// plus one comma-joined Allow literal (H1 encode_method_not_allowed shape). Date rides
// the existing now_http_date sender, no new Date machinery; per-stream fail-closed like
// send_full (HEADERS errors read InternalError, DATA errors FlowControlError, never GOAWAY
// here — the caller maps those to stream RSTs and the conn survives).
#[allow(clippy::too_many_arguments)]
pub(crate) fn send_method_not_allowed(
    server: &mut H2ServerDriver<'_, '_, '_, '_>,
    stream: u32,
    allow: &[u8],
    end_stream: bool,
    send_body: bool,
    output: &mut [u8],
    used: &mut usize,
) -> Result<(), ErrorCode> {
    let mut status_buf = [0; DECIMAL_CAP];
    let status_len = push_decimal(&mut status_buf, 405);
    let mut length_buf = [0; DECIMAL_CAP];
    let length_len = push_decimal(&mut length_buf, METHOD_NOT_ALLOWED_BODY.len() as u64);
    let mut now_buf = [0; HTTP_DATE_BYTES];
    let now_len = now_http_date(&mut now_buf);
    let response_fields = [
        field(b":status", &status_buf[..status_len]),
        field(b"date", &now_buf[..now_len]),
        field(b"content-length", &length_buf[..length_len]),
        field(b"allow", allow),
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
    while offset < METHOD_NOT_ALLOWED_BODY.len() {
        let room = output.len() - *used;
        if room < 10 {
            return Err(ErrorCode::EnhanceYourCalm);
        }
        let take = (METHOD_NOT_ALLOWED_BODY.len() - offset).min(room - 9);
        let last = offset + take == METHOD_NOT_ALLOWED_BODY.len();
        let sent = server
            .send_data(
                stream,
                &METHOD_NOT_ALLOWED_BODY[offset..offset + take],
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

// NOTE: view-level Allow contract without I/O: statics scan before natives in table order,
// duplicates collapse, file-mirror entries never match, HEAD stays dispatch-only (a GET-only
// path lists exactly GET), and unknown paths report no match for the 404 tail.
#[cfg(test)]
mod tests {
    use std::ffi::c_void;

    use crate::static_routes::{NativeRequestView, NativeResponseOut};

    use super::{FrozenNativeRoute, FrozenRoute, FrozenRoutesView};

    #[allow(unsafe_code)]
    unsafe extern "C" fn probe(
        _user_data: *mut c_void,
        _request: *const NativeRequestView,
        _response: *mut NativeResponseOut,
    ) -> i32 {
        0
    }

    #[test]
    fn allow_lists_exact_methods_in_table_order() {
        let file_root = std::path::Path::new("/tmp");
        let routes = [
            FrozenRoute {
                method: b"GET",
                path: b"/a",
                status: 200,
                body: b"a",
                gzip_body: None,
                managed: false,
                file_root: None,
                trailers: &[],
            },
            FrozenRoute {
                method: b"POST",
                path: b"/a",
                status: 200,
                body: b"b",
                gzip_body: None,
                managed: false,
                file_root: None,
                trailers: &[],
            },
            FrozenRoute {
                method: b"GET",
                path: b"/b",
                status: 200,
                body: b"c",
                gzip_body: None,
                managed: false,
                file_root: None,
                trailers: &[],
            },
            FrozenRoute {
                method: b"GET",
                path: b"/files/",
                status: 200,
                body: b"",
                gzip_body: None,
                managed: false,
                file_root: Some(file_root),
                trailers: &[],
            },
        ];
        let natives = [FrozenNativeRoute {
            method: b"PUT",
            path: b"/a",
            handler: probe,
            user_data: 0,
            gzip: false,
        }];
        let view = FrozenRoutesView::new(&routes)
            .expect("frozen routes")
            .with_natives(&natives)
            .expect("natives");
        assert!(view.has_path_match(b"/a"));
        assert!(view.has_path_match(b"/b"));
        assert!(!view.has_path_match(b"/missing"));
        assert!(!view.has_path_match(b"/files/"));
        let mut methods: [&[u8]; 4] = [&[]; 4];
        assert_eq!(view.allowed_methods(b"/a", &mut methods), 3);
        assert_eq!(
            &methods[..3],
            &[b"GET".as_slice(), b"POST".as_slice(), b"PUT".as_slice()]
        );
        assert_eq!(view.allowed_methods(b"/b", &mut methods), 1);
        assert_eq!(&methods[..1], &[b"GET".as_slice()]);
        assert_eq!(view.allowed_methods(b"/missing", &mut methods), 0);
        let mut allow = [0u8; 64];
        let len = view.write_allow(b"/a", &mut allow).expect("allow value");
        assert_eq!(&allow[..len], b"GET, POST, PUT");
        assert!(view.write_allow(b"/missing", &mut allow).is_none());
        let mut tiny = [0u8; 3];
        assert!(view.write_allow(b"/a", &mut tiny).is_none());
    }
}
