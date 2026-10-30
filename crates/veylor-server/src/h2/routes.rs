use std::path::Path;

use veylor_http1::{ALLOW_METHODS, is_token, join_allow_value};
use veylor_http2::{H2ServerDriver, HpackHeader};

use crate::static_files::{FileMounts, FilePlan, FileReject};
use crate::static_routes::{
    MAX_RESPONSE_BODY, MAX_ROUTES, NativeHandler, RouteBuildError, accepts_gzip_value,
};

pub(crate) const NOT_FOUND_BODY: &[u8] = b"Not Found";
pub(crate) const METHOD_NOT_ALLOWED_BODY: &[u8] = b"Method Not Allowed";
pub(crate) const URI_TOO_LONG_BODY: &[u8] = b"URI Too Long";
// NOTE: one target limit for both protocols, same 8 KiB number as the H1 request-line cap
// (Limits::default().request_line_bytes): H1 attributes buffered-line overflow minus the
// minimal framing, while H2 checks the decoded :path directly since its length is known
// exactly — no buffered-line attribution needed.
pub(crate) const MAX_REQUEST_TARGET_BYTES: usize = 8 * 1024;
pub(crate) const MAX_RESPONSE_FIELDS: usize = 5 + crate::host::MAX_STAGED_RESPONSE_HEADERS;
pub(crate) const MAX_BEGIN_FIELDS: usize = 2 + crate::host::MAX_STAGED_RESPONSE_HEADERS;

pub(crate) fn project_extra_headers<'a>(
    out: &mut [(&'a [u8], &'a [u8])],
    src: &'a [(Vec<u8>, Vec<u8>)],
) -> usize {
    let mut count = 0;
    for (name, value) in src.iter() {
        let Some(slot) = out.get_mut(count) else {
            break;
        };
        *slot = (name.as_slice(), value.as_slice());
        count += 1;
    }
    count
}
// NOTE: Allow join and range/multipart scratch live in veylor_http1 (response_write):
// both engines share one join_allow_value plus the four size bounds, so the H2 405
// plus OPTIONS and file-head assembly cannot drift from the H1 shapes.

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
        join_allow_value(&methods[..count], out)
    }

    pub(crate) fn server_methods(&self, out: &mut [&'a [u8]]) -> usize {
        let mut count = 0;
        for route in self.routes.iter() {
            if route.file_root.is_some() {
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

    pub(crate) fn write_server_allow(&self, out: &mut [u8]) -> Option<usize> {
        let mut methods: [&[u8]; ALLOW_METHODS] = [&[]; ALLOW_METHODS];
        let count = self.server_methods(&mut methods);
        if count == 0 {
            return None;
        }
        join_allow_value(&methods[..count], out)
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

// NOTE: query strings strip like the H1 file_target so /assets/a.css?x=1 names the file.
fn strip_file_query(path: &[u8]) -> &[u8] {
    let end = path
        .iter()
        .position(|byte| *byte == b'?')
        .unwrap_or(path.len());
    &path[..end]
}

// NOTE: view-level Allow contract without I/O: statics scan before natives in table order,
// duplicates collapse, file-mirror entries never match, HEAD stays dispatch-only (a GET-only
// path lists exactly GET), and unknown paths report no match for the 404 tail.
#[cfg(test)]
mod tests {
    use std::ffi::c_void;

    use crate::static_routes::{NativeRequestView, NativeResponseOut};

    use super::{FrozenNativeRoute, FrozenRoute, FrozenRoutesView};
    use veylor_http1::{ALLOW_BYTES, ALLOW_METHODS, CONTENT_RANGE_BYTES, MULTIPART_CTYPE_BYTES};

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

    #[test]
    fn allow_scratch_sizes_match_shared_bounds() {
        // NOTE: drift alarm for the converged Allow scratch policy: this engine sizes
        // every Allow, Content-Range, and multipart buffer from the shared veylor_http1
        // bounds, so a shared-bound edit fails here until this consumer follows.
        assert_eq!(ALLOW_METHODS, 16);
        assert_eq!(ALLOW_BYTES, 320);
        assert_eq!(CONTENT_RANGE_BYTES, 80);
        assert_eq!(MULTIPART_CTYPE_BYTES, 128);
    }
}
