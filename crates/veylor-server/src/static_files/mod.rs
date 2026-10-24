#[cfg(unix)]
use std::ffi::OsStr;
use std::fs::File;
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

mod http_date;

pub(crate) use http_date::system_time_secs;
pub use http_date::{HTTP_DATE_BYTES, parse_http_date, write_http_date};

pub const MAX_MOUNTS: usize = 16;
// NOTE: files stream through the pump in bounded turns, so this cap only bounds metadata sanity, never resident memory; it mirrors the Odin STATIC_FILE_MAX_SIZE so every file the old engine planned still plans here.
pub const MAX_FILE_SIZE: u64 = i64::MAX as u64;
// NOTE: at most this many satisfiable windows frame as one multipart body; more valid windows ignore the header to 200 full,
// so framing work and part-header stack stay constant. Framing itself is bounded per part below, and body bytes always add up
// under a checked total, so a set can never stage unbounded memory or wrap the length.
pub const MAX_RANGES: usize = 8;
const PART_HEADER_BYTES: usize = 256;
const MULTIPART_BOUNDARY_BYTES: usize = 64;
const MAX_PREFIX_BYTES: usize = 256;
const MAX_COMPONENT_BYTES: usize = 255;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MountError {
    Invalid,
    Duplicate,
    Full,
    Frozen,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileReject {
    NotFound,
    Forbidden,
    NotAFile,
    TooLarge,
}

#[derive(Clone, Debug)]
pub struct FilePlan {
    pub fs_path: PathBuf,
    pub size: u64,
    pub mtime: u64,
    pub total: u64,
    pub start: u64,
    pub content_type: &'static str,
    pub head_only: bool,
    pub partial: bool,
    pub multipart: bool,
    pub ranges: [(u64, u64); MAX_RANGES],
    pub range_count: u8,
    dev: u64,
    ino: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RangeApply {
    Ignored,
    Ranged,
    Multipart,
    Unsatisfiable,
}

impl FilePlan {
    // NOTE: single-range resolution rides the plan so the pump keeps 0-based offsets:
    // size narrows to the window length and start anchors it, so a 206 streams [start,
    // start + size) through the unchanged completion rule. Malformed values and unknown
    // units ignore back to 200 full (RFC 9110 servers MAY ignore Range); satisfiable sets
    // frame via apply_range_set below, and unsatisfiable windows report for a 416. If-Range is not read here.
    // NOTE: a set with two or more satisfiable windows frames as multipart: bad items drop individually, one survivor keeps the
    // single-206 path, zero survivors keep the old 416-vs-200 split by whether any item parsed as a range at all, and over-cap or
    // overflowing sets ignore to 200 full, never 500.
    pub fn apply_range(&mut self, header: Option<&[u8]>) -> RangeApply {
        let Some(value) = header else {
            return RangeApply::Ignored;
        };
        if range_set_has_comma(value) {
            return self.apply_range_set(value);
        }
        match range_window(self.total, value) {
            Window::Full => RangeApply::Ignored,
            Window::Unsatisfiable => RangeApply::Unsatisfiable,
            Window::Slice { start, end } => {
                self.start = start;
                self.size = end - start;
                self.partial = true;
                RangeApply::Ranged
            }
        }
    }

    fn apply_range_set(&mut self, value: &[u8]) -> RangeApply {
        let trimmed = trim_range_ows(value);
        if trimmed.len() < 6 || !trimmed[..5].eq_ignore_ascii_case(b"bytes") || trimmed[5] != b'=' {
            return RangeApply::Ignored;
        }
        let set = trim_range_ows(&trimmed[6..]);
        if set.is_empty() {
            return RangeApply::Ignored;
        }
        let mut windows = [(0u64, 0u64); MAX_RANGES];
        let mut valid = 0usize;
        let mut unsatisfiable = 0u32;
        for item in set.split(|byte| *byte == b',') {
            match single_window(self.total, trim_range_ows(item)) {
                Window::Full => {}
                Window::Unsatisfiable => unsatisfiable += 1,
                Window::Slice { start, end } => {
                    if valid < MAX_RANGES {
                        windows[valid] = (start, end);
                    }
                    valid += 1;
                }
            }
        }
        if valid == 0 {
            return if unsatisfiable > 0 {
                RangeApply::Unsatisfiable
            } else {
                RangeApply::Ignored
            };
        }
        if valid == 1 {
            let (start, end) = windows[0];
            self.start = start;
            self.size = end - start;
            self.partial = true;
            return RangeApply::Ranged;
        }
        if valid > MAX_RANGES {
            return RangeApply::Ignored;
        }
        let Some(length) = multipart_total(self.total, self.content_type, &windows[..valid]) else {
            return RangeApply::Ignored;
        };
        self.start = 0;
        self.size = length;
        self.partial = true;
        self.multipart = true;
        self.ranges = windows;
        self.range_count = valid as u8;
        RangeApply::Multipart
    }

    pub(crate) fn multipart_content_type(&self, out: &mut [u8]) -> usize {
        let len = push_bytes(out, 0, b"multipart/byteranges; boundary=");
        len + write_boundary(
            self.total,
            self.ranges[0].0,
            self.range_count as u64,
            &mut out[len..],
        )
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn identity(&self) -> (u64, u64) {
        (self.dev, self.ino)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Window {
    Full,
    Slice { start: u64, end: u64 },
    Unsatisfiable,
}

fn range_set_has_comma(value: &[u8]) -> bool {
    let trimmed = trim_range_ows(value);
    if trimmed.len() < 6 || !trimmed[..5].eq_ignore_ascii_case(b"bytes") || trimmed[5] != b'=' {
        return false;
    }
    trim_range_ows(&trimmed[6..]).contains(&b',')
}

fn range_window(size: u64, value: &[u8]) -> Window {
    let trimmed = trim_range_ows(value);
    if trimmed.len() < 6 || !trimmed[..5].eq_ignore_ascii_case(b"bytes") || trimmed[5] != b'=' {
        return Window::Full;
    }
    let set = trim_range_ows(&trimmed[6..]);
    if set.is_empty() || set.contains(&b',') {
        return Window::Full;
    }
    single_window(size, set)
}

fn single_window(size: u64, set: &[u8]) -> Window {
    let dash = match set.iter().position(|byte| *byte == b'-') {
        Some(dash) => dash,
        None => return Window::Full,
    };
    let (first, last) = (&set[..dash], &set[dash + 1..]);
    if first.is_empty() {
        let suffix = match parse_range_digits(last) {
            Some(suffix) => suffix,
            None => return Window::Full,
        };
        // NOTE: a zero suffix selects no bytes, so it ignores to 200 full; an empty file
        // satisfies no suffix, so any range on it reports 416 with bytes */0 downstream.
        if suffix == 0 {
            return Window::Full;
        }
        if size == 0 {
            return Window::Unsatisfiable;
        }
        if suffix >= size {
            return Window::Slice {
                start: 0,
                end: size,
            };
        }
        return Window::Slice {
            start: size - suffix,
            end: size,
        };
    }
    let start = match parse_range_digits(first) {
        Some(start) => start,
        None => return Window::Full,
    };
    if last.is_empty() {
        if start >= size {
            return Window::Unsatisfiable;
        }
        return Window::Slice { start, end: size };
    }
    let last = match parse_range_digits(last) {
        Some(last) => last,
        None => return Window::Full,
    };
    if last < start {
        return Window::Full;
    }
    if start >= size {
        return Window::Unsatisfiable;
    }
    Window::Slice {
        start,
        end: last.saturating_add(1).min(size),
    }
}

// NOTE: digit runs saturate at u64::MAX instead of wrapping, so huge positions fail
// deterministically downstream: a saturated start reads as past end (416), a saturated
// last clamps to size - 1, and a saturated suffix covers the whole file.
fn parse_range_digits(digits: &[u8]) -> Option<u64> {
    if digits.is_empty() {
        return None;
    }
    let mut value: u64 = 0;
    for byte in digits.iter() {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value
            .saturating_mul(10)
            .saturating_add((byte - b'0') as u64);
    }
    Some(value)
}

fn trim_range_ows(mut value: &[u8]) -> &[u8] {
    while matches!(value.first(), Some(b' ') | Some(b'\t')) {
        value = &value[1..];
    }
    while matches!(value.last(), Some(b' ') | Some(b'\t')) {
        value = &value[..value.len() - 1];
    }
    value
}

// NOTE: the boundary is a fixed token plus hex of the file size, first window start, and part count: deterministic per response
// with no shared state, so concurrent connections never coordinate. A static file could still contain the delimiter line verbatim;
// scanning content would break bounded streaming, so operators serving adversarial bytes accept the same residual risk as random-boundary servers.
fn write_boundary(total: u64, first: u64, count: u64, out: &mut [u8]) -> usize {
    let mut len = push_bytes(out, 0, b"VEYLOR_");
    len = push_hex(out, len, total);
    len = push_bytes(out, len, b"_");
    len = push_hex(out, len, first);
    len = push_bytes(out, len, b"_");
    push_decimal(out, len, count)
}

fn write_part_header(
    total: u64,
    content_type: &str,
    boundary: &[u8],
    start: u64,
    end: u64,
    out: &mut [u8],
) -> usize {
    let mut len = push_bytes(out, 0, b"--");
    len = push_bytes(out, len, boundary);
    len = push_bytes(out, len, b"\r\nContent-Type: ");
    len = push_bytes(out, len, content_type.as_bytes());
    len = push_bytes(out, len, b"\r\nContent-Range: bytes ");
    len = push_decimal(out, len, start);
    len = push_bytes(out, len, b"-");
    len = push_decimal(out, len, end - 1);
    len = push_bytes(out, len, b"/");
    len = push_decimal(out, len, total);
    push_bytes(out, len, b"\r\n\r\n")
}

fn multipart_total(total: u64, content_type: &str, windows: &[(u64, u64)]) -> Option<u64> {
    let mut boundary = [0u8; MULTIPART_BOUNDARY_BYTES];
    let blen = write_boundary(total, windows[0].0, windows.len() as u64, &mut boundary) as u64;
    let boundary = &boundary[..blen as usize];
    let mut header = [0u8; PART_HEADER_BYTES];
    let mut length = 0u64;
    for (start, end) in windows.iter().copied() {
        let hlen = write_part_header(total, content_type, boundary, start, end, &mut header) as u64;
        length = length
            .checked_add(hlen)?
            .checked_add(end - start)?
            .checked_add(2)?;
    }
    length.checked_add(2 + blen + 4)
}

fn stage_segment(cursor: u64, offset: u64, out: &mut [u8], staged: usize, segment: &[u8]) -> usize {
    let end = cursor + segment.len() as u64;
    if offset >= end || staged >= out.len() {
        return staged;
    }
    let from = offset.saturating_sub(cursor) as usize;
    let take = (segment.len() - from).min(out.len() - staged);
    out[staged..staged + take].copy_from_slice(&segment[from..from + take]);
    staged + take
}

fn read_multipart_chunk(plan: &FilePlan, offset: u64, out: &mut [u8]) -> std::io::Result<usize> {
    if offset > plan.size {
        return Err(std::io::Error::new(
            ErrorKind::InvalidInput,
            "file offset past planned size",
        ));
    }
    if offset == plan.size || out.is_empty() {
        return Ok(0);
    }
    let mut boundary = [0u8; MULTIPART_BOUNDARY_BYTES];
    let blen = write_boundary(
        plan.total,
        plan.ranges[0].0,
        plan.range_count as u64,
        &mut boundary,
    );
    let boundary = &boundary[..blen];
    let mut header = [0u8; PART_HEADER_BYTES];
    let mut cursor = 0u64;
    let mut staged = 0usize;
    for index in 0..plan.range_count as usize {
        let (start, end) = plan.ranges[index];
        let hlen = write_part_header(
            plan.total,
            plan.content_type,
            boundary,
            start,
            end,
            &mut header,
        );
        staged = stage_segment(cursor, offset, out, staged, &header[..hlen]);
        cursor += hlen as u64;
        if staged == out.len() {
            return Ok(staged);
        }
        let width = end - start;
        let body_end = cursor + width;
        if offset < body_end && staged < out.len() {
            let skip = offset.saturating_sub(cursor);
            let take = (width - skip).min((out.len() - staged) as u64) as usize;
            if take > 0 {
                let got = read_window(plan, start + skip, &mut out[staged..staged + take])?;
                if got == 0 {
                    return Err(std::io::Error::new(
                        ErrorKind::UnexpectedEof,
                        "multipart file shrank underfoot",
                    ));
                }
                staged += got;
            }
        }
        cursor = body_end;
        if staged == out.len() {
            return Ok(staged);
        }
        staged = stage_segment(cursor, offset, out, staged, b"\r\n");
        cursor += 2;
        if staged == out.len() {
            return Ok(staged);
        }
    }
    let mut len = push_bytes(&mut header, 0, b"--");
    len = push_bytes(&mut header, len, boundary);
    len = push_bytes(&mut header, len, b"--\r\n");
    staged = stage_segment(cursor, offset, out, staged, &header[..len]);
    Ok(staged)
}

// NOTE: byte writers shared with static-routes header values so the pair stays single-sourced over fixed buffers.
pub(crate) fn push_bytes(out: &mut [u8], len: usize, bytes: &[u8]) -> usize {
    let end = len + bytes.len();
    out[len..end].copy_from_slice(bytes);
    end
}

pub(crate) fn push_decimal(out: &mut [u8], mut len: usize, value: u64) -> usize {
    let mut digits = [0u8; 20];
    let mut count = 0;
    let mut rest = value;
    loop {
        digits[count] = b'0' + (rest % 10) as u8;
        count += 1;
        rest /= 10;
        if rest == 0 {
            break;
        }
    }
    while count > 0 {
        count -= 1;
        out[len] = digits[count];
        len += 1;
    }
    len
}

fn push_hex(out: &mut [u8], mut len: usize, mut value: u64) -> usize {
    let mut digits = [0u8; 16];
    let mut count = 0;
    loop {
        digits[count] = b"0123456789abcdef"[(value & 0xf) as usize];
        count += 1;
        value >>= 4;
        if value == 0 {
            break;
        }
    }
    while count > 0 {
        count -= 1;
        out[len] = digits[count];
        len += 1;
    }
    len
}

struct FileMount {
    prefix: Vec<u8>,
    root: PathBuf,
}

pub struct FileMounts {
    mounts: Vec<FileMount>,
    frozen: bool,
}

impl FileMounts {
    pub fn new() -> Self {
        Self {
            mounts: Vec::new(),
            frozen: false,
        }
    }

    pub fn register(&mut self, prefix: &[u8], root: &Path) -> Result<(), MountError> {
        if self.frozen {
            return Err(MountError::Frozen);
        }
        if !valid_prefix(prefix) || !root.is_absolute() {
            return Err(MountError::Invalid);
        }
        // NOTE: the configured root opens under the operator's own path (intermediate symlinks allowed); per-component symlink rejection applies to request paths beneath it, not to this root.
        let root_file = File::open(root).map_err(|_| MountError::Invalid)?;
        let root_dir = root_file
            .metadata()
            .map(|meta| meta.is_dir())
            .unwrap_or(false);
        if !root_dir {
            return Err(MountError::Invalid);
        }
        if self.mounts.len() >= MAX_MOUNTS {
            return Err(MountError::Full);
        }
        if self.mounts.iter().any(|mount| mount.prefix == prefix) {
            return Err(MountError::Duplicate);
        }
        self.mounts.push(FileMount {
            prefix: prefix.to_vec(),
            root: root.to_path_buf(),
        });
        Ok(())
    }

    pub fn freeze(&mut self) {
        self.frozen = true;
    }

    pub fn is_frozen(&self) -> bool {
        self.frozen
    }

    pub fn resolve(&self, method: &[u8], path: &[u8]) -> Result<FilePlan, FileReject> {
        // NOTE: no file route exists for other methods; the pump maps those to 405, here they read as no match.
        if method != b"GET" && method != b"HEAD" {
            return Err(FileReject::NotFound);
        }
        let candidates = self
            .mounts
            .iter()
            .filter(|mount| path.starts_with(mount.prefix.as_slice()));
        let mount = match candidates.max_by_key(|mount| mount.prefix.len()) {
            Some(mount) => mount,
            None => return Err(FileReject::NotFound),
        };
        let relative = &path[mount.prefix.len()..];
        if relative.is_empty() {
            // NOTE: the mount root itself is a directory, never a servable file.
            return Err(FileReject::NotAFile);
        }
        if relative.contains(&b'?') || relative.contains(&b'#') {
            // SECURITY: query/fragment bytes must be stripped by the caller; planning past them lets one URL name two resources.
            return Err(FileReject::Forbidden);
        }
        let mut segments: Vec<Vec<u8>> = Vec::new();
        for segment in relative.split(|byte| *byte == b'/') {
            let decoded = decode_segment(segment)?;
            // SECURITY: empty segments (//, trailing /), `.`, `..`, backslash, and NUL reject after decoding, so %2e%2e and %2f cannot dodge the check.
            if !valid_component(&decoded) {
                return Err(FileReject::Forbidden);
            }
            segments.push(decoded);
        }
        let last = segments.len() - 1;
        let mut fs_path = mount.root.clone();
        let mut size = 0;
        let mut mtime = 0;
        let mut dev = 0;
        let mut ino = 0;
        for (index, segment) in segments.iter().enumerate() {
            push_segment(&mut fs_path, segment)?;
            let meta = match std::fs::symlink_metadata(&fs_path) {
                Ok(meta) => meta,
                Err(error)
                    if matches!(error.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) =>
                {
                    return Err(FileReject::NotFound);
                }
                // NOTE: permission and unexpected IO errors fail closed; only absent paths read as NotFound.
                Err(_) => return Err(FileReject::Forbidden),
            };
            // SECURITY: symlink in EVERY component rejects, intermediates included; a linked directory beneath the root escapes the mount.
            if meta.file_type().is_symlink() {
                return Err(FileReject::Forbidden);
            }
            if index == last {
                // NOTE: directories, fifos, sockets, and devices all land here; only regular files plan.
                if !meta.file_type().is_file() {
                    return Err(FileReject::NotAFile);
                }
                size = meta.len();
                mtime = system_time_secs(meta.modified().ok());
                let key = file_key(&meta);
                dev = key.0;
                ino = key.1;
            }
        }
        if size > MAX_FILE_SIZE {
            return Err(FileReject::TooLarge);
        }
        Ok(FilePlan {
            fs_path,
            size,
            mtime,
            total: size,
            start: 0,
            content_type: content_type_for_name(segments[last].as_slice()),
            head_only: method == b"HEAD",
            partial: false,
            multipart: false,
            ranges: [(0, 0); MAX_RANGES],
            range_count: 0,
            dev,
            ino,
        })
    }
}

impl Default for FileMounts {
    fn default() -> Self {
        Self::new()
    }
}

pub fn content_type_for_name(file_name: &[u8]) -> &'static str {
    let mut extension = file_name;
    if let Some(dot) = file_name.iter().rposition(|byte| *byte == b'.') {
        extension = &file_name[dot + 1..];
    }
    // NOTE: const table only, no mime database; unknown or missing extensions serve as generic bytes.
    if extension.eq_ignore_ascii_case(b"html") {
        return "text/html";
    }
    if extension.eq_ignore_ascii_case(b"css") {
        return "text/css";
    }
    if extension.eq_ignore_ascii_case(b"js") {
        return "text/javascript";
    }
    if extension.eq_ignore_ascii_case(b"json") {
        return "application/json";
    }
    if extension.eq_ignore_ascii_case(b"png") {
        return "image/png";
    }
    if extension.eq_ignore_ascii_case(b"jpg") {
        return "image/jpeg";
    }
    if extension.eq_ignore_ascii_case(b"svg") {
        return "image/svg+xml";
    }
    if extension.eq_ignore_ascii_case(b"txt") {
        return "text/plain";
    }
    "application/octet-stream"
}

// NOTE: the pump opens per chunk through this helper, so each chunk pays one open plus one fstat plus one seek. TOCTOU:
// resolve's component checks and each open are still not atomic under stable std (no openat), so a rename race can swap
// the leaf between plan and open; every open therefore re-validates the fd itself (regular-file type plus the
// resolve-time dev+ino identity) and any mismatch fails closed before the seek, so swapped bytes never serve.
// Same-inode mutation (truncate/grow in place) keeps the old clamp/short-read rules, and a leaf swapped to a fifo can
// still block the open itself (std has no nonblocking open). Non-Unix degrades to the type check alone, the same
// file_key shape as the H2 retained-fd path. Short reads from a shrunk file surface as Ok(n); the pump must treat a
// short total as truncation, never as success.
pub fn read_chunk(plan: &FilePlan, offset: u64, out: &mut [u8]) -> std::io::Result<usize> {
    if plan.multipart {
        return read_multipart_chunk(plan, offset, out);
    }
    if offset > plan.size {
        return Err(std::io::Error::new(
            ErrorKind::InvalidInput,
            "file offset past planned size",
        ));
    }
    let remaining = plan.size - offset;
    if remaining == 0 || out.is_empty() {
        return Ok(0);
    }
    let end = remaining.min(out.len() as u64) as usize;
    read_window(plan, plan.start + offset, &mut out[..end])
}

fn read_window(plan: &FilePlan, abs_start: u64, out: &mut [u8]) -> std::io::Result<usize> {
    let mut file = File::open(&plan.fs_path)?;
    let fresh = file.metadata()?;
    if !fresh.file_type().is_file() {
        return Err(std::io::Error::new(
            ErrorKind::InvalidData,
            "open file is not a regular file",
        ));
    }
    if file_key(&fresh) != (plan.dev, plan.ino) {
        return Err(std::io::Error::new(
            ErrorKind::InvalidData,
            "file replaced underfoot",
        ));
    }
    // NOTE: pump offsets stay window-relative, so the seek anchors at plan start; the
    // window always sits inside total, hence start + offset cannot overflow u64 here.
    file.seek(SeekFrom::Start(abs_start))?;
    file.read(out)
}

#[cfg(unix)]
fn file_key(meta: &std::fs::Metadata) -> (u64, u64) {
    use std::os::unix::fs::MetadataExt;
    (meta.dev(), meta.ino())
}

// NOTE: non-Unix identity degrades to the type check alone (the key reads (0, 0) on both sides, so the comparison
// passes trivially); size+mtime cannot pin the leaf because the pump must keep serving clamped/short reads across
// in-place grow/shrink. No Linux-only code, no extra gating.
#[cfg(not(unix))]
fn file_key(_meta: &std::fs::Metadata) -> (u64, u64) {
    (0, 0)
}

fn decode_segment(segment: &[u8]) -> Result<Vec<u8>, FileReject> {
    let mut decoded: Vec<u8> = Vec::with_capacity(segment.len());
    let mut index = 0;
    while index < segment.len() {
        let mut byte = segment[index];
        index += 1;
        if byte == b'%' {
            // SECURITY: malformed escapes reject outright; no best-effort decoding past attacker-controlled bytes.
            let digits = segment.get(index..index + 2).ok_or(FileReject::Forbidden)?;
            let high = hex_value(digits[0]).ok_or(FileReject::Forbidden)?;
            let low = hex_value(digits[1]).ok_or(FileReject::Forbidden)?;
            byte = (high << 4) | low;
            index += 2;
            // SECURITY: escapes decoding to separators or NUL reject: %2f, %5c, and %00 cannot smuggle path structure.
            if byte == b'/' || byte == b'\\' || byte == 0 {
                return Err(FileReject::Forbidden);
            }
        }
        decoded.push(byte);
    }
    Ok(decoded)
}

fn valid_component(component: &[u8]) -> bool {
    if component.is_empty() || component.len() > MAX_COMPONENT_BYTES {
        return false;
    }
    if component == b"." || component == b".." {
        return false;
    }
    !component.iter().any(|byte| *byte == b'\\' || *byte == 0)
}

fn valid_prefix(prefix: &[u8]) -> bool {
    if prefix.is_empty() || prefix.len() > MAX_PREFIX_BYTES {
        return false;
    }
    if prefix[0] != b'/' || prefix[prefix.len() - 1] != b'/' {
        return false;
    }
    if prefix.len() == 1 {
        return true;
    }
    for component in prefix[1..prefix.len() - 1].split(|byte| *byte == b'/') {
        if !valid_component(component) {
            return false;
        }
        if component.contains(&b'?') || component.contains(&b'#') || component.contains(&b'%') {
            return false;
        }
    }
    true
}

// NOTE: stable has no byte-faithful OsStr construction off Unix, so non-UTF-8 names reject
// there instead of misresolving; Unix keeps exact bytes. Keeps this module building for the
// whole native matrix.
#[cfg(unix)]
fn push_segment(path: &mut PathBuf, segment: &[u8]) -> Result<(), FileReject> {
    use std::os::unix::ffi::OsStrExt;
    path.push(OsStr::from_bytes(segment));
    Ok(())
}

#[cfg(not(unix))]
fn push_segment(path: &mut PathBuf, segment: &[u8]) -> Result<(), FileReject> {
    match std::str::from_utf8(segment) {
        Ok(valid) => {
            path.push(valid);
            Ok(())
        }
        Err(_) => Err(FileReject::Forbidden),
    }
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
