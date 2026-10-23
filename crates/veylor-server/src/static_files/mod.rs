#[cfg(unix)]
use std::ffi::OsStr;
use std::fs::File;
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

pub const MAX_MOUNTS: usize = 16;
// NOTE: files stream through the pump in bounded turns, so this cap only bounds metadata sanity, never resident memory; it mirrors the Odin STATIC_FILE_MAX_SIZE so every file the old engine planned still plans here.
pub const MAX_FILE_SIZE: u64 = i64::MAX as u64;
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
    pub total: u64,
    pub start: u64,
    pub content_type: &'static str,
    pub head_only: bool,
    pub partial: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RangeApply {
    Ignored,
    Ranged,
    Unsatisfiable,
}

impl FilePlan {
    // NOTE: single-range resolution rides the plan so the pump keeps 0-based offsets:
    // size narrows to the window length and start anchors it, so a 206 streams [start,
    // start + size) through the unchanged completion rule. Malformed values, unknown
    // units, and multi-range sets ignore back to 200 full (RFC 9110 servers MAY ignore
    // Range), and unsatisfiable windows report for a 416. If-Range is not read here.
    pub fn apply_range(&mut self, header: Option<&[u8]>) -> RangeApply {
        let Some(value) = header else {
            return RangeApply::Ignored;
        };
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Window {
    Full,
    Slice { start: u64, end: u64 },
    Unsatisfiable,
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
            }
        }
        if size > MAX_FILE_SIZE {
            return Err(FileReject::TooLarge);
        }
        Ok(FilePlan {
            fs_path,
            size,
            total: size,
            start: 0,
            content_type: content_type_for_name(segments[last].as_slice()),
            head_only: method == b"HEAD",
            partial: false,
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

// NOTE: the pump opens per response through this helper; no fd cache yet, so each response pays one open plus one seek. TOCTOU: resolve's component checks and this open are not atomic under stable std (no openat); a rename race could swap the leaf between plan and open. The follow-up either re-validates here (fstat type check on the opened fd) or moves to openat2/O_NOFOLLOW. Short reads from a shrunk file surface as Ok(n); the pump must treat a short total as truncation, never as success.
pub fn read_chunk(plan: &FilePlan, offset: u64, out: &mut [u8]) -> std::io::Result<usize> {
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
    let mut file = File::open(&plan.fs_path)?;
    // NOTE: pump offsets stay window-relative, so the seek anchors at plan start; the
    // window always sits inside total, hence start + offset cannot overflow u64 here.
    file.seek(SeekFrom::Start(plan.start + offset))?;
    let end = remaining.min(out.len() as u64) as usize;
    file.read(&mut out[..end])
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
