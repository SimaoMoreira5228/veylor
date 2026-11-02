use std::fs::File;
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::path::PathBuf;

use super::{file_key, push_bytes, push_decimal};

// NOTE: at most this many satisfiable windows frame as one multipart body; more valid windows ignore the header to 200 full,
// so framing work and part-header stack stay constant. Framing itself is bounded per part below, and body bytes always add up
// under a checked total, so a set can never stage unbounded memory or wrap the length.
pub const MAX_RANGES: usize = 8;
const PART_HEADER_BYTES: usize = 256;
const MULTIPART_BOUNDARY_BYTES: usize = 64;

#[derive(Clone, Debug)]
pub struct FilePlan {
    pub fs_path: PathBuf,
    pub size: u64,
    pub mtime: u64,
    pub total: u64,
    pub start: u64,
    pub content_type: &'static str,
    pub content_encoding: Option<super::ContentCoding>,
    pub head_only: bool,
    pub partial: bool,
    pub multipart: bool,
    pub ranges: [(u64, u64); MAX_RANGES],
    pub range_count: u8,
    pub(super) dev: u64,
    pub(super) ino: u64,
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
