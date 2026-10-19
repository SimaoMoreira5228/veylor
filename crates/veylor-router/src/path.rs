use std::cmp::Ordering;

use veylor_http1::{RequestHead, Span, TargetForm, is_token};

use crate::RouteError;

pub const MAX_ROUTE_CAPTURES: usize = 8;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RouteCapture {
    pub name: Span,
    pub value: Span,
}

impl RouteCapture {
    pub fn name(self, pattern: &[u8]) -> Option<&[u8]> {
        pattern.get(self.name.offset..self.name.offset.checked_add(self.name.length)?)
    }

    pub fn value(self, input: &[u8]) -> Option<&[u8]> {
        input.get(self.value.offset..self.value.offset.checked_add(self.value.length)?)
    }
}

#[derive(Clone, Copy)]
pub(crate) struct RequestPath<'a> {
    pub bytes: &'a [u8],
    pub input_offset: usize,
}

pub(crate) fn inspect_pattern(path: &[u8]) -> Option<usize> {
    if !valid_path(path) {
        return None;
    }

    let mut dynamic_count = 0;
    let mut start = 1;
    loop {
        let end = segment_end(path, start);
        if start < end && matches!(path[start], b':' | b'*') {
            let wildcard = path[start] == b'*';
            let name_start = start + 1;
            if name_start == end || (wildcard && end != path.len()) {
                return None;
            }
            if !path[name_start..end].iter().copied().all(is_token) {
                return None;
            }
            if parameter_name_seen(path, name_start, end) {
                return None;
            }
            dynamic_count += 1;
            if dynamic_count > MAX_ROUTE_CAPTURES {
                return None;
            }
        }
        if end == path.len() {
            return Some(dynamic_count);
        }
        start = end + 1;
    }
}

pub(crate) fn compare_paths(left: &[u8], right: &[u8]) -> Ordering {
    let mut left_offset = 0;
    let mut right_offset = 0;
    while left_offset < left.len() && right_offset < right.len() {
        let (left_value, left_encoded, next_left) = path_token(left, left_offset);
        let (right_value, right_encoded, next_right) = path_token(right, right_offset);
        left_offset = next_left;
        right_offset = next_right;
        match left_encoded.cmp(&right_encoded) {
            Ordering::Equal => {}
            ordering => return ordering,
        }
        match left_value.cmp(&right_value) {
            Ordering::Equal => {}
            ordering => return ordering,
        }
    }
    match (left_offset == left.len(), right_offset == right.len()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        (false, false) => Ordering::Equal,
    }
}

pub(crate) fn compare_patterns(left: &[u8], right: &[u8]) -> Ordering {
    let mut left_start = 1;
    let mut right_start = 1;
    loop {
        let left_end = segment_end(left, left_start);
        let right_end = segment_end(right, right_start);
        let left_kind = segment_kind(left, left_start, left_end);
        let right_kind = segment_kind(right, right_start, right_end);
        match left_kind.cmp(&right_kind) {
            Ordering::Equal => {}
            ordering => return ordering,
        }
        if left_kind == 0 {
            match compare_paths(&left[left_start..left_end], &right[right_start..right_end]) {
                Ordering::Equal => {}
                ordering => return ordering,
            }
        }

        let left_finished = left_end == left.len();
        let right_finished = right_end == right.len();
        match (left_finished, right_finished) {
            (true, true) => return Ordering::Equal,
            (true, false) => return Ordering::Less,
            (false, true) => return Ordering::Greater,
            (false, false) => {
                left_start = left_end + 1;
                right_start = right_end + 1;
            }
        }
    }
}

pub(crate) fn match_pattern(
    pattern: &[u8],
    path: &[u8],
    input_offset: usize,
    captures: &mut [RouteCapture; MAX_ROUTE_CAPTURES],
) -> Option<usize> {
    let mut count = 0;
    let mut pattern_start = 1;
    let mut path_start = 1;
    loop {
        let pattern_end = segment_end(pattern, pattern_start);
        let path_end = segment_end(path, path_start);
        let kind = segment_kind(pattern, pattern_start, pattern_end);
        match kind {
            1 => {
                if path_start == path_end {
                    return None;
                }
                let value_offset = input_offset.checked_add(path_start)?;
                captures[count] = RouteCapture {
                    name: Span {
                        offset: pattern_start + 1,
                        length: pattern_end - pattern_start - 1,
                    },
                    value: Span {
                        offset: value_offset,
                        length: path_end - path_start,
                    },
                };
                count += 1;
            }
            2 => {
                let value_offset = input_offset.checked_add(path_start)?;
                captures[count] = RouteCapture {
                    name: Span {
                        offset: pattern_start + 1,
                        length: pattern_end - pattern_start - 1,
                    },
                    value: Span {
                        offset: value_offset,
                        length: path.len() - path_start,
                    },
                };
                return Some(count + 1);
            }
            _ => {
                if compare_paths(
                    &pattern[pattern_start..pattern_end],
                    &path[path_start..path_end],
                ) != Ordering::Equal
                {
                    return None;
                }
            }
        }

        let pattern_finished = pattern_end == pattern.len();
        let path_finished = path_end == path.len();
        if pattern_finished || path_finished {
            return (pattern_finished && path_finished).then_some(count);
        }
        pattern_start = pattern_end + 1;
        path_start = path_end + 1;
    }
}

pub(crate) fn request_path<'a>(
    head: RequestHead,
    input: &'a [u8],
) -> Result<Option<RequestPath<'a>>, RouteError> {
    let target = head.target(input).ok_or(RouteError::InvalidRequest)?;
    let target_offset = head.target.offset;
    let request_path = match head.target_form {
        TargetForm::Origin => {
            let end = query_start(target);
            RequestPath {
                bytes: &target[..end],
                input_offset: target_offset,
            }
        }
        TargetForm::Absolute => {
            let colon = target
                .iter()
                .position(|byte| *byte == b':')
                .ok_or(RouteError::InvalidRequest)?;
            if colon == 0
                || !target[..colon].iter().enumerate().all(|(index, byte)| {
                    if index == 0 {
                        byte.is_ascii_alphabetic()
                    } else {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.')
                    }
                })
            {
                return Err(RouteError::InvalidRequest);
            }
            let authority_start = colon.checked_add(3).ok_or(RouteError::InvalidRequest)?;
            if target.get(colon + 1..authority_start) != Some(&b"//"[..]) {
                return Err(RouteError::InvalidRequest);
            }
            let authority_end = target[authority_start..]
                .iter()
                .position(|byte| matches!(byte, b'/' | b'?'))
                .map_or(target.len(), |offset| authority_start + offset);
            if authority_end == authority_start {
                return Err(RouteError::InvalidRequest);
            }
            if authority_end == target.len() || target[authority_end] == b'?' {
                let path_offset = authority_end
                    .checked_sub(1)
                    .ok_or(RouteError::InvalidRequest)?;
                RequestPath {
                    bytes: b"/",
                    input_offset: target_offset
                        .checked_add(path_offset)
                        .ok_or(RouteError::InvalidRequest)?,
                }
            } else {
                let path = &target[authority_end..];
                let end = query_start(path);
                RequestPath {
                    bytes: &path[..end],
                    input_offset: target_offset
                        .checked_add(authority_end)
                        .ok_or(RouteError::InvalidRequest)?,
                }
            }
        }
        TargetForm::Authority | TargetForm::Asterisk => return Ok(None),
    };
    if !valid_path(request_path.bytes) {
        return Err(RouteError::InvalidRequest);
    }
    Ok(Some(request_path))
}

fn valid_path(path: &[u8]) -> bool {
    if path.first() != Some(&b'/') {
        return false;
    }
    let mut offset = 0;
    while offset < path.len() {
        let byte = path[offset];
        if byte == b'%' {
            let Some(end) = offset.checked_add(3) else {
                return false;
            };
            let Some(escape) = path.get(offset + 1..end) else {
                return false;
            };
            if !escape.iter().all(u8::is_ascii_hexdigit) {
                return false;
            }
            offset += 3;
            continue;
        }
        if !is_unreserved(byte) && !is_sub_delimiter(byte) && !matches!(byte, b':' | b'@' | b'/') {
            return false;
        }
        offset += 1;
    }
    true
}

fn parameter_name_seen(path: &[u8], name_start: usize, name_end: usize) -> bool {
    let mut start = 1;
    let current_segment_start = name_start - 1;
    while start < current_segment_start {
        let end = segment_end(path, start);
        if start < end && matches!(path[start], b':' | b'*') {
            let previous_start = start + 1;
            if path[previous_start..end] == path[name_start..name_end] {
                return true;
            }
        }
        if end == path.len() {
            break;
        }
        start = end + 1;
    }
    false
}

fn segment_end(path: &[u8], start: usize) -> usize {
    path[start..]
        .iter()
        .position(|byte| *byte == b'/')
        .map_or(path.len(), |offset| start + offset)
}

fn segment_kind(path: &[u8], start: usize, end: usize) -> u8 {
    if start == end {
        return 0;
    }
    match path[start] {
        b':' => 1,
        b'*' => 2,
        _ => 0,
    }
}

fn query_start(path: &[u8]) -> usize {
    path.iter()
        .position(|byte| *byte == b'?')
        .unwrap_or(path.len())
}

fn path_token(path: &[u8], offset: usize) -> (u8, bool, usize) {
    if path[offset] != b'%' {
        return (path[offset], false, offset + 1);
    }
    let value = hex_value(path[offset + 1]) * 16 + hex_value(path[offset + 2]);
    (value, !is_unreserved(value), offset + 3)
}

fn hex_value(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => byte - b'A' + 10,
    }
}

fn is_unreserved(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"-._~".contains(&byte)
}

fn is_sub_delimiter(byte: u8) -> bool {
    b"!$&'()*+,;=".contains(&byte)
}
