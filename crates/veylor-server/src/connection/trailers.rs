use veylor_http1::{Header, Span, is_token};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StagedTrailer {
    pub(crate) name: Vec<u8>,
    pub(crate) value: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TrailerOutcome {
    Ok,
    NotReady,
    #[default]
    Stale,
    Gone,
    Duplicate,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TrailerReport {
    pub outcome: TrailerOutcome,
    pub fields: usize,
    pub names_copied: usize,
    pub values_copied: usize,
    pub names_required: usize,
    pub values_required: usize,
}

// NOTE: the worker-side decoder runs the response trailer policy, which permits keep-alive and
// proxy-connection; RFC 9110 forbids both in request trailers, so the allowlist gate rejects them
// here alongside unlisted and Connection-nominated names. Decoder verdicts stay authoritative for
// syntax and its own prohibited set; anything refused anywhere fails the connection closed.
pub(crate) fn trailer_allowed(allow: &[Vec<u8>], connection: &[u8], name: &[u8]) -> bool {
    if name.eq_ignore_ascii_case(b"keep-alive") || name.eq_ignore_ascii_case(b"proxy-connection") {
        return false;
    }
    if !allow
        .iter()
        .any(|allowed| name.eq_ignore_ascii_case(allowed))
    {
        return false;
    }
    !connection_nominates(connection, name)
}

pub(crate) fn connection_nominates(value: &[u8], name: &[u8]) -> bool {
    if value.is_empty() {
        return false;
    }
    for option in value.split(|byte| *byte == b',') {
        let trimmed = trim_ows(option);
        if trimmed.is_empty() || !trimmed.iter().copied().all(is_token) {
            return true;
        }
        if trimmed.eq_ignore_ascii_case(name) {
            return true;
        }
    }
    false
}

pub(crate) fn connection_value<'a>(
    headers: &'a [Header],
    head: veylor_http1::RequestHead,
    input: &'a [u8],
) -> &'a [u8] {
    for header in headers.iter().take(head.header_count) {
        let (Some(name), Some(value)) = (field(input, header.name), field(input, header.value))
        else {
            continue;
        };
        if name.eq_ignore_ascii_case(b"connection") {
            return value;
        }
    }
    b""
}

fn trim_ows(value: &[u8]) -> &[u8] {
    let start = value
        .iter()
        .position(|byte| *byte != b' ' && *byte != b'\t');
    let end = value
        .iter()
        .rposition(|byte| *byte != b' ' && *byte != b'\t');
    match (start, end) {
        (Some(start), Some(end)) => &value[start..end + 1],
        _ => b"",
    }
}

pub(crate) fn field(input: &[u8], span: Span) -> Option<&[u8]> {
    input.get(span.offset..span.offset.checked_add(span.length)?)
}
