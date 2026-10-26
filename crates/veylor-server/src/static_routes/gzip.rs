use std::io::Write;

use flate2::Compression;
use flate2::write::GzEncoder;

use crate::static_routes::RouteBuildError;

pub(crate) const GZIP_MAX_BYTES: usize = crate::static_routes::MAX_RESPONSE_BODY + 64;

pub(crate) const CONTENT_ENCODING_GZIP: &[u8] = b"gzip";

pub(crate) fn compress_route_body(body: &[u8]) -> Result<Option<Vec<u8>>, RouteBuildError> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(body)
        .map_err(|_| RouteBuildError::Invalid)?;
    let compressed = encoder.finish().map_err(|_| RouteBuildError::Invalid)?;
    if compressed.len() < body.len() {
        return Ok(Some(compressed));
    }
    Ok(None)
}

pub(crate) fn accepts_gzip_value(value: &[u8]) -> bool {
    let mut star: Option<bool> = None;
    let mut explicit: Option<bool> = None;
    for member in value.split(|byte| *byte == b',') {
        let member = trim_ows(member);
        let (token, params) = match member.iter().position(|byte| *byte == b';') {
            Some(index) => (&member[..index], &member[index + 1..]),
            None => (member, &[][..]),
        };
        if token.eq_ignore_ascii_case(b"gzip") {
            explicit = Some(weight_accepts(params));
        } else if token == b"*" && star.is_none() {
            star = Some(weight_accepts(params));
        }
    }
    match explicit {
        Some(accepted) => accepted,
        None => star.unwrap_or(false),
    }
}

fn weight_accepts(params: &[u8]) -> bool {
    for param in params.split(|byte| *byte == b';') {
        let param = trim_ows(param);
        if param.len() >= 2
            && (param[0] == b'q' || param[0] == b'Q')
            && param[1] == b'='
            && is_zero_weight(trim_ows(&param[2..]))
        {
            return false;
        }
    }
    true
}

fn is_zero_weight(value: &[u8]) -> bool {
    if value.is_empty() || value[0] != b'0' {
        return false;
    }
    let rest = &value[1..];
    if rest.is_empty() {
        return true;
    }
    if rest[0] != b'.' {
        return false;
    }
    !rest[1..].is_empty() && rest[1..].iter().all(|byte| *byte == b'0')
}

fn trim_ows(value: &[u8]) -> &[u8] {
    let start = value
        .iter()
        .position(|byte| *byte != b' ' && *byte != b'\t')
        .unwrap_or(value.len());
    let end = value
        .iter()
        .rposition(|byte| *byte != b' ' && *byte != b'\t')
        .map(|index| index + 1)
        .unwrap_or(0);
    if start >= end {
        return &[][..];
    }
    &value[start..end]
}

#[cfg(test)]
mod tests {
    use super::accepts_gzip_value;

    #[test]
    fn encoding_matrix() {
        assert!(accepts_gzip_value(b"gzip"));
        assert!(accepts_gzip_value(b"GZip"));
        assert!(accepts_gzip_value(b"deflate, gzip"));
        assert!(accepts_gzip_value(b"gzip, deflate, br"));
        assert!(accepts_gzip_value(b"  gzip  "));
        assert!(accepts_gzip_value(b"*"));
        assert!(accepts_gzip_value(b"gzip;q=1.0"));
        assert!(accepts_gzip_value(b"gzip; q=0.5"));
        assert!(!accepts_gzip_value(b""));
        assert!(!accepts_gzip_value(b"deflate"));
        assert!(!accepts_gzip_value(b"deflate, br"));
        assert!(!accepts_gzip_value(b"gzip;q=0"));
        assert!(!accepts_gzip_value(b"gzip; q=0.0"));
        assert!(!accepts_gzip_value(b"gzip;q=0, *;q=1"));
        assert!(accepts_gzip_value(b"*;q=0, gzip;q=1"));
        assert!(accepts_gzip_value(b"*;q=0.5"));
        assert!(!accepts_gzip_value(b"*;q=0"));
    }
}
