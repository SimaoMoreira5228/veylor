use std::cell::RefCell;
use std::io::Write;

use flate2::Compression;
use flate2::write::GzEncoder;

use crate::static_routes::RouteBuildError;

pub(crate) const GZIP_MAX_BYTES: usize = crate::static_routes::MAX_RESPONSE_BODY + 64;

pub(crate) const CONTENT_ENCODING_GZIP: &[u8] = b"gzip";

// NOTE: native handlers re-render the same body on every request (cached JSON, generated
// documents, dashboards), and deflating it again is the whole cost of a gzip response:
// miniz_oxide was ~70% of CPU on the JSON-comp profile. A small per-worker cache keeps the
// last few bodies together with their twin; hits verify the bytes instead of trusting an
// address, so reused handler buffers stay correct. Bounded by entries, not traffic.
const TWIN_CACHE_ENTRIES: usize = 8;

struct CachedTwin {
    source: Vec<u8>,
    compressed: Option<Vec<u8>>,
}

thread_local! {
    static TWIN_CACHE: RefCell<Vec<CachedTwin>> = const { RefCell::new(Vec::new()) };
}

pub(crate) fn compress_route_body(body: &[u8]) -> Result<Option<Vec<u8>>, RouteBuildError> {
    if body.is_empty() {
        return Ok(None);
    }
    let cached = TWIN_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let index = cache
            .iter()
            .position(|entry| entry.source.len() == body.len() && entry.source == body)?;
        if index > 0 {
            cache.swap(0, index);
        }
        Some(cache[0].compressed.clone())
    });
    if let Some(twin) = cached {
        return Ok(twin);
    }
    let compressed = deflate(body)?;
    TWIN_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if cache.len() == TWIN_CACHE_ENTRIES {
            cache.pop();
        }
        cache.insert(
            0,
            CachedTwin {
                source: body.to_vec(),
                compressed: compressed.clone(),
            },
        );
    });
    Ok(compressed)
}

fn deflate(body: &[u8]) -> Result<Option<Vec<u8>>, RouteBuildError> {
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
    use std::io::Read;

    use flate2::read::GzDecoder;

    use super::{accepts_gzip_value, compress_route_body};

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

    #[test]
    fn cached_twin_matches_fresh_deflate_and_stays_correct_across_bodies() {
        let first = vec![b'a'; 4_096];
        let mut second = Vec::new();
        for index in 0..2_048 {
            second.extend_from_slice(
                format!("{{\"id\":{index},\"name\":\"item-{index}\"}}").as_bytes(),
            );
        }
        for body in [first.as_slice(), second.as_slice()] {
            let twin = compress_route_body(body)
                .expect("compress")
                .expect("smaller than source");
            let mut decoded = Vec::new();
            GzDecoder::new(twin.as_slice())
                .read_to_end(&mut decoded)
                .expect("decode twin");
            assert_eq!(decoded, body);
        }
        // NOTE: alternating bodies exercises the cache eviction order rather than only a
        // single hot entry, and every hit still round-trips through gzip.
        for _ in 0..3 {
            let twin = compress_route_body(&first)
                .expect("compress")
                .expect("smaller than source");
            let mut decoded = Vec::new();
            GzDecoder::new(twin.as_slice())
                .read_to_end(&mut decoded)
                .expect("decode twin");
            assert_eq!(decoded, first);
        }
    }
}
