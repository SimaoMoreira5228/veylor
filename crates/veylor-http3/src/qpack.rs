use std::ops::Range;

use veylor_http2::{HuffmanError, decode_hpack_integer, decode_huffman, encode_hpack_integer};

pub type Field = (Vec<u8>, Vec<u8>);

// NOTE: borrowed decode result into a caller scratch. One scratch + one view Vec per
// header block replaces two heap Vecs per field (static fields included).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FieldView {
    pub name: Range<usize>,
    pub value: Range<usize>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QpackError {
    DecompressionFailed,
    FieldSectionTooLarge,
    InvalidInteger,
    InvalidString,
}

pub(crate) static STATIC_TABLE: [(&[u8], &[u8]); 99] = [
    (b":authority", b""),
    (b":path", b"/"),
    (b"age", b"0"),
    (b"content-disposition", b""),
    (b"content-length", b"0"),
    (b"cookie", b""),
    (b"date", b""),
    (b"etag", b""),
    (b"if-modified-since", b""),
    (b"if-none-match", b""),
    (b"last-modified", b""),
    (b"link", b""),
    (b"location", b""),
    (b"referer", b""),
    (b"set-cookie", b""),
    (b":method", b"CONNECT"),
    (b":method", b"DELETE"),
    (b":method", b"GET"),
    (b":method", b"HEAD"),
    (b":method", b"OPTIONS"),
    (b":method", b"POST"),
    (b":method", b"PUT"),
    (b":scheme", b"http"),
    (b":scheme", b"https"),
    (b":status", b"103"),
    (b":status", b"200"),
    (b":status", b"304"),
    (b":status", b"404"),
    (b":status", b"503"),
    (b"accept", b"*/*"),
    (b"accept", b"application/dns-message"),
    (b"accept-encoding", b"gzip, deflate, br"),
    (b"accept-ranges", b"bytes"),
    (b"access-control-allow-headers", b"cache-control"),
    (b"access-control-allow-headers", b"content-type"),
    (b"access-control-allow-origin", b"*"),
    (b"cache-control", b"max-age=0"),
    (b"cache-control", b"max-age=2592000"),
    (b"cache-control", b"max-age=604800"),
    (b"cache-control", b"no-cache"),
    (b"cache-control", b"no-store"),
    (b"cache-control", b"public, max-age=31536000"),
    (b"content-encoding", b"br"),
    (b"content-encoding", b"gzip"),
    (b"content-type", b"application/dns-message"),
    (b"content-type", b"application/javascript"),
    (b"content-type", b"application/json"),
    (b"content-type", b"application/x-www-form-urlencoded"),
    (b"content-type", b"image/gif"),
    (b"content-type", b"image/jpeg"),
    (b"content-type", b"image/png"),
    (b"content-type", b"text/css"),
    (b"content-type", b"text/html; charset=utf-8"),
    (b"content-type", b"text/plain"),
    (b"content-type", b"text/plain;charset=utf-8"),
    (b"range", b"bytes=0-"),
    (b"strict-transport-security", b"max-age=31536000"),
    (
        b"strict-transport-security",
        b"max-age=31536000; includesubdomains",
    ),
    (
        b"strict-transport-security",
        b"max-age=31536000; includesubdomains; preload",
    ),
    (b"vary", b"accept-encoding"),
    (b"vary", b"origin"),
    (b"x-content-type-options", b"nosniff"),
    (b"x-xss-protection", b"1; mode=block"),
    (b":status", b"100"),
    (b":status", b"204"),
    (b":status", b"206"),
    (b":status", b"302"),
    (b":status", b"400"),
    (b":status", b"403"),
    (b":status", b"421"),
    (b":status", b"425"),
    (b":status", b"500"),
    (b"accept-language", b""),
    (b"access-control-allow-credentials", b"FALSE"),
    (b"access-control-allow-credentials", b"TRUE"),
    (b"access-control-allow-headers", b"*"),
    (b"access-control-allow-methods", b"get"),
    (b"access-control-allow-methods", b"get, post, options"),
    (b"access-control-allow-methods", b"options"),
    (b"access-control-expose-headers", b"content-length"),
    (b"access-control-request-headers", b"content-type"),
    (b"access-control-request-method", b"get"),
    (b"access-control-request-method", b"post"),
    (b"alt-svc", b"clear"),
    (b"authorization", b""),
    (
        b"content-security-policy",
        b"script-src 'none'; object-src 'none'; base-uri 'none'",
    ),
    (b"early-data", b"1"),
    (b"expect-ct", b""),
    (b"forwarded", b""),
    (b"if-range", b""),
    (b"origin", b""),
    (b"purpose", b"prefetch"),
    (b"server", b""),
    (b"timing-allow-origin", b"*"),
    (b"upgrade-insecure-requests", b"1"),
    (b"user-agent", b""),
    (b"x-forwarded-for", b""),
    (b"x-frame-options", b"deny"),
    (b"x-frame-options", b"sameorigin"),
];

pub fn encode(fields: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut out = vec![0, 0];
    for (name, value) in fields {
        encode_field(name, value, &mut out);
    }
    out
}

pub fn decode(block: &[u8], max_field_section: usize) -> Result<Vec<Field>, QpackError> {
    let mut scratch = Vec::new();
    let mut views = Vec::new();
    decode_into(block, max_field_section, &mut scratch, &mut views)?;
    Ok(views
        .into_iter()
        .map(|view| (scratch[view.name].to_vec(), scratch[view.value].to_vec()))
        .collect())
}

pub fn decode_into(
    block: &[u8],
    max_field_section: usize,
    scratch: &mut Vec<u8>,
    fields: &mut Vec<FieldView>,
) -> Result<(), QpackError> {
    scratch.clear();
    fields.clear();
    let (required, used) =
        decode_hpack_integer(block, 8).map_err(|_| QpackError::InvalidInteger)?;
    if required != 0 {
        return Err(QpackError::DecompressionFailed);
    }
    let (_, base_length) =
        decode_hpack_integer(&block[used..], 7).map_err(|_| QpackError::InvalidInteger)?;
    let mut offset = used + base_length;
    let mut size = 0usize;

    while offset < block.len() {
        let (field, consumed) = decode_field_into(&block[offset..], scratch)?;
        offset += consumed;
        size = size.saturating_add(field.name.len() + field.value.len() + 32);
        if size > max_field_section {
            return Err(QpackError::FieldSectionTooLarge);
        }
        fields.push(field);
    }
    Ok(())
}

fn append(scratch: &mut Vec<u8>, bytes: &[u8]) -> Range<usize> {
    let start = scratch.len();
    scratch.extend_from_slice(bytes);
    start..scratch.len()
}

fn decode_field_into(
    input: &[u8],
    scratch: &mut Vec<u8>,
) -> Result<(FieldView, usize), QpackError> {
    let Some(&first) = input.first() else {
        return Err(QpackError::DecompressionFailed);
    };

    if first & 0x80 != 0 {
        let (index, used) =
            decode_hpack_integer(input, 6).map_err(|_| QpackError::InvalidInteger)?;
        if first & 0x40 == 0 {
            return Err(QpackError::DecompressionFailed);
        }
        let (name, value) = static_field(index).ok_or(QpackError::DecompressionFailed)?;
        return Ok((
            FieldView {
                name: append(scratch, name),
                value: append(scratch, value),
            },
            used,
        ));
    }

    if first & 0xC0 == 0x40 {
        let (index, used) =
            decode_hpack_integer(input, 4).map_err(|_| QpackError::InvalidInteger)?;
        if first & 0x10 == 0 {
            return Err(QpackError::DecompressionFailed);
        }
        let (name, _) = static_field(index).ok_or(QpackError::DecompressionFailed)?;
        let name = append(scratch, name);
        let (value, consumed) = decode_string_into(&input[used..], scratch)?;
        return Ok((FieldView { name, value }, used + consumed));
    }

    if first & 0xE0 == 0x20 {
        let (name_length, used) =
            decode_hpack_integer(input, 3).map_err(|_| QpackError::InvalidInteger)?;
        let end = used
            .checked_add(name_length as usize)
            .ok_or(QpackError::DecompressionFailed)?;
        let raw = input.get(used..end).ok_or(QpackError::InvalidString)?;
        let name = if first & 0x08 != 0 {
            decode_huffman_into(raw, scratch)?
        } else {
            append(scratch, raw)
        };
        let (value, consumed) = decode_string_into(&input[end..], scratch)?;
        return Ok((FieldView { name, value }, end + consumed));
    }

    Err(QpackError::DecompressionFailed)
}

fn decode_string_into(
    input: &[u8],
    scratch: &mut Vec<u8>,
) -> Result<(Range<usize>, usize), QpackError> {
    let Some(&first) = input.first() else {
        return Err(QpackError::InvalidString);
    };
    let (length, used) = decode_hpack_integer(input, 7).map_err(|_| QpackError::InvalidInteger)?;
    let end = used
        .checked_add(length as usize)
        .ok_or(QpackError::InvalidString)?;
    let raw = input.get(used..end).ok_or(QpackError::InvalidString)?;
    let range = if first & 0x80 != 0 {
        decode_huffman_into(raw, scratch)?
    } else {
        append(scratch, raw)
    };
    Ok((range, end))
}

fn decode_huffman_into(input: &[u8], scratch: &mut Vec<u8>) -> Result<Range<usize>, QpackError> {
    let mut small = [0u8; 256];
    match decode_huffman(input, &mut small) {
        Ok(length) => {
            let start = scratch.len();
            scratch.extend_from_slice(&small[..length]);
            Ok(start..scratch.len())
        }
        Err(HuffmanError::OutputTooSmall { required }) => {
            let start = scratch.len();
            scratch.resize(start + required, 0);
            let length = decode_huffman(input, &mut scratch[start..])
                .map_err(|_| QpackError::InvalidString)?;
            scratch.truncate(start + length);
            Ok(start..scratch.len())
        }
        Err(_) => Err(QpackError::InvalidString),
    }
}

fn encode_field(name: &[u8], value: &[u8], out: &mut Vec<u8>) {
    if let Some(index) = find(name, value) {
        encode_integer(0xC0, index, 6, out);
        return;
    }
    if let Some(index) = find_name(name) {
        encode_integer(0x50, index, 4, out);
        encode_string(value, out);
        return;
    }
    encode_integer(0x20, name.len() as u64, 3, out);
    out.extend_from_slice(name);
    encode_string(value, out);
}

fn encode_integer(prefix: u8, value: u64, bits: u8, out: &mut Vec<u8>) {
    let mut header = [0u8; 10];
    let length = encode_hpack_integer(value, bits, &mut header).expect("integer fits");
    header[0] |= prefix;
    out.extend_from_slice(&header[..length]);
}

fn encode_string(value: &[u8], out: &mut Vec<u8>) {
    let mut header = [0u8; 10];
    let length = encode_hpack_integer(value.len() as u64, 7, &mut header).expect("length fits");
    out.extend_from_slice(&header[..length]);
    out.extend_from_slice(value);
}

fn static_field(index: u64) -> Option<(&'static [u8], &'static [u8])> {
    STATIC_TABLE.get(usize::try_from(index).ok()?).copied()
}

fn find(name: &[u8], value: &[u8]) -> Option<u64> {
    STATIC_TABLE
        .iter()
        .position(|(entry_name, entry_value)| *entry_name == name && *entry_value == value)
        .map(|index| index as u64)
}

fn find_name(name: &[u8]) -> Option<u64> {
    STATIC_TABLE
        .iter()
        .position(|(entry_name, _)| *entry_name == name)
        .map(|index| index as u64)
}

#[cfg(test)]
mod tests {
    use super::{QpackError, decode, encode};

    #[test]
    fn round_trips_static_and_literal_fields() {
        let fields: &[(&[u8], &[u8])] = &[
            (b":status", b"200"),
            (b"content-type", b"text/plain"),
            (b"x-custom", b"value"),
            (b"date", b"Mon, 02 Nov 2026 00:00:00 GMT"),
        ];
        let block = encode(fields);
        let decoded = decode(&block, 16 * 1024).unwrap();
        assert_eq!(decoded.len(), fields.len());
        for (index, (name, value)) in fields.iter().enumerate() {
            assert_eq!(decoded[index].0, *name);
            assert_eq!(decoded[index].1, *value);
        }
    }

    #[test]
    fn rejects_dynamic_table_references() {
        assert_eq!(
            decode(&[1, 0], 16 * 1024),
            Err(QpackError::DecompressionFailed)
        );
        assert_eq!(
            decode(&[0, 0, 0x80], 16 * 1024),
            Err(QpackError::DecompressionFailed)
        );
    }

    #[test]
    fn enforces_decoded_field_section_limit() {
        let block = encode(&[(b"x-large", b"0123456789")]);
        assert_eq!(decode(&block, 16), Err(QpackError::FieldSectionTooLarge));
    }
}
