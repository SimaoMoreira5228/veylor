use std::io;

use veylor_client::h2::{DEFAULT_MAX_FIELDS, HpackFieldToEncode, HpackRepresentation};

use super::Command;
use super::shared::Failure;

pub(super) type Fields<'a> = ([HpackFieldToEncode<'a>; DEFAULT_MAX_FIELDS], usize);

const CONNECTION_SPECIFIC: [&str; 6] = [
    "host",
    "connection",
    "keep-alive",
    "proxy-connection",
    "transfer-encoding",
    "upgrade",
];

pub(super) fn field<'a>(name: &'a [u8], value: &'a [u8]) -> HpackFieldToEncode<'a> {
    HpackFieldToEncode {
        name,
        value,
        representation: HpackRepresentation::Default,
        huffman_name: false,
        huffman_value: true,
    }
}

pub(super) fn lowered(headers: &[(String, String)]) -> Vec<(String, String)> {
    headers
        .iter()
        .filter(|(name, _)| {
            !CONNECTION_SPECIFIC
                .iter()
                .any(|reserved| name.eq_ignore_ascii_case(reserved))
                && !is_framing_header(name)
        })
        .map(|(name, value)| (name.to_ascii_lowercase(), value.clone()))
        .collect()
}

// NOTE: the frame stream is authoritative for the body length, so a caller-supplied framing
// header would either duplicate or contradict the one emitted from the body itself.
fn is_framing_header(name: &str) -> bool {
    name.eq_ignore_ascii_case("content-length") || name.eq_ignore_ascii_case("transfer-encoding")
}

pub(super) fn build<'a>(
    command: &'a Command,
    digits: &'a mut [u8; 20],
) -> Result<Fields<'a>, Failure> {
    let mut fields = [field(b"", b""); DEFAULT_MAX_FIELDS];
    let mut used = 0;
    let mut push = |entry: HpackFieldToEncode<'a>| -> Result<(), Failure> {
        if used == DEFAULT_MAX_FIELDS {
            return Err(Failure::new(
                io::ErrorKind::InvalidInput,
                "too many request headers",
            ));
        }

        fields[used] = entry;
        used += 1;

        Ok(())
    };

    push(field(b":method", command.method.as_bytes()))?;
    push(field(b":scheme", b"https"))?;
    push(field(b":authority", command.authority.as_bytes()))?;
    push(field(b":path", command.path.as_bytes()))?;

    for (name, value) in &command.headers {
        push(field(name.as_bytes(), value.as_bytes()))?;
    }

    if command.expects_body
        && let Some(length) = command.body.declared_length()
    {
        push(field(b"content-length", length_digits(length, digits)))?;
    }

    Ok((fields, used))
}

fn length_digits(mut value: u64, digits: &mut [u8; 20]) -> &[u8] {
    let mut start = digits.len();

    loop {
        start -= 1;
        digits[start] = b'0' + (value % 10) as u8;
        value /= 10;

        if value == 0 {
            break;
        }
    }

    &digits[start..]
}
