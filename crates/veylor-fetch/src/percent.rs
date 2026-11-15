const HEX: &[u8; 16] = b"0123456789ABCDEF";

pub(crate) fn encode(out: &mut String, input: &str, plus_for_space: bool) {
    for &byte in input.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char);
            }
            b' ' if plus_for_space => out.push('+'),
            _ => {
                out.push('%');
                out.push(HEX[usize::from(byte >> 4)] as char);
                out.push(HEX[usize::from(byte & 15)] as char);
            }
        }
    }
}

pub(crate) fn push_pair(out: &mut String, name: &str, value: &str, plus_for_space: bool) {
    if !out.is_empty() {
        out.push('&');
    }

    encode(out, name, plus_for_space);
    out.push('=');
    encode(out, value, plus_for_space);
}

#[cfg(test)]
mod tests {
    use super::{encode, push_pair};

    #[test]
    fn encodes_reserved_bytes() {
        let mut out = String::new();

        encode(&mut out, "a b/é", true);

        assert_eq!(out, "a+b%2F%C3%A9");
    }

    #[test]
    fn joins_pairs_with_ampersands() {
        let mut out = String::new();

        push_pair(&mut out, "a b", "c&d", false);
        push_pair(&mut out, "e", "f", false);

        assert_eq!(out, "a%20b=c%26d&e=f");
    }
}
