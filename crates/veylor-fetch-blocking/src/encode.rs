const BASE64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const HEX: &[u8; 16] = b"0123456789ABCDEF";

pub(crate) fn percent(input: &str, plus_for_space: bool, out: &mut String) {
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

pub(crate) fn base64(input: &[u8], out: &mut String) {
    for chunk in input.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);

        out.push(BASE64[usize::from(first >> 2)] as char);
        out.push(BASE64[usize::from((first & 3) << 4 | second >> 4)] as char);

        if chunk.len() > 1 {
            out.push(BASE64[usize::from((second & 15) << 2 | third >> 6)] as char);
        } else {
            out.push('=');
        }

        if chunk.len() > 2 {
            out.push(BASE64[usize::from(third & 63)] as char);
        } else {
            out.push('=');
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{base64, percent};

    #[test]
    fn base64_matches_rfc_vectors() {
        for (input, expected) in [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foobar", "Zm9vYmFy"),
        ] {
            let mut out = String::new();

            base64(input.as_bytes(), &mut out);

            assert_eq!(out, expected);
        }
    }

    #[test]
    fn percent_encodes_reserved_bytes() {
        let mut out = String::new();

        percent("a b/é", true, &mut out);

        assert_eq!(out, "a+b%2F%C3%A9");
    }
}
