const HEX: &[u8; 16] = b"0123456789ABCDEF";
const BASE64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

pub fn push_pair<K, V>(out: &mut String, key: K, value: V)
where
    K: AsRef<str>,
    V: AsRef<str>,
{
    if !out.is_empty() {
        out.push('&');
    }

    percent_encode(out, key.as_ref(), true);
    out.push('=');
    percent_encode(out, value.as_ref(), true);
}

pub fn percent_encode(out: &mut String, input: &str, plus_for_space: bool) {
    for &byte in input.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char);
            }
            b' ' if plus_for_space => out.push('+'),
            _ => {
                out.push('%');
                out.push(HEX[(byte >> 4) as usize] as char);
                out.push(HEX[(byte & 15) as usize] as char);
            }
        }
    }
}

pub fn base64(input: &[u8]) -> String {
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);

    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let word = (b0 << 16) | (b1 << 8) | b2;

        out.push(BASE64[(word >> 18) as usize & 63] as char);
        out.push(BASE64[(word >> 12) as usize & 63] as char);

        if chunk.len() > 1 {
            out.push(BASE64[(word >> 6) as usize & 63] as char);
        } else {
            out.push('=');
        }

        if chunk.len() > 2 {
            out.push(BASE64[word as usize & 63] as char);
        } else {
            out.push('=');
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::{base64, percent_encode, push_pair};

    #[test]
    fn base64_matches_rfc4648_vectors() {
        assert_eq!(base64(b""), "");
        assert_eq!(base64(b"f"), "Zg==");
        assert_eq!(base64(b"fo"), "Zm8=");
        assert_eq!(base64(b"foo"), "Zm9v");
        assert_eq!(base64(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn encodes_reserved_bytes() {
        let mut out = String::new();

        percent_encode(&mut out, "a b&c=d/é", false);

        assert_eq!(out, "a%20b%26c%3Dd%2F%C3%A9");

        let mut form = String::new();

        push_pair(&mut form, "k y", "v+1");
        push_pair(&mut form, "z", "");

        assert_eq!(form, "k+y=v%2B1&z=");
    }
}
