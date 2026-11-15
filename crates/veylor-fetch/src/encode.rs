const HEX: &[u8; 16] = b"0123456789ABCDEF";

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
