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

#[cfg(test)]
mod tests {
    use super::percent;

    #[test]
    fn percent_encodes_reserved_bytes() {
        let mut out = String::new();

        percent("a b/é", true, &mut out);

        assert_eq!(out, "a+b%2F%C3%A9");
    }
}
