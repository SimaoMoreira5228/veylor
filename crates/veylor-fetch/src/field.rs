pub(crate) fn is_token(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte)
}

pub(crate) fn is_valid_name(name: &str) -> bool {
    !name.is_empty() && name.bytes().all(is_token)
}

// SECURITY: every control byte is rejected, not only CR, LF and NUL, so a value can never smuggle
// a second field or confuse a downstream parser.
pub(crate) fn is_valid_value(value: &[u8]) -> bool {
    value
        .iter()
        .all(|&byte| (byte >= 0x20 && byte != 0x7f) || byte == b'\t')
}

#[cfg(test)]
mod tests {
    use super::{is_valid_name, is_valid_value};

    #[test]
    fn names_must_be_tokens() {
        assert!(is_valid_name("x-custom_1"));
        assert!(!is_valid_name(""));
        assert!(!is_valid_name("bad name"));
        assert!(!is_valid_name("a:b"));
    }

    #[test]
    fn values_reject_control_bytes() {
        assert!(is_valid_value(b"a\tb c"));
        assert!(!is_valid_value(b"a\r\nb"));
        assert!(!is_valid_value(b"a\0b"));
        assert!(!is_valid_value(b"a\x7fb"));
    }
}
