pub(crate) fn parse(value: &[u8]) -> Result<bool, ExpectationError> {
    let mut offset = 0;
    let mut expect_continue = false;
    let mut unsupported = false;

    loop {
        skip_ows(value, &mut offset);
        let name_start = offset;
        while value.get(offset).is_some_and(|byte| is_token(*byte)) {
            offset += 1;
        }
        if name_start == offset {
            return Err(ExpectationError::Invalid);
        }
        let is_continue = value[name_start..offset].eq_ignore_ascii_case(b"100-continue");
        let mut has_value = false;
        let mut has_parameters = false;

        skip_ows(value, &mut offset);
        if value.get(offset) == Some(&b'=') {
            has_value = true;
            offset += 1;
            skip_ows(value, &mut offset);
            if !parse_value(value, &mut offset) {
                return Err(ExpectationError::Invalid);
            }
        }

        loop {
            skip_ows(value, &mut offset);
            if value.get(offset) != Some(&b';') {
                break;
            }
            has_parameters = true;
            offset += 1;
            skip_ows(value, &mut offset);

            let parameter_start = offset;
            while value.get(offset).is_some_and(|byte| is_token(*byte)) {
                offset += 1;
            }
            if parameter_start == offset {
                return Err(ExpectationError::Invalid);
            }
            skip_ows(value, &mut offset);
            if value.get(offset) != Some(&b'=') {
                return Err(ExpectationError::Invalid);
            }
            offset += 1;
            skip_ows(value, &mut offset);
            if !parse_value(value, &mut offset) {
                return Err(ExpectationError::Invalid);
            }
        }

        if is_continue && !has_value && !has_parameters {
            expect_continue = true;
        } else {
            unsupported = true;
        }

        skip_ows(value, &mut offset);
        if offset == value.len() {
            break;
        }
        if value[offset] != b',' {
            return Err(ExpectationError::Invalid);
        }
        offset += 1;
        if offset == value.len() {
            return Err(ExpectationError::Invalid);
        }
    }

    if unsupported {
        return Err(ExpectationError::Unsupported);
    }
    if !expect_continue {
        return Err(ExpectationError::Invalid);
    }
    Ok(true)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExpectationError {
    Invalid,
    Unsupported,
}

fn parse_value(value: &[u8], offset: &mut usize) -> bool {
    if value.get(*offset) != Some(&b'"') {
        let start = *offset;
        while value.get(*offset).is_some_and(|byte| is_token(*byte)) {
            *offset += 1;
        }
        return start != *offset;
    }

    *offset += 1;
    while let Some(byte) = value.get(*offset).copied() {
        match byte {
            b'"' => {
                *offset += 1;
                return true;
            }
            b'\\' => {
                *offset += 1;
                let Some(escaped) = value.get(*offset).copied() else {
                    return false;
                };
                if escaped != b'\t' && !(0x20..=0x7e).contains(&escaped) && escaped < 0x80 {
                    return false;
                }
                *offset += 1;
            }
            b'\t' | b' ' | 0x21 | 0x23..=0x5b | 0x5d..=0x7e | 0x80..=0xff => {
                *offset += 1;
            }
            _ => return false,
        }
    }
    false
}

fn skip_ows(value: &[u8], offset: &mut usize) {
    while value
        .get(*offset)
        .is_some_and(|byte| matches!(byte, b' ' | b'\t'))
    {
        *offset += 1;
    }
}

fn is_token(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte)
}

#[cfg(test)]
mod tests {
    use super::{ExpectationError, parse};

    #[test]
    fn parses_continue_and_extension_syntax() {
        assert_eq!(parse(b"100-continue"), Ok(true));
        assert_eq!(parse(b"100-continue, 100-CONTINUE"), Ok(true));
        assert_eq!(
            parse(b"example; key=token; quoted=\"a\\\"b\""),
            Err(ExpectationError::Unsupported)
        );
    }

    #[test]
    fn separates_malformed_from_unsupported_expectations() {
        assert_eq!(parse(b"magic"), Err(ExpectationError::Unsupported));
        assert_eq!(parse(b"100-continue=1"), Err(ExpectationError::Unsupported));
        assert_eq!(parse(b"100-continue,,"), Err(ExpectationError::Invalid));
        assert_eq!(parse(b"100-continue;bad"), Err(ExpectationError::Invalid));
    }
}
