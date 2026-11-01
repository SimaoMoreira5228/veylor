#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConnectionOptions {
    pub close: bool,
    pub keep_alive: bool,
    pub upgrade: bool,
}

pub(crate) fn parse_content_length(value: &[u8]) -> Option<u64> {
    let mut offset = 0;
    let mut parsed = None;
    loop {
        while value.get(offset).is_some_and(|byte| is_ows(*byte)) {
            offset += 1;
        }

        let start = offset;
        let mut length = 0_u64;
        while let Some(byte @ b'0'..=b'9') = value.get(offset).copied() {
            length = length
                .checked_mul(10)?
                .checked_add(u64::from(byte - b'0'))?;
            offset += 1;
        }
        if offset == start || parsed.is_some_and(|previous| previous != length) {
            return None;
        }
        parsed = Some(length);

        while value.get(offset).is_some_and(|byte| is_ows(*byte)) {
            offset += 1;
        }
        if offset == value.len() {
            return parsed;
        }
        if value[offset] != b',' {
            return None;
        }
        offset += 1;
    }
}

pub(crate) fn parse_connection_options(value: &[u8]) -> Option<ConnectionOptions> {
    let mut offset = 0;
    let mut options = ConnectionOptions::default();
    loop {
        while value.get(offset).is_some_and(|byte| is_ows(*byte)) {
            offset += 1;
        }
        let start = offset;
        while value.get(offset).is_some_and(|byte| is_token(*byte)) {
            offset += 1;
        }
        if start == offset {
            return None;
        }

        let token = &value[start..offset];
        options.close |= token.eq_ignore_ascii_case(b"close");
        options.keep_alive |= token.eq_ignore_ascii_case(b"keep-alive");
        options.upgrade |= token.eq_ignore_ascii_case(b"upgrade");

        while value.get(offset).is_some_and(|byte| is_ows(*byte)) {
            offset += 1;
        }
        if offset == value.len() {
            return Some(options);
        }
        if value[offset] != b',' {
            return None;
        }
        offset += 1;
        if offset == value.len() {
            return None;
        }
    }
}

const TOKEN_TABLE: [bool; 256] = {
    let mut table = [false; 256];
    let mut index = 0usize;
    while index < 256 {
        let byte = index as u8;
        table[index] = matches!(
            byte,
            b'0'..=b'9'
                | b'a'..=b'z'
                | b'A'..=b'Z'
                | b'!'
                | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        );
        index += 1;
    }
    table
};

#[inline]
pub fn is_token(byte: u8) -> bool {
    TOKEN_TABLE[byte as usize]
}

pub(crate) fn is_ows(byte: u8) -> bool {
    byte == b' ' || byte == b'\t'
}

pub(crate) fn valid_upgrade_field(value: &[u8]) -> bool {
    let mut offset = 0;
    loop {
        if next_upgrade_protocol(value, &mut offset).is_none() {
            return false;
        }
        if offset == value.len() {
            return true;
        }
    }
}

pub(crate) fn next_upgrade_protocol<'a>(value: &'a [u8], offset: &mut usize) -> Option<&'a [u8]> {
    while value.get(*offset).is_some_and(|byte| is_ows(*byte)) {
        *offset += 1;
    }
    let start = *offset;
    while value.get(*offset).is_some_and(|byte| is_token(*byte)) {
        *offset += 1;
    }
    if start == *offset {
        return None;
    }
    if value.get(*offset) == Some(&b'/') {
        *offset += 1;
        let version_start = *offset;
        while value.get(*offset).is_some_and(|byte| is_token(*byte)) {
            *offset += 1;
        }
        if version_start == *offset {
            return None;
        }
    }
    let end = *offset;
    while value.get(*offset).is_some_and(|byte| is_ows(*byte)) {
        *offset += 1;
    }
    if *offset < value.len() {
        if value[*offset] != b',' {
            return None;
        }
        *offset += 1;
        if *offset == value.len() {
            return None;
        }
    }
    Some(&value[start..end])
}

pub(crate) fn upgrade_protocols_offered(response: &[u8], request: &[u8]) -> bool {
    if !valid_upgrade_field(request) {
        return false;
    }
    let mut response_offset = 0;
    while response_offset < response.len() {
        let Some(protocol) = next_upgrade_protocol(response, &mut response_offset) else {
            return false;
        };
        let mut request_offset = 0;
        let mut found = false;
        while request_offset < request.len() {
            let Some(offered) = next_upgrade_protocol(request, &mut request_offset) else {
                return false;
            };
            if same_protocol(protocol, offered) {
                found = true;
                break;
            }
        }
        if !found {
            return false;
        }
    }
    response_offset > 0
}

fn same_protocol(left: &[u8], right: &[u8]) -> bool {
    let left_separator = left.iter().position(|byte| *byte == b'/');
    let right_separator = right.iter().position(|byte| *byte == b'/');
    if left_separator != right_separator {
        return false;
    }
    match (left_separator, right_separator) {
        (Some(left_separator), Some(right_separator)) => {
            left[..left_separator].eq_ignore_ascii_case(&right[..right_separator])
                && left[left_separator + 1..] == right[right_separator + 1..]
        }
        (None, None) => left.eq_ignore_ascii_case(right),
        _ => false,
    }
}
