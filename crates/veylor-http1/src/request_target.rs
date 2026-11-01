use crate::Span;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TargetForm {
    Origin,
    Absolute,
    Authority,
    Asterisk,
}

pub(crate) fn classify(method: &[u8], target: &[u8]) -> Option<(TargetForm, Span)> {
    if target.is_empty() {
        return None;
    }

    if target == b"*" {
        return (method == b"OPTIONS").then_some((TargetForm::Asterisk, Span::default()));
    }

    if method == b"CONNECT" {
        let authority = valid_authority(target, true)?;
        return Some((
            TargetForm::Authority,
            Span {
                offset: authority,
                length: target.len() - authority,
            },
        ));
    }

    if target[0] == b'/' {
        return valid_uri_component(target).then_some((TargetForm::Origin, Span::default()));
    }

    let colon = target.iter().position(|byte| *byte == b':')?;
    if colon == 0
        || !target[..colon]
            .iter()
            .enumerate()
            .all(|(index, byte)| is_scheme_byte(*byte, index == 0))
        || target.get(colon + 1..colon + 3)? != b"//"
    {
        return None;
    }

    let authority_start = colon + 3;
    let authority_end = target[authority_start..]
        .iter()
        .position(|byte| *byte == b'/' || *byte == b'?')
        .map_or(target.len(), |offset| authority_start + offset);
    let authority = &target[authority_start..authority_end];
    let host_offset = valid_authority(authority, false)?;
    if !valid_uri_component(&target[authority_end..]) {
        return None;
    }

    Some((
        TargetForm::Absolute,
        Span {
            offset: authority_start + host_offset,
            length: authority.len() - host_offset,
        },
    ))
}

pub(crate) fn valid_host_field(value: &[u8]) -> bool {
    value.is_empty() || (!value.contains(&b'@') && valid_authority(value, false).is_some())
}

pub(crate) fn valid_uri_component(bytes: &[u8]) -> bool {
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == b'%' {
            if !has_percent_escape(bytes, index) {
                return false;
            }
            index += 3;
            continue;
        }

        if !is_unreserved(byte)
            && !is_sub_delimiter(byte)
            && !matches!(byte, b':' | b'@' | b'/' | b'?')
        {
            return false;
        }
        index += 1;
    }
    true
}

fn valid_authority(authority: &[u8], require_port: bool) -> Option<usize> {
    if !require_port {
        let mut colon = None;
        let mut special = false;
        for (index, byte) in authority.iter().enumerate() {
            match *byte {
                b':' => {
                    if colon.replace(index).is_some() {
                        special = true;
                    }
                }
                b'@' | b'[' => special = true,
                _ => {}
            }
        }
        if !special {
            let (host, port) = match colon {
                Some(colon) => (&authority[..colon], Some(&authority[colon + 1..])),
                None => (authority, None),
            };
            if host.is_empty() {
                return None;
            }
            if let Some(port) = port {
                if port.is_empty() {
                    return None;
                }
                let mut value = 0_u32;
                for digit in port {
                    if !digit.is_ascii_digit() {
                        return None;
                    }
                    value = value
                        .checked_mul(10)?
                        .checked_add(u32::from(*digit - b'0'))?;
                }
            }
            return Some(0);
        }
    }
    let userinfo_end = if require_port {
        None
    } else {
        authority.iter().position(|byte| *byte == b'@')
    };
    let host_start = userinfo_end.map_or(0, |offset| offset + 1);
    if userinfo_end == Some(0) || host_start >= authority.len() {
        return None;
    }

    if let Some(userinfo_end) = userinfo_end {
        let userinfo = &authority[..userinfo_end];
        if !valid_userinfo(userinfo) {
            return None;
        }
    }

    let host_port = &authority[host_start..];
    let (host, port) = parse_host_port(host_port)?;
    if host.is_empty() || (require_port && port.is_none()) {
        return None;
    }
    if let Some(port) = port {
        if port.is_empty() || !port.iter().all(u8::is_ascii_digit) {
            return None;
        }
        let mut value = 0_u32;
        for digit in port {
            value = value
                .checked_mul(10)?
                .checked_add(u32::from(*digit - b'0'))?;
            if require_port && value > u16::MAX.into() {
                return None;
            }
        }
    }

    Some(host_start)
}

fn parse_host_port(value: &[u8]) -> Option<(&[u8], Option<&[u8]>)> {
    if value[0] == b'[' {
        let close = value.iter().position(|byte| *byte == b']')?;
        if close <= 1 || !valid_ip_literal(&value[1..close]) {
            return None;
        }
        let host = &value[..close + 1];
        return match value.get(close + 1..) {
            Some([]) => Some((host, None)),
            Some([b':', port @ ..]) => Some((host, Some(port))),
            _ => None,
        };
    }

    let mut colon = None;
    for (index, byte) in value.iter().enumerate() {
        if *byte == b':' {
            if colon.replace(index).is_some() {
                return None;
            }
            continue;
        }
        if *byte == b'%' {
            if !has_percent_escape(value, index) {
                return None;
            }
            continue;
        }
        if !is_unreserved(*byte) && !is_sub_delimiter(*byte) {
            return None;
        }
    }

    match colon {
        Some(index) => Some((&value[..index], Some(&value[index + 1..]))),
        None => Some((value, None)),
    }
}

fn valid_ip_literal(value: &[u8]) -> bool {
    if value
        .first()
        .is_some_and(|byte| matches!(byte, b'v' | b'V'))
    {
        let Some(dot) = value.iter().position(|byte| *byte == b'.') else {
            return false;
        };
        return dot > 1
            && dot + 1 < value.len()
            && value[1..dot].iter().all(u8::is_ascii_hexdigit)
            && value[dot + 1..]
                .iter()
                .all(|byte| is_unreserved(*byte) || is_sub_delimiter(*byte) || *byte == b':');
    }

    std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse::<std::net::Ipv6Addr>().ok())
        .is_some()
}

fn valid_userinfo(value: &[u8]) -> bool {
    let mut index = 0;
    while index < value.len() {
        let byte = value[index];
        if byte == b'%' {
            if !has_percent_escape(value, index) {
                return false;
            }
            index += 3;
            continue;
        }
        if !is_unreserved(byte) && !is_sub_delimiter(byte) && byte != b':' {
            return false;
        }
        index += 1;
    }
    true
}

fn has_percent_escape(bytes: &[u8], index: usize) -> bool {
    bytes
        .get(index + 1..index + 3)
        .is_some_and(|digits| digits.iter().all(u8::is_ascii_hexdigit))
}

fn is_scheme_byte(byte: u8, first: bool) -> bool {
    byte.is_ascii_alphabetic()
        || (!first && (byte.is_ascii_digit() || matches!(byte, b'+' | b'-' | b'.')))
}

fn is_unreserved(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')
}

fn is_sub_delimiter(byte: u8) -> bool {
    matches!(
        byte,
        b'!' | b'$' | b'&' | b'\'' | b'(' | b')' | b'*' | b'+' | b',' | b';' | b'='
    )
}

#[cfg(test)]
mod tests {
    use crate::Span;

    use super::{TargetForm, classify};

    #[test]
    fn classifies_request_target_forms_and_authority_spans() {
        assert_eq!(
            classify(b"GET", b"/items?q=1"),
            Some((TargetForm::Origin, Span::default()))
        );
        assert_eq!(
            classify(b"OPTIONS", b"*"),
            Some((TargetForm::Asterisk, Span::default()))
        );
        assert_eq!(classify(b"GET", b"*"), None);
        assert_eq!(
            classify(b"CONNECT", b"example.test:443"),
            Some((
                TargetForm::Authority,
                Span {
                    offset: 0,
                    length: 16,
                }
            ))
        );
        assert_eq!(classify(b"CONNECT", b"example.test:70000"), None);
        assert_eq!(
            classify(b"GET", b"http://user:pass@example.test/a"),
            Some((
                TargetForm::Absolute,
                Span {
                    offset: 17,
                    length: 12,
                }
            ))
        );
    }

    #[test]
    fn rejects_malformed_request_targets() {
        for target in [
            &b"/bad path"[..],
            &b"/bad%2"[..],
            &b"/fragment#x"[..],
            &b"http:///missing-host"[..],
            &b"http://example.test/%xx"[..],
            &b"example.test"[..],
        ] {
            assert_eq!(classify(b"GET", target), None);
        }
    }
}
