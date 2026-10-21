#[cfg(test)]
mod tests;

use crate::connection::H2FrameDriver;
use crate::hpack::HpackHeader;
use crate::stream::H2Role;
use std::net::Ipv6Addr;
use std::str::from_utf8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServerRequestError {
    MissingMethod,
    MissingScheme,
    MissingPath,
    MissingAuthority,
    DuplicatePseudoHeader,
    PseudoHeaderAfterRegular,
    UnsupportedPseudoHeader,
    InvalidMethod,
    InvalidScheme,
    InvalidAuthority,
    InvalidPath,
    AuthorityNotAllowed,
    InvalidFieldName,
    InvalidFieldValue,
    HostFieldUnsupported,
    ConnectionSpecificField,
    InvalidTe,
    InvalidContentLength,
    InvalidConnectPseudoHeaders,
    OptionsAsteriskOnly,
    InvalidHpackField,
    WrongRole,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValidatedServerRequest<'decoded> {
    pub method: &'decoded [u8],
    pub scheme: Option<&'decoded [u8]>,
    pub authority: Option<&'decoded [u8]>,
    pub path: Option<&'decoded [u8]>,
    pub content_length: Option<u64>,
}

pub fn validate_server_request_fields<'decoded>(
    driver: &'decoded H2FrameDriver<'_, '_, '_, '_>,
    fields: &[HpackHeader],
) -> Result<ValidatedServerRequest<'decoded>, ServerRequestError> {
    if driver.streams().role() != H2Role::Server {
        return Err(ServerRequestError::WrongRole);
    }

    let mut regular_fields_started = false;
    let mut method = None;
    let mut scheme = None;
    let mut authority = None;
    let mut path = None;
    let mut content_length = None;

    for field in fields {
        let (name, value) = driver
            .resolve_header(*field)
            .ok_or(ServerRequestError::InvalidHpackField)?;
        validate_field_value(value)?;

        if name.first() == Some(&b':') {
            if regular_fields_started {
                return Err(ServerRequestError::PseudoHeaderAfterRegular);
            }

            match name {
                b":method" => set_once(&mut method, value)?,
                b":scheme" => set_once(&mut scheme, value)?,
                b":authority" => set_once(&mut authority, value)?,
                b":path" => set_once(&mut path, value)?,
                _ => return Err(ServerRequestError::UnsupportedPseudoHeader),
            }

            continue;
        }

        regular_fields_started = true;

        if !valid_field_name(name) {
            return Err(ServerRequestError::InvalidFieldName);
        }

        if name == b"host" {
            return Err(ServerRequestError::HostFieldUnsupported);
        }

        if is_prohibited_connection_field(name) {
            return Err(ServerRequestError::ConnectionSpecificField);
        }

        if name == b"te" && !te_only_advertises_trailers(value) {
            return Err(ServerRequestError::InvalidTe);
        }

        if name == b"content-length" {
            let length =
                parse_content_length(value).ok_or(ServerRequestError::InvalidContentLength)?;

            if content_length.is_some_and(|previous| previous != length) {
                return Err(ServerRequestError::InvalidContentLength);
            }

            content_length = Some(length);
        }
    }

    let method = method.ok_or(ServerRequestError::MissingMethod)?;

    if !valid_token(method) {
        return Err(ServerRequestError::InvalidMethod);
    }

    if method == b"CONNECT" {
        if scheme.is_some() || path.is_some() {
            return Err(ServerRequestError::InvalidConnectPseudoHeaders);
        }

        let authority = authority.ok_or(ServerRequestError::MissingAuthority)?;

        if !valid_connect_authority(authority) {
            return Err(ServerRequestError::InvalidAuthority);
        }

        return Ok(ValidatedServerRequest {
            method,
            scheme: None,
            authority: Some(authority),
            path: None,
            content_length,
        });
    }

    let scheme = scheme.ok_or(ServerRequestError::MissingScheme)?;

    if !valid_scheme(scheme) {
        return Err(ServerRequestError::InvalidScheme);
    }

    let path = path.ok_or(ServerRequestError::MissingPath)?;

    if path == b"*" {
        if method != b"OPTIONS" {
            return Err(ServerRequestError::OptionsAsteriskOnly);
        }

        if authority.is_some() {
            return Err(ServerRequestError::AuthorityNotAllowed);
        }

        return Ok(ValidatedServerRequest {
            method,
            scheme: Some(scheme),
            authority: None,
            path: Some(path),
            content_length,
        });
    }

    if path.contains(&b'#') || (is_http_scheme(scheme) && !valid_origin_path(path)) {
        return Err(ServerRequestError::InvalidPath);
    }

    if let Some(authority) = authority {
        if !valid_uri_authority(authority, false, is_http_scheme(scheme), None) {
            return Err(ServerRequestError::InvalidAuthority);
        }
    } else if is_http_scheme(scheme) {
        return Err(ServerRequestError::MissingAuthority);
    }

    Ok(ValidatedServerRequest {
        method,
        scheme: Some(scheme),
        authority,
        path: Some(path),
        content_length,
    })
}

fn set_once<'decoded>(
    destination: &mut Option<&'decoded [u8]>,
    value: &'decoded [u8],
) -> Result<(), ServerRequestError> {
    if destination.is_some() {
        return Err(ServerRequestError::DuplicatePseudoHeader);
    }

    *destination = Some(value);

    Ok(())
}

fn parse_content_length(value: &[u8]) -> Option<u64> {
    if value.is_empty() {
        return None;
    }

    let mut length = 0_u64;

    for byte in value {
        if !byte.is_ascii_digit() {
            return None;
        }

        length = length
            .checked_mul(10)?
            .checked_add(u64::from(*byte - b'0'))?;
    }

    Some(length)
}

fn valid_field_name(name: &[u8]) -> bool {
    !name.is_empty()
        && name
            .iter()
            .copied()
            .all(|byte| is_token(byte) && !byte.is_ascii_uppercase())
}

fn valid_token(value: &[u8]) -> bool {
    !value.is_empty() && value.iter().copied().all(is_token)
}

fn is_token(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
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
        )
}

fn valid_scheme(scheme: &[u8]) -> bool {
    let Some(first) = scheme.first() else {
        return false;
    };

    if !first.is_ascii_alphabetic() {
        return false;
    }

    scheme[1..]
        .iter()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.'))
}

fn is_http_scheme(scheme: &[u8]) -> bool {
    scheme.eq_ignore_ascii_case(b"http") || scheme.eq_ignore_ascii_case(b"https")
}

fn valid_origin_path(path: &[u8]) -> bool {
    if path.first() != Some(&b'/') {
        return false;
    }

    let mut in_query = false;
    let mut offset = 0;

    while offset < path.len() {
        let byte = path[offset];

        if byte == b'?' {
            in_query = true;
            offset += 1;
            continue;
        }

        if byte == b'%' {
            if offset + 2 >= path.len()
                || !path[offset + 1].is_ascii_hexdigit()
                || !path[offset + 2].is_ascii_hexdigit()
            {
                return false;
            }

            offset += 3;
            continue;
        }

        let pchar = is_uri_unreserved_or_sub_delim(byte) || matches!(byte, b':' | b'@');

        if !pchar && byte != b'/' && !(in_query && byte == b'?') {
            return false;
        }

        offset += 1;
    }

    true
}

fn valid_connect_authority(authority: &[u8]) -> bool {
    valid_uri_authority(authority, true, true, Some(u16::MAX))
}

fn valid_uri_authority(
    authority: &[u8],
    require_port: bool,
    reject_userinfo: bool,
    maximum_port: Option<u16>,
) -> bool {
    if authority.is_empty() {
        return false;
    }

    let authority = if let Some(at) = authority.iter().position(|byte| *byte == b'@') {
        if reject_userinfo
            || authority[at + 1..].contains(&b'@')
            || !valid_userinfo(&authority[..at])
        {
            return false;
        }

        &authority[at + 1..]
    } else {
        authority
    };

    let (host, port) = if authority.first() == Some(&b'[') {
        let Some(closing_bracket) = authority.iter().position(|byte| *byte == b']') else {
            return false;
        };
        let host = &authority[..=closing_bracket];

        if !valid_ip_literal(host) {
            return false;
        }

        let remainder = &authority[closing_bracket + 1..];

        if remainder.is_empty() {
            (host, None)
        } else if remainder.first() == Some(&b':') {
            (host, Some(&remainder[1..]))
        } else {
            return false;
        }
    } else {
        if authority.contains(&b'[') || authority.contains(&b']') {
            return false;
        }

        let mut colons = authority
            .iter()
            .enumerate()
            .filter(|(_, byte)| **byte == b':');

        match (colons.next(), colons.next()) {
            (None, _) => (authority, None),
            (Some((index, _)), None) => (&authority[..index], Some(&authority[index + 1..])),
            _ => return false,
        }
    };

    if host.is_empty() || (host[0] != b'[' && !valid_reg_name(host)) {
        return false;
    }

    if require_port && port.is_none() {
        return false;
    }

    if let Some(port) = port {
        return valid_port(port, require_port, maximum_port);
    }

    true
}

fn valid_ip_literal(value: &[u8]) -> bool {
    if value.len() < 3 || value.first() != Some(&b'[') || value.last() != Some(&b']') {
        return false;
    }

    let literal = &value[1..value.len() - 1];

    if literal
        .first()
        .is_some_and(|byte| byte.eq_ignore_ascii_case(&b'v'))
    {
        return valid_ipv_future(literal);
    }

    from_utf8(literal)
        .ok()
        .and_then(|literal| literal.parse::<Ipv6Addr>().ok())
        .is_some()
}

fn valid_ipv_future(value: &[u8]) -> bool {
    let Some(dot) = value.iter().position(|byte| *byte == b'.') else {
        return false;
    };

    if dot < 2 || !value[1..dot].iter().all(u8::is_ascii_hexdigit) || dot + 1 == value.len() {
        return false;
    }

    value[dot + 1..]
        .iter()
        .copied()
        .all(is_uri_unreserved_or_sub_delim)
        || value[dot + 1..]
            .iter()
            .all(|byte| is_uri_unreserved_or_sub_delim(*byte) || *byte == b':')
}

fn valid_reg_name(value: &[u8]) -> bool {
    !value.is_empty() && valid_uri_component(value, false)
}

fn valid_userinfo(value: &[u8]) -> bool {
    valid_uri_component(value, true)
}

fn valid_uri_component(value: &[u8], allow_colon: bool) -> bool {
    let mut offset = 0;

    while offset < value.len() {
        let byte = value[offset];

        if byte == b'%' {
            if offset + 2 >= value.len()
                || !value[offset + 1].is_ascii_hexdigit()
                || !value[offset + 2].is_ascii_hexdigit()
            {
                return false;
            }

            offset += 3;
            continue;
        }

        if !is_uri_unreserved_or_sub_delim(byte) && !(allow_colon && byte == b':') {
            return false;
        }

        offset += 1;
    }

    true
}

fn is_uri_unreserved_or_sub_delim(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'-' | b'.'
                | b'_'
                | b'~'
                | b'!'
                | b'$'
                | b'&'
                | b'\''
                | b'('
                | b')'
                | b'*'
                | b'+'
                | b','
                | b';'
                | b'='
        )
}

fn valid_port(value: &[u8], required: bool, maximum: Option<u16>) -> bool {
    if value.is_empty() {
        return !required;
    }

    if !value.iter().all(u8::is_ascii_digit) {
        return false;
    }

    let Some(maximum) = maximum else {
        return true;
    };

    let mut port = 0_u32;

    for byte in value {
        let Some(next) = port
            .checked_mul(10)
            .and_then(|port| port.checked_add(u32::from(*byte - b'0')))
        else {
            return false;
        };

        port = next;

        if port > u32::from(maximum) {
            return false;
        }
    }

    true
}

fn validate_field_value(value: &[u8]) -> Result<(), ServerRequestError> {
    if value.first().is_some_and(is_ows) || value.last().is_some_and(is_ows) {
        return Err(ServerRequestError::InvalidFieldValue);
    }

    if value.iter().any(|byte| {
        *byte == 0
            || *byte == b'\r'
            || *byte == b'\n'
            || *byte == 0x7f
            || (*byte < 0x20 && *byte != b'\t')
    }) {
        return Err(ServerRequestError::InvalidFieldValue);
    }

    Ok(())
}

fn is_ows(byte: &u8) -> bool {
    *byte == b' ' || *byte == b'\t'
}

fn te_only_advertises_trailers(value: &[u8]) -> bool {
    let mut offset = 0;

    loop {
        while value.get(offset).is_some_and(is_ows) {
            offset += 1;
        }

        let start = offset;

        while value.get(offset).is_some_and(|byte| is_token(*byte)) {
            offset += 1;
        }

        if start == offset || !value[start..offset].eq_ignore_ascii_case(b"trailers") {
            return false;
        }

        while value.get(offset).is_some_and(is_ows) {
            offset += 1;
        }

        if offset == value.len() {
            return true;
        }

        if value[offset] != b',' {
            return false;
        }

        offset += 1;

        if offset == value.len() {
            return false;
        }
    }
}

fn is_prohibited_connection_field(name: &[u8]) -> bool {
    matches!(
        name,
        b"connection" | b"proxy-connection" | b"keep-alive" | b"transfer-encoding" | b"upgrade"
    )
}
