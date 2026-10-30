use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use sha1::{Digest, Sha1};
use veylor_http1::{Header, RequestHead, RequestVersion, ResponseHead, ResponseKind, Span};

const GUID: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const ACCEPT_BYTES: usize = 28;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HandshakeError {
    InvalidMethod,
    InvalidVersion,
    MissingHost,
    MissingUpgrade,
    DuplicateUpgrade,
    MissingConnectionUpgrade,
    MissingKey,
    DuplicateKey,
    InvalidKey,
    MissingVersion,
    DuplicateVersion,
    UnsupportedVersion,
    BodyNotAllowed,
    NotSwitchingProtocols,
    MissingAccept,
    DuplicateAccept,
    DuplicateProtocol,
    MalformedField,
    AcceptMismatch,
    UnexpectedExtensions,
    InvalidPath,
    BufferTooSmall { required: usize },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServerHandshake<'a> {
    accept: [u8; ACCEPT_BYTES],
    protocol: Option<&'a [u8]>,
}

impl<'a> ServerHandshake<'a> {
    pub const fn accept(&self) -> &[u8; ACCEPT_BYTES] {
        &self.accept
    }

    pub const fn protocol(&self) -> Option<&'a [u8]> {
        self.protocol
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientHandshake<'a> {
    protocol: Option<&'a [u8]>,
}

impl<'a> ClientHandshake<'a> {
    pub const fn protocol(&self) -> Option<&'a [u8]> {
        self.protocol
    }
}

pub fn server_accept<'a>(
    request: RequestHead,
    input: &'a [u8],
    headers: &[Header],
) -> Result<ServerHandshake<'a>, HandshakeError> {
    if request.method(input) != Some(b"GET") {
        return Err(HandshakeError::InvalidMethod);
    }
    if request.protocol_version(input) != Some(RequestVersion::Http11) {
        return Err(HandshakeError::InvalidVersion);
    }

    let mut host = false;
    let mut upgrade = false;
    let mut connection_upgrade = false;
    let mut version = false;
    let mut key = false;
    let mut protocol = None;
    let mut accept = [0u8; ACCEPT_BYTES];

    for header in &headers[..request.header_count] {
        let name = field(input, header.name).ok_or(HandshakeError::MalformedField)?;
        let value = field(input, header.value).ok_or(HandshakeError::MalformedField)?;

        if name.eq_ignore_ascii_case(b"host") {
            host = !value.is_empty();
        } else if name.eq_ignore_ascii_case(b"upgrade") {
            if upgrade {
                return Err(HandshakeError::DuplicateUpgrade);
            }
            if !has_token(value, b"websocket") {
                return Err(HandshakeError::MissingUpgrade);
            }
            upgrade = true;
        } else if name.eq_ignore_ascii_case(b"connection") {
            connection_upgrade |= has_token(value, b"upgrade");
        } else if name.eq_ignore_ascii_case(b"sec-websocket-version") {
            if version {
                return Err(HandshakeError::DuplicateVersion);
            }
            if value != b"13" {
                return Err(HandshakeError::UnsupportedVersion);
            }
            version = true;
        } else if name.eq_ignore_ascii_case(b"sec-websocket-key") {
            if key {
                return Err(HandshakeError::DuplicateKey);
            }
            accept_key(value, &mut accept)?;
            key = true;
        } else if name.eq_ignore_ascii_case(b"sec-websocket-protocol") {
            if protocol.is_some() {
                return Err(HandshakeError::DuplicateProtocol);
            }
            protocol = Some(value);
        } else if name.eq_ignore_ascii_case(b"content-length") {
            if value != b"0" {
                return Err(HandshakeError::BodyNotAllowed);
            }
        } else if name.eq_ignore_ascii_case(b"transfer-encoding") {
            return Err(HandshakeError::BodyNotAllowed);
        }
    }

    if !host {
        return Err(HandshakeError::MissingHost);
    }
    if !upgrade {
        return Err(HandshakeError::MissingUpgrade);
    }
    if !connection_upgrade {
        return Err(HandshakeError::MissingConnectionUpgrade);
    }
    if !version {
        return Err(HandshakeError::MissingVersion);
    }
    if !key {
        return Err(HandshakeError::MissingKey);
    }

    Ok(ServerHandshake { accept, protocol })
}

pub fn client_accept<'a>(
    response: ResponseHead,
    input: &'a [u8],
    headers: &[Header],
    expected_accept: &[u8; ACCEPT_BYTES],
) -> Result<ClientHandshake<'a>, HandshakeError> {
    if response.status != 101 || response.kind != ResponseKind::Upgrade {
        return Err(HandshakeError::NotSwitchingProtocols);
    }
    if response.version(input) != Some(&b"HTTP/1.1"[..]) {
        return Err(HandshakeError::InvalidVersion);
    }

    let mut upgrade = false;
    let mut connection_upgrade = false;
    let mut accept = false;
    let mut protocol = None;

    for header in &headers[..response.header_count] {
        let name = field(input, header.name).ok_or(HandshakeError::MalformedField)?;
        let value = field(input, header.value).ok_or(HandshakeError::MalformedField)?;

        if name.eq_ignore_ascii_case(b"upgrade") {
            if upgrade {
                return Err(HandshakeError::DuplicateUpgrade);
            }
            if !has_token(value, b"websocket") {
                return Err(HandshakeError::MissingUpgrade);
            }
            upgrade = true;
        } else if name.eq_ignore_ascii_case(b"connection") {
            connection_upgrade |= has_token(value, b"upgrade");
        } else if name.eq_ignore_ascii_case(b"sec-websocket-accept") {
            if accept {
                return Err(HandshakeError::DuplicateAccept);
            }
            if value != expected_accept {
                return Err(HandshakeError::AcceptMismatch);
            }
            accept = true;
        } else if name.eq_ignore_ascii_case(b"sec-websocket-protocol") {
            if protocol.is_some() {
                return Err(HandshakeError::DuplicateProtocol);
            }
            protocol = Some(value);
        } else if name.eq_ignore_ascii_case(b"sec-websocket-extensions") {
            return Err(HandshakeError::UnexpectedExtensions);
        }
    }

    if !upgrade {
        return Err(HandshakeError::MissingUpgrade);
    }
    if !connection_upgrade {
        return Err(HandshakeError::MissingConnectionUpgrade);
    }
    if !accept {
        return Err(HandshakeError::MissingAccept);
    }

    Ok(ClientHandshake { protocol })
}

pub fn accept_key(key: &[u8], out: &mut [u8; ACCEPT_BYTES]) -> Result<(), HandshakeError> {
    let mut decoded = [0u8; 24];
    let length = STANDARD
        .decode_slice(key, &mut decoded)
        .map_err(|_| HandshakeError::InvalidKey)?;
    if length != 16 {
        return Err(HandshakeError::InvalidKey);
    }

    let mut hasher = Sha1::new();
    hasher.update(key);
    hasher.update(GUID);
    let digest = hasher.finalize();
    STANDARD
        .encode_slice(digest, out)
        .map_err(|_| HandshakeError::InvalidKey)?;
    Ok(())
}

pub fn encode_server_handshake(
    handshake: &ServerHandshake<'_>,
    out: &mut [u8],
) -> Result<usize, HandshakeError> {
    encode_server_accept(handshake.accept(), handshake.protocol(), out)
}

pub fn encode_server_accept(
    accept: &[u8],
    protocol: Option<&[u8]>,
    out: &mut [u8],
) -> Result<usize, HandshakeError> {
    let mut required = 0;
    required += b"HTTP/1.1 101 Switching Protocols\r\n".len();
    required += b"Upgrade: websocket\r\n".len();
    required += b"Connection: Upgrade\r\n".len();
    required += b"Sec-WebSocket-Accept: ".len() + ACCEPT_BYTES + 2;
    if let Some(protocol) = protocol {
        required += b"Sec-WebSocket-Protocol: ".len() + protocol.len() + 2;
    }
    required += 2;
    if out.len() < required {
        return Err(HandshakeError::BufferTooSmall { required });
    }

    let mut cursor = 0;
    cursor = write(out, cursor, b"HTTP/1.1 101 Switching Protocols\r\n");
    cursor = write(out, cursor, b"Upgrade: websocket\r\n");
    cursor = write(out, cursor, b"Connection: Upgrade\r\n");
    cursor = write(out, cursor, b"Sec-WebSocket-Accept: ");
    cursor = write(out, cursor, accept);
    cursor = write(out, cursor, b"\r\n");
    if let Some(protocol) = protocol {
        cursor = write(out, cursor, b"Sec-WebSocket-Protocol: ");
        cursor = write(out, cursor, protocol);
        cursor = write(out, cursor, b"\r\n");
    }
    cursor = write(out, cursor, b"\r\n");
    Ok(cursor)
}

pub fn encode_client_handshake(
    host: &[u8],
    path: &[u8],
    key: &[u8],
    protocols: &[&[u8]],
    out: &mut [u8],
) -> Result<usize, HandshakeError> {
    if host.is_empty() {
        return Err(HandshakeError::MissingHost);
    }
    if !path.starts_with(b"/") {
        return Err(HandshakeError::InvalidPath);
    }
    let mut decoded = [0u8; 24];
    let length = STANDARD
        .decode_slice(key, &mut decoded)
        .map_err(|_| HandshakeError::InvalidKey)?;
    if length != 16 {
        return Err(HandshakeError::InvalidKey);
    }

    let mut required = 0;
    required += b"GET ".len() + path.len() + b" HTTP/1.1\r\n".len();
    required += b"Host: ".len() + host.len() + 2;
    required += b"Upgrade: websocket\r\n".len();
    required += b"Connection: Upgrade\r\n".len();
    required += b"Sec-WebSocket-Key: ".len() + key.len() + 2;
    required += b"Sec-WebSocket-Version: 13\r\n".len();
    if !protocols.is_empty() {
        required += b"Sec-WebSocket-Protocol: ".len();
        for (index, protocol) in protocols.iter().enumerate() {
            if index != 0 {
                required += 2;
            }
            required += protocol.len();
        }
        required += 2;
    }
    required += 2;
    if out.len() < required {
        return Err(HandshakeError::BufferTooSmall { required });
    }

    let mut cursor = 0;
    cursor = write(out, cursor, b"GET ");
    cursor = write(out, cursor, path);
    cursor = write(out, cursor, b" HTTP/1.1\r\n");
    cursor = write(out, cursor, b"Host: ");
    cursor = write(out, cursor, host);
    cursor = write(out, cursor, b"\r\n");
    cursor = write(out, cursor, b"Upgrade: websocket\r\n");
    cursor = write(out, cursor, b"Connection: Upgrade\r\n");
    cursor = write(out, cursor, b"Sec-WebSocket-Key: ");
    cursor = write(out, cursor, key);
    cursor = write(out, cursor, b"\r\n");
    cursor = write(out, cursor, b"Sec-WebSocket-Version: 13\r\n");
    if !protocols.is_empty() {
        cursor = write(out, cursor, b"Sec-WebSocket-Protocol: ");
        for (index, protocol) in protocols.iter().enumerate() {
            if index != 0 {
                cursor = write(out, cursor, b", ");
            }
            cursor = write(out, cursor, protocol);
        }
        cursor = write(out, cursor, b"\r\n");
    }
    cursor = write(out, cursor, b"\r\n");
    Ok(cursor)
}

fn field(input: &[u8], span: Span) -> Option<&[u8]> {
    input.get(span.offset..span.offset.checked_add(span.length)?)
}

fn has_token(value: &[u8], token: &[u8]) -> bool {
    value
        .split(|byte| *byte == b',')
        .any(|part| trim(part).eq_ignore_ascii_case(token))
}

fn trim(value: &[u8]) -> &[u8] {
    let start = value
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(value.len());
    let end = value
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
        .map_or(start, |index| index + 1);
    &value[start..end]
}

fn write(out: &mut [u8], cursor: usize, bytes: &[u8]) -> usize {
    let end = cursor + bytes.len();
    out[cursor..end].copy_from_slice(bytes);
    end
}
