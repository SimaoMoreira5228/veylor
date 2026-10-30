use veylor_runtime::ConnectionToken;
use veylor_websocket::{accept_key, encode_server_accept};

use crate::connection::streams::{StreamCmd, StreamState};
use crate::host::staged::ParkedEntry;
use crate::host::{
    HOST_MAX_METHOD_BYTES, HOST_QUEUE_DEPTH, HostEvent, HostEventKind, HostInner, HostShared,
    RespondOutcome, TombCause, find_tomb,
};

pub const HOST_TUNNEL_CAP: usize = 64 * 1024;
pub const HOST_MAX_PROTOCOL_BYTES: usize = 128;
const UPGRADE_RESPONSE_CAP: usize = 320;

fn tunnel_entry(inner: &mut HostInner, token: ConnectionToken) -> Option<&mut ParkedEntry> {
    inner
        .parked
        .get_mut(token.worker_id as usize)
        .and_then(|table| table.get_mut(token.slot as usize))
        .and_then(|entry| entry.as_mut())
        .filter(|entry| entry.token == token)
}

fn missing_outcome(inner: &HostInner, token: ConnectionToken) -> RespondOutcome {
    match find_tomb(inner, token.worker_id as usize, token) {
        Some(TombCause::Responded) => RespondOutcome::Duplicate,
        Some(TombCause::Gone) => RespondOutcome::Gone,
        Some(TombCause::Cancelled) | None => RespondOutcome::Stale,
    }
}

fn is_token(value: &[u8]) -> bool {
    !value.is_empty()
        && value.iter().all(|byte| {
            matches!(
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
                    | b'0'..=b'9'
                    | b'A'..=b'Z'
                    | b'a'..=b'z'
            )
        })
}

fn has_token(value: &[u8], token: &[u8]) -> bool {
    value
        .split(|byte| *byte == b',')
        .any(|part| trim(part).eq_ignore_ascii_case(token))
}

fn offers_protocol(value: &[u8], protocol: &[u8]) -> bool {
    value
        .split(|byte| *byte == b',')
        .any(|part| trim(part) == protocol)
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

fn websocket_response(entry: &ParkedEntry, protocol: Option<&[u8]>) -> Option<Vec<u8>> {
    if &entry.method[..usize::from(entry.method_len)] != b"GET" {
        return None;
    }
    let request = entry.request.as_ref()?;

    let mut upgrade = false;
    let mut connection = false;
    let mut version = false;
    let mut offered = protocol.is_none();
    let mut accept = [0u8; 28];
    let mut key_seen = false;

    for (name, value) in request.fields() {
        if name.eq_ignore_ascii_case(b"upgrade") {
            if upgrade || !has_token(value, b"websocket") {
                return None;
            }
            upgrade = true;
        } else if name.eq_ignore_ascii_case(b"connection") {
            connection |= has_token(value, b"upgrade");
        } else if name.eq_ignore_ascii_case(b"sec-websocket-version") {
            if version || value != b"13" {
                return None;
            }
            version = true;
        } else if name.eq_ignore_ascii_case(b"sec-websocket-key") {
            if key_seen {
                return None;
            }
            accept_key(value, &mut accept).ok()?;
            key_seen = true;
        } else if name.eq_ignore_ascii_case(b"sec-websocket-protocol") {
            if let Some(protocol) = protocol {
                offered |= offers_protocol(value, protocol);
            }
        } else if name.eq_ignore_ascii_case(b"transfer-encoding")
            || (name.eq_ignore_ascii_case(b"content-length") && value != b"0")
        {
            return None;
        }
    }

    if !upgrade || !connection || !version || !key_seen || !offered {
        return None;
    }

    let mut buffer = [0u8; UPGRADE_RESPONSE_CAP];
    let length = encode_server_accept(&accept, protocol, &mut buffer).ok()?;
    Some(buffer[..length].to_vec())
}

impl HostShared {
    pub fn upgrade_websocket(&self, token: ConnectionToken) -> RespondOutcome {
        self.upgrade_websocket_with_protocol(token, None)
    }

    pub fn upgrade_websocket_with_protocol(
        &self,
        token: ConnectionToken,
        protocol: Option<&[u8]>,
    ) -> RespondOutcome {
        if protocol
            .is_some_and(|protocol| protocol.len() > HOST_MAX_PROTOCOL_BYTES || !is_token(protocol))
        {
            return RespondOutcome::Invalid;
        }
        let Some((result, waker)) = self.with_inner(|inner| {
            let Some(entry) = tunnel_entry(inner, token) else {
                return (missing_outcome(inner, token), None);
            };
            if entry.staged.is_some() || entry.stream.is_some() {
                return (RespondOutcome::Duplicate, None);
            }
            let Some(response) = websocket_response(entry, protocol) else {
                return (RespondOutcome::Invalid, None);
            };
            entry.stream = Some(StreamState {
                upgraded: true,
                ..StreamState::default()
            });
            entry.stream_cmd = Some(StreamCmd::Upgrade { response });
            let waker = inner
                .wakers
                .get(token.worker_id as usize)
                .and_then(Clone::clone);
            (RespondOutcome::Ok, waker)
        }) else {
            return RespondOutcome::Stale;
        };
        if result == RespondOutcome::Ok {
            if let Some(waker) = waker {
                let _ = waker.wake();
            }
        }
        result
    }

    pub fn tunnel_read(&self, token: ConnectionToken, out: &mut [u8]) -> (RespondOutcome, usize) {
        let Some(((outcome, take), waker)) = self.with_inner(|inner| {
            let (outcome, take) = {
                let Some(entry) = tunnel_entry(inner, token) else {
                    return ((missing_outcome(inner, token), 0), None);
                };
                let available = entry.tunnel_in.len() - entry.tunnel_in_start;
                let take = out.len().min(available);
                out[..take].copy_from_slice(
                    &entry.tunnel_in[entry.tunnel_in_start..entry.tunnel_in_start + take],
                );
                entry.tunnel_in_start += take;
                if entry.tunnel_in_start == entry.tunnel_in.len() {
                    entry.tunnel_in.clear();
                    entry.tunnel_in_start = 0;
                }
                let outcome = if take == 0 {
                    RespondOutcome::WouldBlock
                } else {
                    RespondOutcome::Ok
                };
                (outcome, take)
            };
            let waker = if take > 0 {
                inner
                    .wakers
                    .get(token.worker_id as usize)
                    .and_then(Clone::clone)
            } else {
                None
            };
            ((outcome, take), waker)
        }) else {
            return (RespondOutcome::Stale, 0);
        };
        if let Some(waker) = waker {
            let _ = waker.wake();
        }
        (outcome, take)
    }

    pub fn tunnel_write(&self, token: ConnectionToken, bytes: &[u8]) -> RespondOutcome {
        if bytes.is_empty() {
            return RespondOutcome::Invalid;
        }
        let Some((result, waker)) = self.with_inner(|inner| {
            let Some(entry) = tunnel_entry(inner, token) else {
                return (missing_outcome(inner, token), None);
            };
            if entry.tunnel_close {
                return (RespondOutcome::Gone, None);
            }
            let available = entry.tunnel_out.len() - entry.tunnel_out_start;
            if available.saturating_add(bytes.len()) > HOST_TUNNEL_CAP {
                return (RespondOutcome::WouldBlock, None);
            }
            if entry.tunnel_out.len().saturating_add(bytes.len()) > HOST_TUNNEL_CAP {
                entry.tunnel_out.drain(..entry.tunnel_out_start);
                entry.tunnel_out_start = 0;
            }
            entry.tunnel_out.extend_from_slice(bytes);
            let waker = inner
                .wakers
                .get(token.worker_id as usize)
                .and_then(Clone::clone);
            (RespondOutcome::Ok, waker)
        }) else {
            return RespondOutcome::Stale;
        };
        if result == RespondOutcome::Ok {
            if let Some(waker) = waker {
                let _ = waker.wake();
            }
        }
        result
    }

    pub fn tunnel_close(&self, token: ConnectionToken) -> RespondOutcome {
        let Some((result, waker)) = self.with_inner(|inner| {
            let Some(entry) = tunnel_entry(inner, token) else {
                return (missing_outcome(inner, token), None);
            };
            entry.tunnel_close = true;
            let waker = inner
                .wakers
                .get(token.worker_id as usize)
                .and_then(Clone::clone);
            (RespondOutcome::Ok, waker)
        }) else {
            return RespondOutcome::Stale;
        };
        if result == RespondOutcome::Ok {
            if let Some(waker) = waker {
                let _ = waker.wake();
            }
        }
        result
    }

    pub(crate) fn push_tunnel_in(&self, token: ConnectionToken, bytes: &[u8]) -> usize {
        let take = self
            .with_inner(|inner| {
                let Some(entry) = tunnel_entry(inner, token) else {
                    return (0, None);
                };
                let available = entry.tunnel_in.len() - entry.tunnel_in_start;
                let room = HOST_TUNNEL_CAP.saturating_sub(available);
                let take = room.min(bytes.len());
                if take == 0 {
                    return (0, None);
                }
                if entry.tunnel_in.len().saturating_add(take) > HOST_TUNNEL_CAP {
                    entry.tunnel_in.drain(..entry.tunnel_in_start);
                    entry.tunnel_in_start = 0;
                }
                entry.tunnel_in.extend_from_slice(&bytes[..take]);
                (take, None)
            })
            .map(|(take, _)| take)
            .unwrap_or(0);
        if take > 0 {
            self.stage_tunnel_data(token);
        }
        take
    }

    pub(crate) fn take_tunnel_out(&self, token: ConnectionToken, out: &mut [u8]) -> usize {
        self.with_inner(|inner| {
            let Some(entry) = tunnel_entry(inner, token) else {
                return (0, None);
            };
            let available = entry.tunnel_out.len() - entry.tunnel_out_start;
            let take = out.len().min(available);
            out[..take].copy_from_slice(
                &entry.tunnel_out[entry.tunnel_out_start..entry.tunnel_out_start + take],
            );
            entry.tunnel_out_start += take;
            if entry.tunnel_out_start == entry.tunnel_out.len() {
                entry.tunnel_out.clear();
                entry.tunnel_out_start = 0;
            }
            (take, None)
        })
        .map(|(take, _)| take)
        .unwrap_or(0)
    }

    pub(crate) fn mark_tunnel_eof(&self, token: ConnectionToken) {
        self.with_inner(|inner| {
            if let Some(entry) = tunnel_entry(inner, token) {
                entry.tunnel_eof = true;
            }
            ((), None)
        });
    }

    pub(crate) fn tunnel_input_done(&self, token: ConnectionToken) -> bool {
        self.with_inner(|inner| {
            let done = tunnel_entry(inner, token).is_some_and(|entry| {
                entry.tunnel_eof && entry.tunnel_in_start == entry.tunnel_in.len()
            });
            (done, None)
        })
        .is_some_and(|(done, _)| done)
    }

    pub(crate) fn tunnel_close_requested(&self, token: ConnectionToken) -> bool {
        self.with_inner(|inner| {
            let closed = tunnel_entry(inner, token).is_none_or(|entry| entry.tunnel_close);
            (closed, None)
        })
        .map(|(closed, _)| closed)
        .unwrap_or(true)
    }

    pub(crate) fn stage_tunnel_data(&self, token: ConnectionToken) {
        self.with_inner(|inner| {
            let worker = token.worker_id as usize;
            let Some(queue) = inner.events.get_mut(worker) else {
                return ((), None);
            };
            if queue
                .iter()
                .any(|event| event.kind == HostEventKind::TunnelData && event.token == token)
            {
                return ((), None);
            }
            if queue.len() >= HOST_QUEUE_DEPTH {
                inner.counters.queue_full += 1;
                return ((), None);
            }
            queue.push_back(HostEvent {
                kind: HostEventKind::TunnelData,
                token,
                route_id: 0,
                method_len: 0,
                method: [0; HOST_MAX_METHOD_BYTES],
                path_len: 0,
                path: [0; crate::host::HOST_MAX_PATH_BYTES],
                body_ready: 0,
                body_complete: false,
            });
            ((), None)
        });
    }

    pub(crate) fn stage_tunnel_closed(&self, token: ConnectionToken) -> bool {
        self.with_inner(|inner| {
            let worker = token.worker_id as usize;
            let Some(queue) = inner.events.get_mut(worker) else {
                return (false, None);
            };
            if queue
                .iter()
                .any(|event| event.kind == HostEventKind::TunnelClosed && event.token == token)
            {
                return (true, None);
            }
            if queue.len() >= HOST_QUEUE_DEPTH {
                inner.counters.queue_full += 1;
                return (false, None);
            }
            queue.push_back(HostEvent {
                kind: HostEventKind::TunnelClosed,
                token,
                route_id: 0,
                method_len: 0,
                method: [0; HOST_MAX_METHOD_BYTES],
                path_len: 0,
                path: [0; crate::host::HOST_MAX_PATH_BYTES],
                body_ready: 0,
                body_complete: false,
            });
            (true, None)
        })
        .is_some_and(|(staged, _)| staged)
    }
}

impl crate::host::HostHandle {
    pub fn upgrade_websocket(&self, token: ConnectionToken) -> RespondOutcome {
        self.shared.upgrade_websocket(token)
    }

    pub fn upgrade_websocket_with_protocol(
        &self,
        token: ConnectionToken,
        protocol: &[u8],
    ) -> RespondOutcome {
        self.shared
            .upgrade_websocket_with_protocol(token, Some(protocol))
    }

    pub fn tunnel_read(&self, token: ConnectionToken, out: &mut [u8]) -> (RespondOutcome, usize) {
        self.shared.tunnel_read(token, out)
    }

    pub fn tunnel_write(&self, token: ConnectionToken, bytes: &[u8]) -> RespondOutcome {
        self.shared.tunnel_write(token, bytes)
    }

    pub fn tunnel_close(&self, token: ConnectionToken) -> RespondOutcome {
        self.shared.tunnel_close(token)
    }
}
