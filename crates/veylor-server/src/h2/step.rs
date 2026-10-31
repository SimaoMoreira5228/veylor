use veylor_http2::{
    DataWriteError, ErrorCode, H2ServerDriver, HeadersFrameWriter, ServerError, StreamState,
    encode_header_block,
};
use veylor_runtime::ConnectionToken;

use crate::connection::parked::TRAILER_SLOTS;
use crate::connection::streams::{StreamCmd, take_stream_cmd};
use crate::host::HostShared;

use super::routes::project_extra_headers;
use super::send::{field, send_begin};
use super::serve::valid_trailer_field;
use super::{H2Conn, Phase, STREAM_SLOTS, TUNNEL_STEP};
use super::{slots, turn};

#[derive(Clone, Copy)]
enum StreamStep {
    Done {
        used: usize,
        served: bool,
        refused: bool,
    },
    Dead {
        used: usize,
    },
}

fn abort_stream(
    server: &mut H2ServerDriver<'_, '_, '_, '_>,
    host: &HostShared,
    token: ConnectionToken,
    code: ErrorCode,
    out: &mut [u8],
) -> StreamStep {
    let used = match server.rst_stream(token.stream_id, code, out) {
        Ok(writer) => writer.pending_bytes().len(),
        Err(_) => 0,
    };
    host.unpark_closed(token.worker_id, token.slot);
    StreamStep::Dead { used }
}

fn is_head_park(host: &HostShared, token: ConnectionToken) -> bool {
    let gated = host.with_inner(|inner| {
        let table = inner.parked.get(token.worker_id as usize);
        let slot = table.and_then(|table| table.get(token.slot as usize));
        let live = slot.and_then(|slot| slot.as_ref());
        let head = live.is_some_and(|entry| {
            entry.token == token
                && entry.method[..usize::from(entry.method_len)].eq_ignore_ascii_case(b"HEAD")
        });
        (head, None)
    });
    gated.is_some_and(|(head, _)| head)
}

#[allow(clippy::too_many_arguments)]
fn tunnel_step(
    meta: &mut slots::H2Meta,
    server: &mut H2ServerDriver<'_, '_, '_, '_>,
    host: &HostShared,
    worker: u32,
    base: u32,
    scratch: &mut [u8; TUNNEL_STEP],
) -> bool {
    let start = usize::from(meta.tunnel_cursor) % STREAM_SLOTS;

    for offset in 0..STREAM_SLOTS {
        let sub = (start + offset) % STREAM_SLOTS;
        let slot = base + sub as u32;
        let info = host.with_inner(|inner| {
            let entry = inner
                .parked
                .get_mut(worker as usize)
                .and_then(|table| table.get_mut(slot as usize))
                .and_then(|entry| entry.as_mut())
                .filter(|entry| {
                    entry.token.stream_id != 0
                        && entry.stream_cmd.is_none()
                        && entry
                            .stream
                            .as_ref()
                            .is_some_and(|stream| stream.upgraded && stream.head_sent)
                });
            let Some(entry) = entry else {
                return (None, None);
            };
            let grant = std::mem::take(&mut entry.tunnel_grant);
            let out = entry.tunnel_out.len() - entry.tunnel_out_start;
            (Some((entry.token, grant, entry.tunnel_close, out)), None)
        });
        let Some((token, grant, close, out)) = info.and_then(|(info, _)| info) else {
            continue;
        };
        let stream = token.stream_id;
        meta.tunnel_cursor = ((sub + 1) % STREAM_SLOTS) as u8;
        let mut used = 0;

        if grant > 0 {
            let spare = slots::pending_spare(meta);
            if spare.len() >= 26 {
                used += super::grant_window(server, 0, grant, &mut spare[used..]);
                used += super::grant_window(server, stream, grant, &mut spare[used..]);
            }
        }

        if out > 0 {
            let spare = slots::pending_spare(meta);
            let room = spare.len().saturating_sub(used);
            if room < 10 {
                meta.pending_len += used;
                return true;
            }
            let want = out.min(scratch.len()).min(room - 9);
            let chunk = host.peek_tunnel_out(token, &mut scratch[..want]);
            let mut sent = 0;

            while sent < chunk {
                match server.send_data(stream, &scratch[sent..chunk], false, &mut spare[used..]) {
                    Ok(frame) => {
                        let payload = frame.frame.payload_length;
                        used += frame.writer.pending_bytes().len();
                        sent += payload;
                        if payload == 0 {
                            break;
                        }
                    }
                    Err(ServerError::Data(DataWriteError::NoSendCredit)) => break,
                    Err(ServerError::StreamClosed | ServerError::UnknownStream) => {
                        host.unpark_closed(token.worker_id, token.slot);
                        meta.pending_len += used;
                        return true;
                    }
                    Err(_) => break,
                }
            }

            if sent > 0 {
                host.consume_tunnel_out(token, sent);
                host.stage_writable(token);
            }
            if sent < out {
                meta.pending_len += used;
                return true;
            }
        }

        if close {
            let spare = slots::pending_spare(meta);
            if spare.len().saturating_sub(used) < 9 {
                meta.pending_len += used;
                return true;
            }
            match server.send_data(stream, &[], true, &mut spare[used..]) {
                Ok(frame) => used += frame.writer.pending_bytes().len(),
                Err(ServerError::StreamClosed | ServerError::UnknownStream) => {}
                Err(_) => {}
            }
            host.stage_tunnel_closed(token);
            host.unpark_closed(token.worker_id, token.slot);
            meta.pending_len += used;
            return true;
        }

        meta.pending_len += used;
        return used > 0;
    }

    false
}

pub(crate) fn apply_h2_streams(
    meta: &mut slots::H2Meta,
    conn: &mut Option<H2Conn<'_>>,
    turn: &mut turn::H2Turn<'_>,
) -> bool {
    if slots::pending_spare(meta).len() < 2048 {
        return false;
    }
    let Some(live) = conn.as_mut().filter(|active| !active.is_closed()) else {
        return false;
    };
    let index = meta.token.saturating_sub(slots::H2_STREAM_TOKEN_BASE);
    let base = (crate::static_server::MAX_CONNECTIONS + index * STREAM_SLOTS) as u32;
    let host = turn.host;
    let fail = ErrorCode::InternalError;
    let mut progressed = false;
    if host.h2_tunnel_active(turn.worker_id) {
        let H2Conn {
            phase,
            tunnel_scratch,
            ..
        } = &mut *live;
        if let Phase::Serving(server) = phase {
            progressed = tunnel_step(meta, server, host, turn.worker_id, base, tunnel_scratch);
        }
    }
    let peek = host.with_inner(|inner| {
        let mut found = None;
        let mut rearmed: [Option<ConnectionToken>; STREAM_SLOTS] = [None; STREAM_SLOTS];
        let mut rearmed_len = 0;
        for sub in 0..STREAM_SLOTS {
            let slot = base + sub as u32;
            let entry = inner
                .parked
                .get_mut(turn.worker_id as usize)
                .and_then(|table| table.get_mut(slot as usize))
                .and_then(|entry| entry.as_mut())
                .filter(|entry| entry.stream.is_some());
            let Some(entry) = entry else {
                continue;
            };
            if entry.stream_cmd.is_some() || entry.staged.is_some() {
                if found.is_none() {
                    let token = entry.token;
                    let write = match &entry.stream_cmd {
                        Some(StreamCmd::Write { chunk }) => Some(chunk.len()),
                        _ => None,
                    };
                    found = Some((token, write));
                }
                continue;
            }
            if entry.staged.is_none() {
                if let Some(stream) = entry.stream.as_mut() {
                    if stream.head_sent && !stream.upgraded {
                        stream.writable = true;
                        // NOTE: one coalesced WRITABLE per release, staged after unlock below:
                        // HostShared::stage_writable would deadlock under with_inner and the
                        // queue fields are private to host/, so releases collect here and emit
                        // there. A full queue leaves the arm set for the next turn to re-signal.
                        rearmed[rearmed_len] = Some(entry.token);
                        rearmed_len += 1;
                    }
                }
            }
        }
        ((found, rearmed, rearmed_len), None)
    });
    let Some(((found, rearmed, rearmed_len), _)) = peek else {
        return progressed;
    };
    for token in rearmed[..rearmed_len].iter().flatten() {
        host.stage_writable(*token);
    }
    let Some((token, write)) = found else {
        return progressed;
    };
    let stream = token.stream_id;
    let Phase::Serving(server) = &mut live.phase else {
        host.unpark_closed(token.worker_id, token.slot);
        return true;
    };
    let spare = slots::pending_spare(meta).len();
    let found = server.driver().streams().find(stream);
    let credit = found
        .filter(|found| found.state != StreamState::Closed)
        .map(|found| found.send_window.value());
    if write.is_some() {
        let send = server.driver().connection_send_window().value();
        let flow = credit.unwrap_or(0);
        if spare < 10 || send <= 0 || flow <= 0 {
            return progressed;
        }
    }

    let Some((cmd, _)) = host.with_inner(|inner| {
        let command = take_stream_cmd(inner, token);
        (command, None)
    }) else {
        return progressed;
    };
    let Some(cmd) = cmd else {
        return progressed;
    };

    let spare = slots::pending_spare(meta);
    let step = match cmd {
        StreamCmd::Begin {
            status, headers, ..
        } => {
            // NOTE: HEAD-park parity with H1 Begin fail-closed via the ParkedEntry method snapshot.
            let head = is_head_park(host, token);
            if head {
                abort_stream(server, host, token, fail, spare)
            } else {
                let mut used = 0;
                let mut extra = [(&b""[..], &b""[..]); crate::host::MAX_STAGED_RESPONSE_HEADERS];
                let extra_len = project_extra_headers(&mut extra, headers.as_slice());
                match send_begin(
                    server,
                    stream,
                    status,
                    &extra[..extra_len],
                    spare,
                    &mut used,
                ) {
                    Ok(()) => StreamStep::Done {
                        used,
                        served: false,
                        refused: false,
                    },
                    Err(_) => abort_stream(server, host, token, fail, slots::pending_spare(meta)),
                }
            }
        }
        StreamCmd::Write { chunk } => {
            let mut used = 0;
            let mut off = 0;
            let mut failed = false;

            while off < chunk.len() {
                match server.send_data(stream, &chunk[off..], false, &mut spare[used..]) {
                    Ok(sent) => {
                        used += sent.writer.pending_bytes().len();
                        off += sent.frame.payload_length;
                        if sent.frame.payload_length == 0 {
                            break;
                        }
                    }
                    Err(ServerError::Data(DataWriteError::NoSendCredit)) => break,
                    Err(ServerError::StreamClosed | ServerError::UnknownStream) => {
                        failed = true;
                        break;
                    }
                    Err(_) => {
                        failed = true;
                        break;
                    }
                }
            }
            if failed {
                match abort_stream(server, host, token, fail, &mut spare[used..]) {
                    StreamStep::Dead { used: reset } => StreamStep::Dead { used: used + reset },
                    done => done,
                }
            } else if off == chunk.len() {
                StreamStep::Done {
                    used,
                    served: false,
                    refused: false,
                }
            } else {
                let mut remainder = chunk;
                remainder.drain(..off);
                if !host.defer_stream_write(token, remainder) {
                    match abort_stream(server, host, token, fail, &mut spare[used..]) {
                        StreamStep::Dead { used: reset } => StreamStep::Dead { used: used + reset },
                        done => done,
                    }
                } else {
                    StreamStep::Done {
                        used,
                        served: false,
                        refused: false,
                    }
                }
            }
        }
        StreamCmd::End => match server.send_data(stream, b"", true, spare) {
            Ok(sent) => {
                let used = sent.writer.pending_bytes().len();
                host.unpark_closed(token.worker_id, token.slot);
                StreamStep::Done {
                    used,
                    served: true,
                    refused: false,
                }
            }
            Err(_) => abort_stream(server, host, token, fail, slots::pending_spare(meta)),
        },
        StreamCmd::EndWithTrailers { trailers } => {
            // NOTE: terminal HEADERS stay per stream: the token-gated take plus the slot-derived
            // unpark bind the set to its stream only, zero-DATA trailers are legal, never-begun
            // sets reject at stage, late sets follow the tomb discipline, and assembly is
            // all-or-nothing through the shared serve validator.
            // NOTE: HEAD-park parity with H1 Begin fail-closed via the ParkedEntry method snapshot.
            let head = is_head_park(host, token);
            if head {
                abort_stream(server, host, token, fail, spare)
            } else {
                let mut fields = [field(b"", b""); TRAILER_SLOTS];
                let mut valid = trailers.len() <= TRAILER_SLOTS;
                for (slot, staged) in trailers.iter().enumerate().take(TRAILER_SLOTS) {
                    valid = valid && valid_trailer_field(&staged.name, &staged.value);
                    fields[slot] = field(&staged.name, &staged.value);
                }
                let mut block = [0; 1152];
                let encoded = valid.then(|| {
                    encode_header_block(
                        &fields[..trailers.len()],
                        server.driver_mut().hpack_encoder_mut(),
                        &mut block,
                    )
                });
                match encoded {
                    Some(Ok(len)) => {
                        let mut writer = HeadersFrameWriter::new(spare);
                        let queued =
                            writer.queue_headers(server.driver_mut(), stream, &block[..len], true);
                        match queued {
                            Ok(queued) => {
                                let used = queued.output_length;
                                host.unpark_closed(token.worker_id, token.slot);
                                StreamStep::Done {
                                    used,
                                    served: true,
                                    refused: false,
                                }
                            }
                            Err(_) => {
                                abort_stream(server, host, token, fail, slots::pending_spare(meta))
                            }
                        }
                    }
                    _ => abort_stream(server, host, token, fail, slots::pending_spare(meta)),
                }
            }
        }
        StreamCmd::Upgrade { .. } => {
            abort_stream(server, host, token, fail, slots::pending_spare(meta))
        }
        StreamCmd::H2Upgrade { protocol } => {
            let mut extra = [(&b""[..], &b""[..]); 1];
            let extra_len = match protocol.as_deref() {
                Some(protocol) => {
                    extra[0] = (b"sec-websocket-protocol", protocol);
                    1
                }
                None => 0,
            };
            let mut used = 0;
            match send_begin(server, stream, 200, &extra[..extra_len], spare, &mut used) {
                Ok(()) => StreamStep::Done {
                    used,
                    served: false,
                    refused: false,
                },
                Err(_) => abort_stream(server, host, token, fail, slots::pending_spare(meta)),
            }
        }
        StreamCmd::Cancel => {
            let used =
                match server.rst_stream(stream, ErrorCode::Cancel, slots::pending_spare(meta)) {
                    Ok(writer) => writer.pending_bytes().len(),
                    Err(_) => 0,
                };
            host.unpark_closed(token.worker_id, token.slot);
            StreamStep::Done {
                used,
                served: false,
                refused: true,
            }
        }
    };
    let (used, served, refused, progress) = match step {
        StreamStep::Done {
            used,
            served,
            refused,
        } => (used, served, refused, true),
        StreamStep::Dead { used } => (used, false, false, true),
    };
    meta.pending_len += used;
    turn.counts.h2_served += u64::from(served);
    turn.counts.h2_refused += u64::from(refused);
    progress || progressed
}
