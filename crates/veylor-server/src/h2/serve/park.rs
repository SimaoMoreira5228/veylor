use veylor_http2::{H2ServerDriver, HpackHeader, ValidatedServerRequest};
use veylor_runtime::ConnectionToken;

use crate::host::{HOST_TUNNEL_CAP, HostEventKind, stage_tunnel_signal};

use crate::h2::routes::FrozenRoutesView;
use crate::h2::{H2HostCtx, STREAM_SLOTS, StepOutcome};

// NOTE: queue-full split: H1 closes the conn when the host queue is full, but one H2 conn
// carries siblings, so a refused park RSTs only its stream and multiplexing continues.
#[allow(clippy::too_many_arguments)]
pub(super) fn park_stream(
    ctx: H2HostCtx<'_>,
    routes: &FrozenRoutesView<'_>,
    stream: u32,
    request: ValidatedServerRequest<'_>,
    end: bool,
    body_limit: u64,
    headers: &[HpackHeader],
    server: &H2ServerDriver<'_, '_, '_, '_>,
    outcome: &mut StepOutcome,
    tunnel: bool,
) -> bool {
    let total = if tunnel {
        0
    } else {
        match request.content_length {
            Some(total) => total,
            None if end => 0,
            None => u64::MAX,
        }
    };
    let path = request.path.unwrap_or(b"/");
    let route_id = routes.find(request.method, path).map(|index| index as u64);
    let route_token = routes
        .find(request.method, path)
        .map(|index| routes.token(index))
        .unwrap_or(0);
    let route_id = route_id.unwrap_or(u64::MAX);
    let trailers = routes.managed_trailers(request.method, path);
    let sub = (stream >> 1) as usize % STREAM_SLOTS;
    let base = ConnectionToken {
        stream_id: stream,
        request_seq: 0,
        slot: ctx.base.slot + sub as u32,
        ..ctx.base
    };
    let request_headers = headers
        .iter()
        .map(|header| server.driver().resolve_header(*header));

    match ctx.host.try_park_with_request(
        ctx.base.worker_id,
        base,
        route_id,
        route_token,
        request.method,
        path,
        total,
        trailers,
        Some(body_limit),
        tunnel,
        request_headers,
    ) {
        Ok(event_token) => {
            let _ = event_token;
            outcome.parked += 1;
            true
        }
        Err(_) => false,
    }
}

pub(super) enum H2DataOutcome {
    Stored,
    Short,
    OverLimit,
    OverDeclared,
    Gone,
}

pub(super) fn push_h2_data(
    ctx: H2HostCtx<'_>,
    stream: u32,
    bytes: &[u8],
    end: bool,
) -> H2DataOutcome {
    let slot = ctx.base.slot + ((stream >> 1) as usize % STREAM_SLOTS) as u32;
    let outcome = ctx.host.with_inner(|inner| {
        let live = inner
            .parked
            .get_mut(ctx.base.worker_id as usize)
            .and_then(|table| table.get_mut(slot as usize))
            .and_then(|entry| entry.as_mut())
            .filter(|live| live.token.stream_id == stream);
        let Some(live) = live else {
            return (H2DataOutcome::Gone, None);
        };
        if live.h2_tunnel {
            live.tunnel_eof |= end;
            if bytes.is_empty() {
                if end {
                    let token = live.token;
                    if live.stream.as_ref().is_some_and(|stream| stream.upgraded) {
                        stage_tunnel_signal(
                            inner,
                            ctx.base.worker_id as usize,
                            token,
                            HostEventKind::TunnelClosed,
                        );
                    }
                }
                return (H2DataOutcome::Stored, None);
            }
            let available = live.tunnel_in.len() - live.tunnel_in_start;
            if available.saturating_add(bytes.len()) > HOST_TUNNEL_CAP {
                return (H2DataOutcome::OverLimit, None);
            }
            if live.tunnel_in.len().saturating_add(bytes.len()) > HOST_TUNNEL_CAP {
                live.tunnel_in.drain(..live.tunnel_in_start);
                live.tunnel_in_start = 0;
            }
            live.tunnel_in.extend_from_slice(bytes);
            let token = live.token;
            let upgraded = live.stream.as_ref().is_some_and(|stream| stream.upgraded);
            if !upgraded {
                return (H2DataOutcome::Stored, None);
            }
            if !stage_tunnel_signal(
                inner,
                ctx.base.worker_id as usize,
                token,
                HostEventKind::TunnelData,
            ) {
                let back = inner
                    .parked
                    .get_mut(ctx.base.worker_id as usize)
                    .and_then(|table| table.get_mut(slot as usize))
                    .and_then(|entry| entry.as_mut())
                    .filter(|entry| entry.token == token);
                if let Some(entry) = back {
                    entry
                        .tunnel_in
                        .truncate(entry.tunnel_in.len() - bytes.len());
                }
                return (H2DataOutcome::OverLimit, None);
            }
            if end {
                stage_tunnel_signal(
                    inner,
                    ctx.base.worker_id as usize,
                    token,
                    HostEventKind::TunnelClosed,
                );
            }
            return (H2DataOutcome::Stored, None);
        }
        let received = live.body_pulled + live.body.len() as u64;
        if live
            .body_limit
            .is_some_and(|limit| received.saturating_add(bytes.len() as u64) > limit)
        {
            return (H2DataOutcome::OverLimit, None);
        }
        let room = live.body_total.saturating_sub(received);
        let fit = room.min(bytes.len() as u64) as usize;
        if fit < bytes.len() {
            return (H2DataOutcome::OverDeclared, None);
        }
        live.body.extend_from_slice(bytes);
        if end && live.body_total == u64::MAX {
            live.body_total = received + bytes.len() as u64;
        }
        if end && live.body_pulled + live.body.len() as u64 != live.body_total {
            return (H2DataOutcome::Short, None);
        }
        // NOTE: H2 body admission signals like the H1 fixed/chunked/park paths: an empty
        // terminal frame stages no signal (completion is pull-observed), and a full queue
        // retracts the admission and refuses the stream, so bytes and signals stay atomic.
        if bytes.is_empty() {
            return (H2DataOutcome::Stored, None);
        }
        let token = live.token;
        if crate::host::stage_body_available(inner, ctx.base.worker_id as usize, token) {
            return (H2DataOutcome::Stored, None);
        }
        let back = inner
            .parked
            .get_mut(ctx.base.worker_id as usize)
            .and_then(|table| table.get_mut(slot as usize))
            .and_then(|entry| entry.as_mut())
            .filter(|entry| entry.token == token);
        if let Some(entry) = back {
            entry.body.truncate(entry.body.len().saturating_sub(fit));
        }
        (H2DataOutcome::Gone, None)
    });
    let (result, _) = outcome;
    if matches!(result, H2DataOutcome::Stored) && !bytes.is_empty() {
        ctx.host.notify_pollers();
    }
    result
}
