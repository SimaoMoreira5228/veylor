use std::time::Duration;

use veylor_h3::http::{Request, StatusCode};
use veylor_h3::{Bytes, ManagedOutcome, RequestStream, ResponseWriter};
use veylor_runtime::ConnectionToken;

use crate::connection::streams::StreamCmd;
use crate::host::HOST_TUNNEL_CAP;

use super::{HostWork, Waiter, release, staged_response, text};

const TUNNEL_TICK: Duration = Duration::from_millis(2);
const TUNNEL_CHUNK: usize = 64 * 1024;

impl super::Handler {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn tunnel(
        &self,
        request: Request<()>,
        mut stream: RequestStream,
        writer: &mut ResponseWriter<'_>,
        route_id: u64,
        path: &[u8],
        trailer_allow: &[Vec<u8>],
    ) -> ManagedOutcome {
        let Some((slot, generation)) = self.slots.lock().ok().and_then(|mut slots| slots.acquire())
        else {
            return ManagedOutcome::Handled(text(StatusCode::SERVICE_UNAVAILABLE, b"busy"));
        };
        let key = (self.worker, slot, generation);
        let (sender, mut receiver) = tokio::sync::mpsc::channel(2);
        let token = ConnectionToken {
            worker_id: self.worker,
            slot,
            request_seq: 0,
            stream_id: 0,
            worker_generation: 1,
            generation,
        };
        let registered = self
            .waiters
            .lock()
            .map(|mut waiters| waiters.insert(key, Waiter { token, sender }).is_none())
            .unwrap_or(false);
        if !registered {
            release(&self.slots, slot);
            return ManagedOutcome::Handled(text(StatusCode::SERVICE_UNAVAILABLE, b"busy"));
        }

        let header_pairs: Vec<(Vec<u8>, Vec<u8>)> = request
            .headers()
            .iter()
            .map(|(name, value)| (name.as_str().as_bytes().to_vec(), value.as_bytes().to_vec()))
            .collect();
        let headers = header_pairs
            .iter()
            .map(|(name, value)| Some((name.as_slice(), value.as_slice())));
        let parked = self.host.try_park_with_request(
            self.worker,
            token,
            route_id,
            b"CONNECT",
            path,
            0,
            trailer_allow,
            Some(HOST_TUNNEL_CAP as u64),
            true,
            headers,
        );
        let Ok(event_token) = parked else {
            self.remove_waiter(key);
            release(&self.slots, slot);
            return ManagedOutcome::Handled(text(StatusCode::SERVICE_UNAVAILABLE, b"busy"));
        };
        if let Ok(mut waiters) = self.waiters.lock() {
            if let Some(waiter) = waiters.get_mut(&key) {
                waiter.token = event_token;
            }
        }

        let upgraded = match tokio::time::timeout(self.timeout, receiver.recv()).await {
            Ok(Some(HostWork::Command(StreamCmd::H2Upgrade { protocol }))) => {
                let mut response_headers = Vec::new();
                if let Some(protocol) = protocol {
                    response_headers.push((b"sec-websocket-protocol".to_vec(), protocol));
                }
                writer
                    .send_headers(StatusCode::OK, &response_headers)
                    .await
                    .is_ok()
            }
            Ok(Some(HostWork::Response(work))) => {
                release(&self.slots, slot);
                return ManagedOutcome::Handled(staged_response(work));
            }
            Ok(Some(_)) | Ok(None) | Err(_) => false,
        };
        if !upgraded {
            self.retire(key, slot);
            return ManagedOutcome::Streamed;
        }

        self.pump_tunnel(event_token, key, slot, &mut stream, writer, receiver)
            .await;
        ManagedOutcome::Streamed
    }

    async fn push_tunnel_in(&self, token: ConnectionToken, mut bytes: Bytes) -> bool {
        while !bytes.is_empty() {
            let accepted = self.host.push_tunnel_in(token, &bytes);
            if accepted == 0 {
                if self.host.tunnel_close_requested(token) {
                    return false;
                }
                tokio::time::sleep(TUNNEL_TICK).await;
                continue;
            }
            bytes = bytes.slice(accepted..);
            self.host.stage_tunnel_data(token);
        }
        true
    }

    async fn pump_tunnel(
        &self,
        token: ConnectionToken,
        key: super::WaiterKey,
        slot: u32,
        stream: &mut RequestStream,
        writer: &mut ResponseWriter<'_>,
        mut receiver: tokio::sync::mpsc::Receiver<HostWork>,
    ) {
        let mut scratch = [0u8; TUNNEL_CHUNK];
        let mut eof = false;
        loop {
            tokio::select! {
                signal = receiver.recv() => {
                    match signal {
                        Some(HostWork::Command(StreamCmd::Cancel)) | None => {
                            stream.stop(0);
                            break;
                        }
                        Some(HostWork::Response(_)) => {
                            stream.stop(0);
                            break;
                        }
                        Some(HostWork::Command(StreamCmd::End))
                        | Some(HostWork::Command(StreamCmd::EndWithTrailers { .. })) => {
                            break;
                        }
                        Some(_) => {}
                    }
                }
                chunk = stream.read_data(TUNNEL_CHUNK), if !eof => {
                    match chunk {
                        Ok(Some(bytes)) => {
                            if !self.push_tunnel_in(token, bytes).await {
                                break;
                            }
                        }
                        Ok(None) => {
                            self.host.mark_tunnel_eof(token);
                            self.host.stage_tunnel_closed(token);
                            eof = true;
                        }
                        Err(_) => break,
                    }
                }
                _ = tokio::time::sleep(TUNNEL_TICK) => {}
            }

            loop {
                let count = self.host.take_tunnel_out(token, &mut scratch);
                if count == 0 {
                    break;
                }
                if writer.write(&scratch[..count]).await.is_err() {
                    stream.stop(0);
                    self.retire(key, slot);
                    return;
                }
            }
            if self.host.tunnel_close_requested(token) {
                let _ = writer.finish().await;
                break;
            }
        }
        self.retire(key, slot);
    }
}
