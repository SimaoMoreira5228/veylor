use std::io::{self, Read, Write};

use veylor_runtime::{PollInterest, Poller, TcpStream};

use crate::connection::ConnClose;
use crate::connection::tls::{TlsWriteOutcome, tls_encrypt_bytes, tls_write_once};

use super::slots::H2Meta;

pub(crate) enum H2Flush {
    Done,
    Blocked,
    Retry,
    Closed(ConnClose),
}

enum H2Write {
    Accepted(usize),
    Blocked,
    Interrupted,
    Closed(ConnClose),
}

pub(crate) enum H2Read {
    Progress(usize),
    Blocked,
    Eof,
    Closed(ConnClose),
}

// NOTE: runtime-dispatch contract minus tokens (EINTR once, WouldBlock backpressures,
// RST/EPIPE map like H1); double-INTR reports Interrupted so the turn retries promptly.
fn write_once(stream: &mut TcpStream, bytes: &[u8]) -> H2Write {
    if bytes.is_empty() {
        return H2Write::Accepted(0);
    }
    for _ in 0..2 {
        match stream.write(bytes) {
            Ok(accepted) => return H2Write::Accepted(accepted),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return H2Write::Blocked,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == io::ErrorKind::ConnectionReset => {
                return H2Write::Closed(ConnClose::Reset);
            }
            Err(error) if error.kind() == io::ErrorKind::BrokenPipe => {
                return H2Write::Closed(ConnClose::PeerClosed);
            }
            Err(_) => return H2Write::Closed(ConnClose::TransportError),
        }
    }
    H2Write::Interrupted
}

pub(crate) fn read_once(stream: &mut TcpStream, spare: &mut [u8]) -> H2Read {
    if spare.is_empty() {
        return H2Read::Blocked;
    }
    for _ in 0..2 {
        match stream.read(spare) {
            Ok(0) => return H2Read::Eof,
            Ok(read) => return H2Read::Progress(read),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return H2Read::Blocked,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) if error.kind() == io::ErrorKind::ConnectionReset => {
                return H2Read::Closed(ConnClose::Reset);
            }
            Err(_) => return H2Read::Closed(ConnClose::TransportError),
        }
    }
    H2Read::Blocked
}

// NOTE: h2c upgrades to ReadWrite while blocked so writability wakes the flush, then
// downgrades. No turn ever waits inside h2c: every path ends Done/Blocked/Retry/Closed.
fn flush_pending(meta: &mut H2Meta, stream_poller: &mut Poller, budget: &mut u64) -> H2Flush {
    if !meta.pending_unflushed() {
        return H2Flush::Done;
    }
    let Some(stream) = meta.stream.as_mut() else {
        return H2Flush::Closed(ConnClose::TransportError);
    };
    let Some(staged) = meta.pending.as_deref() else {
        return H2Flush::Closed(ConnClose::TransportError);
    };
    let mut sent = meta.pending_sent;
    let mut allowance = *budget;
    let mut outcome = H2Flush::Done;
    while sent < meta.pending_len {
        if allowance == 0 {
            outcome = H2Flush::Blocked;
            break;
        }
        let end = (meta.pending_len - sent).min(allowance as usize) + sent;
        match write_once(stream, &staged[sent..end]) {
            H2Write::Accepted(accepted) => {
                sent += accepted;
                allowance -= accepted as u64;
                if accepted == 0 {
                    outcome = H2Flush::Blocked;
                    break;
                }
            }
            H2Write::Blocked => {
                outcome = H2Flush::Blocked;
                break;
            }
            H2Write::Interrupted => {
                outcome = H2Flush::Retry;
                break;
            }
            H2Write::Closed(reason) => {
                outcome = H2Flush::Closed(reason);
                break;
            }
        }
    }
    *budget = allowance;
    meta.pending_sent = sent;
    match outcome {
        H2Flush::Done => {
            meta.pending_len = 0;
            meta.pending_sent = 0;
            meta.write_blocked = false;
            if meta.watching_write {
                meta.watching_write = false;
                if meta.token != 0 {
                    let _ = stream_poller.reregister(stream, meta.token, PollInterest::Readable);
                }
            }
            H2Flush::Done
        }
        H2Flush::Blocked => {
            meta.write_blocked = true;
            if !meta.watching_write {
                meta.watching_write = true;
                if meta.token != 0 {
                    let _ = stream_poller.reregister(stream, meta.token, PollInterest::ReadWrite);
                }
            }
            H2Flush::Blocked
        }
        H2Flush::Retry | H2Flush::Closed(_) => outcome,
    }
}

pub(crate) fn flush_h2(meta: &mut H2Meta, stream_poller: &mut Poller, budget: &mut u64) -> H2Flush {
    if meta.tls.is_some() {
        tls_flush_h2(meta, stream_poller, budget)
    } else {
        flush_pending(meta, stream_poller, budget)
    }
}

fn tls_flush_h2(meta: &mut H2Meta, stream_poller: &mut Poller, budget: &mut u64) -> H2Flush {
    if meta.tls_pending.len() <= meta.tls_pending_sent && meta.pending_len > meta.pending_sent {
        let Some(staged) = meta.pending.as_deref() else {
            return H2Flush::Closed(ConnClose::TransportError);
        };
        let plain_end = meta.pending_len.min(staged.len());
        let plain_start = meta.pending_sent.min(plain_end);
        meta.tls_pending.clear();
        meta.tls_pending_sent = 0;
        let Some(tls) = meta.tls.as_mut() else {
            return H2Flush::Closed(ConnClose::TransportError);
        };
        if tls_encrypt_bytes(tls, &staged[plain_start..plain_end], &mut meta.tls_pending).is_err() {
            return H2Flush::Closed(ConnClose::TransportError);
        }
        meta.pending_sent = meta.pending_len;
    }
    let Some(stream) = meta.stream.as_mut() else {
        return H2Flush::Closed(ConnClose::TransportError);
    };
    let staged_end = meta.tls_pending.len();
    let mut sent = meta.tls_pending_sent;
    let mut allowance = *budget;
    let mut outcome = H2Flush::Done;
    let mut attempts = 0;
    while sent < staged_end {
        if allowance == 0 {
            outcome = H2Flush::Blocked;
            break;
        }
        let end = (staged_end - sent).min(allowance as usize) + sent;
        let width = end - sent;
        match tls_write_once(stream, &meta.tls_pending[sent..end]) {
            TlsWriteOutcome::Accepted(accepted) => {
                sent += accepted;
                allowance -= accepted as u64;
                if accepted == 0 {
                    outcome = H2Flush::Blocked;
                    break;
                }
                if accepted < width {
                    outcome = H2Flush::Blocked;
                    break;
                }
            }
            TlsWriteOutcome::Blocked => {
                outcome = H2Flush::Blocked;
                break;
            }
            TlsWriteOutcome::Interrupted => {
                attempts += 1;
                if attempts >= 2 {
                    break;
                }
            }
            TlsWriteOutcome::Closed(reason) => {
                outcome = H2Flush::Closed(reason);
                break;
            }
        }
    }
    *budget = allowance;
    meta.tls_pending_sent = sent;
    match outcome {
        H2Flush::Done => {
            meta.tls_pending.clear();
            meta.tls_pending_sent = 0;
            meta.write_blocked = false;
            if meta.watching_write {
                meta.watching_write = false;
                if meta.token != 0 {
                    let _ = stream_poller.reregister(stream, meta.token, PollInterest::Readable);
                }
            }
            H2Flush::Done
        }
        H2Flush::Blocked => {
            meta.write_blocked = true;
            if !meta.watching_write {
                meta.watching_write = true;
                if meta.token != 0 {
                    let _ = stream_poller.reregister(stream, meta.token, PollInterest::ReadWrite);
                }
            }
            H2Flush::Blocked
        }
        H2Flush::Retry | H2Flush::Closed(_) => outcome,
    }
}
