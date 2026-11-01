use std::io::{self, Read, Write};

use crate::{ConnectionTable, ConnectionToken, DeadlineError, TcpStream, TimerExpiration};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CloseReason {
    PeerClosed,
    Reset,
    Error(i32),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadStatus {
    Idle,
    Progress(usize),
    WouldBlock,
    BudgetExhausted,
    PausedForWrite,
    Eof,
    Interrupted,
    Closed(CloseReason),
    Error(i32),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteStatus {
    Idle,
    Progress(usize),
    WouldBlock(usize),
    BudgetExhausted(usize),
    Interrupted(usize),
    Closed(CloseReason),
    Error(i32),
}

pub struct IoBatch<'a> {
    pub token: ConnectionToken,
    pub stream: &'a mut TcpStream,
    pub read_scratch: &'a mut [u8],
    pub read_ready: bool,
    pub write_src: &'a [u8],
    pub write_ready: bool,
    pub read: ReadStatus,
    pub write: WriteStatus,
    pub progressed: bool,
    pub closed: Option<CloseReason>,
}

impl<'a> IoBatch<'a> {
    pub fn new(
        token: ConnectionToken,
        stream: &'a mut TcpStream,
        read_scratch: &'a mut [u8],
        read_ready: bool,
        write_src: &'a [u8],
        write_ready: bool,
    ) -> Self {
        Self {
            token,
            stream,
            read_scratch,
            read_ready,
            write_src,
            write_ready,
            read: ReadStatus::Idle,
            write: WriteStatus::Idle,
            progressed: false,
            closed: None,
        }
    }

    fn has_write_work(&self) -> bool {
        self.write_ready && !self.write_src.is_empty()
    }

    fn has_read_work(&self) -> bool {
        self.read_ready && !self.read_scratch.is_empty()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IoOutcome {
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub would_block: usize,
    pub closed: usize,
}

// NOTE: one pass over the batch in presented order; each entry is capped at
// max(1, budget / n) combined read+write bytes so a hot entry cannot consume the
// turn while others are ready. Unspent budget is left for the next turn, which
// matches edge-triggered semantics: the host rotates the presented order across
// event-limited turns instead of this pass re-arming anything.
pub fn dispatch(byte_budget: u64, batch: &mut [IoBatch<'_>]) -> IoOutcome {
    let mut outcome = IoOutcome::default();
    if batch.is_empty() {
        return outcome;
    }
    if byte_budget == 0 {
        for entry in batch.iter_mut() {
            if entry.has_write_work() {
                entry.write = WriteStatus::BudgetExhausted(0);
            }
            if entry.has_read_work() {
                entry.read = ReadStatus::BudgetExhausted;
            }
        }
        return outcome;
    }
    let entry_cap = (byte_budget / batch.len() as u64).max(1);
    let mut remaining = byte_budget;
    for entry in batch.iter_mut() {
        if remaining == 0 {
            mark_entry_exhausted(entry);
            continue;
        }
        let mut allowance = entry_cap.min(remaining);
        serve_entry(entry, &mut allowance);
        let spent = entry_cap.min(remaining) - allowance;
        remaining -= spent;
        outcome.bytes_read += entry_read_bytes(entry);
        outcome.bytes_written += entry_written_bytes(entry);
        if matches!(entry.read, ReadStatus::WouldBlock)
            || matches!(entry.write, WriteStatus::WouldBlock(_))
        {
            outcome.would_block += 1;
        }
        if entry.closed.is_some() {
            outcome.closed += 1;
        }
    }
    outcome
}

fn mark_entry_exhausted(entry: &mut IoBatch<'_>) {
    if entry.has_write_work() {
        entry.write = WriteStatus::BudgetExhausted(accepted_of(entry.write));
    }
    if entry.has_read_work() {
        entry.read = ReadStatus::BudgetExhausted;
    }
}

fn accepted_of(status: WriteStatus) -> usize {
    match status {
        WriteStatus::Progress(accepted)
        | WriteStatus::WouldBlock(accepted)
        | WriteStatus::BudgetExhausted(accepted)
        | WriteStatus::Interrupted(accepted) => accepted,
        WriteStatus::Idle | WriteStatus::Closed(_) | WriteStatus::Error(_) => 0,
    }
}

fn entry_read_bytes(entry: &IoBatch<'_>) -> u64 {
    match entry.read {
        ReadStatus::Progress(bytes) => bytes as u64,
        _ => 0,
    }
}

fn entry_written_bytes(entry: &IoBatch<'_>) -> u64 {
    match entry.write {
        WriteStatus::Progress(accepted)
        | WriteStatus::WouldBlock(accepted)
        | WriteStatus::BudgetExhausted(accepted)
        | WriteStatus::Interrupted(accepted) => accepted as u64,
        WriteStatus::Idle | WriteStatus::Closed(_) | WriteStatus::Error(_) => 0,
    }
}

fn serve_entry(entry: &mut IoBatch<'_>, allowance: &mut u64) {
    if entry.has_write_work() {
        write_entry(entry, allowance);
        // NOTE: reads pause while output is still pending; the writer either owns a
        // saturated socket or a bounded turn, and reading more request bytes only
        // grows the queued work behind it.
        let pending = entry.write_src.len() - accepted_of(entry.write);
        if pending > 0 {
            if entry.has_read_work() {
                entry.read = ReadStatus::PausedForWrite;
            }
            return;
        }
    }
    if entry.has_read_work() {
        read_entry(entry, allowance);
    }
}

fn write_entry(entry: &mut IoBatch<'_>, allowance: &mut u64) {
    let mut accepted = 0;
    while accepted < entry.write_src.len() {
        if *allowance == 0 {
            entry.write = WriteStatus::BudgetExhausted(accepted);
            entry.progressed = accepted > 0;
            return;
        }
        let chunk = entry.write_src.len() - accepted;
        let limit = (*allowance as usize).min(chunk);
        match write_once(entry.stream, &entry.write_src[accepted..accepted + limit]) {
            WriteAttempt::Accepted(bytes) => {
                accepted += bytes;
                *allowance -= bytes as u64;
                if bytes == 0 {
                    break;
                }
            }
            WriteAttempt::Blocked => {
                entry.write = WriteStatus::WouldBlock(accepted);
                entry.progressed = accepted > 0;
                return;
            }
            WriteAttempt::Interrupted => {
                entry.write = WriteStatus::Interrupted(accepted);
                entry.progressed = accepted > 0;
                return;
            }
            WriteAttempt::Closed(reason) => {
                entry.write = WriteStatus::Closed(reason);
                entry.closed = Some(reason);
                entry.progressed = accepted > 0;
                return;
            }
            WriteAttempt::Error(code) => {
                entry.write = WriteStatus::Error(code);
                entry.progressed = accepted > 0;
                return;
            }
        }
    }
    entry.write = WriteStatus::Progress(accepted);
    entry.progressed = accepted > 0;
}

fn read_entry(entry: &mut IoBatch<'_>, allowance: &mut u64) {
    let mut total = 0;
    while total < entry.read_scratch.len() {
        if *allowance == 0 {
            if total > 0 {
                entry.read = ReadStatus::Progress(total);
                entry.progressed = true;
            } else {
                entry.read = ReadStatus::BudgetExhausted;
            }
            return;
        }
        let remaining = entry.read_scratch.len() - total;
        let limit = (*allowance as usize).min(remaining);
        match read_once(entry.stream, &mut entry.read_scratch[total..total + limit]) {
            ReadAttempt::Accepted(bytes) => {
                total += bytes;
                *allowance -= bytes as u64;
                if bytes == 0 {
                    break;
                }
                // NOTE: level-triggered readiness re-reports a socket with more
                // buffered bytes, so a short read can stop here instead of paying
                // a second recv that is expected to return EAGAIN.
                if bytes < limit {
                    break;
                }
            }
            ReadAttempt::Blocked => {
                if total > 0 {
                    entry.read = ReadStatus::Progress(total);
                    entry.progressed = true;
                } else {
                    entry.read = ReadStatus::WouldBlock;
                }
                return;
            }
            ReadAttempt::Interrupted => {
                if total > 0 {
                    entry.read = ReadStatus::Progress(total);
                    entry.progressed = true;
                } else {
                    entry.read = ReadStatus::Interrupted;
                }
                return;
            }
            ReadAttempt::Eof => {
                if total > 0 {
                    entry.read = ReadStatus::Progress(total);
                    entry.progressed = true;
                } else {
                    entry.read = ReadStatus::Eof;
                    entry.closed = Some(CloseReason::PeerClosed);
                }
                return;
            }
            ReadAttempt::Closed(reason) => {
                if total > 0 {
                    entry.read = ReadStatus::Progress(total);
                    entry.progressed = true;
                } else {
                    entry.read = ReadStatus::Closed(reason);
                    entry.closed = Some(reason);
                }
                return;
            }
            ReadAttempt::Error(code) => {
                if total > 0 {
                    entry.read = ReadStatus::Progress(total);
                    entry.progressed = true;
                } else {
                    entry.read = ReadStatus::Error(code);
                }
                return;
            }
        }
    }
    entry.read = ReadStatus::Progress(total);
    entry.progressed = total > 0;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReadAttempt {
    Accepted(usize),
    Blocked,
    Interrupted,
    Eof,
    Closed(CloseReason),
    Error(i32),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriteAttempt {
    Accepted(usize),
    Blocked,
    Interrupted,
    Closed(CloseReason),
    Error(i32),
}

fn read_once(stream: &mut TcpStream, scratch: &mut [u8]) -> ReadAttempt {
    if scratch.is_empty() {
        return ReadAttempt::Accepted(0);
    }
    for _ in 0..2 {
        match stream.read(scratch) {
            Ok(0) => return ReadAttempt::Eof,
            Ok(bytes) => return ReadAttempt::Accepted(bytes),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return ReadAttempt::Blocked,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return map_read_error(&error),
        }
    }
    ReadAttempt::Interrupted
}

fn write_once(stream: &mut TcpStream, pending: &[u8]) -> WriteAttempt {
    if pending.is_empty() {
        return WriteAttempt::Accepted(0);
    }
    for _ in 0..2 {
        match stream.write(pending) {
            Ok(bytes) => return WriteAttempt::Accepted(bytes),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                return WriteAttempt::Blocked;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return map_write_error(&error),
        }
    }
    WriteAttempt::Interrupted
}

// NOTE: ECONNRESET is the kernel's reset report; EPIPE means the peer closed
// its read side first, which counts as an orderly-ish peer close rather than a
// reset. Anything else keeps its raw code so the host can log it exactly.
fn map_read_error(error: &io::Error) -> ReadAttempt {
    if error.kind() == io::ErrorKind::ConnectionReset {
        return ReadAttempt::Closed(CloseReason::Reset);
    }
    ReadAttempt::Error(error.raw_os_error().unwrap_or(0))
}

fn map_write_error(error: &io::Error) -> WriteAttempt {
    if error.kind() == io::ErrorKind::ConnectionReset {
        return WriteAttempt::Closed(CloseReason::Reset);
    }
    if error.kind() == io::ErrorKind::BrokenPipe {
        return WriteAttempt::Closed(CloseReason::PeerClosed);
    }
    WriteAttempt::Error(error.raw_os_error().unwrap_or(0))
}

// NOTE: refresh happens only on byte progress; a WouldBlock turn leaves the
// armed deadline alone so slow peers still time out. Completion cancels.
pub fn note_read_progress(
    table: &mut ConnectionTable<'_, '_>,
    token: ConnectionToken,
    bytes_read: usize,
    now_ns: u64,
    timeouts: crate::ConnectionTimeouts,
) -> Result<bool, DeadlineError> {
    if bytes_read == 0 {
        return Ok(false);
    }
    refresh_progress_deadline(table, token, now_ns, timeouts).map(|_| true)
}

pub fn note_write_progress(
    table: &mut ConnectionTable<'_, '_>,
    token: ConnectionToken,
    bytes_written: usize,
    now_ns: u64,
    timeouts: crate::ConnectionTimeouts,
) -> Result<bool, DeadlineError> {
    if bytes_written == 0 {
        return Ok(false);
    }
    refresh_progress_deadline(table, token, now_ns, timeouts).map(|_| true)
}

pub fn note_completion(
    table: &mut ConnectionTable<'_, '_>,
    token: ConnectionToken,
) -> Result<(), DeadlineError> {
    match crate::cancel_phase_deadline(table, token) {
        Ok(()) => Ok(()),
        Err(DeadlineError::NotFound | DeadlineError::TimerMissing) => Ok(()),
        Err(error) => Err(error),
    }
}

fn refresh_progress_deadline(
    table: &mut ConnectionTable<'_, '_>,
    token: ConnectionToken,
    now_ns: u64,
    timeouts: crate::ConnectionTimeouts,
) -> Result<TimerExpiration, DeadlineError> {
    crate::refresh_phase_deadline(table, token, now_ns, timeouts)
}

#[cfg(test)]
mod tests;
