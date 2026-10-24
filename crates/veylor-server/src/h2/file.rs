use veylor_http2::{ErrorCode, H2ServerDriver, HpackHeader, StreamState};

use crate::static_files::{FilePlan, FileReject, RangeApply, parse_http_date, read_chunk};
use crate::static_routes::MAX_STATIC_BODY_BYTES;

use super::STREAM_SLOTS;
use super::routes::{FileHead, send_file_head};
use super::serve::Serve;

// NOTE: per-stream file progress: the plan owns its fs path, offset counts plan-relative
// bytes already framed, and the entry drops on completion, peer reset, or conn close.
pub(crate) struct H2FileTransfer {
    pub(crate) stream: u32,
    pub(crate) plan: FilePlan,
    pub(crate) offset: u64,
}

impl<'s, 'o, 'b, 't, 'd, 'e, 'h, 'u> Serve<'s, 'o, 'b, 't, 'd, 'e, 'h, 'u> {
    // NOTE: H2 file dispatch with the H1 respond_file contract: NotFound/Forbidden/NotAFile
    // read as Miss (the caller falls through to the untouched static 404 shapes), TooLarge
    // reads as 413, If-Modified-Since drives 304 before Range for GET and HEAD alike, HEAD
    // ignores Range to 200 full headers, one satisfiable range narrows to 206, an
    // unsatisfiable window fails closed as 416, and two or more satisfiable windows frame
    // as one 206 multipart body through the shared read_chunk windows (H1 byte-identical).
    pub(crate) fn plan_file(
        &self,
        method: &[u8],
        path: &[u8],
        declared: u64,
        fields: &[HpackHeader],
    ) -> FileDecision {
        if declared > MAX_STATIC_BODY_BYTES {
            return FileDecision::TooLarge;
        }
        let mut plan = match self.routes.resolve_file(method, path) {
            Ok(plan) => plan,
            Err(FileReject::TooLarge) => return FileDecision::TooLarge,
            Err(_) => return FileDecision::Miss,
        };
        let since = self.request_header(fields, b"if-modified-since");
        let fresh = since
            .and_then(parse_http_date)
            .is_some_and(|date| plan.mtime <= date);
        if fresh {
            let head = FileHead {
                status: 304,
                content_length: 0,
                content_type: None,
                mtime: Some(plan.mtime),
                total: plan.total,
                window: None,
                accept_ranges: false,
                multipart: false,
            };
            return FileDecision::Serve(plan, head, false);
        }
        if plan.head_only {
            let head = file_head_200(plan.size, plan.content_type, plan.mtime, plan.total, None);
            return FileDecision::Serve(plan, head, false);
        }
        let range = (method == b"GET")
            .then(|| self.request_header(fields, b"range"))
            .flatten();
        match plan.apply_range(range) {
            RangeApply::Multipart => {
                let head = file_head_206_multipart(plan.size, plan.mtime, plan.total);
                FileDecision::Serve(plan, head, true)
            }
            RangeApply::Unsatisfiable => {
                let head = FileHead {
                    status: 416,
                    content_length: 0,
                    content_type: None,
                    mtime: None,
                    total: plan.total,
                    window: None,
                    accept_ranges: false,
                    multipart: false,
                };
                FileDecision::Serve(plan, head, false)
            }
            RangeApply::Ranged => {
                let window = Some((plan.start, plan.start + plan.size - 1));
                let head =
                    file_head_200(plan.size, plan.content_type, plan.mtime, plan.total, window);
                FileDecision::Serve(plan, head, true)
            }
            RangeApply::Ignored => {
                let head =
                    file_head_200(plan.size, plan.content_type, plan.mtime, plan.total, None);
                FileDecision::Serve(plan, head, true)
            }
        }
    }

    // NOTE: a declared body at or under the cap with the stream still open mirrors the
    // static refusal exactly (would-be HEADERS open, then RST plus a connection close);
    // HEAD/304/416/empty heads complete with HEADERS alone, and only GET bodies with bytes
    // stage a transfer that pump_files resumes across steps and turns.
    pub(crate) fn emit_file(
        &mut self,
        stream: u32,
        end: bool,
        plan: FilePlan,
        head: FileHead,
        send_body: bool,
    ) {
        let sized = send_body && plan.size > 0;
        let end_stream = end && !sized;
        if send_file_head(
            self.server,
            stream,
            &plan,
            &head,
            end_stream,
            self.output,
            self.used,
        )
        .is_err()
        {
            self.fail(ErrorCode::InternalError);
            return;
        }
        if !end {
            self.deny(stream, ErrorCode::RefusedStream, true);
            return;
        }
        if !sized {
            self.outcome.served += 1;
            return;
        }
        let base = *self.used;
        let mut offset = 0u64;
        let pump = pump_file_data(
            self.server,
            stream,
            &plan,
            &mut offset,
            self.scratch,
            self.output,
            self.used,
        );
        match pump {
            FilePump::Done => self.outcome.served += 1,
            FilePump::Fail(code) => self.fail(code),
            FilePump::More => {
                if self.file_insert(stream, plan, offset).is_err() {
                    self.deny(stream, ErrorCode::RefusedStream, false);
                } else if *self.used > base {
                    self.outcome.pending = true;
                }
            }
        }
    }

    fn request_header(&self, fields: &[HpackHeader], name: &[u8]) -> Option<&[u8]> {
        fields.iter().find_map(|header| {
            let (found, value) = self.server.driver().resolve_header(*header)?;
            if found.eq_ignore_ascii_case(name) {
                Some(value)
            } else {
                None
            }
        })
    }

    fn file_insert(&mut self, stream: u32, plan: FilePlan, offset: u64) -> Result<(), ()> {
        let Some(slot) = self.files.iter_mut().find(|slot| slot.is_none()) else {
            return Err(());
        };
        *slot = Some(H2FileTransfer {
            stream,
            plan,
            offset,
        });
        Ok(())
    }

    // NOTE: per-step file resume for transfers wider than one output buffer: stale entries
    // (peer reset, raced close) drop without a frame, completions count served, and anything
    // left sets pending only when this step framed bytes, so a saturated socket idles until
    // the next turn instead of spinning the step cap.
    pub(crate) fn pump_files(&mut self) {
        let base = *self.used;
        let mut more = false;
        let mut index = 0;
        while index < STREAM_SLOTS {
            match self.pump_file_slot(index) {
                FileSlot::Empty | FileSlot::Dropped => {}
                FileSlot::Done => self.outcome.served += 1,
                FileSlot::More => more = true,
                FileSlot::Fail(code) => {
                    self.fail(code);
                    return;
                }
            }
            index += 1;
        }
        if more && *self.used > base {
            self.outcome.pending = true;
        }
    }

    fn pump_file_slot(&mut self, index: usize) -> FileSlot {
        let stream = match self.files[index].as_ref() {
            Some(transfer) => transfer.stream,
            None => return FileSlot::Empty,
        };
        let live = self.server.driver().streams().find(stream);
        if live.is_none_or(|found| found.state == StreamState::Closed) {
            self.files[index] = None;
            return FileSlot::Dropped;
        }
        let Some(transfer) = self.files[index].as_mut() else {
            return FileSlot::Empty;
        };
        let pump = pump_file_data(
            self.server,
            stream,
            &transfer.plan,
            &mut transfer.offset,
            self.scratch,
            self.output,
            self.used,
        );
        match pump {
            FilePump::Done => {
                self.files[index] = None;
                FileSlot::Done
            }
            FilePump::More => FileSlot::More,
            FilePump::Fail(code) => {
                self.files[index] = None;
                FileSlot::Fail(code)
            }
        }
    }
}

// NOTE: large by construction (FilePlan rides by value so no per-request alloc lands
// beside the already-boxed transfer table); the allow mirrors the tradeoff explicitly.
#[allow(clippy::large_enum_variant)]
pub(crate) enum FileDecision {
    Miss,
    TooLarge,
    Serve(FilePlan, FileHead, bool),
}

fn file_head_200(
    size: u64,
    content_type: &'static str,
    mtime: u64,
    total: u64,
    window: Option<(u64, u64)>,
) -> FileHead {
    FileHead {
        status: if window.is_some() { 206 } else { 200 },
        content_length: size,
        content_type: Some(content_type),
        mtime: Some(mtime),
        total,
        window,
        accept_ranges: true,
        multipart: false,
    }
}

fn file_head_206_multipart(size: u64, mtime: u64, total: u64) -> FileHead {
    FileHead {
        status: 206,
        content_length: size,
        content_type: None,
        mtime: Some(mtime),
        total,
        window: None,
        accept_ranges: true,
        multipart: true,
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum FilePump {
    Done,
    More,
    Fail(ErrorCode),
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum FileSlot {
    Empty,
    Dropped,
    Done,
    More,
    Fail(ErrorCode),
}

// NOTE: bounded file DATA framing through the shared chunk scratch: each iteration stages
// at most one scratch slice and frames exactly one DATA frame, so output room (the turn
// budget) bounds a step and the plan window bounds the stream. A short read means the file
// shrank underfoot and fails closed like the H1 truncation rule; an exhausted window or a
// refused frame fails the same way the static DATA path does, never as wrong bytes.
fn pump_file_data(
    server: &mut H2ServerDriver<'_, '_, '_, '_>,
    stream: u32,
    plan: &FilePlan,
    offset: &mut u64,
    scratch: &mut [u8],
    output: &mut [u8],
    used: &mut usize,
) -> FilePump {
    while *offset < plan.size {
        let room = output.len() - *used;
        if room < 10 {
            return FilePump::More;
        }
        let width = (room - 9).min(scratch.len());
        let take = (plan.size - *offset).min(width as u64) as usize;
        if take == 0 {
            return FilePump::More;
        }
        let got = match read_chunk(plan, *offset, &mut scratch[..take]) {
            Ok(got) => got,
            Err(_) => return FilePump::Fail(ErrorCode::InternalError),
        };
        if got == 0 {
            return FilePump::Fail(ErrorCode::InternalError);
        }
        let last = *offset + got as u64 == plan.size;
        match server.send_data(stream, &scratch[..got], last, &mut output[*used..]) {
            Ok(sent) => {
                *used += sent.writer.pending_bytes().len();
                if sent.frame.payload_length == 0 {
                    return FilePump::Fail(ErrorCode::FlowControlError);
                }
                *offset += sent.frame.payload_length as u64;
            }
            Err(_) => return FilePump::Fail(ErrorCode::FlowControlError),
        }
    }
    FilePump::Done
}
