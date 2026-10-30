use std::fs::File;
use std::io::ErrorKind;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::time::SystemTime;

use veylor_http2::{
    DataWriteError, ErrorCode, H2ServerDriver, HpackHeader, ServerError, StreamState,
};

use crate::static_files::{FilePlan, FileReject, RangeApply, parse_http_date, read_chunk};
use crate::static_routes::MAX_STATIC_BODY_BYTES;

use super::STREAM_SLOTS;
use super::send::{FileHead, send_file_head};
use super::serve::Serve;

// NOTE: per-stream file progress: the plan owns its fs path, offset counts plan-relative
// bytes already framed, and the entry drops on completion, peer reset, or conn close.
// NOTE: the retained fd opens once per transfer and serves every plain chunk through
// seeks on that handle, so a 1 MiB file at 8 KiB chunks pays one open, not ~128. A
// per-connection single entry would thrash here: pump_files round-robins all slots each
// step, so multiplexed distinct files would evict it on every chunk (the H1 churn test
// documents miss-on-every-switch for a single entry); per-transfer retention never
// thrashes and costs one fd per live transfer, bounded by STREAM_SLOTS and released by
// drop on completion, reset, close, or the table-full refuse path.
pub(crate) struct H2FileTransfer {
    pub(crate) stream: u32,
    pub(crate) plan: FilePlan,
    pub(crate) offset: u64,
    file: Option<File>,
    dev: u64,
    ino: u64,
    size: u64,
    mtime: Option<SystemTime>,
    opens: u64,
}

impl H2FileTransfer {
    fn new(stream: u32, plan: FilePlan, offset: u64) -> Self {
        Self {
            stream,
            plan,
            offset,
            file: None,
            dev: 0,
            ino: 0,
            size: 0,
            mtime: None,
            opens: 0,
        }
    }

    // NOTE: opens pin mirroring the H1 FdCache counter: one retained open per transfer no
    // matter how many chunks stage across steps and turns; multipart still opens per chunk.
    #[cfg(test)]
    pub(crate) fn opens(&self) -> u64 {
        self.opens
    }

    // NOTE: plain-window read through the retained handle with the H1 keying discipline
    // (dev+ino+size+mtime, Unix identity, size+mtime elsewhere). Deliberate divergence on
    // mismatch: H1 reopens, but H2 already committed a fixed content-length in HEADERS, so
    // a vanished or replaced leaf fails closed instead of splicing generations mid-body.
    // A same-size same-mtime swap stays invisible, the same H1 boundary; the pump maps any
    // error here to InternalError, and a shrink racing validation still surfaces as
    // short/zero bytes that fail closed the same way.
    fn read_plain(&mut self, offset: u64, out: &mut [u8]) -> std::io::Result<usize> {
        if offset > self.plan.size {
            return Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                "offset past planned size",
            ));
        }
        let remaining = self.plan.size - offset;
        if remaining == 0 || out.is_empty() {
            return Ok(0);
        }
        let end = remaining.min(out.len() as u64) as usize;
        let start = self.plan.start;
        let file = self.retained()?;
        file.seek(SeekFrom::Start(start + offset))?;
        file.read(&mut out[..end])
    }

    fn retained(&mut self) -> std::io::Result<&mut File> {
        let meta = std::fs::metadata(&self.plan.fs_path)?;
        let size = meta.len();
        let mtime = meta.modified().ok();
        let (dev, ino) = file_key(&meta);
        let hit = self.file.is_some();
        let hit = hit && (dev, ino) == (self.dev, self.ino);
        let hit = hit && size == self.size && mtime == self.mtime;
        if !hit {
            self.file = None;
            if self.opens > 0 {
                return Err(std::io::Error::new(
                    ErrorKind::InvalidData,
                    "file replaced underfoot",
                ));
            }
            let file = File::open(&self.plan.fs_path)?;
            let fresh = file.metadata()?;
            (self.dev, self.ino) = file_key(&fresh);
            self.size = fresh.len();
            self.mtime = fresh.modified().ok();
            self.file = Some(file);
            self.opens += 1;
        }
        self.file
            .as_mut()
            .ok_or_else(|| std::io::Error::from(ErrorKind::NotFound))
    }
}

#[cfg(unix)]
fn file_key(meta: &std::fs::Metadata) -> (u64, u64) {
    use std::os::unix::fs::MetadataExt;
    (meta.dev(), meta.ino())
}

// NOTE: non-Unix identity degrades to size+mtime (key reads (0, 0) here, so the check
// below still rejects swaps that change either); no Linux-only code, no extra gating.
#[cfg(not(unix))]
fn file_key(_meta: &std::fs::Metadata) -> (u64, u64) {
    (0, 0)
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
        let mut transfer = H2FileTransfer::new(stream, plan, 0);
        let pump = pump_file_data(
            self.server,
            &mut transfer,
            self.scratch,
            self.output,
            self.used,
        );
        match pump {
            FilePump::Done => self.outcome.served += 1,
            FilePump::Fail(code) => self.fail(code),
            FilePump::More => {
                if !self.file_insert(transfer) {
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

    // NOTE: a refused insert drops the transfer inline, so its retained fd closes with it;
    // the table-full RST path releases cleanly by construction, never by a separate sweep.
    fn file_insert(&mut self, transfer: H2FileTransfer) -> bool {
        let Some(slot) = self.files.iter_mut().find(|slot| slot.is_none()) else {
            return false;
        };
        *slot = Some(transfer);
        true
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
        let pump = pump_file_data(self.server, transfer, self.scratch, self.output, self.used);
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
// shrank underfoot and fails closed like the H1 truncation rule; a refused frame fails
// the same way the static DATA path does, never as wrong bytes. An exhausted send window
// instead defers (More): the transfer keeps its offset and retries on WINDOW_UPDATE.
// NOTE: plain windows read through the transfer's retained fd; multipart keeps the
// per-chunk open+seek+read helper because its assembler (boundary framing interleaved
// with file windows) lives in static_files, and duplicating it here would double the
// framing truth. Multipart sets cap at 8 ranges, so the retained-fd win stays on the
// large-plain path that pays ~128 opens/MiB today.
// NOTE: no sendfile here by construction: DATA framing interleaves file bytes with frame
// headers under flow control, so zero-copy to the socket cannot express the stream.
fn pump_file_data(
    server: &mut H2ServerDriver<'_, '_, '_, '_>,
    transfer: &mut H2FileTransfer,
    scratch: &mut [u8],
    output: &mut [u8],
    used: &mut usize,
) -> FilePump {
    while transfer.offset < transfer.plan.size {
        let room = output.len() - *used;
        if room < 10 {
            return FilePump::More;
        }
        let width = (room - 9).min(scratch.len());
        let take = (transfer.plan.size - transfer.offset).min(width as u64) as usize;
        if take == 0 {
            return FilePump::More;
        }
        let offset = transfer.offset;
        let plain = !transfer.plan.multipart;
        let got = if plain {
            transfer.read_plain(offset, &mut scratch[..take])
        } else {
            read_chunk(&transfer.plan, offset, &mut scratch[..take])
        };
        let got = match got {
            Ok(got) => got,
            Err(_) => return FilePump::Fail(ErrorCode::InternalError),
        };
        if got == 0 {
            return FilePump::Fail(ErrorCode::InternalError);
        }
        let stream = transfer.stream;
        let last = transfer.offset + got as u64 == transfer.plan.size;
        match server.send_data(stream, &scratch[..got], last, &mut output[*used..]) {
            Ok(sent) => {
                *used += sent.writer.pending_bytes().len();
                if sent.frame.payload_length == 0 {
                    return FilePump::Fail(ErrorCode::FlowControlError);
                }
                transfer.offset += sent.frame.payload_length as u64;
            }
            // NOTE: credit exhaustion is backpressure, not a peer error: the transfer keeps
            // its offset and the slot retries when WINDOW_UPDATE arrives (readable re-drives
            // the pump), so a passive peer or a zero window stalls instead of tearing down
            // the connection. Every other send failure still fails closed below.
            Err(ServerError::Data(DataWriteError::NoSendCredit)) => return FilePump::More,
            Err(_) => return FilePump::Fail(ErrorCode::FlowControlError),
        }
    }
    FilePump::Done
}

// NOTE: transfer-scoped retained-fd pins (unit level: H2FileTransfer stays crate-private,
// so the opens hook mirrors H1's FdCache::opens here instead of through lib.rs).
#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;

    use crate::static_files::FileMounts;

    use super::H2FileTransfer;

    static NEXT_ID: AtomicU64 = AtomicU64::new(0);

    struct Fixture {
        root: std::path::PathBuf,
        blob: Vec<u8>,
    }

    impl Fixture {
        fn build() -> Self {
            let id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
            let root =
                std::env::temp_dir().join(format!("veylor-h2fd-unit-{}-{id}", std::process::id()));
            std::fs::create_dir_all(&root).expect("fixture root");
            let mut blob = vec![0u8; 1 << 20];
            for (index, byte) in blob.iter_mut().enumerate() {
                *byte = (index % 251) as u8;
            }
            std::fs::write(root.join("blob.bin"), &blob).expect("fixture blob");
            Self { root, blob }
        }

        fn transfer(&self) -> H2FileTransfer {
            let mut mounts = FileMounts::new();
            mounts.register(b"/assets/", &self.root).expect("register");
            mounts.freeze();
            let plan = mounts.resolve(b"GET", b"/assets/blob.bin").expect("plan");
            H2FileTransfer::new(1, plan, 0)
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    fn read_all(transfer: &mut H2FileTransfer, size: u64) -> Vec<u8> {
        let mut assembled = Vec::new();
        let mut offset = 0u64;
        while offset < size {
            let width = 8192.min((size - offset) as usize);
            let mut out = vec![0u8; width];
            let got = transfer.read_plain(offset, &mut out).expect("plain read");
            assert!(got > 0, "retained fd stalls before plan size");
            assembled.extend_from_slice(&out[..got]);
            offset += got as u64;
        }
        assembled
    }

    #[test]
    fn retained_fd_opens_once_for_1mib_of_8kib_slices() {
        let fixture = Fixture::build();
        let mut transfer = fixture.transfer();
        let size = transfer.plan.size;
        assert_eq!(size, 1 << 20);
        assert_eq!(read_all(&mut transfer, size), fixture.blob);
        assert_eq!(transfer.opens(), 1, "128 slices share one open");
        assert_eq!(read_all(&mut transfer, size), fixture.blob);
        assert_eq!(transfer.opens(), 1, "repeat transfer reuses the handle");
    }

    #[test]
    fn shrink_under_send_fails_closed_never_success() {
        let fixture = Fixture::build();
        let mut transfer = fixture.transfer();
        let size = transfer.plan.size;
        let mut out = vec![0u8; 8192];
        let got = transfer.read_plain(0, &mut out).expect("first slice");
        assert_eq!(got, 8192);
        let got = transfer.read_plain(size, &mut out).expect("boundary read");
        assert_eq!(got, 0, "zero marks completion, never a short success");
        std::fs::write(fixture.root.join("blob.bin"), &fixture.blob[..100]).expect("shrink");
        let _ = transfer
            .read_plain(50, &mut out)
            .expect_err("shrink fails closed");
        assert_eq!(transfer.opens(), 1, "shrink never reopens");
    }

    #[test]
    fn vanished_file_fails_closed() {
        let fixture = Fixture::build();
        let mut transfer = fixture.transfer();
        let mut out = vec![0u8; 8192];
        assert_eq!(transfer.read_plain(0, &mut out).expect("first slice"), 8192);
        std::fs::remove_file(fixture.root.join("blob.bin")).expect("vanish");
        let _ = transfer
            .read_plain(8192, &mut out)
            .expect_err("vanish fails closed");
    }

    #[test]
    fn replaced_file_fails_closed_without_stale_bytes() {
        let fixture = Fixture::build();
        let mut transfer = fixture.transfer();
        let mut out = vec![0u8; 8192];
        assert_eq!(transfer.read_plain(0, &mut out).expect("first slice"), 8192);
        assert_eq!(&out, &fixture.blob[..8192]);
        std::fs::write(fixture.root.join("blob.bin"), vec![0xAAu8; 1 << 20]).expect("replace");
        let _ = transfer
            .read_plain(8192, &mut out)
            .expect_err("replace fails closed");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn dropped_transfer_releases_its_fd() {
        use std::os::fd::{AsFd, AsRawFd};

        let fixture = Fixture::build();
        let mut transfer = fixture.transfer();
        let mut out = vec![0u8; 8192];
        assert_eq!(transfer.read_plain(0, &mut out).expect("first slice"), 8192);
        let raw = transfer
            .file
            .as_ref()
            .expect("retained")
            .as_fd()
            .as_raw_fd();
        let link = format!("/proc/self/fd/{raw}");
        let target = std::fs::read_link(&link).expect("fd link");
        assert!(
            target.ends_with("blob.bin"),
            "retained handle points at the leaf"
        );
        drop(transfer);
        match std::fs::read_link(&link) {
            Err(_) => {}
            Ok(reused) => assert!(reused != target, "fd number reused by the same leaf"),
        }
    }
}
