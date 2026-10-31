use super::http_date::parse_http_date;
use super::ranges::{FilePlan, RangeApply};

pub struct FileHead {
    pub status: u16,
    pub content_length: u64,
    pub content_type: Option<&'static str>,
    pub mtime: Option<u64>,
    pub total: u64,
    pub window: Option<(u64, u64)>,
    pub accept_ranges: bool,
    pub multipart: bool,
}

pub fn plan_file_head(
    mut plan: FilePlan,
    method: &[u8],
    range: Option<&[u8]>,
    if_modified_since: Option<&[u8]>,
) -> (FilePlan, FileHead, bool) {
    let fresh = if_modified_since
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
        return (plan, head, false);
    }
    if plan.head_only {
        let head = file_head_200(plan.size, plan.content_type, plan.mtime, plan.total, None);
        return (plan, head, false);
    }
    let range = (method == b"GET").then_some(range).flatten();
    match plan.apply_range(range) {
        RangeApply::Multipart => {
            let head = file_head_206_multipart(plan.size, plan.mtime, plan.total);
            (plan, head, true)
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
            (plan, head, false)
        }
        RangeApply::Ranged => {
            let window = Some((plan.start, plan.start + plan.size - 1));
            let head = file_head_200(plan.size, plan.content_type, plan.mtime, plan.total, window);
            (plan, head, true)
        }
        RangeApply::Ignored => {
            let head = file_head_200(plan.size, plan.content_type, plan.mtime, plan.total, None);
            (plan, head, true)
        }
    }
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

pub fn write_content_range(out: &mut [u8], window: Option<(u64, u64)>, total: u64) -> usize {
    use super::{push_bytes, push_decimal};

    let mut len = push_bytes(out, 0, b"bytes ");
    match window {
        Some((start, last)) => {
            len = push_decimal(out, len, start);
            len = push_bytes(out, len, b"-");
            len = push_decimal(out, len, last);
            len = push_bytes(out, len, b"/");
        }
        None => len = push_bytes(out, len, b"*/"),
    }
    push_decimal(out, len, total)
}
