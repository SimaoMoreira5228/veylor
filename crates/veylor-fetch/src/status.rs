pub(crate) fn is_success(status: u16) -> bool {
    (200..300).contains(&status)
}

pub(crate) fn is_redirect(status: u16) -> bool {
    matches!(status, 301 | 302 | 303 | 307 | 308)
}

pub(crate) fn is_error(status: u16) -> bool {
    status >= 400
}
