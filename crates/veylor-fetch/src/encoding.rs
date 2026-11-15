pub(crate) const ACCEPT_GZIP: &str = "gzip";

pub(crate) fn is_gzip(value: &[u8]) -> bool {
    let value = value.trim_ascii();

    value.eq_ignore_ascii_case(b"gzip") || value.eq_ignore_ascii_case(b"x-gzip")
}
