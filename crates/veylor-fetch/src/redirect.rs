use crate::method::Method;

const DEFAULT_LIMIT: usize = 10;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedirectPolicy {
    limit: usize,
}

impl RedirectPolicy {
    pub fn limited(max: usize) -> Self {
        Self { limit: max }
    }

    pub fn none() -> Self {
        Self { limit: 0 }
    }

    pub fn limit(&self) -> usize {
        self.limit
    }
}

impl Default for RedirectPolicy {
    fn default() -> Self {
        Self::limited(DEFAULT_LIMIT)
    }
}

// RFC 9110 15.4: a 303 turns everything but HEAD into GET, and 301/302 do the same for POST.
pub(crate) fn rewrites_to_get(status: u16, method: &Method) -> bool {
    match status {
        303 => *method != Method::Head,
        301 | 302 => *method == Method::Post,
        _ => false,
    }
}

// SECURITY: credentials must not follow a redirect to another origin (RFC 9110 15.4).
pub(crate) fn is_credential_header(name: &str) -> bool {
    name.eq_ignore_ascii_case("authorization")
        || name.eq_ignore_ascii_case("proxy-authorization")
        || name.eq_ignore_ascii_case("cookie")
}

pub(crate) fn describes_body(name: &str) -> bool {
    [
        "content-type",
        "content-encoding",
        "content-language",
        "content-location",
        "digest",
    ]
    .iter()
    .any(|candidate| name.eq_ignore_ascii_case(candidate))
}

#[cfg(test)]
mod tests {
    use super::{describes_body, is_credential_header, rewrites_to_get};
    use crate::method::Method;

    #[test]
    fn rewrite_rules_follow_rfc_9110() {
        assert!(rewrites_to_get(303, &Method::Put));
        assert!(!rewrites_to_get(303, &Method::Head));
        assert!(rewrites_to_get(301, &Method::Post));
        assert!(!rewrites_to_get(301, &Method::Put));
        assert!(!rewrites_to_get(307, &Method::Post));
    }

    #[test]
    fn classifies_headers_case_insensitively() {
        assert!(is_credential_header("Authorization"));
        assert!(is_credential_header("cookie"));
        assert!(!is_credential_header("accept"));
        assert!(describes_body("Content-Type"));
        assert!(!describes_body("accept"));
    }
}
