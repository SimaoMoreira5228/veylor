use base64::Engine;
use base64::engine::general_purpose::STANDARD;

pub(crate) fn basic(user: &str, password: Option<&str>) -> String {
    let mut credentials = user.to_owned();

    credentials.push(':');

    if let Some(password) = password {
        credentials.push_str(password);
    }

    format!("Basic {}", STANDARD.encode(credentials.as_bytes()))
}

pub(crate) fn bearer(token: &str) -> String {
    format!("Bearer {token}")
}

#[cfg(test)]
mod tests {
    use super::{basic, bearer};

    #[test]
    fn builds_header_values() {
        assert_eq!(basic("u", Some("p")), "Basic dTpw");
        assert_eq!(basic("u", None), "Basic dTo=");
        assert_eq!(bearer("t"), "Bearer t");
    }
}
