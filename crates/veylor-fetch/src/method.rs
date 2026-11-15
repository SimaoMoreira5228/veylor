use std::fmt;

use crate::field::is_valid_name;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Method {
    Get,
    Head,
    Post,
    Put,
    Patch,
    Delete,
    Options,
    Other(String),
}

impl Method {
    pub fn parse(name: &str) -> Option<Self> {
        let method = match name {
            "GET" => Self::Get,
            "HEAD" => Self::Head,
            "POST" => Self::Post,
            "PUT" => Self::Put,
            "PATCH" => Self::Patch,
            "DELETE" => Self::Delete,
            "OPTIONS" => Self::Options,
            other if is_valid_name(other) => Self::Other(other.to_owned()),
            _ => return None,
        };

        Some(method)
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Get => "GET",
            Self::Head => "HEAD",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
            Self::Options => "OPTIONS",
            Self::Other(name) => name,
        }
    }

    pub fn is_idempotent(&self) -> bool {
        match self {
            Self::Get | Self::Head | Self::Put | Self::Delete | Self::Options => true,
            Self::Other(name) => name == "TRACE",
            _ => false,
        }
    }
}

impl fmt::Display for Method {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
