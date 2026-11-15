use veylor_client_driver::HeaderIter as RawHeaderIter;

use super::error::{Error, Result};
use crate::field;

#[derive(Clone, Debug, Default)]
pub(crate) struct Headers {
    items: Vec<(String, Vec<u8>)>,
}

impl Headers {
    pub fn push(&mut self, name: &str, value: &[u8]) -> Result<()> {
        if !field::is_valid_name(name) {
            return Err(Error::builder(format!("invalid header name {name:?}")));
        }

        if !field::is_valid_value(value) {
            return Err(Error::builder(format!("invalid value for header {name}")));
        }

        self.items
            .push((name.to_ascii_lowercase(), value.trim_ascii().to_vec()));

        Ok(())
    }

    pub fn contains(&self, name: &str) -> bool {
        self.items.iter().any(|(item, _)| item == name)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &[u8])> {
        self.items
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_slice()))
    }
}

pub type HeaderBytesIter<'a> = RawHeaderIter<'a>;

pub struct HeaderIter<'a> {
    inner: RawHeaderIter<'a>,
}

impl<'a> HeaderIter<'a> {
    pub(crate) fn new(inner: RawHeaderIter<'a>) -> Self {
        Self { inner }
    }
}

impl<'a> Iterator for HeaderIter<'a> {
    type Item = (&'a str, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        for (name, value) in self.inner.by_ref() {
            if let (Ok(name), Ok(value)) = (std::str::from_utf8(name), std::str::from_utf8(value)) {
                return Some((name, value));
            }
        }

        None
    }
}
