use crate::error::{Error, Result};
use crate::method::is_token;

#[derive(Clone, Debug, Default)]
pub(crate) struct Headers {
    items: Vec<(String, Vec<u8>)>,
}

impl Headers {
    pub fn push(&mut self, name: &str, value: &[u8]) -> Result<()> {
        if name.is_empty() || !name.bytes().all(is_token) {
            return Err(Error::builder(format!("invalid header name {name:?}")));
        }

        if value.iter().any(|&b| b == b'\r' || b == b'\n' || b == 0) {
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
