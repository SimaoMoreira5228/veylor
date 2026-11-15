use crate::error::Error;
use crate::field;

#[derive(Clone, Debug, Default)]
pub(crate) struct Headers {
    bytes: Vec<u8>,
    entries: Vec<Entry>,
}

#[derive(Clone, Copy, Debug)]
struct Entry {
    start: usize,
    split: usize,
    end: usize,
}

impl Headers {
    pub(crate) fn push(&mut self, name: &[u8], value: &[u8]) {
        let start = self.bytes.len();

        self.bytes.extend_from_slice(name);

        let split = self.bytes.len();

        self.bytes.extend_from_slice(value);

        self.entries.push(Entry {
            start,
            split,
            end: self.bytes.len(),
        });
    }

    pub(crate) fn get(&self, name: &str) -> Option<&[u8]> {
        self.iter_bytes()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name.as_bytes()))
            .map(|(_, value)| value)
    }

    pub(crate) fn iter_bytes(&self) -> HeaderBytesIter<'_> {
        HeaderBytesIter {
            headers: self,
            index: 0,
        }
    }

    pub(crate) fn iter(&self) -> HeaderIter<'_> {
        HeaderIter {
            headers: self,
            index: 0,
        }
    }
}

pub(crate) fn check(name: &str, value: &str) -> Result<(), Error> {
    if !field::is_valid_name(name) {
        return Err(Error::InvalidRequest(format!("bad header name {name:?}")));
    }

    if !field::is_valid_value(value.as_bytes()) {
        return Err(Error::InvalidRequest(format!(
            "bad value for header {name}"
        )));
    }

    Ok(())
}

pub struct HeaderBytesIter<'a> {
    headers: &'a Headers,
    index: usize,
}

impl<'a> Iterator for HeaderBytesIter<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        let entry = self.headers.entries.get(self.index)?;

        self.index += 1;

        Some((
            &self.headers.bytes[entry.start..entry.split],
            &self.headers.bytes[entry.split..entry.end],
        ))
    }
}

pub struct HeaderIter<'a> {
    headers: &'a Headers,
    index: usize,
}

impl<'a> Iterator for HeaderIter<'a> {
    type Item = (&'a str, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(entry) = self.headers.entries.get(self.index) {
            self.index += 1;

            let name = std::str::from_utf8(&self.headers.bytes[entry.start..entry.split]);
            let value = std::str::from_utf8(&self.headers.bytes[entry.split..entry.end]);

            if let (Ok(name), Ok(value)) = (name, value) {
                return Some((name, value));
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::Headers;

    #[test]
    fn non_utf8_values_stay_reachable_as_bytes() {
        let mut headers = Headers::default();

        headers.push(b"x-raw", b"a\xffb");
        headers.push(b"x-ok", b"fine");

        assert_eq!(headers.get("X-Raw"), Some(&b"a\xffb"[..]));
        assert_eq!(headers.iter().count(), 1);
        assert_eq!(headers.iter_bytes().count(), 2);
    }
}
