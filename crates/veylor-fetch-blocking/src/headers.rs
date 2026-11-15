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

    pub(crate) fn get(&self, name: &str) -> Option<&str> {
        self.iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .map(|(_, value)| value)
    }

    pub(crate) fn iter(&self) -> HeaderIter<'_> {
        HeaderIter {
            headers: self,
            index: 0,
        }
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
