#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HeaderList {
    bytes: Vec<u8>,
    lengths: Vec<(u32, u32)>,
}

impl HeaderList {
    pub(crate) fn push(&mut self, name: &[u8], value: &[u8]) {
        self.bytes.extend_from_slice(name);
        self.bytes.extend_from_slice(value);
        self.lengths.push((name.len() as u32, value.len() as u32));
    }

    pub fn len(&self) -> usize {
        self.lengths.len()
    }

    pub fn is_empty(&self) -> bool {
        self.lengths.is_empty()
    }

    pub fn iter(&self) -> HeaderIter<'_> {
        HeaderIter {
            list: self,
            index: 0,
            offset: 0,
        }
    }

    pub fn get(&self, name: &[u8]) -> Option<&[u8]> {
        self.iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .map(|(_, value)| value)
    }
}

pub struct HeaderIter<'a> {
    list: &'a HeaderList,
    index: usize,
    offset: usize,
}

impl<'a> Iterator for HeaderIter<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        let (name_length, value_length) = *self.list.lengths.get(self.index)?;
        let name_end = self.offset + name_length as usize;
        let value_end = name_end + value_length as usize;
        let item = (
            &self.list.bytes[self.offset..name_end],
            &self.list.bytes[name_end..value_end],
        );
        self.offset = value_end;
        self.index += 1;
        Some(item)
    }
}
