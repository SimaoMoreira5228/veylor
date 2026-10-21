mod static_table;
#[cfg(test)]
mod tests;

use static_table::STATIC_TABLE;

pub const HPACK_STATIC_TABLE_LEN: usize = 61;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HpackSpan {
    pub offset: usize,
    pub length: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HpackBytes {
    Static(&'static [u8]),
    Scratch(HpackSpan),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HpackField {
    pub name: HpackBytes,
    pub value: HpackBytes,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HpackEntrySlot {
    field: Option<HpackField>,
    size: u64,
}

impl HpackEntrySlot {
    pub const EMPTY: Self = Self {
        field: None,
        size: 0,
    };
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HpackTableError {
    InvalidScratchSpan,
    EntrySizeOverflow,
    ScratchCapacity { required: usize },
    DescriptorCapacity,
    MaxSizeExceedsProtocol,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableInsert {
    Inserted,
    DiscardedOversized,
}

pub struct HpackTable<'a> {
    entries: &'a mut [HpackEntrySlot],
    scratch: &'a mut [u8],
    scratch_used: usize,
    count: usize,
    size: u64,
    max_size: u64,
    protocol_max_size: u64,
    signalled_max: u64,
    pending_min_size: Option<u64>,
}

impl<'a> HpackTable<'a> {
    pub fn new(
        entries: &'a mut [HpackEntrySlot],
        scratch: &'a mut [u8],
        protocol_max_size: u32,
    ) -> Self {
        entries.fill(HpackEntrySlot::EMPTY);
        let protocol_max_size = u64::from(protocol_max_size);
        Self {
            entries,
            scratch,
            scratch_used: 0,
            count: 0,
            size: 0,
            max_size: protocol_max_size,
            protocol_max_size,
            signalled_max: protocol_max_size,
            pending_min_size: None,
        }
    }

    pub const fn len(&self) -> usize {
        self.count
    }

    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub const fn size(&self) -> u64 {
        self.size
    }

    pub const fn max_size(&self) -> u64 {
        self.max_size
    }

    pub const fn protocol_max_size(&self) -> u64 {
        self.protocol_max_size
    }

    pub const fn signalled_max(&self) -> u64 {
        self.signalled_max
    }

    pub const fn pending_min_size(&self) -> Option<u64> {
        self.pending_min_size
    }

    pub const fn scratch_used(&self) -> usize {
        self.scratch_used
    }

    pub fn resolve(&self, value: HpackBytes) -> Option<&[u8]> {
        match value {
            HpackBytes::Static(bytes) => Some(bytes),
            HpackBytes::Scratch(span) => {
                let end = span.offset.checked_add(span.length)?;
                if end > self.scratch_used {
                    return None;
                }
                self.scratch.get(span.offset..end)
            }
        }
    }

    pub fn get(&self, index: u64) -> Option<HpackField> {
        if index == 0 {
            return None;
        }
        let static_length = HPACK_STATIC_TABLE_LEN as u64;
        if index <= static_length {
            return STATIC_TABLE.get(usize::try_from(index - 1).ok()?).copied();
        }

        let dynamic_index = usize::try_from(index - static_length - 1).ok()?;
        if dynamic_index >= self.count {
            return None;
        }
        self.entries[dynamic_index].field
    }

    pub fn find_name_index(&self, name: &[u8]) -> Option<u64> {
        for (index, field) in STATIC_TABLE.iter().enumerate() {
            if self.resolve(field.name)? == name {
                return Some(index as u64 + 1);
            }
        }
        for index in 0..self.count {
            let field = self.entries[index].field?;
            if self.resolve(field.name)? == name {
                return Some(HPACK_STATIC_TABLE_LEN as u64 + index as u64 + 1);
            }
        }
        None
    }

    pub fn find_exact_index(&self, name: &[u8], value: &[u8]) -> Option<u64> {
        for (index, field) in STATIC_TABLE.iter().enumerate() {
            if self.resolve(field.name)? == name && self.resolve(field.value)? == value {
                return Some(index as u64 + 1);
            }
        }
        for index in 0..self.count {
            let field = self.entries[index].field?;
            if self.resolve(field.name)? == name && self.resolve(field.value)? == value {
                return Some(HPACK_STATIC_TABLE_LEN as u64 + index as u64 + 1);
            }
        }
        None
    }

    pub fn set_max_size(&mut self, new_max: u64) -> Result<(), HpackTableError> {
        if new_max > self.protocol_max_size {
            return Err(HpackTableError::MaxSizeExceedsProtocol);
        }
        if new_max != self.max_size {
            self.pending_min_size = Some(
                self.pending_min_size
                    .map_or(new_max, |old| old.min(new_max)),
            );
        }
        self.max_size = new_max;
        while self.size > self.max_size {
            self.evict_oldest();
        }
        Ok(())
    }

    pub fn insert_bytes(
        &mut self,
        name: &[u8],
        value: &[u8],
    ) -> Result<TableInsert, HpackTableError> {
        let size = entry_size(name.len(), value.len())?;
        if size > self.max_size {
            self.clear_dynamic_entries();
            return Ok(TableInsert::DiscardedOversized);
        }
        self.ensure_insert_slot(size)?;
        let total_bytes = name
            .len()
            .checked_add(value.len())
            .ok_or(HpackTableError::EntrySizeOverflow)?;
        let end =
            self.scratch_used
                .checked_add(total_bytes)
                .ok_or(HpackTableError::ScratchCapacity {
                    required: usize::MAX,
                })?;
        if end > self.scratch.len() {
            return Err(HpackTableError::ScratchCapacity { required: end });
        }

        let name_span = self.append_bytes(name);
        let value_span = self.append_bytes(value);
        self.insert_field(HpackField {
            name: HpackBytes::Scratch(name_span),
            value: HpackBytes::Scratch(value_span),
        })
    }

    pub fn insert_field(&mut self, field: HpackField) -> Result<TableInsert, HpackTableError> {
        let name = self
            .resolve(field.name)
            .ok_or(HpackTableError::InvalidScratchSpan)?;
        let value = self
            .resolve(field.value)
            .ok_or(HpackTableError::InvalidScratchSpan)?;
        let size = entry_size(name.len(), value.len())?;
        if size > self.max_size {
            self.clear_dynamic_entries();
            return Ok(TableInsert::DiscardedOversized);
        }
        self.ensure_insert_slot(size)?;

        while self.size > self.max_size - size {
            self.evict_oldest();
        }
        for index in (0..self.count).rev() {
            self.entries[index + 1] = self.entries[index];
        }
        self.entries[0] = HpackEntrySlot {
            field: Some(field),
            size,
        };
        self.count += 1;
        self.size += size;
        Ok(TableInsert::Inserted)
    }

    fn ensure_insert_slot(&self, new_size: u64) -> Result<(), HpackTableError> {
        let mut count = self.count;
        let mut size = self.size;
        while count > 0 && size > self.max_size - new_size {
            count -= 1;
            size -= self.entries[count].size;
        }
        if count >= self.entries.len() {
            return Err(HpackTableError::DescriptorCapacity);
        }
        Ok(())
    }

    fn append_bytes(&mut self, bytes: &[u8]) -> HpackSpan {
        let offset = self.scratch_used;
        let end = offset + bytes.len();
        self.scratch[offset..end].copy_from_slice(bytes);
        self.scratch_used = end;
        HpackSpan {
            offset,
            length: bytes.len(),
        }
    }

    fn evict_oldest(&mut self) {
        self.count -= 1;
        self.size -= self.entries[self.count].size;
        self.entries[self.count] = HpackEntrySlot::EMPTY;
    }

    fn clear_dynamic_entries(&mut self) {
        self.entries[..self.count].fill(HpackEntrySlot::EMPTY);
        self.count = 0;
        self.size = 0;
    }
}

fn entry_size(name_length: usize, value_length: usize) -> Result<u64, HpackTableError> {
    let name_length = u64::try_from(name_length).map_err(|_| HpackTableError::EntrySizeOverflow)?;
    let value_length =
        u64::try_from(value_length).map_err(|_| HpackTableError::EntrySizeOverflow)?;
    name_length
        .checked_add(value_length)
        .and_then(|size| size.checked_add(32))
        .ok_or(HpackTableError::EntrySizeOverflow)
}
