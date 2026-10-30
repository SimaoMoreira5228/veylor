use super::super::string::{encode_hpack_string_with_plan, plan_hpack_string};
use super::super::table::{
    HPACK_STATIC_TABLE_LEN, HpackTable, HpackTableError, TableInsert, entry_size,
};
use super::super::{HpackError, encode_hpack_integer};
use super::{
    HpackRepresentation, default_literal_representation, is_sensitive_name, map_integer_error,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HpackFieldToEncode<'a> {
    pub name: &'a [u8],
    pub value: &'a [u8],
    pub representation: HpackRepresentation,
    pub huffman_name: bool,
    pub huffman_value: bool,
}

struct EncoderTableState {
    retained_original_entries: usize,
    inserted_entries: usize,
    first_inserted_field: usize,
    size: u64,
    scratch_used: usize,
}

impl EncoderTableState {
    fn new(table: &HpackTable<'_>) -> Self {
        Self {
            retained_original_entries: table.len(),
            inserted_entries: 0,
            first_inserted_field: 0,
            size: table.size(),
            scratch_used: table.scratch_used(),
        }
    }

    fn clear(&mut self, next_field: usize) {
        self.retained_original_entries = 0;
        self.inserted_entries = 0;
        self.first_inserted_field = next_field;
        self.size = 0;
    }
}

pub fn encode_indexed_header(index: u64, output: &mut [u8]) -> Result<usize, HpackError> {
    if index == 0 {
        return Err(HpackError::InvalidIndex);
    }
    let written = encode_hpack_integer(index, 7, output).map_err(map_integer_error)?;
    output[0] |= 0x80;
    Ok(written)
}

pub fn encode_literal_header(
    representation: HpackRepresentation,
    name_index: u64,
    name: &[u8],
    value: &[u8],
    huffman_name: bool,
    huffman_value: bool,
    output: &mut [u8],
) -> Result<usize, HpackError> {
    let (prefix_bits, flag) = match representation {
        HpackRepresentation::Incremental => (6, 0x40),
        HpackRepresentation::WithoutIndexing => (4, 0),
        HpackRepresentation::NeverIndexed => (4, 0x10),
        HpackRepresentation::Indexed | HpackRepresentation::Default => {
            return Err(HpackError::InvalidRepresentation);
        }
    };

    let mut index_prefix = [0; 11];
    let index_length = encode_hpack_integer(name_index, prefix_bits, &mut index_prefix)
        .map_err(map_integer_error)?;
    let name_encoding = if name_index == 0 {
        Some(plan_hpack_string(name, huffman_name, 7).map_err(HpackError::String)?)
    } else {
        None
    };
    let value_encoding = plan_hpack_string(value, huffman_value, 7).map_err(HpackError::String)?;
    let total_length = index_length
        .checked_add(name_encoding.map_or(0, |encoding| encoding.total_length()))
        .and_then(|length| length.checked_add(value_encoding.total_length()))
        .ok_or(HpackError::BufferTooSmall {
            required: usize::MAX,
        })?;
    if output.len() < total_length {
        return Err(HpackError::BufferTooSmall {
            required: total_length,
        });
    }

    output[..index_length].copy_from_slice(&index_prefix[..index_length]);
    output[0] |= flag;
    let mut offset = index_length;
    if let Some(name_encoding) = name_encoding {
        offset += encode_hpack_string_with_plan(name_encoding, &mut output[offset..])
            .map_err(HpackError::String)?;
    }
    offset += encode_hpack_string_with_plan(value_encoding, &mut output[offset..])
        .map_err(HpackError::String)?;
    Ok(offset)
}

pub fn encode_table_size_update(new_max: u64, output: &mut [u8]) -> Result<usize, HpackError> {
    let written = encode_hpack_integer(new_max, 5, output).map_err(map_integer_error)?;
    output[0] |= 0x20;
    Ok(written)
}

pub fn encode_pending_table_size_updates(
    table: &mut HpackTable<'_>,
    output: &mut [u8],
) -> Result<usize, HpackError> {
    let current = table.max_size();
    let minimum = table.pending_min_size();
    if minimum.is_none() && current == table.signalled_max() {
        return Ok(0);
    }

    let first = minimum.unwrap_or(current);
    let second = (first != current).then_some(current);
    let mut encoded = [0; 22];
    let first_length = encode_table_size_update(first, &mut encoded)?;
    let mut total_length = first_length;
    if let Some(second) = second {
        let second_length = encode_table_size_update(second, &mut encoded[total_length..])?;
        total_length += second_length;
    }
    if output.len() < total_length {
        return Err(HpackError::BufferTooSmall {
            required: total_length,
        });
    }
    output[..total_length].copy_from_slice(&encoded[..total_length]);
    table.mark_size_updates_sent();
    Ok(total_length)
}

pub fn encode_header_block(
    fields: &[HpackFieldToEncode<'_>],
    table: &mut HpackTable<'_>,
    output: &mut [u8],
) -> Result<usize, HpackError> {
    let size_update_length = pending_table_size_update_length(table)?;
    let mut table_state = EncoderTableState::new(table);
    // NOTE: per-field resolutions live in this stack buffer for ordinary blocks; only blocks
    // past MAX_INLINE_RESOLVED fields spill to the heap. The two encoder loops need random
    // access to earlier decisions (preflight scans, loop-2 replay), which a streaming resolve
    // cannot provide without flapping under eviction — hence the buffer, not a recompute.
    const MAX_INLINE_RESOLVED: usize = 64;
    let mut inline = [HpackRepresentation::WithoutIndexing; MAX_INLINE_RESOLVED];
    let mut spilled: Option<Vec<HpackRepresentation>> = None;
    let mut inline_len = 0usize;
    let has_default = fields
        .iter()
        .any(|field| field.representation == HpackRepresentation::Default);
    let mut required_length = size_update_length;
    for (field_index, field) in fields.iter().enumerate() {
        let recorded: &[HpackRepresentation] = if has_default {
            spilled.as_deref().unwrap_or(&inline[..inline_len])
        } else {
            &[]
        };
        let (name_index, indexed_field, effective) = resolve_field(
            fields,
            table,
            &table_state,
            if has_default { Some(recorded) } else { None },
            field_index,
        )?;
        if has_default {
            if let Some(spill) = spilled.as_mut() {
                spill.push(effective);
            } else if inline_len < MAX_INLINE_RESOLVED {
                inline[inline_len] = effective;
                inline_len += 1;
            } else {
                let mut spill = inline.to_vec();
                spill.push(effective);
                spilled = Some(spill);
            }
        }
        let field_length = field_encoding_length(*field, effective, name_index, indexed_field)?;
        required_length =
            required_length
                .checked_add(field_length)
                .ok_or(HpackError::BufferTooSmall {
                    required: usize::MAX,
                })?;
        if effective == HpackRepresentation::Incremental {
            let recorded: &[HpackRepresentation] = if has_default {
                spilled.as_deref().unwrap_or(&inline[..inline_len])
            } else {
                &[]
            };
            preflight_insertion(
                fields,
                table,
                &mut table_state,
                if has_default { Some(recorded) } else { None },
                field_index,
            )?;
        }
    }
    if output.len() < required_length {
        return Err(HpackError::BufferTooSmall {
            required: required_length,
        });
    }

    let recorded: &[HpackRepresentation] = if has_default {
        spilled.as_deref().unwrap_or(&inline[..inline_len])
    } else {
        &[]
    };
    let mut offset = size_update_length;
    for (field_index, field) in fields.iter().enumerate() {
        let effective = if has_default {
            recorded[field_index]
        } else {
            field.representation
        };
        if effective == HpackRepresentation::Indexed {
            let index = table
                .find_exact_index(field.name, field.value)
                .ok_or(HpackError::InvalidIndex)?;
            offset += encode_indexed_header(index, &mut output[offset..])?;
        } else {
            let name_index = table.find_name_index(field.name).unwrap_or(0);
            offset += encode_literal_header(
                effective,
                name_index,
                field.name,
                field.value,
                field.huffman_name,
                field.huffman_value,
                &mut output[offset..],
            )?;
        }
        if effective == HpackRepresentation::Incremental {
            match table
                .insert_bytes(field.name, field.value)
                .map_err(HpackError::Table)?
            {
                TableInsert::Inserted | TableInsert::DiscardedOversized => {}
            }
        }
    }

    if size_update_length != 0 {
        let written = encode_pending_table_size_updates(table, &mut output[..size_update_length])?;
        debug_assert_eq!(written, size_update_length);
    }
    Ok(offset)
}

fn pending_table_size_update_length(table: &HpackTable<'_>) -> Result<usize, HpackError> {
    let current = table.max_size();
    let minimum = table.pending_min_size();
    if minimum.is_none() && current == table.signalled_max() {
        return Ok(0);
    }

    let first = minimum.unwrap_or(current);
    let mut scratch = [0; 11];
    let mut length = encode_table_size_update(first, &mut scratch)?;
    if first != current {
        length = length
            .checked_add(encode_table_size_update(current, &mut scratch)?)
            .ok_or(HpackError::BufferTooSmall {
                required: usize::MAX,
            })?;
    }
    Ok(length)
}

fn field_encoding_length(
    field: HpackFieldToEncode<'_>,
    effective: HpackRepresentation,
    name_index: u64,
    indexed_field: Option<u64>,
) -> Result<usize, HpackError> {
    let (prefix_bits, _) = match effective {
        HpackRepresentation::WithoutIndexing | HpackRepresentation::NeverIndexed => (4, 0),
        HpackRepresentation::Incremental => (6, 0x40),
        HpackRepresentation::Indexed => (7, 0x80),
        HpackRepresentation::Default => return Err(HpackError::InvalidRepresentation),
    };
    let mut prefix = [0; 11];
    let index = if effective == HpackRepresentation::Indexed {
        indexed_field.ok_or(HpackError::InvalidIndex)?
    } else {
        name_index
    };
    let index_length =
        encode_hpack_integer(index, prefix_bits, &mut prefix).map_err(map_integer_error)?;
    if effective == HpackRepresentation::Indexed {
        return Ok(index_length);
    }
    let name_length = if name_index == 0 {
        plan_hpack_string(field.name, field.huffman_name, 7)
            .map_err(HpackError::String)?
            .total_length()
    } else {
        0
    };
    let value_length = plan_hpack_string(field.value, field.huffman_value, 7)
        .map_err(HpackError::String)?
        .total_length();

    index_length
        .checked_add(name_length)
        .and_then(|length| length.checked_add(value_length))
        .ok_or(HpackError::BufferTooSmall {
            required: usize::MAX,
        })
}

fn resolve_field(
    fields: &[HpackFieldToEncode<'_>],
    table: &HpackTable<'_>,
    state: &EncoderTableState,
    resolved: Option<&[HpackRepresentation]>,
    field_index: usize,
) -> Result<(u64, Option<u64>, HpackRepresentation), HpackError> {
    let field = fields[field_index];
    let lookup = |value| {
        block_table_index(
            fields,
            table,
            state,
            resolved,
            field_index,
            field.name,
            value,
        )
    };
    match field.representation {
        HpackRepresentation::Indexed => {
            let index = lookup(Some(field.value)).ok_or(HpackError::InvalidIndex)?;
            Ok((0, Some(index), HpackRepresentation::Indexed))
        }
        HpackRepresentation::Default => {
            if !is_sensitive_name(field.name) {
                if let Some(index) = lookup(Some(field.value)) {
                    return Ok((0, Some(index), HpackRepresentation::Indexed));
                }
            }
            let representation = default_literal_representation(field.name);
            Ok((lookup(None).unwrap_or(0), None, representation))
        }
        representation => Ok((lookup(None).unwrap_or(0), None, representation)),
    }
}

fn is_inserted(
    fields: &[HpackFieldToEncode<'_>],
    resolved: Option<&[HpackRepresentation]>,
    index: usize,
) -> bool {
    if let Some(resolved) = resolved {
        return resolved
            .get(index)
            .is_some_and(|representation| *representation == HpackRepresentation::Incremental);
    }
    fields[index].representation == HpackRepresentation::Incremental
}

fn block_table_index(
    fields: &[HpackFieldToEncode<'_>],
    table: &HpackTable<'_>,
    state: &EncoderTableState,
    resolved: Option<&[HpackRepresentation]>,
    field_index: usize,
    name: &[u8],
    value: Option<&[u8]>,
) -> Option<u64> {
    for index in 1..=HPACK_STATIC_TABLE_LEN as u64 {
        let field = table.get(index)?;
        if table.resolve(field.name)? == name
            && value.is_none_or(|expected| table.resolve(field.value) == Some(expected))
        {
            return Some(index);
        }
    }

    let mut newer_insertions = 0_u64;
    for previous_index in (state.first_inserted_field..field_index).rev() {
        if !is_inserted(fields, resolved, previous_index) {
            continue;
        }
        let previous = fields[previous_index];
        if previous.name == name && value.is_none_or(|expected| previous.value == expected) {
            return u64::try_from(HPACK_STATIC_TABLE_LEN)
                .ok()?
                .checked_add(newer_insertions)?
                .checked_add(1);
        }
        newer_insertions = newer_insertions.checked_add(1)?;
    }
    if usize::try_from(newer_insertions).ok()? != state.inserted_entries {
        return None;
    }

    for original_index in 0..state.retained_original_entries {
        let original_table_index = u64::try_from(HPACK_STATIC_TABLE_LEN)
            .ok()?
            .checked_add(u64::try_from(original_index).ok()?)?
            .checked_add(1)?;
        let field = table.get(original_table_index)?;
        if table.resolve(field.name)? == name
            && value.is_none_or(|expected| table.resolve(field.value) == Some(expected))
        {
            return u64::try_from(HPACK_STATIC_TABLE_LEN)
                .ok()?
                .checked_add(newer_insertions)?
                .checked_add(u64::try_from(original_index).ok()?)?
                .checked_add(1);
        }
    }
    None
}

fn preflight_insertion(
    fields: &[HpackFieldToEncode<'_>],
    table: &HpackTable<'_>,
    state: &mut EncoderTableState,
    resolved: Option<&[HpackRepresentation]>,
    field_index: usize,
) -> Result<(), HpackError> {
    let field = fields[field_index];
    let size = entry_size(field.name.len(), field.value.len()).map_err(HpackError::Table)?;
    if size > table.max_size() {
        state.clear(field_index + 1);
        return Ok(());
    }

    while state.size > table.max_size() - size {
        evict_oldest(fields, table, state, resolved, field_index)?;
    }
    let entry_count = state
        .retained_original_entries
        .checked_add(state.inserted_entries)
        .ok_or(HpackError::Table(HpackTableError::DescriptorCapacity))?;
    if entry_count >= table.entries.len() {
        return Err(HpackError::Table(HpackTableError::DescriptorCapacity));
    }

    let bytes_length = field
        .name
        .len()
        .checked_add(field.value.len())
        .ok_or(HpackError::Table(HpackTableError::ScratchCapacity {
            required: usize::MAX,
        }))?;
    let scratch_end = state
        .scratch_used
        .checked_add(bytes_length)
        .ok_or(HpackError::Table(HpackTableError::ScratchCapacity {
            required: usize::MAX,
        }))?;
    if scratch_end > table.scratch.len() {
        return Err(HpackError::Table(HpackTableError::ScratchCapacity {
            required: scratch_end,
        }));
    }

    state.scratch_used = scratch_end;
    state.size = state
        .size
        .checked_add(size)
        .ok_or(HpackError::Table(HpackTableError::EntrySizeOverflow))?;
    if state.inserted_entries == 0 {
        state.first_inserted_field = field_index;
    }
    state.inserted_entries += 1;
    Ok(())
}

fn evict_oldest(
    fields: &[HpackFieldToEncode<'_>],
    table: &HpackTable<'_>,
    state: &mut EncoderTableState,
    resolved: Option<&[HpackRepresentation]>,
    next_field: usize,
) -> Result<(), HpackError> {
    if state.retained_original_entries > 0 {
        let index = u64::try_from(HPACK_STATIC_TABLE_LEN)
            .ok()
            .and_then(|static_length| {
                static_length.checked_add(state.retained_original_entries as u64)
            })
            .ok_or(HpackError::Table(HpackTableError::EntrySizeOverflow))?;
        let field = table
            .get(index)
            .ok_or(HpackError::Table(HpackTableError::InvalidScratchSpan))?;
        let name = table
            .resolve(field.name)
            .ok_or(HpackError::Table(HpackTableError::InvalidScratchSpan))?;
        let value = table
            .resolve(field.value)
            .ok_or(HpackError::Table(HpackTableError::InvalidScratchSpan))?;
        let size = entry_size(name.len(), value.len()).map_err(HpackError::Table)?;
        state.size = state
            .size
            .checked_sub(size)
            .ok_or(HpackError::Table(HpackTableError::EntrySizeOverflow))?;
        state.retained_original_entries -= 1;
        return Ok(());
    }

    if state.inserted_entries == 0 {
        return Err(HpackError::Table(HpackTableError::DescriptorCapacity));
    }
    let oldest = (state.first_inserted_field..next_field)
        .find(|index| is_inserted(fields, resolved, *index))
        .ok_or(HpackError::Table(HpackTableError::DescriptorCapacity))?;
    let field = fields[oldest];
    let size = entry_size(field.name.len(), field.value.len()).map_err(HpackError::Table)?;
    state.size = state
        .size
        .checked_sub(size)
        .ok_or(HpackError::Table(HpackTableError::EntrySizeOverflow))?;
    state.inserted_entries -= 1;
    state.first_inserted_field = if state.inserted_entries == 0 {
        next_field
    } else {
        (oldest + 1..next_field)
            .find(|index| is_inserted(fields, resolved, *index))
            .ok_or(HpackError::Table(HpackTableError::DescriptorCapacity))?
    };
    Ok(())
}
