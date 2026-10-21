#[cfg(test)]
mod tests;

use super::string::{
    HpackStringError, decode_hpack_string_into, encode_hpack_string_with_plan, plan_hpack_string,
};
use super::table::{HpackBytes, HpackField, HpackTable, HpackTableError};
use super::{HpackError, HpackIntegerError, decode_hpack_integer, encode_hpack_integer};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HpackRepresentation {
    Indexed,
    Incremental,
    WithoutIndexing,
    NeverIndexed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HpackHeader {
    pub field: HpackField,
    pub representation: HpackRepresentation,
}

impl HpackHeader {
    pub const EMPTY: Self = Self {
        field: HpackField {
            name: HpackBytes::Static(&[]),
            value: HpackBytes::Static(&[]),
        },
        representation: HpackRepresentation::Indexed,
    };
}

pub fn decode_header_field(
    input: &[u8],
    table: &mut HpackTable<'_>,
) -> Result<(HpackHeader, usize), HpackError> {
    let Some(&first) = input.first() else {
        return Err(HpackError::String(HpackStringError::InputTooShort));
    };

    if first & 0x80 != 0 {
        let (index, consumed) = decode_hpack_integer(input, 7).map_err(HpackError::Integer)?;
        if index == 0 {
            return Err(HpackError::InvalidIndex);
        }
        let field = table.get(index).ok_or(HpackError::InvalidIndex)?;
        return Ok((
            HpackHeader {
                field,
                representation: HpackRepresentation::Indexed,
            },
            consumed,
        ));
    }

    let (representation, prefix_bits) = if first & 0xc0 == 0x40 {
        (HpackRepresentation::Incremental, 6)
    } else if first & 0xf0 == 0x10 {
        (HpackRepresentation::NeverIndexed, 4)
    } else if first & 0xf0 == 0x00 {
        (HpackRepresentation::WithoutIndexing, 4)
    } else {
        return Err(HpackError::InvalidRepresentation);
    };

    decode_literal_field(input, table, representation, prefix_bits)
}

pub fn decode_header_block(
    input: &[u8],
    table: &mut HpackTable<'_>,
    headers_output: &mut [HpackHeader],
) -> Result<(usize, usize), HpackError> {
    let required_size_update = table.required_size_update();
    if let Some(maximum) = required_size_update {
        if !input.first().is_some_and(|byte| byte & 0xe0 == 0x20) {
            return Err(HpackError::RequiredTableSizeUpdate { maximum });
        }
    }

    let mut offset = 0;
    let mut header_count = 0;
    let mut saw_field = false;
    let mut required_size_update = required_size_update;

    while offset < input.len() {
        if input[offset] & 0xe0 == 0x20 {
            if saw_field {
                return Err(HpackError::TableSizeUpdateAfterField);
            }
            let (max_size, consumed) =
                decode_hpack_integer(&input[offset..], 5).map_err(HpackError::Integer)?;
            if let Some(maximum) = required_size_update {
                if max_size > maximum {
                    return Err(HpackError::RequiredTableSizeUpdate { maximum });
                }
            }
            table.set_max_size(max_size).map_err(HpackError::Table)?;
            if required_size_update.is_some() {
                table.accept_required_size_update();
                required_size_update = None;
            }
            offset += consumed;
            continue;
        }

        if header_count == headers_output.len() {
            return Err(HpackError::HeaderCapacity {
                required: header_count + 1,
            });
        }
        let (header, consumed) = decode_header_field(&input[offset..], table)?;
        headers_output[header_count] = header;
        header_count += 1;
        offset += consumed;
        saw_field = true;
    }

    Ok((header_count, offset))
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
        HpackRepresentation::Indexed => return Err(HpackError::InvalidRepresentation),
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

fn decode_literal_field(
    input: &[u8],
    table: &mut HpackTable<'_>,
    representation: HpackRepresentation,
    prefix_bits: u8,
) -> Result<(HpackHeader, usize), HpackError> {
    let (name_index, index_length) =
        decode_hpack_integer(input, prefix_bits).map_err(HpackError::Integer)?;
    let mut offset = index_length;
    let name = if name_index == 0 {
        let (name, consumed) = decode_string_into_table(&input[offset..], table)?;
        offset += consumed;
        name
    } else {
        table.get(name_index).ok_or(HpackError::InvalidIndex)?.name
    };
    let (value, value_length) = decode_string_into_table(&input[offset..], table)?;
    offset += value_length;
    let field = HpackField { name, value };

    if representation == HpackRepresentation::Incremental {
        table.insert_field(field).map_err(HpackError::Table)?;
    }

    Ok((
        HpackHeader {
            field,
            representation,
        },
        offset,
    ))
}

fn decode_string_into_table(
    input: &[u8],
    table: &mut HpackTable<'_>,
) -> Result<(HpackBytes, usize), HpackError> {
    let start = table.scratch_used;
    let output = &mut table.scratch[start..];
    let (length, consumed) = match decode_hpack_string_into(input, 7, output) {
        Ok(decoded) => decoded,
        Err(HpackStringError::OutputTooSmall { required }) => {
            let required = start.saturating_add(required);
            return Err(HpackError::Table(HpackTableError::ScratchCapacity {
                required,
            }));
        }
        Err(error) => return Err(HpackError::String(error)),
    };
    let end =
        start
            .checked_add(length)
            .ok_or(HpackError::Table(HpackTableError::ScratchCapacity {
                required: usize::MAX,
            }))?;
    table.scratch_used = end;
    Ok((
        HpackBytes::Scratch(super::table::HpackSpan {
            offset: start,
            length,
        }),
        consumed,
    ))
}

fn map_integer_error(error: HpackIntegerError) -> HpackError {
    HpackError::Integer(error)
}
