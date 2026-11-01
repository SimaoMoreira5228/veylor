#[cfg(test)]
mod tests;

mod encode;

pub use encode::{
    HpackFieldToEncode, encode_header_block, encode_indexed_header, encode_literal_header,
    encode_pending_table_size_updates, encode_table_size_update,
};

use super::string::{HpackStringError, decode_hpack_string_into};
use super::table::{HpackBytes, HpackField, HpackTable, HpackTableError};
use super::{HpackError, HpackIntegerError, decode_hpack_integer};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HpackRepresentation {
    Indexed,
    Incremental,
    WithoutIndexing,
    NeverIndexed,

    Default,
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

// NOTE (privacy boundary): the dynamic table is connection-shared state observable across streams.
// Indexing a secret lets a peer that injects headers run CRIME-style compression oracles: an
// indexed-versus-literal length difference reveals whether a guessed value already sits in the
// table. Sensitive names therefore resolve to never-indexed even on an exact table match, so
// secrets never enter the table in either direction.
// Policy lives beside the representation enum it maps onto: one match site, one precedence rule.
const NEVER_INDEXED_NAMES: [&[u8]; 4] = [
    b"authorization",
    b"proxy-authorization",
    b"cookie",
    b"set-cookie",
];
const INCREMENTAL_NAMES: [&[u8]; 8] = [
    b":method",
    b":scheme",
    b":authority",
    b":status",
    b"content-type",
    b"content-encoding",
    b"cache-control",
    b"server",
];

fn is_sensitive_name(name: &[u8]) -> bool {
    NEVER_INDEXED_NAMES
        .iter()
        .any(|known| name.eq_ignore_ascii_case(known))
}

fn is_stable_name(name: &[u8]) -> bool {
    INCREMENTAL_NAMES
        .iter()
        .any(|known| name.eq_ignore_ascii_case(known))
}

pub fn default_literal_representation(name: &[u8]) -> HpackRepresentation {
    if is_sensitive_name(name) {
        return HpackRepresentation::NeverIndexed;
    }
    if is_stable_name(name) {
        return HpackRepresentation::Incremental;
    }
    HpackRepresentation::WithoutIndexing
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
    table.compact_scratch();
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

        let (header, consumed) = decode_header_field(&input[offset..], table)?;
        if header_count < headers_output.len() {
            headers_output[header_count] = header;
        }
        header_count += 1;
        offset += consumed;
        saw_field = true;
    }

    if header_count > headers_output.len() {
        return Err(HpackError::HeaderCapacity {
            required: header_count,
        });
    }

    Ok((header_count, offset))
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
    let name_is_fresh = name_index == 0;
    let name = if name_index == 0 {
        let (name, consumed) = decode_string_into_table(&input[offset..], table)?;
        offset += consumed;
        name
    } else {
        table.get(name_index).ok_or(HpackError::InvalidIndex)?.name
    };
    if representation == HpackRepresentation::Incremental {
        let name = if name_is_fresh {
            name
        } else {
            table.copy_span_forward(name).map_err(HpackError::Table)?
        };
        let (value, value_length) = decode_string_into_table(&input[offset..], table)?;
        offset += value_length;
        let HpackBytes::Scratch(value_span) = value else {
            return Err(HpackError::InvalidIndex);
        };
        table
            .insert_owned_parts(name, value_span)
            .map_err(HpackError::Table)?;
        return Ok((
            HpackHeader {
                field: HpackField {
                    name,
                    value: HpackBytes::Scratch(value_span),
                },
                representation,
            },
            offset,
        ));
    }

    let (value, value_length) = decode_string_into_table(&input[offset..], table)?;
    offset += value_length;
    let field = HpackField { name, value };

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
