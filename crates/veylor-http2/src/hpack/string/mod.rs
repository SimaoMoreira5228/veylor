pub mod huffman;
#[cfg(test)]
mod tests;

use self::huffman::HuffmanError;
use super::{HpackIntegerError, decode_hpack_integer, encode_hpack_integer};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HpackStringError {
    InvalidPrefixWidth,
    InvalidPrefixBits,
    InputTooShort,
    LengthTooLarge,
    OutputTooSmall { required: usize },
    Huffman(HuffmanError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecodedHpackString<'input, 'output> {
    Raw(&'input [u8]),
    Huffman(&'output [u8]),
}

#[derive(Clone, Copy)]
pub(super) struct HpackStringEncoding<'a> {
    value: &'a [u8],
    huffman: bool,
    prefix_bits: u8,
    payload_length: usize,
    prefix_length: usize,
    total_length: usize,
}

impl HpackStringEncoding<'_> {
    pub(super) const fn total_length(self) -> usize {
        self.total_length
    }
}

struct ParsedHpackString<'a> {
    encoded_value: &'a [u8],
    huffman: bool,
    consumed: usize,
}

pub fn encode_hpack_string(
    value: &[u8],
    huffman: bool,
    prefix_bits: u8,
    output: &mut [u8],
) -> Result<usize, HpackStringError> {
    let plan = plan_hpack_string(value, huffman, prefix_bits)?;
    encode_hpack_string_with_plan(plan, output)
}

pub(super) fn plan_hpack_string(
    value: &[u8],
    huffman: bool,
    prefix_bits: u8,
) -> Result<HpackStringEncoding<'_>, HpackStringError> {
    validate_prefix_width(prefix_bits)?;

    let payload_length = if huffman {
        huffman::encoded_length(value).map_err(HpackStringError::Huffman)?
    } else {
        value.len()
    };
    let encoded_length =
        u64::try_from(payload_length).map_err(|_| HpackStringError::LengthTooLarge)?;
    let mut prefix = [0; 11];
    let prefix_length = encode_hpack_integer(encoded_length, prefix_bits, &mut prefix)
        .map_err(map_integer_error)?;
    let total_length = prefix_length
        .checked_add(payload_length)
        .ok_or(HpackStringError::LengthTooLarge)?;

    Ok(HpackStringEncoding {
        value,
        huffman,
        prefix_bits,
        payload_length,
        prefix_length,
        total_length,
    })
}

pub(super) fn encode_hpack_string_with_plan(
    plan: HpackStringEncoding<'_>,
    output: &mut [u8],
) -> Result<usize, HpackStringError> {
    if output.len() < plan.total_length {
        return Err(HpackStringError::OutputTooSmall {
            required: plan.total_length,
        });
    }

    let payload_length =
        u64::try_from(plan.payload_length).map_err(|_| HpackStringError::LengthTooLarge)?;
    let mut prefix = [0; 11];
    let prefix_length = encode_hpack_integer(payload_length, plan.prefix_bits, &mut prefix)
        .map_err(map_integer_error)?;
    if prefix_length != plan.prefix_length {
        return Err(HpackStringError::LengthTooLarge);
    }

    output[..prefix_length].copy_from_slice(&prefix[..prefix_length]);
    if plan.huffman {
        output[0] |= 1 << plan.prefix_bits;
        let encoded = huffman::encode_huffman_with_length(
            plan.value,
            &mut output[prefix_length..plan.total_length],
            plan.payload_length,
        )
        .map_err(HpackStringError::Huffman)?;
        if encoded != plan.payload_length {
            return Err(HpackStringError::LengthTooLarge);
        }
    } else {
        output[prefix_length..plan.total_length].copy_from_slice(plan.value);
    }
    Ok(plan.total_length)
}

pub fn decode_hpack_string<'input, 'output>(
    input: &'input [u8],
    prefix_bits: u8,
    output: &'output mut [u8],
) -> Result<(DecodedHpackString<'input, 'output>, usize), HpackStringError> {
    let parsed = parse_hpack_string(input, prefix_bits)?;
    if !parsed.huffman {
        return Ok((
            DecodedHpackString::Raw(parsed.encoded_value),
            parsed.consumed,
        ));
    }

    let decoded_length = match huffman::decode_huffman(parsed.encoded_value, output) {
        Ok(decoded_length) => decoded_length,
        Err(HuffmanError::OutputTooSmall { required }) => {
            return Err(HpackStringError::OutputTooSmall { required });
        }
        Err(error) => return Err(HpackStringError::Huffman(error)),
    };
    Ok((
        DecodedHpackString::Huffman(&output[..decoded_length]),
        parsed.consumed,
    ))
}

pub(super) fn decode_hpack_string_into(
    input: &[u8],
    prefix_bits: u8,
    output: &mut [u8],
) -> Result<(usize, usize), HpackStringError> {
    let parsed = parse_hpack_string(input, prefix_bits)?;
    if parsed.huffman {
        let decoded_length = match huffman::decode_huffman(parsed.encoded_value, output) {
            Ok(decoded_length) => decoded_length,
            Err(HuffmanError::OutputTooSmall { required }) => {
                return Err(HpackStringError::OutputTooSmall { required });
            }
            Err(error) => return Err(HpackStringError::Huffman(error)),
        };
        return Ok((decoded_length, parsed.consumed));
    }

    if output.len() < parsed.encoded_value.len() {
        return Err(HpackStringError::OutputTooSmall {
            required: parsed.encoded_value.len(),
        });
    }
    output[..parsed.encoded_value.len()].copy_from_slice(parsed.encoded_value);
    Ok((parsed.encoded_value.len(), parsed.consumed))
}

fn parse_hpack_string(
    input: &[u8],
    prefix_bits: u8,
) -> Result<ParsedHpackString<'_>, HpackStringError> {
    validate_prefix_width(prefix_bits)?;
    let Some(&first) = input.first() else {
        return Err(HpackStringError::InputTooShort);
    };

    let huffman_flag = 1 << prefix_bits;
    let valid_bits = ((1_u16 << (prefix_bits + 1)) - 1) as u8;
    if first & !valid_bits != 0 {
        return Err(HpackStringError::InvalidPrefixBits);
    }

    let (encoded_length, prefix_length) =
        decode_hpack_integer(input, prefix_bits).map_err(map_integer_error)?;
    let encoded_length =
        usize::try_from(encoded_length).map_err(|_| HpackStringError::LengthTooLarge)?;
    let end = prefix_length
        .checked_add(encoded_length)
        .ok_or(HpackStringError::LengthTooLarge)?;
    let Some(encoded_value) = input.get(prefix_length..end) else {
        return Err(HpackStringError::InputTooShort);
    };

    Ok(ParsedHpackString {
        encoded_value,
        huffman: first & huffman_flag != 0,
        consumed: end,
    })
}

fn validate_prefix_width(prefix_bits: u8) -> Result<(), HpackStringError> {
    if !(1..=7).contains(&prefix_bits) {
        return Err(HpackStringError::InvalidPrefixWidth);
    }
    Ok(())
}

fn map_integer_error(error: HpackIntegerError) -> HpackStringError {
    match error {
        HpackIntegerError::InvalidPrefixWidth => HpackStringError::InvalidPrefixWidth,
        HpackIntegerError::InputTooShort => HpackStringError::InputTooShort,
        HpackIntegerError::IntegerOverflow => HpackStringError::LengthTooLarge,
        HpackIntegerError::OutputTooSmall { required } => {
            HpackStringError::OutputTooSmall { required }
        }
    }
}
