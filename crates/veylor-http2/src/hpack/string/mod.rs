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

pub fn encode_hpack_string(
    value: &[u8],
    huffman: bool,
    prefix_bits: u8,
    output: &mut [u8],
) -> Result<usize, HpackStringError> {
    validate_prefix_width(prefix_bits)?;

    let payload_length = if huffman {
        huffman::encoded_length(value).map_err(HpackStringError::Huffman)?
    } else {
        value.len()
    };
    let payload_length =
        u64::try_from(payload_length).map_err(|_| HpackStringError::LengthTooLarge)?;
    let mut prefix = [0; 11];
    let prefix_length = encode_hpack_integer(payload_length, prefix_bits, &mut prefix)
        .map_err(map_integer_error)?;
    let payload_length =
        usize::try_from(payload_length).map_err(|_| HpackStringError::LengthTooLarge)?;
    let total_length = prefix_length
        .checked_add(payload_length)
        .ok_or(HpackStringError::LengthTooLarge)?;
    if output.len() < total_length {
        return Err(HpackStringError::OutputTooSmall {
            required: total_length,
        });
    }

    output[..prefix_length].copy_from_slice(&prefix[..prefix_length]);
    if huffman {
        output[0] |= 1 << prefix_bits;
        let encoded = huffman::encode_huffman_with_length(
            value,
            &mut output[prefix_length..total_length],
            payload_length,
        )
        .map_err(HpackStringError::Huffman)?;
        if encoded != payload_length {
            return Err(HpackStringError::LengthTooLarge);
        }
    } else {
        output[prefix_length..total_length].copy_from_slice(value);
    }
    Ok(total_length)
}

pub fn decode_hpack_string<'input, 'output>(
    input: &'input [u8],
    prefix_bits: u8,
    output: &'output mut [u8],
) -> Result<(DecodedHpackString<'input, 'output>, usize), HpackStringError> {
    validate_prefix_width(prefix_bits)?;
    let Some(&first) = input.first() else {
        return Err(HpackStringError::InputTooShort);
    };

    let huffman_flag = 1 << prefix_bits;
    let valid_bits = ((1_u16 << (prefix_bits + 1)) - 1) as u8;
    if first & !valid_bits != 0 {
        return Err(HpackStringError::InvalidPrefixBits);
    }

    let is_huffman = first & huffman_flag != 0;
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

    if !is_huffman {
        return Ok((DecodedHpackString::Raw(encoded_value), end));
    }

    let decoded_length = match huffman::decode_huffman(encoded_value, output) {
        Ok(decoded_length) => decoded_length,
        Err(HuffmanError::OutputTooSmall { required }) => {
            return Err(HpackStringError::OutputTooSmall { required });
        }
        Err(error) => return Err(HpackStringError::Huffman(error)),
    };
    Ok((DecodedHpackString::Huffman(&output[..decoded_length]), end))
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
