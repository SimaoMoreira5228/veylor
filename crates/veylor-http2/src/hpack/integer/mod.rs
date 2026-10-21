#[cfg(test)]
mod tests;

pub const MAX_HPACK_INTEGER: u64 = u64::MAX;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HpackIntegerError {
    InvalidPrefixWidth,
    InputTooShort,
    IntegerOverflow,
    OutputTooSmall { required: usize },
}

pub fn encode_hpack_integer(
    value: u64,
    prefix_bits: u8,
    output: &mut [u8],
) -> Result<usize, HpackIntegerError> {
    let prefix_mask = prefix_mask(prefix_bits)?;
    let prefix_value = u64::from(prefix_mask);
    let required = if value < prefix_value {
        1
    } else {
        1 + continuation_length(value - prefix_value)
    };
    if output.len() < required {
        return Err(HpackIntegerError::OutputTooSmall { required });
    }

    if value < prefix_value {
        output[0] = value as u8;
        return Ok(1);
    }

    output[0] = prefix_mask;
    let mut remaining = value - prefix_value;
    let mut offset = 1;
    while remaining >= 128 {
        output[offset] = (remaining as u8 & 0x7f) | 0x80;
        remaining >>= 7;
        offset += 1;
    }
    output[offset] = remaining as u8;
    Ok(offset + 1)
}

pub fn decode_hpack_integer(
    input: &[u8],
    prefix_bits: u8,
) -> Result<(u64, usize), HpackIntegerError> {
    let prefix_mask = prefix_mask(prefix_bits)?;
    let Some(&first) = input.first() else {
        return Err(HpackIntegerError::InputTooShort);
    };

    let mut value = u64::from(first & prefix_mask);
    if value < u64::from(prefix_mask) {
        return Ok((value, 1));
    }

    let mut shift = 0_u32;
    let mut consumed = 1;
    loop {
        let Some(&byte) = input.get(consumed) else {
            return Err(HpackIntegerError::InputTooShort);
        };
        if shift >= u64::BITS {
            return Err(HpackIntegerError::IntegerOverflow);
        }

        let payload = u64::from(byte & 0x7f);
        if payload > (MAX_HPACK_INTEGER - value) >> shift {
            return Err(HpackIntegerError::IntegerOverflow);
        }
        value += payload << shift;
        consumed += 1;

        if byte & 0x80 == 0 {
            return Ok((value, consumed));
        }
        shift += 7;
    }
}

fn prefix_mask(prefix_bits: u8) -> Result<u8, HpackIntegerError> {
    if !(1..=8).contains(&prefix_bits) {
        return Err(HpackIntegerError::InvalidPrefixWidth);
    }
    Ok(((1_u16 << prefix_bits) - 1) as u8)
}

fn continuation_length(mut value: u64) -> usize {
    let mut length = 1;
    while value >= 128 {
        value >>= 7;
        length += 1;
    }
    length
}
