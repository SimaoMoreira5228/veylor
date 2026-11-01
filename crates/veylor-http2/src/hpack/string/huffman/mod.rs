mod table;
#[cfg(test)]
mod tests;

use table::HUFFMAN_CODES;

const HUFFMAN_NODE_COUNT: usize = 256;
const MISSING_BRANCH: i16 = i16::MIN;
const EOS_SYMBOL: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HuffmanError {
    InvalidCode,
    EndOfStreamSymbol,
    InvalidPadding,
    OutputTooSmall { required: usize },
    EncodedLengthOverflow,
}

#[derive(Clone, Copy)]
struct HuffmanCode(u32, u8);

impl HuffmanCode {
    const fn new(code: u32, bit_length: u8) -> Self {
        Self(code, bit_length)
    }

    const fn code(self) -> u32 {
        self.0
    }

    const fn bit_length(self) -> u8 {
        self.1
    }
}

#[derive(Clone, Copy)]
struct HuffmanNode {
    branches: [i16; 2],
}

const EMPTY_NODE: HuffmanNode = HuffmanNode {
    branches: [MISSING_BRANCH; 2],
};

const HUFFMAN_TREE: [HuffmanNode; HUFFMAN_NODE_COUNT] = build_huffman_tree();

const fn build_huffman_tree() -> [HuffmanNode; HUFFMAN_NODE_COUNT] {
    let mut nodes = [EMPTY_NODE; HUFFMAN_NODE_COUNT];
    let mut next_node = 1;
    let mut symbol = 0;

    while symbol < HUFFMAN_CODES.len() {
        let code = HUFFMAN_CODES[symbol];
        let mut node = 0;
        let mut bit_index = code.bit_length();

        while bit_index > 0 {
            bit_index -= 1;
            let branch = ((code.code() >> bit_index) & 1) as usize;
            if bit_index == 0 {
                nodes[node].branches[branch] = -((symbol as i16) + 1);
            } else {
                let child = nodes[node].branches[branch];
                if child == MISSING_BRANCH {
                    nodes[node].branches[branch] = next_node as i16;
                    node = next_node;
                    next_node += 1;
                } else {
                    node = child as usize;
                }
            }
        }
        symbol += 1;
    }
    nodes
}

pub fn encoded_length(input: &[u8]) -> Result<usize, HuffmanError> {
    let mut bits = 0_u64;
    for byte in input {
        let length = u64::from(HUFFMAN_CODES[usize::from(*byte)].bit_length());
        bits = bits
            .checked_add(length)
            .ok_or(HuffmanError::EncodedLengthOverflow)?;
    }
    let rounded = bits
        .checked_add(7)
        .ok_or(HuffmanError::EncodedLengthOverflow)?
        / 8;
    usize::try_from(rounded).map_err(|_| HuffmanError::EncodedLengthOverflow)
}

pub fn encode_huffman(input: &[u8], output: &mut [u8]) -> Result<usize, HuffmanError> {
    let required = encoded_length(input)?;
    encode_huffman_with_length(input, output, required)
}

pub(super) fn encode_huffman_with_length(
    input: &[u8],
    output: &mut [u8],
    required: usize,
) -> Result<usize, HuffmanError> {
    if output.len() < required {
        return Err(HuffmanError::OutputTooSmall { required });
    }

    let mut accumulator = 0_u64;
    let mut pending_bits = 0_u32;
    let mut written = 0;
    for byte in input {
        let code = HUFFMAN_CODES[usize::from(*byte)];
        accumulator = (accumulator << code.bit_length()) | u64::from(code.code());
        pending_bits += u32::from(code.bit_length());

        while pending_bits >= 8 {
            pending_bits -= 8;
            output[written] = (accumulator >> pending_bits) as u8;
            written += 1;
        }
        accumulator &= (1_u64 << pending_bits) - 1;
    }

    if pending_bits > 0 {
        let padding_bits = 8 - pending_bits;
        output[written] = (accumulator << padding_bits | ((1_u64 << padding_bits) - 1)) as u8;
        written += 1;
    }
    Ok(written)
}

const FAST_FALLBACK: u16 = u16::MAX;

// NOTE: HPACK's common symbols (digits, lowercase, '-', '.') fit in eight bits, so one
// byte-wide lookup replaces eight tree steps for those strings. Longer codes fall back
// to the tree walk, which keeps the table small and the decoder unchanged semantically.
const FAST_TABLE: [u16; 256] = build_fast_table();

const fn build_fast_table() -> [u16; 256] {
    let mut table = [FAST_FALLBACK; 256];
    let mut symbol = 0;
    while symbol < HUFFMAN_CODES.len() {
        let code = HUFFMAN_CODES[symbol];
        let length = code.bit_length() as usize;
        if length <= 8 {
            let base = ((code.code() << (8 - length)) & 0xFF) as usize;
            let count = 1 << (8 - length);
            let mut offset = 0;
            while offset < count {
                table[base + offset] = ((symbol as u16) << 4) | length as u16;
                offset += 1;
            }
        }
        symbol += 1;
    }
    table
}

fn decode_huffman_fast(input: &[u8], output: &mut [u8], required: usize) -> Option<usize> {
    let mut accumulator = 0_u64;
    let mut bits = 0_u32;
    let mut index = 0usize;
    let mut written = 0usize;
    loop {
        while bits < 24 && index < input.len() {
            accumulator = (accumulator << 8) | u64::from(input[index]);
            bits += 8;
            index += 1;
        }
        if bits < 8 {
            break;
        }
        let prefix = ((accumulator >> (bits - 8)) & 0xFF) as usize;
        let entry = FAST_TABLE[prefix];
        if entry == FAST_FALLBACK {
            return None;
        }
        let length = u32::from(entry & 0xF);
        let symbol = (entry >> 4) as u8;
        bits -= length;
        *output.get_mut(written)? = symbol;
        written += 1;
    }
    if bits > 7 {
        return None;
    }
    if bits > 0 {
        let mask = (1_u64 << bits) - 1;
        if accumulator & mask != mask {
            return None;
        }
    }
    // The size pass already validated the stream; matching length means every code hit
    // and the padding bytes decoded to nothing extra.
    if written == required {
        Some(written)
    } else {
        None
    }
}

pub fn decode_huffman(input: &[u8], output: &mut [u8]) -> Result<usize, HuffmanError> {
    let required = decode_huffman_inner(input, None)?;
    if output.len() < required {
        return Err(HuffmanError::OutputTooSmall { required });
    }
    if let Some(length) = decode_huffman_fast(input, &mut output[..required], required) {
        return Ok(length);
    }
    decode_huffman_inner(input, Some(&mut output[..required]))
}

fn decode_huffman_inner(
    input: &[u8],
    mut output: Option<&mut [u8]>,
) -> Result<usize, HuffmanError> {
    let mut node = 0;
    let mut trailing_bits = 0_u8;
    let mut written = 0;

    for byte in input {
        for bit_index in (0..8).rev() {
            let bit = usize::from((byte >> bit_index) & 1);
            let child = HUFFMAN_TREE[node].branches[bit];
            if child == MISSING_BRANCH {
                return Err(HuffmanError::InvalidCode);
            }
            trailing_bits += 1;

            if child >= 0 {
                node = child as usize;
                continue;
            }

            let symbol =
                usize::try_from(-i32::from(child) - 1).map_err(|_| HuffmanError::InvalidCode)?;
            if symbol == EOS_SYMBOL {
                return Err(HuffmanError::EndOfStreamSymbol);
            }
            if let Some(output) = output.as_deref_mut() {
                let Some(destination) = output.get_mut(written) else {
                    return Err(HuffmanError::OutputTooSmall {
                        required: written + 1,
                    });
                };
                *destination = symbol as u8;
            }
            written += 1;
            trailing_bits = 0;
            node = 0;
        }
    }

    if node != 0 {
        if trailing_bits > 7 {
            return Err(HuffmanError::InvalidPadding);
        }
        let padding_mask = ((1_u16 << trailing_bits) - 1) as u8;
        if input
            .last()
            .is_none_or(|byte| byte & padding_mask != padding_mask)
        {
            return Err(HuffmanError::InvalidPadding);
        }
    }
    Ok(written)
}
