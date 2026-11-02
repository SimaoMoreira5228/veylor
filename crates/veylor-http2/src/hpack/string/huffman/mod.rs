#![allow(unsafe_code)]

mod table;
#[cfg(test)]
mod tests;

use std::sync::OnceLock;

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

const TAIL_5: [u16; 32] = build_width_table::<32>(5);
const TAIL_6: [u16; 64] = build_width_table::<64>(6);
const TAIL_7: [u16; 128] = build_width_table::<128>(7);

const fn build_width_table<const N: usize>(bits: u32) -> [u16; N] {
    let mut table = [FAST_FALLBACK; N];
    let mut symbol = 0;
    while symbol < HUFFMAN_CODES.len() {
        let code = HUFFMAN_CODES[symbol];
        if code.bit_length() as u32 == bits {
            let index = (code.code() & (N as u32 - 1)) as usize;
            table[index] = ((symbol as u16) << 4) | bits as u16;
        }
        symbol += 1;
    }
    table
}

pub const fn decoded_bound(input_len: usize) -> usize {
    input_len.saturating_mul(8) / 5 + 1
}

const PAIR_BITS: u32 = 10;
const PAIR_SIZE: usize = 1 << PAIR_BITS;
const PAIR_FALLBACK: u32 = u32::MAX;

struct PairTable {
    entries: [u32; PAIR_SIZE],
}

static PAIR_TABLE: OnceLock<PairTable> = OnceLock::new();

fn pair_table() -> &'static PairTable {
    PAIR_TABLE.get_or_init(|| {
        let mut entries = [PAIR_FALLBACK; PAIR_SIZE];
        for (window, entry) in entries.iter_mut().enumerate() {
            *entry = pair_entry(window as u32).unwrap_or(PAIR_FALLBACK);
        }
        PairTable { entries }
    })
}

fn pair_entry(window: u32) -> Option<u32> {
    let mut offset = 0u32;
    let mut symbols = [0u8; 2];
    let mut count = 0usize;
    while count < 2 {
        let remaining = PAIR_BITS - offset;
        let found = (0..256usize).find_map(|symbol| {
            let code = HUFFMAN_CODES[symbol];
            let length = u32::from(code.bit_length());
            if length > remaining {
                return None;
            }
            let shift = remaining - length;
            let matches = (window >> shift) & ((1 << length) - 1) == code.code();
            matches.then_some((symbol as u8, length))
        });
        let Some((symbol, length)) = found else {
            break;
        };
        symbols[count] = symbol;
        count += 1;
        offset += length;
    }
    if count == 0 {
        return None;
    }
    let mut entry = offset | ((count as u32) << 5) | (u32::from(symbols[0]) << 8);
    if count == 2 {
        entry |= u32::from(symbols[1]) << 16;
    }
    Some(entry)
}

fn decode_huffman_fast(input: &[u8], output: &mut [u8]) -> Option<usize> {
    let pairs = pair_table();
    let mut accumulator = 0_u64;
    let mut bits = 0_u32;
    let mut index = 0usize;
    let mut written = 0usize;
    loop {
        while bits < PAIR_BITS && index < input.len() {
            accumulator = (accumulator << 8) | u64::from(input[index]);
            bits += 8;
            index += 1;
        }
        if bits >= PAIR_BITS {
            let window = ((accumulator >> (bits - PAIR_BITS)) & (PAIR_SIZE as u64 - 1)) as usize;
            let entry = pairs.entries[window];
            if entry == PAIR_FALLBACK {
                return None;
            }
            if (entry >> 5) & 0x3 == 2 {
                *output.get_mut(written)? = ((entry >> 8) & 0xFF) as u8;
                *output.get_mut(written + 1)? = ((entry >> 16) & 0xFF) as u8;
                written += 2;
            } else {
                *output.get_mut(written)? = ((entry >> 8) & 0xFF) as u8;
                written += 1;
            }
            bits -= entry & 0x1F;
            continue;
        }

        let mut decoded = false;
        for width in (5..=bits.min(8)).rev() {
            let index = ((accumulator >> (bits - width)) & ((1 << width) - 1)) as usize;
            let entry = match width {
                8 => FAST_TABLE[index],
                7 => TAIL_7[index],
                6 => TAIL_6[index],
                _ => TAIL_5[index],
            };
            if entry == FAST_FALLBACK {
                continue;
            }
            *output.get_mut(written)? = (entry >> 4) as u8;
            written += 1;
            bits -= u32::from(entry & 0xF);
            decoded = true;
            break;
        }

        if !decoded {
            break;
        }
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

    Some(written)
}

pub fn decode_huffman(input: &[u8], output: &mut [u8]) -> Result<usize, HuffmanError> {
    if output.len() >= decoded_bound(input.len()) {
        if !contains_eos(input) {
            if let Some(length) = decode_huffman_fast(input, output) {
                return Ok(length);
            }
        }
        return decode_huffman_inner(input, Some(output));
    }
    let required = decode_huffman_inner(input, None)?;
    if output.len() < required {
        return Err(HuffmanError::OutputTooSmall { required });
    }
    if let Some(length) = decode_huffman_fast(input, &mut output[..required]) {
        return Ok(length);
    }
    decode_huffman_inner(input, Some(&mut output[..required]))
}

const SIMD_SCAN_MIN: usize = 64;

fn contains_eos(input: &[u8]) -> bool {
    #[cfg(target_arch = "x86_64")]
    if input.len() >= SIMD_SCAN_MIN && simd_enabled() {
        // SAFETY: AVX2 availability was checked at startup by `simd_enabled`.
        return unsafe { contains_eos_avx2(input) };
    }
    contains_eos_scalar(input)
}

fn contains_eos_scalar(input: &[u8]) -> bool {
    let mut incoming = 0u32;
    for &byte in input {
        if incoming + byte.leading_ones() >= 30 {
            return true;
        }
        incoming = if byte == 0xFF {
            incoming + 8
        } else {
            byte.trailing_ones()
        };
    }
    false
}

fn simd_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        if std::env::var_os("VEYLOR_SIMD").is_some_and(|value| value == "0") {
            return false;
        }
        #[cfg(target_arch = "x86_64")]
        {
            std::arch::is_x86_feature_detected!("avx2")
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            false
        }
    })
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn contains_eos_avx2(input: &[u8]) -> bool {
    use std::arch::x86_64::{
        __m256i, _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_movemask_epi8, _mm256_set1_epi8,
    };

    let mut index = 0usize;
    let mut previous_last = false;
    while index + 32 <= input.len() {
        // SAFETY: the chunk is in bounds and AVX2 is enabled for this call.
        let chunk = unsafe { _mm256_loadu_si256(input.as_ptr().add(index) as *const __m256i) };
        let ones = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8(-1));
        let mask = _mm256_movemask_epi8(ones) as u32;
        if mask & (mask >> 1) != 0 || (previous_last && mask & 1 != 0) {
            return true;
        }
        previous_last = mask & 0x8000_0000 != 0;
        index += 32;
    }
    let mut incoming = u32::from(previous_last);
    for &byte in &input[index..] {
        if incoming + byte.leading_ones() >= 30 {
            return true;
        }
        incoming = if byte == 0xFF {
            incoming + 8
        } else {
            byte.trailing_ones()
        };
    }
    false
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
