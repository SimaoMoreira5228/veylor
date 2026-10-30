#![no_main]
use libfuzzer_sys::fuzz_target;
use veylor_http2::{
    HpackEntrySlot, HpackFieldToEncode, HpackHeader, HpackRepresentation, HpackTable,
    decode_header_block, encode_header_block,
};


const MAX_INPUT: usize = 2_048;


const TABLE_ENTRIES: usize = 32;

const TABLE_SCRATCH: usize = 8_192;

const BLOCK_OUT: usize = 8_192;

const OUTPUT_HEADERS: usize = 16;

const MAX_FIELDS_PER_BLOCK: usize = 8;

const MAX_NAME_LEN: usize = 32;

const MAX_VALUE_LEN: usize = 64;

const PROTOCOL_SIZES: [u32; 5] = [0, 64, 256, 1_024, 4_096];

const DICT_NAMES: [&[u8]; 12] = [
    b":method",
    b":path",
    b":authority",
    b":scheme",
    b":status",
    b"content-type",
    b"cache-control",
    b"cookie",
    b"authorization",
    b"x-custom",
    b"a",
    b"date",
];

const DICT_VALUES: [&[u8]; 13] = [
    b"GET",
    b"POST",
    b"/",
    b"/index.html",
    b"http",
    b"https",
    b"200",
    b"no-cache",
    b"text/plain",
    b"private",
    b"secret-value",
    b"1",
    b"",
];

fn representation_of(ctrl: u8) -> HpackRepresentation {
    match ctrl % 5 {
        0 => HpackRepresentation::Indexed,
        1 => HpackRepresentation::Incremental,
        2 => HpackRepresentation::WithoutIndexing,
        3 => HpackRepresentation::NeverIndexed,
        _ => HpackRepresentation::Default,
    }
}

struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn next(&mut self) -> Option<u8> {
        let byte = *self.data.get(self.pos)?;
        self.pos += 1;
        Some(byte)
    }

    fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(len)?;
        let slice = self.data.get(self.pos..end)?;
        self.pos = end;
        Some(slice)
    }
}

fn parse_field<'a>(cursor: &mut Cursor<'a>) -> Option<HpackFieldToEncode<'a>> {
    let ctrl = cursor.next()?;
    let sel_name = cursor.next()?;
    let sel_value = cursor.next()?;
    let name = if ctrl & 0x20 != 0 {
        DICT_NAMES[usize::from(sel_name) % DICT_NAMES.len()]
    } else {
        cursor.take(usize::from(sel_name) % (MAX_NAME_LEN + 1))?
    };
    let value = if ctrl & 0x40 != 0 {
        DICT_VALUES[usize::from(sel_value) % DICT_VALUES.len()]
    } else {
        cursor.take(usize::from(sel_value) % (MAX_VALUE_LEN + 1))?
    };
    Some(HpackFieldToEncode {
        name,
        value,
        representation: representation_of(ctrl),
        huffman_name: ctrl & 0x08 != 0,
        huffman_value: ctrl & 0x10 != 0,
    })
}

fn is_capacity_skip(error: veylor_http2::HpackError) -> bool {
    use veylor_http2::HpackError;
    matches!(
        error,
        HpackError::InvalidIndex
            | HpackError::BufferTooSmall { .. }
            | HpackError::HeaderCapacity { .. }
            | HpackError::Table(
                veylor_http2::HpackTableError::DescriptorCapacity
                    | veylor_http2::HpackTableError::ScratchCapacity { .. }
            )
    )
}

fn check_round_trip(
    fields: &[HpackFieldToEncode<'_>],
    encoder: &mut HpackTable<'_>,
    decoder: &mut HpackTable<'_>,
    block: &mut [u8],
    out: &mut [HpackHeader],
) {
    let written = match encode_header_block(fields, encoder, block) {
        Ok(written) => written,
        Err(error) if is_capacity_skip(error) => return,
        Err(error) => panic!("encoder rejected bounded field list: {error:?} (finding)"),
    };
    

    let (count, consumed) = decode_header_block(&block[..written], decoder, out)
        .unwrap_or_else(|error| panic!("round-trip decode failed: {error:?} (finding)"));
    assert_eq!(count, fields.len(), "field count mismatch (finding)");
    assert_eq!(consumed, written, "decode did not consume block (finding)");
    for (index, field) in fields.iter().enumerate() {
        let decoded = &out[index];
        assert_eq!(
            decoder.resolve(decoded.field.name),
            Some(field.name),
            "name bytes mismatch at field {index} (finding)"
        );
        assert_eq!(
            decoder.resolve(decoded.field.value),
            Some(field.value),
            "value bytes mismatch at field {index} (finding)"
        );
        if field.representation != HpackRepresentation::Default {
            assert_eq!(
                decoded.representation, field.representation,
                "explicit representation not preserved at field {index} (finding)"
            );
        }
    }
    assert_eq!(
        encoder.len(),
        decoder.len(),
        "dynamic-table entry count diverged (finding)"
    );
    assert_eq!(
        encoder.size(),
        decoder.size(),
        "dynamic-table size diverged (finding)"
    );
    assert_eq!(
        encoder.max_size(),
        decoder.max_size(),
        "dynamic-table max size diverged (finding)"
    );
}

fuzz_target!(|data: &[u8]| {
    let data = if data.len() > MAX_INPUT {
        &data[..MAX_INPUT]
    } else {
        data
    };
    if data.len() < 3 {
        return;
    }
    let config = data[0];
    let block_counts = [
        data[1] % (MAX_FIELDS_PER_BLOCK as u8 + 1),
        data[2] % (MAX_FIELDS_PER_BLOCK as u8 + 1),
    ];
    let protocol_max = PROTOCOL_SIZES[usize::from(config & 0x07) % PROTOCOL_SIZES.len()];

    let mut encoder_entries = [HpackEntrySlot::EMPTY; TABLE_ENTRIES];
    let mut encoder_scratch = [0u8; TABLE_SCRATCH];
    let mut decoder_entries = [HpackEntrySlot::EMPTY; TABLE_ENTRIES];
    let mut decoder_scratch = [0u8; TABLE_SCRATCH];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, protocol_max);
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_scratch, protocol_max);

    

    match (config >> 3) & 0x03 {
        1 => {
            let half = u64::from(protocol_max) / 2;
            encoder
                .set_max_size(half)
                .expect("halving protocol max is legal");
        }
        2 => {
            encoder.set_max_size(0).expect("zero max is legal");
            encoder
                .set_max_size(u64::from(protocol_max))
                .expect("restoring protocol max is legal");
        }
        3 => {
            encoder
                .set_max_size(u64::from(protocol_max))
                .expect("no-op max set is legal");
        }
        _ => {}
    }

    let mut block = [0u8; BLOCK_OUT];
    let mut out = [HpackHeader::EMPTY; OUTPUT_HEADERS];
    let mut cursor = Cursor {
        data,
        pos: 3,
    };
    for want in block_counts {
        

        let dummy = HpackFieldToEncode {
            name: &data[..0],
            value: &data[..0],
            representation: HpackRepresentation::WithoutIndexing,
            huffman_name: false,
            huffman_value: false,
        };
        let mut fields = [dummy; MAX_FIELDS_PER_BLOCK];
        let mut field_count = 0usize;
        for slot in fields.iter_mut().take(usize::from(want)) {
            match parse_field(&mut cursor) {
                Some(field) => {
                    *slot = field;
                    field_count += 1;
                }
                None => break,
            }
        }
        


        if field_count == 0 && cursor.pos >= data.len() && want != 0 {
            break;
        }
        check_round_trip(
            &fields[..field_count],
            &mut encoder,
            &mut decoder,
            &mut block,
            &mut out,
        );
    }
});
