mod encode_block;

use super::{
    HpackError, HpackHeader, HpackRepresentation, decode_header_block, decode_header_field,
    encode_indexed_header, encode_literal_header, encode_pending_table_size_updates,
    encode_table_size_update,
};
use crate::hpack::{HpackEntrySlot, HpackTable, HpackTableError, TableInsert};

const RFC_C21: &[u8] = &[
    0x40, 0x0a, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d, 0x6b, 0x65, 0x79, 0x0d, 0x63, 0x75, 0x73,
    0x74, 0x6f, 0x6d, 0x2d, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72,
];
const RFC_C22: &[u8] = &[
    0x04, 0x0c, 0x2f, 0x73, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2f, 0x70, 0x61, 0x74, 0x68,
];
const RFC_C23: &[u8] = &[
    0x10, 0x08, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x06, 0x73, 0x65, 0x63, 0x72, 0x65,
    0x74,
];
const RFC_C24: &[u8] = &[0x82];

const RFC_C31: &[u8] = &[
    0x82, 0x86, 0x84, 0x41, 0x0f, 0x77, 0x77, 0x77, 0x2e, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c, 0x65,
    0x2e, 0x63, 0x6f, 0x6d,
];
const RFC_C32: &[u8] = &[
    0x82, 0x86, 0x84, 0xbe, 0x58, 0x08, 0x6e, 0x6f, 0x2d, 0x63, 0x61, 0x63, 0x68, 0x65,
];
const RFC_C33: &[u8] = &[
    0x82, 0x87, 0x85, 0xbf, 0x40, 0x0a, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d, 0x6b, 0x65, 0x79,
    0x0c, 0x63, 0x75, 0x73, 0x74, 0x6f, 0x6d, 0x2d, 0x76, 0x61, 0x6c, 0x75, 0x65,
];

const RFC_C41: &[u8] = &[
    0x82, 0x86, 0x84, 0x41, 0x8c, 0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4,
    0xff,
];
const RFC_C42: &[u8] = &[
    0x82, 0x86, 0x84, 0xbe, 0x58, 0x86, 0xa8, 0xeb, 0x10, 0x64, 0x9c, 0xbf,
];
const RFC_C43: &[u8] = &[
    0x82, 0x87, 0x85, 0xbf, 0x40, 0x88, 0x25, 0xa8, 0x49, 0xe9, 0x5b, 0xa9, 0x7d, 0x7f, 0x89, 0x25,
    0xa8, 0x49, 0xe9, 0x5b, 0xb8, 0xe8, 0xb4, 0xbf,
];

const RFC_C51: &[u8] = &[
    0x48, 0x03, 0x33, 0x30, 0x32, 0x58, 0x07, 0x70, 0x72, 0x69, 0x76, 0x61, 0x74, 0x65, 0x61, 0x1d,
    0x4d, 0x6f, 0x6e, 0x2c, 0x20, 0x32, 0x31, 0x20, 0x4f, 0x63, 0x74, 0x20, 0x32, 0x30, 0x31, 0x33,
    0x20, 0x32, 0x30, 0x3a, 0x31, 0x33, 0x3a, 0x32, 0x31, 0x20, 0x47, 0x4d, 0x54, 0x6e, 0x17, 0x68,
    0x74, 0x74, 0x70, 0x73, 0x3a, 0x2f, 0x2f, 0x77, 0x77, 0x77, 0x2e, 0x65, 0x78, 0x61, 0x6d, 0x70,
    0x6c, 0x65, 0x2e, 0x63, 0x6f, 0x6d,
];
const RFC_C52: &[u8] = &[0x48, 0x03, 0x33, 0x30, 0x37, 0xc1, 0xc0, 0xbf];
const RFC_C53: &[u8] = &[
    0x88, 0xc1, 0x61, 0x1d, 0x4d, 0x6f, 0x6e, 0x2c, 0x20, 0x32, 0x31, 0x20, 0x4f, 0x63, 0x74, 0x20,
    0x32, 0x30, 0x31, 0x33, 0x20, 0x32, 0x30, 0x3a, 0x31, 0x33, 0x3a, 0x32, 0x32, 0x20, 0x47, 0x4d,
    0x54, 0xc0, 0x5a, 0x04, 0x67, 0x7a, 0x69, 0x70, 0x77, 0x38, 0x66, 0x6f, 0x6f, 0x3d, 0x41, 0x53,
    0x44, 0x4a, 0x4b, 0x48, 0x51, 0x4b, 0x42, 0x5a, 0x58, 0x4f, 0x51, 0x57, 0x45, 0x4f, 0x50, 0x49,
    0x55, 0x41, 0x58, 0x51, 0x57, 0x45, 0x4f, 0x49, 0x55, 0x3b, 0x20, 0x6d, 0x61, 0x78, 0x2d, 0x61,
    0x67, 0x65, 0x3d, 0x33, 0x36, 0x30, 0x30, 0x3b, 0x20, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e,
    0x3d, 0x31,
];

const RFC_C61: &[u8] = &[
    0x48, 0x82, 0x64, 0x02, 0x58, 0x85, 0xae, 0xc3, 0x77, 0x1a, 0x4b, 0x61, 0x96, 0xd0, 0x7a, 0xbe,
    0x94, 0x10, 0x54, 0xd4, 0x44, 0xa8, 0x20, 0x05, 0x95, 0x04, 0x0b, 0x81, 0x66, 0xe0, 0x82, 0xa6,
    0x2d, 0x1b, 0xff, 0x6e, 0x91, 0x9d, 0x29, 0xad, 0x17, 0x18, 0x63, 0xc7, 0x8f, 0x0b, 0x97, 0xc8,
    0xe9, 0xae, 0x82, 0xae, 0x43, 0xd3,
];
const RFC_C62: &[u8] = &[0x48, 0x83, 0x64, 0x0e, 0xff, 0xc1, 0xc0, 0xbf];
const RFC_C63: &[u8] = &[
    0x88, 0xc1, 0x61, 0x96, 0xd0, 0x7a, 0xbe, 0x94, 0x10, 0x54, 0xd4, 0x44, 0xa8, 0x20, 0x05, 0x95,
    0x04, 0x0b, 0x81, 0x66, 0xe0, 0x84, 0xa6, 0x2d, 0x1b, 0xff, 0xc0, 0x5a, 0x83, 0x9b, 0xd9, 0xab,
    0x77, 0xad, 0x94, 0xe7, 0x82, 0x1d, 0xd7, 0xf2, 0xe6, 0xc7, 0xb3, 0x35, 0xdf, 0xdf, 0xcd, 0x5b,
    0x39, 0x60, 0xd5, 0xaf, 0x27, 0x08, 0x7f, 0x36, 0x72, 0xc1, 0xab, 0x27, 0x0f, 0xb5, 0x29, 0x1f,
    0x95, 0x87, 0x31, 0x60, 0x65, 0xc0, 0x03, 0xed, 0x4e, 0xe5, 0xb1, 0x06, 0x3d, 0x50, 0x07,
];

#[test]
fn encodes_rfc_representation_golden_vectors() {
    let mut output = [0; 64];
    assert_eq!(encode_indexed_header(2, &mut output), Ok(1));
    assert_eq!(output[0], 0x82);

    let written = encode_table_size_update(256, &mut output).unwrap();
    assert_eq!(&output[..written], &[0x3f, 0xe1, 0x01]);

    let written = encode_literal_header(
        HpackRepresentation::Incremental,
        0,
        b"custom-key",
        b"custom-header",
        false,
        false,
        &mut output,
    )
    .unwrap();
    assert_eq!(&output[..written], RFC_C21);

    let written = encode_literal_header(
        HpackRepresentation::WithoutIndexing,
        4,
        b"",
        b"/sample/path",
        false,
        false,
        &mut output,
    )
    .unwrap();
    assert_eq!(&output[..written], RFC_C22);

    let written = encode_literal_header(
        HpackRepresentation::NeverIndexed,
        0,
        b"password",
        b"secret",
        false,
        false,
        &mut output,
    )
    .unwrap();
    assert_eq!(&output[..written], RFC_C23);
}

#[test]
fn decodes_literal_representations_and_preserves_indexing_kind() {
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 256];
    let mut table = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut headers = [HpackHeader::EMPTY; 2];

    let (header, consumed) = decode_header_field(RFC_C21, &mut table).unwrap();
    assert_eq!(consumed, RFC_C21.len());
    assert_field(&table, header, b"custom-key", b"custom-header");
    assert_eq!(header.representation, HpackRepresentation::Incremental);
    assert_field_index(&table, 62, b"custom-key", b"custom-header");

    let (header, consumed) = decode_header_field(RFC_C22, &mut table).unwrap();
    assert_eq!(consumed, RFC_C22.len());
    assert_field(&table, header, b":path", b"/sample/path");
    assert_eq!(header.representation, HpackRepresentation::WithoutIndexing);

    let (header, consumed) = decode_header_field(RFC_C23, &mut table).unwrap();
    assert_eq!(consumed, RFC_C23.len());
    assert_field(&table, header, b"password", b"secret");
    assert_eq!(header.representation, HpackRepresentation::NeverIndexed);

    let (count, consumed) = decode_header_block(RFC_C24, &mut table, &mut headers).unwrap();
    assert_eq!((count, consumed), (1, RFC_C24.len()));
    assert_field(&table, headers[0], b":method", b"GET");
    assert_eq!(headers[0].representation, HpackRepresentation::Indexed);
}

#[test]
fn decodes_rfc_request_blocks_without_huffman() {
    assert_request_vectors([RFC_C31, RFC_C32, RFC_C33]);
}

#[test]
fn decodes_rfc_request_blocks_with_huffman() {
    assert_request_vectors([RFC_C41, RFC_C42, RFC_C43]);
}

fn assert_request_vectors(blocks: [&[u8]; 3]) {
    let mut entries = [HpackEntrySlot::EMPTY; 128];
    let mut scratch = [0; 4_096];
    let mut table = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut headers = [HpackHeader::EMPTY; 16];

    let (count, consumed) = decode_header_block(blocks[0], &mut table, &mut headers).unwrap();
    assert_eq!((count, consumed), (4, blocks[0].len()));
    assert_fields(
        &table,
        &headers[..count],
        &[
            (b":method", b"GET", HpackRepresentation::Indexed),
            (b":scheme", b"http", HpackRepresentation::Indexed),
            (b":path", b"/", HpackRepresentation::Indexed),
            (
                b":authority",
                b"www.example.com",
                HpackRepresentation::Incremental,
            ),
        ],
    );
    assert_eq!(table.size(), 57);
    assert_field_index(&table, 62, b":authority", b"www.example.com");

    let (count, consumed) = decode_header_block(blocks[1], &mut table, &mut headers).unwrap();
    assert_eq!((count, consumed), (5, blocks[1].len()));
    assert_fields(
        &table,
        &headers[..count],
        &[
            (b":method", b"GET", HpackRepresentation::Indexed),
            (b":scheme", b"http", HpackRepresentation::Indexed),
            (b":path", b"/", HpackRepresentation::Indexed),
            (
                b":authority",
                b"www.example.com",
                HpackRepresentation::Indexed,
            ),
            (
                b"cache-control",
                b"no-cache",
                HpackRepresentation::Incremental,
            ),
        ],
    );
    assert_eq!(table.size(), 110);
    assert_field_index(&table, 62, b"cache-control", b"no-cache");
    assert_field_index(&table, 63, b":authority", b"www.example.com");

    let (count, consumed) = decode_header_block(blocks[2], &mut table, &mut headers).unwrap();
    assert_eq!((count, consumed), (5, blocks[2].len()));
    assert_fields(
        &table,
        &headers[..count],
        &[
            (b":method", b"GET", HpackRepresentation::Indexed),
            (b":scheme", b"https", HpackRepresentation::Indexed),
            (b":path", b"/index.html", HpackRepresentation::Indexed),
            (
                b":authority",
                b"www.example.com",
                HpackRepresentation::Indexed,
            ),
            (
                b"custom-key",
                b"custom-value",
                HpackRepresentation::Incremental,
            ),
        ],
    );
    assert_eq!(table.size(), 164);
    assert_field_index(&table, 62, b"custom-key", b"custom-value");
    assert_field_index(&table, 63, b"cache-control", b"no-cache");
    assert_field_index(&table, 64, b":authority", b"www.example.com");
}

#[test]
fn decodes_rfc_response_blocks_without_huffman() {
    assert_response_vectors([RFC_C51, RFC_C52, RFC_C53]);
}

#[test]
fn decodes_rfc_response_blocks_with_huffman() {
    assert_response_vectors([RFC_C61, RFC_C62, RFC_C63]);
}

fn assert_response_vectors(blocks: [&[u8]; 3]) {
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 4_096];
    let mut table = HpackTable::new(&mut entries, &mut scratch, 256);
    let mut headers = [HpackHeader::EMPTY; 16];
    let date21 = b"Mon, 21 Oct 2013 20:13:21 GMT";
    let location = b"https://www.example.com";

    let (count, consumed) = decode_header_block(blocks[0], &mut table, &mut headers).unwrap();
    assert_eq!((count, consumed), (4, blocks[0].len()));
    assert_fields(
        &table,
        &headers[..count],
        &[
            (b":status", b"302", HpackRepresentation::Incremental),
            (
                b"cache-control",
                b"private",
                HpackRepresentation::Incremental,
            ),
            (b"date", date21, HpackRepresentation::Incremental),
            (b"location", location, HpackRepresentation::Incremental),
        ],
    );
    assert_eq!(table.size(), 222);
    assert_field_index(&table, 62, b"location", location);
    assert_field_index(&table, 65, b":status", b"302");

    let (count, consumed) = decode_header_block(blocks[1], &mut table, &mut headers).unwrap();
    assert_eq!((count, consumed), (4, blocks[1].len()));
    assert_fields(
        &table,
        &headers[..count],
        &[
            (b":status", b"307", HpackRepresentation::Incremental),
            (b"cache-control", b"private", HpackRepresentation::Indexed),
            (b"date", date21, HpackRepresentation::Indexed),
            (b"location", location, HpackRepresentation::Indexed),
        ],
    );
    assert_eq!(table.size(), 222);
    assert_field_index(&table, 62, b":status", b"307");
    assert_field_index(&table, 63, b"location", location);

    let (count, consumed) = decode_header_block(blocks[2], &mut table, &mut headers).unwrap();
    assert_eq!((count, consumed), (6, blocks[2].len()));
    assert_fields(
        &table,
        &headers[..count],
        &[
            (b":status", b"200", HpackRepresentation::Indexed),
            (b"cache-control", b"private", HpackRepresentation::Indexed),
            (
                b"date",
                b"Mon, 21 Oct 2013 20:13:22 GMT",
                HpackRepresentation::Incremental,
            ),
            (b"location", location, HpackRepresentation::Indexed),
            (
                b"content-encoding",
                b"gzip",
                HpackRepresentation::Incremental,
            ),
            (
                b"set-cookie",
                b"foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1",
                HpackRepresentation::Incremental,
            ),
        ],
    );
    assert_eq!(table.size(), 215);
    assert_eq!(table.len(), 3);
    assert_field_index(
        &table,
        62,
        b"set-cookie",
        b"foo=ASDJKHQKBZXOQWEOPIUAXQWEOIU; max-age=3600; version=1",
    );
    assert_field_index(&table, 63, b"content-encoding", b"gzip");
    assert_field_index(&table, 64, b"date", b"Mon, 21 Oct 2013 20:13:22 GMT");
}

#[test]
fn emits_table_size_updates_at_block_start_and_tracks_pending_minimum() {
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 128];
    let mut table = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut output = [0; 16];

    table.set_max_size(256).unwrap();
    let written = encode_pending_table_size_updates(&mut table, &mut output).unwrap();
    assert_eq!(&output[..written], &[0x3f, 0xe1, 0x01]);
    assert_eq!(table.signalled_max(), 256);
    assert_eq!(table.pending_min_size(), None);

    table.set_max_size(100).unwrap();
    table.set_max_size(0).unwrap();
    table.set_max_size(256).unwrap();
    let mut short_output = [0xa5; 3];
    assert_eq!(
        encode_pending_table_size_updates(&mut table, &mut short_output),
        Err(HpackError::BufferTooSmall { required: 4 })
    );
    assert_eq!(short_output, [0xa5; 3]);
    assert_eq!(table.signalled_max(), 256);
    assert_eq!(table.pending_min_size(), Some(0));

    let written = encode_pending_table_size_updates(&mut table, &mut output).unwrap();
    assert_eq!(&output[..written], &[0x20, 0x3f, 0xe1, 0x01]);
    assert_eq!(table.pending_min_size(), None);

    let mut decoded_entries = [HpackEntrySlot::EMPTY; 8];
    let mut decoded_scratch = [0; 128];
    let mut decoded_table = HpackTable::new(&mut decoded_entries, &mut decoded_scratch, 4_096);
    let mut headers = [HpackHeader::EMPTY; 1];
    assert_eq!(
        decode_header_block(&output[..written], &mut decoded_table, &mut headers),
        Ok((0, written))
    );
    assert_eq!(decoded_table.max_size(), 256);
}

#[test]
fn rejects_size_updates_after_fields_and_indices_outside_the_table() {
    let mut entries = [HpackEntrySlot::EMPTY; 4];
    let mut scratch = [0; 128];
    let mut table = HpackTable::new(&mut entries, &mut scratch, 4_096);
    let mut headers = [HpackHeader::EMPTY; 2];

    assert_eq!(
        decode_header_field(&[0x80], &mut table),
        Err(HpackError::InvalidIndex)
    );
    assert_eq!(
        decode_header_field(&[0xff, 0], &mut table),
        Err(HpackError::InvalidIndex)
    );
    assert_eq!(
        decode_header_block(&[0x82, 0x20], &mut table, &mut headers),
        Err(HpackError::TableSizeUpdateAfterField)
    );
    assert_eq!(
        decode_header_block(&[0x3f, 0xe9, 0x26], &mut table, &mut headers),
        Err(HpackError::Table(HpackTableError::MaxSizeExceedsProtocol))
    );
}

#[test]
fn decoder_requires_a_conforming_update_after_acknowledged_reduction() {
    let mut entries = [HpackEntrySlot::EMPTY; 4];
    let mut scratch = [0; 256];
    let mut table = HpackTable::new(&mut entries, &mut scratch, 4_096);
    table.insert_bytes(b"x", &[b'a'; 128]).unwrap();
    assert!(table.size() > 128);
    table.set_protocol_max_size(128);

    let mut headers = [HpackHeader::EMPTY; 3];
    assert_eq!(
        decode_header_block(&[0x82, 0x86, 0x84], &mut table, &mut headers),
        Err(HpackError::RequiredTableSizeUpdate { maximum: 128 })
    );

    let mut block = [0; 8];
    let update_length = encode_table_size_update(128, &mut block).unwrap();
    block[update_length..update_length + 3].copy_from_slice(&[0x82, 0x86, 0x84]);
    let written = update_length + 3;
    assert_eq!(
        decode_header_block(&block[..written], &mut table, &mut headers),
        Ok((3, written))
    );
    assert_eq!(table.max_size(), 128);
    assert_eq!(table.size(), 0);
    assert_eq!(table.required_size_update(), None);
}

#[test]
fn fails_closed_on_truncated_literal_and_caller_capacity_limits() {
    for end in 1..RFC_C21.len() {
        let mut entries = [HpackEntrySlot::EMPTY; 8];
        let mut scratch = [0; 128];
        let mut table = HpackTable::new(&mut entries, &mut scratch, 4_096);
        let mut headers = [HpackHeader::EMPTY; 1];
        assert!(decode_header_block(&RFC_C21[..end], &mut table, &mut headers).is_err());
        assert!(table.is_empty());
    }

    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 128];
    let mut table = HpackTable::new(&mut entries, &mut scratch, 4_096);
    assert_eq!(
        decode_header_block(RFC_C24, &mut table, &mut []),
        Err(HpackError::HeaderCapacity { required: 1 })
    );
}

#[test]
fn encoder_updates_table_explicitly_before_indexed_references() {
    let name = b"x-state";
    let value = b"ready";
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 4];
    let mut encoder_scratch = [0; 128];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 4_096);

    let mut block = [0; 64];
    let incremental_length = encode_literal_header(
        HpackRepresentation::Incremental,
        0,
        name,
        value,
        false,
        false,
        &mut block,
    )
    .unwrap();
    assert_eq!(encoder.insert_bytes(name, value), Ok(TableInsert::Inserted));

    let indexed_length = encode_indexed_header(62, &mut block[incremental_length..]).unwrap();
    let block_length = incremental_length + indexed_length;

    let mut decoder_entries = [HpackEntrySlot::EMPTY; 4];
    let mut decoder_scratch = [0; 128];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_scratch, 4_096);
    let mut headers = [HpackHeader::EMPTY; 2];
    assert_eq!(
        decode_header_block(&block[..block_length], &mut decoder, &mut headers),
        Ok((2, block_length))
    );
    assert_fields(
        &decoder,
        &headers,
        &[
            (name, value, HpackRepresentation::Incremental),
            (name, value, HpackRepresentation::Indexed),
        ],
    );
}

fn assert_fields(
    table: &HpackTable<'_>,
    headers: &[HpackHeader],
    expected: &[(&[u8], &[u8], HpackRepresentation)],
) {
    assert_eq!(headers.len(), expected.len());
    for (header, (name, value, representation)) in headers.iter().zip(expected) {
        assert_eq!(table.resolve(header.field.name), Some(*name));
        assert_eq!(table.resolve(header.field.value), Some(*value));
        assert_eq!(header.representation, *representation);
    }
}

fn assert_field(table: &HpackTable<'_>, header: HpackHeader, name: &[u8], value: &[u8]) {
    assert_eq!(table.resolve(header.field.name), Some(name));
    assert_eq!(table.resolve(header.field.value), Some(value));
}

fn assert_field_index(table: &HpackTable<'_>, index: u64, name: &[u8], value: &[u8]) {
    let field = table.get(index).unwrap();
    assert_eq!(table.resolve(field.name), Some(name));
    assert_eq!(table.resolve(field.value), Some(value));
}
