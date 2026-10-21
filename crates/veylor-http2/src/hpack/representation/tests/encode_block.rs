use super::assert_field_index;
use crate::hpack::{
    HpackEntrySlot, HpackError, HpackFieldToEncode, HpackHeader, HpackRepresentation, HpackTable,
    HpackTableError, decode_header_block, decode_hpack_integer, encode_header_block,
};

#[test]
fn encodes_borrowed_fields_and_round_trips_without_dynamic_insertion() {
    let fields = [
        HpackFieldToEncode {
            name: b"content-type",
            value: b"application/json",
            representation: HpackRepresentation::WithoutIndexing,
            huffman_name: false,
            huffman_value: false,
        },
        HpackFieldToEncode {
            name: b"x-private",
            value: b"sensitive value",
            representation: HpackRepresentation::NeverIndexed,
            huffman_name: true,
            huffman_value: true,
        },
    ];
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 4];
    let mut encoder_scratch = [0; 128];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 4_096);
    let mut output = [0; 128];

    let written = encode_header_block(&fields, &mut encoder, &mut output).unwrap();
    let (name_index, prefix_length) = decode_hpack_integer(&output[..written], 4).unwrap();
    assert_eq!(
        name_index,
        encoder.find_name_index(b"content-type").unwrap()
    );
    assert_eq!(prefix_length, 2);
    assert_eq!(encoder.len(), 0);

    let mut decoder_entries = [HpackEntrySlot::EMPTY; 4];
    let mut decoder_scratch = [0; 128];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_scratch, 4_096);
    let mut headers = [HpackHeader::EMPTY; 2];
    assert_eq!(
        decode_header_block(&output[..written], &mut decoder, &mut headers),
        Ok((2, written))
    );
    assert_eq!(
        headers[0].representation,
        HpackRepresentation::WithoutIndexing
    );
    assert_eq!(headers[1].representation, HpackRepresentation::NeverIndexed);
    assert_eq!(
        decoder.resolve(headers[0].field.name),
        Some(&b"content-type"[..])
    );
    assert_eq!(
        decoder.resolve(headers[0].field.value),
        Some(&b"application/json"[..])
    );
    assert_eq!(
        decoder.resolve(headers[1].field.name),
        Some(&b"x-private"[..])
    );
    assert_eq!(
        decoder.resolve(headers[1].field.value),
        Some(&b"sensitive value"[..])
    );
    assert!(decoder.is_empty());
}

#[test]
fn indexes_prior_incremental_fields_and_evicts_oldest_entries_in_order() {
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 2];
    let mut encoder_scratch = [0; 128];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 70);
    encoder.insert_bytes(b"a", b"1").unwrap();
    encoder.insert_bytes(b"b", b"2").unwrap();
    let fields = [
        HpackFieldToEncode {
            name: b"c",
            value: b"3",
            representation: HpackRepresentation::Incremental,
            huffman_name: false,
            huffman_value: false,
        },
        HpackFieldToEncode {
            name: b"b",
            value: b"3",
            representation: HpackRepresentation::Incremental,
            huffman_name: false,
            huffman_value: false,
        },
        HpackFieldToEncode {
            name: b"c",
            value: b"4",
            representation: HpackRepresentation::Incremental,
            huffman_name: false,
            huffman_value: false,
        },
    ];
    let mut output = [0; 32];

    let written = encode_header_block(&fields, &mut encoder, &mut output).unwrap();

    assert_eq!(&output[5..7], &[0x7f, 0]);
    assert_eq!(&output[9..11], &[0x7f, 0]);
    assert_eq!(encoder.len(), 2);
    assert_eq!(encoder.size(), 68);
    assert_eq!(encoder.scratch_used(), 10);
    assert_field_index(&encoder, 62, b"c", b"4");
    assert_field_index(&encoder, 63, b"b", b"3");

    let mut decoder_entries = [HpackEntrySlot::EMPTY; 2];
    let mut decoder_scratch = [0; 128];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_scratch, 70);
    decoder.insert_bytes(b"a", b"1").unwrap();
    decoder.insert_bytes(b"b", b"2").unwrap();
    let mut headers = [HpackHeader::EMPTY; 3];
    assert_eq!(
        decode_header_block(&output[..written], &mut decoder, &mut headers),
        Ok((3, written))
    );
    assert_field_index(&decoder, 62, b"c", b"4");
    assert_field_index(&decoder, 63, b"b", b"3");
}

#[test]
fn evicts_mixed_size_original_entries_while_incremental_entries_are_newer() {
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 3];
    let mut encoder_scratch = [0; 192];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 150);
    encoder.insert_bytes(b"l", &[b'a'; 57]).unwrap();
    encoder.insert_bytes(b"s", b"1").unwrap();
    let fields = [
        HpackFieldToEncode {
            name: b"c",
            value: &[b'c'; 17],
            representation: HpackRepresentation::Incremental,
            huffman_name: false,
            huffman_value: false,
        },
        HpackFieldToEncode {
            name: b"d",
            value: &[b'd'; 47],
            representation: HpackRepresentation::Incremental,
            huffman_name: false,
            huffman_value: false,
        },
    ];
    let mut output = [0; 192];

    let written = encode_header_block(&fields, &mut encoder, &mut output).unwrap();

    assert_eq!(encoder.len(), 2);
    assert_eq!(encoder.size(), 130);
    assert_eq!(encoder.scratch_used(), 126);
    assert_field_index(&encoder, 62, b"d", &[b'd'; 47]);
    assert_field_index(&encoder, 63, b"c", &[b'c'; 17]);

    let mut decoder_entries = [HpackEntrySlot::EMPTY; 3];
    let mut decoder_scratch = [0; 192];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_scratch, 150);
    decoder.insert_bytes(b"l", &[b'a'; 57]).unwrap();
    decoder.insert_bytes(b"s", b"1").unwrap();
    let mut headers = [HpackHeader::EMPTY; 2];
    assert_eq!(
        decode_header_block(&output[..written], &mut decoder, &mut headers),
        Ok((2, written))
    );
    assert_eq!(decoder.len(), encoder.len());
    assert_eq!(decoder.size(), encoder.size());
    assert_eq!(decoder.scratch_used(), encoder.scratch_used());
    assert_field_index(&decoder, 62, b"d", &[b'd'; 47]);
    assert_field_index(&decoder, 63, b"c", &[b'c'; 17]);
}

#[test]
fn oversized_incremental_entry_clears_the_table_without_using_encoder_scratch() {
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 2];
    let mut encoder_scratch = [0; 128];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 64);
    encoder.insert_bytes(b"b", b"2").unwrap();
    let field = [HpackFieldToEncode {
        name: &[b'x'; 40],
        value: b"",
        representation: HpackRepresentation::Incremental,
        huffman_name: false,
        huffman_value: false,
    }];
    let mut output = [0; 64];

    let written = encode_header_block(&field, &mut encoder, &mut output).unwrap();

    assert!(encoder.is_empty());
    assert_eq!(encoder.size(), 0);
    assert_eq!(encoder.scratch_used(), 2);

    let mut decoder_entries = [HpackEntrySlot::EMPTY; 2];
    let mut decoder_scratch = [0; 128];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_scratch, 64);
    decoder.insert_bytes(b"b", b"2").unwrap();
    let mut headers = [HpackHeader::EMPTY; 1];
    assert_eq!(
        decode_header_block(&output[..written], &mut decoder, &mut headers),
        Ok((1, written))
    );
    assert!(decoder.is_empty());
}

#[test]
fn preflights_the_complete_block_before_writing_pending_updates() {
    let fields = [HpackFieldToEncode {
        name: b"content-type",
        value: b"text/plain",
        representation: HpackRepresentation::WithoutIndexing,
        huffman_name: false,
        huffman_value: false,
    }];
    let mut full_entries = [HpackEntrySlot::EMPTY; 4];
    let mut full_scratch = [0; 64];
    let mut full_table = HpackTable::new(&mut full_entries, &mut full_scratch, 4_096);
    full_table.set_max_size(100).unwrap();
    full_table.set_max_size(0).unwrap();
    full_table.set_max_size(256).unwrap();
    let mut full_output = [0; 64];
    let required = encode_header_block(&fields, &mut full_table, &mut full_output).unwrap();
    assert_eq!(&full_output[..4], &[0x20, 0x3f, 0xe1, 0x01]);

    let mut decoder_entries = [HpackEntrySlot::EMPTY; 4];
    let mut decoder_scratch = [0; 64];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_scratch, 4_096);
    let mut headers = [HpackHeader::EMPTY; 1];
    assert_eq!(
        decode_header_block(&full_output[..required], &mut decoder, &mut headers),
        Ok((1, required))
    );
    assert_eq!(decoder.max_size(), 256);

    let mut short_entries = [HpackEntrySlot::EMPTY; 4];
    let mut short_scratch = [0; 64];
    let mut short_table = HpackTable::new(&mut short_entries, &mut short_scratch, 4_096);
    short_table.set_max_size(100).unwrap();
    short_table.set_max_size(0).unwrap();
    short_table.set_max_size(256).unwrap();
    let mut short_output = [0xa5; 64];
    assert_eq!(
        encode_header_block(&fields, &mut short_table, &mut short_output[..required - 1],),
        Err(HpackError::BufferTooSmall { required })
    );
    assert_eq!(short_output, [0xa5; 64]);
    assert_eq!(short_table.signalled_max(), 4_096);
    assert_eq!(short_table.pending_min_size(), Some(0));
}

#[test]
fn rejects_unsupported_representations_before_changing_output_or_table_state() {
    let field = [HpackFieldToEncode {
        name: b"x-state",
        value: b"ready",
        representation: HpackRepresentation::Indexed,
        huffman_name: false,
        huffman_value: false,
    }];
    let mut entries = [HpackEntrySlot::EMPTY; 4];
    let mut scratch = [0; 64];
    let mut table = HpackTable::new(&mut entries, &mut scratch, 4_096);
    table.set_max_size(128).unwrap();
    let mut output = [0xa5; 32];

    assert_eq!(
        encode_header_block(&field, &mut table, &mut output),
        Err(HpackError::InvalidRepresentation)
    );
    assert_eq!(output, [0xa5; 32]);
    assert_eq!(table.signalled_max(), 4_096);
    assert_eq!(table.pending_min_size(), Some(128));
}

#[test]
fn incremental_capacity_errors_leave_output_and_table_state_unchanged() {
    let fields = [
        HpackFieldToEncode {
            name: b"a",
            value: b"1",
            representation: HpackRepresentation::Incremental,
            huffman_name: false,
            huffman_value: false,
        },
        HpackFieldToEncode {
            name: b"b",
            value: b"2",
            representation: HpackRepresentation::Incremental,
            huffman_name: false,
            huffman_value: false,
        },
    ];
    let mut entries = [HpackEntrySlot::EMPTY; 1];
    let mut scratch = [0; 16];
    let mut table = HpackTable::new(&mut entries, &mut scratch, 4_096);
    table.set_max_size(128).unwrap();
    let mut output = [0xa5; 64];

    let mut short_entries = [HpackEntrySlot::EMPTY; 4];
    let mut short_scratch = [0; 16];
    let mut short_table = HpackTable::new(&mut short_entries, &mut short_scratch, 4_096);
    short_table.set_max_size(128).unwrap();
    let mut short_output = [0xa5; 1];
    assert!(matches!(
        encode_header_block(&fields, &mut short_table, &mut short_output),
        Err(HpackError::BufferTooSmall { required }) if required > short_output.len()
    ));
    assert_eq!(short_output, [0xa5; 1]);
    assert!(short_table.is_empty());
    assert_eq!(short_table.scratch_used(), 0);
    assert_eq!(short_table.signalled_max(), 4_096);
    assert_eq!(short_table.pending_min_size(), Some(128));

    assert_eq!(
        encode_header_block(&fields, &mut table, &mut output),
        Err(HpackError::Table(HpackTableError::DescriptorCapacity))
    );
    assert_eq!(output, [0xa5; 64]);
    assert!(table.is_empty());
    assert_eq!(table.scratch_used(), 0);
    assert_eq!(table.signalled_max(), 4_096);
    assert_eq!(table.pending_min_size(), Some(128));

    let mut scratch_limited_entries = [HpackEntrySlot::EMPTY; 4];
    let mut scratch_limited = [0; 3];
    let mut scratch_table =
        HpackTable::new(&mut scratch_limited_entries, &mut scratch_limited, 4_096);
    scratch_table.set_max_size(128).unwrap();

    assert_eq!(
        encode_header_block(&fields, &mut scratch_table, &mut output),
        Err(HpackError::Table(HpackTableError::ScratchCapacity {
            required: 4,
        }))
    );
    assert_eq!(output, [0xa5; 64]);
    assert!(scratch_table.is_empty());
    assert_eq!(scratch_table.scratch_used(), 0);
    assert_eq!(scratch_table.signalled_max(), 4_096);
    assert_eq!(scratch_table.pending_min_size(), Some(128));
}
