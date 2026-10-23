use crate::hpack::{
    HpackEntrySlot, HpackError, HpackFieldToEncode, HpackHeader, HpackRepresentation, HpackTable,
    decode_header_block, default_literal_representation, encode_header_block,
    encode_literal_header,
};

fn default_field(name: &'static [u8], value: &'static [u8]) -> HpackFieldToEncode<'static> {
    HpackFieldToEncode {
        name,
        value,
        representation: HpackRepresentation::Default,
        huffman_name: false,
        huffman_value: false,
    }
}

fn response_fields() -> [HpackFieldToEncode<'static>; 5] {
    [
        default_field(b":status", b"200"),
        default_field(b"content-type", b"text/plain"),
        default_field(b"cache-control", b"no-cache"),
        HpackFieldToEncode {
            huffman_value: true,
            ..default_field(b"server", b"veylor")
        },
        HpackFieldToEncode {
            huffman_name: true,
            huffman_value: true,
            ..default_field(b"x-request-id", b"abc-123")
        },
    ]
}

fn assert_same_fields(
    table: &HpackTable<'_>,
    headers: &[HpackHeader],
    expected: &[(&[u8], &[u8])],
) {
    assert_eq!(headers.len(), expected.len());
    for (header, (name, value)) in headers.iter().zip(expected) {
        assert_eq!(table.resolve(header.field.name), Some(*name));
        assert_eq!(table.resolve(header.field.value), Some(*value));
    }
}

#[test]
fn default_policy_round_trips_and_references_the_table_on_repeat() {
    let fields = response_fields();
    let expected = [
        (&b":status"[..], &b"200"[..]),
        (b"content-type", b"text/plain"),
        (b"cache-control", b"no-cache"),
        (b"server", b"veylor"),
        (b"x-request-id", b"abc-123"),
    ];
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut encoder_scratch = [0; 1_024];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 4_096);
    let mut decoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut decoder_scratch = [0; 1_024];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_scratch, 4_096);
    let mut block = [0; 512];
    let mut headers = [HpackHeader::EMPTY; 5];

    let first = encode_header_block(&fields, &mut encoder, &mut block).unwrap();
    let (count, consumed) =
        decode_header_block(&block[..first], &mut decoder, &mut headers).unwrap();
    assert_eq!((count, consumed), (5, first));
    assert_same_fields(&decoder, &headers, &expected);
    assert_eq!(
        headers.map(|header| header.representation),
        [
            HpackRepresentation::Indexed,
            HpackRepresentation::Incremental,
            HpackRepresentation::Incremental,
            HpackRepresentation::Incremental,
            HpackRepresentation::WithoutIndexing,
        ]
    );
    assert_eq!(encoder.len(), decoder.len());
    assert_eq!(encoder.size(), decoder.size());
    assert!(encoder.size() <= encoder.max_size());

    let second = encode_header_block(&fields, &mut encoder, &mut block).unwrap();
    assert!(second < first);
    let (count, consumed) =
        decode_header_block(&block[..second], &mut decoder, &mut headers).unwrap();
    assert_eq!((count, consumed), (5, second));
    assert_same_fields(&decoder, &headers, &expected);
    assert!(
        headers[1..4]
            .iter()
            .all(|header| header.representation == HpackRepresentation::Indexed)
    );
    assert_eq!(encoder.len(), decoder.len());
    assert_eq!(encoder.size(), decoder.size());
    assert!(encoder.size() <= encoder.max_size());
}

#[test]
fn default_policy_never_indexes_sensitive_fields() {
    let fields = [
        default_field(b"authorization", b"Bearer token123"),
        default_field(b"proxy-authorization", b"Basic abc"),
        default_field(b"cookie", b"session=xyz"),
        default_field(b"set-cookie", b"id=1"),
    ];
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut encoder_scratch = [0; 1_024];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 4_096);
    let mut decoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut decoder_scratch = [0; 1_024];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_scratch, 4_096);
    let mut block = [0; 512];
    let mut headers = [HpackHeader::EMPTY; 4];

    let first = encode_header_block(&fields, &mut encoder, &mut block).unwrap();
    assert_eq!(block[0] & 0xf0, 0x10);
    assert!(encoder.is_empty());
    assert_eq!(encoder.size(), 0);
    let (count, _) = decode_header_block(&block[..first], &mut decoder, &mut headers).unwrap();
    assert_eq!(count, 4);
    assert!(
        headers
            .iter()
            .all(|header| header.representation == HpackRepresentation::NeverIndexed)
    );
    assert!(decoder.is_empty());

    let second = encode_header_block(&fields, &mut encoder, &mut block).unwrap();
    assert_eq!(second, first);
    assert!(encoder.is_empty());
}

#[test]
fn sensitive_fields_stay_unindexed_even_with_an_exact_table_match() {
    let secret = [HpackFieldToEncode {
        name: b"authorization",
        value: b"Bearer token123",
        representation: HpackRepresentation::Incremental,
        huffman_name: false,
        huffman_value: false,
    }];
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut encoder_scratch = [0; 1_024];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 4_096);
    let mut block = [0; 512];
    encode_header_block(&secret, &mut encoder, &mut block).unwrap();
    let size = encoder.size();
    assert_ne!(size, 0);

    let probe = [default_field(b"authorization", b"Bearer token123")];
    let written = encode_header_block(&probe, &mut encoder, &mut block).unwrap();
    assert_eq!(block[0] & 0x80, 0);
    assert_eq!(block[0] & 0xf0, 0x10);
    assert_eq!(encoder.size(), size);

    let mut decoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut decoder_scratch = [0; 1_024];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_scratch, 4_096);
    let mut headers = [HpackHeader::EMPTY; 1];
    let (count, _) = decode_header_block(&block[..written], &mut decoder, &mut headers).unwrap();
    assert_eq!(count, 1);
    assert_eq!(
        decoder.resolve(headers[0].field.value),
        Some(&b"Bearer token123"[..])
    );
    assert_eq!(headers[0].representation, HpackRepresentation::NeverIndexed);
}

#[test]
fn explicit_representations_override_the_policy() {
    let fields = [
        HpackFieldToEncode {
            representation: HpackRepresentation::WithoutIndexing,
            ..default_field(b"content-type", b"text/plain")
        },
        HpackFieldToEncode {
            representation: HpackRepresentation::NeverIndexed,
            ..default_field(b"server", b"veylor")
        },
        HpackFieldToEncode {
            representation: HpackRepresentation::Incremental,
            ..default_field(b"x-custom", b"v")
        },
        default_field(b"x-other", b"w"),
    ];
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut encoder_scratch = [0; 1_024];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 4_096);
    let mut decoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut decoder_scratch = [0; 1_024];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_scratch, 4_096);
    let mut block = [0; 512];
    let mut headers = [HpackHeader::EMPTY; 4];

    let written = encode_header_block(&fields, &mut encoder, &mut block).unwrap();
    let (count, _) = decode_header_block(&block[..written], &mut decoder, &mut headers).unwrap();
    assert_eq!(count, 4);
    assert_eq!(
        headers.map(|header| header.representation),
        [
            HpackRepresentation::WithoutIndexing,
            HpackRepresentation::NeverIndexed,
            HpackRepresentation::Incremental,
            HpackRepresentation::WithoutIndexing,
        ]
    );
    assert_eq!(encoder.len(), 1);
    assert_eq!(encoder.len(), decoder.len());
    assert_eq!(encoder.size(), decoder.size());
}

#[test]
fn default_policy_upgrades_static_exact_matches_to_indexed() {
    let fields = [
        default_field(b":method", b"GET"),
        default_field(b":scheme", b"https"),
        default_field(b":path", b"/"),
    ];
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut encoder_scratch = [0; 256];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 4_096);
    let mut block = [0; 64];

    let written = encode_header_block(&fields, &mut encoder, &mut block).unwrap();
    assert_eq!(&block[..written], &[0x82, 0x87, 0x84]);
    assert!(encoder.is_empty());

    let mut decoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut decoder_scratch = [0; 256];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_scratch, 4_096);
    let mut headers = [HpackHeader::EMPTY; 3];
    let (count, _) = decode_header_block(&block[..written], &mut decoder, &mut headers).unwrap();
    assert_eq!(count, 3);
    assert!(
        headers
            .iter()
            .all(|header| header.representation == HpackRepresentation::Indexed)
    );
}

#[test]
fn mixed_default_and_explicit_blocks_stay_encoder_decoder_in_sync() {
    let fields = [
        default_field(b":status", b"200"),
        default_field(b"content-type", b"application/json"),
        default_field(b"content-type", b"application/json"),
        default_field(b"authorization", b"s3cr3t"),
        HpackFieldToEncode {
            representation: HpackRepresentation::WithoutIndexing,
            ..default_field(b"x-custom", b"v")
        },
        HpackFieldToEncode {
            representation: HpackRepresentation::Incremental,
            ..default_field(b"x-trace", b"t")
        },
    ];
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut encoder_scratch = [0; 1_024];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 4_096);
    let mut decoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut decoder_scratch = [0; 1_024];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_scratch, 4_096);
    let mut block = [0; 512];
    let mut headers = [HpackHeader::EMPTY; 6];

    let first = encode_header_block(&fields, &mut encoder, &mut block).unwrap();
    let (count, _) = decode_header_block(&block[..first], &mut decoder, &mut headers).unwrap();
    assert_eq!(count, 6);
    assert_eq!(
        headers.map(|header| header.representation),
        [
            HpackRepresentation::Indexed,
            HpackRepresentation::Incremental,
            HpackRepresentation::Indexed,
            HpackRepresentation::NeverIndexed,
            HpackRepresentation::WithoutIndexing,
            HpackRepresentation::Incremental,
        ]
    );
    assert_eq!(encoder.len(), decoder.len());
    assert_eq!(encoder.size(), decoder.size());

    let second = encode_header_block(&fields, &mut encoder, &mut block).unwrap();
    assert!(second < first);
    let (count, _) = decode_header_block(&block[..second], &mut decoder, &mut headers).unwrap();
    assert_eq!(count, 6);
    assert_eq!(encoder.len(), decoder.len());
    assert_eq!(encoder.size(), decoder.size());
}

#[test]
fn repeated_policy_encodes_stay_within_peer_table_bounds() {
    let fields = [
        default_field(b":status", b"200"),
        default_field(b"content-type", b"text/html"),
        default_field(b"cache-control", b"max-age=60"),
        default_field(b"server", b"veylor"),
        default_field(b"authorization", b"Bearer rotating"),
    ];
    let mut encoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut encoder_scratch = [0; 1_024];
    let mut encoder = HpackTable::new(&mut encoder_entries, &mut encoder_scratch, 256);
    let mut decoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut decoder_scratch = [0; 1_024];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_scratch, 256);
    let mut block = [0; 512];
    let mut headers = [HpackHeader::EMPTY; 5];

    for _ in 0..30 {
        let written = encode_header_block(&fields, &mut encoder, &mut block).unwrap();
        assert!(encoder.size() <= encoder.max_size());
        let (count, _) =
            decode_header_block(&block[..written], &mut decoder, &mut headers).unwrap();
        assert_eq!(count, 5);
        assert_eq!(decoder.size(), encoder.size());
        assert!(decoder.size() <= decoder.max_size());
    }
}

#[test]
fn default_literal_representation_maps_names_case_insensitively() {
    for name in [
        b"authorization" as &[u8],
        b"proxy-authorization",
        b"cookie",
        b"set-cookie",
    ] {
        assert_eq!(
            default_literal_representation(name),
            HpackRepresentation::NeverIndexed
        );
    }
    assert_eq!(
        default_literal_representation(b"Authorization"),
        HpackRepresentation::NeverIndexed
    );
    assert_eq!(
        default_literal_representation(b"Cookie"),
        HpackRepresentation::NeverIndexed
    );
    for name in [
        b":method" as &[u8],
        b":scheme",
        b":authority",
        b":status",
        b"content-type",
        b"content-encoding",
        b"cache-control",
        b"server",
    ] {
        assert_eq!(
            default_literal_representation(name),
            HpackRepresentation::Incremental
        );
    }
    assert_eq!(
        default_literal_representation(b"Content-Type"),
        HpackRepresentation::Incremental
    );
    assert_eq!(
        default_literal_representation(b"SERVER"),
        HpackRepresentation::Incremental
    );
    for name in [
        b"x-request-id" as &[u8],
        b"date",
        b"etag",
        b":path",
        b"content-length",
    ] {
        assert_eq!(
            default_literal_representation(name),
            HpackRepresentation::WithoutIndexing
        );
    }
}

#[test]
fn literal_encoder_rejects_unresolved_default() {
    let mut output = [0; 64];
    assert_eq!(
        encode_literal_header(
            HpackRepresentation::Default,
            0,
            b"content-type",
            b"text/plain",
            false,
            false,
            &mut output,
        ),
        Err(HpackError::InvalidRepresentation)
    );
}
