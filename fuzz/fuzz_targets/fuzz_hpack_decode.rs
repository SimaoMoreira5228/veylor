#![no_main]













use libfuzzer_sys::fuzz_target;
use veylor_http2::{
    HpackEntrySlot, HpackHeader, HpackTable, decode_header_block, decode_header_field,
};


const MAX_INPUT: usize = 8_192;

const TABLE_ENTRIES: usize = 64;

const TABLE_SCRATCH: usize = 4_096;

const OUTPUT_HEADERS: usize = 32;

const PROTOCOL_MAX_SIZE: u32 = 4_096;

fn check_block(table: &HpackTable<'_>, out: &[HpackHeader], count: usize, consumed: usize, input_len: usize) {
    assert!(count <= out.len(), "count escapes caller buffer");
    assert!(consumed <= input_len, "consumed escapes input");
    for header in out.iter().take(count) {
        assert!(
            table.resolve(header.field.name).is_some(),
            "decoded name does not resolve in table storage"
        );
        assert!(
            table.resolve(header.field.value).is_some(),
            "decoded value does not resolve in table storage"
        );
    }
}

fuzz_target!(|data: &[u8]| {
    let data = if data.len() > MAX_INPUT {
        &data[..MAX_INPUT]
    } else {
        data
    };

    
    {
        let mut entries = [HpackEntrySlot::EMPTY; TABLE_ENTRIES];
        let mut scratch = [0u8; TABLE_SCRATCH];
        let mut table = HpackTable::new(&mut entries, &mut scratch, PROTOCOL_MAX_SIZE);
        let mut out = [HpackHeader::EMPTY; OUTPUT_HEADERS];
        match decode_header_block(data, &mut table, &mut out) {
            Ok((count, consumed)) => check_block(&table, &out, count, consumed, data.len()),
            Err(_) => {}
        }
    }

    


    {
        let mut entries = [HpackEntrySlot::EMPTY; TABLE_ENTRIES];
        let mut scratch = [0u8; TABLE_SCRATCH];
        let mut table = HpackTable::new(&mut entries, &mut scratch, PROTOCOL_MAX_SIZE);
        let mut offset = 0usize;
        let mut fields = 0usize;
        while offset < data.len() && fields < OUTPUT_HEADERS {
            match decode_header_field(&data[offset..], &mut table) {
                Ok((header, consumed)) => {
                    

                    assert!(consumed > 0, "header-field decode made no progress (finding)");
                    assert!(
                        offset + consumed <= data.len(),
                        "field consumed escapes input"
                    );
                    assert!(
                        table.resolve(header.field.name).is_some()
                            && table.resolve(header.field.value).is_some(),
                        "decoded field does not resolve in table storage"
                    );
                    offset += consumed;
                    fields += 1;
                }
                Err(_) => break,
            }
        }
    }
});
