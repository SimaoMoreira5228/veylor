use super::{HPACK_STATIC_TABLE_LEN, HpackEntrySlot, HpackTable, HpackTableError, TableInsert};

#[test]
fn looks_up_static_entries_and_rejects_invalid_indices() {
    let mut entries = [HpackEntrySlot::EMPTY; 4];
    let mut scratch = [0; 128];
    let table = HpackTable::new(&mut entries, &mut scratch, 4_096);

    assert_field(&table, 1, b":authority", b"");
    assert_field(&table, 2, b":method", b"GET");
    assert_field(&table, 16, b"accept-encoding", b"gzip, deflate");
    assert_field(&table, 24, b"cache-control", b"");
    assert_field(&table, 33, b"date", b"");
    assert_field(&table, 46, b"location", b"");
    assert_field(&table, 55, b"set-cookie", b"");
    assert_field(&table, 61, b"www-authenticate", b"");

    assert_eq!(table.get(0), None);
    assert_eq!(table.get(HPACK_STATIC_TABLE_LEN as u64 + 1), None);
    assert_eq!(table.find_name_index(b"cache-control"), Some(24));
    assert_eq!(table.find_exact_index(b":method", b"POST"), Some(3));
    assert_eq!(table.find_exact_index(b":method", b"PATCH"), None);
}

#[test]
fn inserts_newest_first_with_raw_octet_sizes_and_eviction() {
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut table = HpackTable::new(&mut entries, &mut scratch, 256);

    assert_eq!(
        table.insert_bytes(b":status", b"302"),
        Ok(TableInsert::Inserted)
    );
    assert_eq!(
        table.insert_bytes(b"cache-control", b"private"),
        Ok(TableInsert::Inserted)
    );
    assert_eq!(
        table.insert_bytes(b"date", b"Mon, 21 Oct 2013 20:13:21 GMT"),
        Ok(TableInsert::Inserted)
    );
    assert_eq!(
        table.insert_bytes(b"location", b"https://www.example.com"),
        Ok(TableInsert::Inserted)
    );

    assert_eq!(table.len(), 4);
    assert_eq!(table.size(), 222);
    assert_field(&table, 62, b"location", b"https://www.example.com");
    assert_field(&table, 63, b"date", b"Mon, 21 Oct 2013 20:13:21 GMT");
    assert_field(&table, 64, b"cache-control", b"private");
    assert_field(&table, 65, b":status", b"302");
    assert_eq!(table.get(66), None);

    assert_eq!(
        table.insert_bytes(b":status", b"307"),
        Ok(TableInsert::Inserted)
    );
    assert_eq!(table.len(), 4);
    assert_eq!(table.size(), 222);
    assert_field(&table, 62, b":status", b"307");
    assert_field(&table, 63, b"location", b"https://www.example.com");
}

#[test]
fn accepts_duplicate_entries_and_finds_the_lowest_matching_index() {
    let mut entries = [HpackEntrySlot::EMPTY; 4];
    let mut scratch = [0; 128];
    let mut table = HpackTable::new(&mut entries, &mut scratch, 256);

    assert_eq!(
        table.insert_bytes(b"x-name", b"value"),
        Ok(TableInsert::Inserted)
    );
    assert_eq!(
        table.insert_bytes(b"x-name", b"value"),
        Ok(TableInsert::Inserted)
    );
    assert_eq!(table.len(), 2);
    assert_eq!(table.find_exact_index(b"x-name", b"value"), Some(62));
    assert_field(&table, 62, b"x-name", b"value");
    assert_field(&table, 63, b"x-name", b"value");
}

#[test]
fn discards_oversized_entries_and_empties_the_table() {
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut table = HpackTable::new(&mut entries, &mut scratch, 256);
    table.insert_bytes(b":status", b"302").unwrap();
    let scratch_used = table.scratch_used();
    let large_value = [b'x'; 300];

    assert_eq!(
        table.insert_bytes(b"set-cookie", &large_value),
        Ok(TableInsert::DiscardedOversized)
    );
    assert!(table.is_empty());
    assert_eq!(table.size(), 0);
    assert_eq!(table.get(62), None);
    assert_eq!(table.scratch_used(), scratch_used);

    assert_eq!(
        table.insert_bytes(b":status", b"307"),
        Ok(TableInsert::Inserted)
    );
    assert_field(&table, 62, b":status", b"307");
}

#[test]
fn table_size_updates_evict_old_entries_and_track_smallest_pending_size() {
    let mut entries = [HpackEntrySlot::EMPTY; 8];
    let mut scratch = [0; 512];
    let mut table = HpackTable::new(&mut entries, &mut scratch, 256);
    table.insert_bytes(b":status", b"302").unwrap();
    table.insert_bytes(b"cache-control", b"private").unwrap();
    table
        .insert_bytes(b"date", b"Mon, 21 Oct 2013 20:13:21 GMT")
        .unwrap();
    table
        .insert_bytes(b"location", b"https://www.example.com")
        .unwrap();

    assert_eq!(table.set_max_size(100), Ok(()));
    assert_eq!(table.size(), 63);
    assert_eq!(table.len(), 1);
    assert_field(&table, 62, b"location", b"https://www.example.com");
    assert_eq!(table.pending_min_size(), Some(100));
    assert_eq!(
        table.set_max_size(300),
        Err(HpackTableError::MaxSizeExceedsProtocol)
    );
    assert_eq!(table.max_size(), 100);

    assert_eq!(table.set_max_size(0), Ok(()));
    assert!(table.is_empty());
    assert_eq!(table.size(), 0);
    assert_eq!(table.pending_min_size(), Some(0));
    assert_eq!(table.set_max_size(256), Ok(()));
    assert_eq!(table.max_size(), 256);
    assert_eq!(table.pending_min_size(), Some(0));
}

#[test]
fn descriptor_and_scratch_limits_fail_without_invalidating_existing_entries() {
    let mut entries = [HpackEntrySlot::EMPTY; 1];
    let mut scratch = [0; 64];
    let mut table = HpackTable::new(&mut entries, &mut scratch, 1_024);
    table.insert_bytes(b"a", b"1").unwrap();
    let original_field = table.get(62).unwrap();
    let original_size = table.size();
    let original_used = table.scratch_used();

    assert_eq!(
        table.insert_bytes(b"b", b"2"),
        Err(HpackTableError::DescriptorCapacity)
    );
    assert_eq!(table.get(62), Some(original_field));
    assert_eq!(table.size(), original_size);
    assert_eq!(table.scratch_used(), original_used);

    let mut small_entries = [HpackEntrySlot::EMPTY; 4];
    let mut small_scratch = [0; 1];
    let mut small_table = HpackTable::new(&mut small_entries, &mut small_scratch, 1_024);
    assert_eq!(
        small_table.insert_bytes(b"a", b"1"),
        Err(HpackTableError::ScratchCapacity { required: 2 })
    );
    assert!(small_table.is_empty());
    assert_eq!(small_table.scratch_used(), 0);
}

fn assert_field(table: &HpackTable<'_>, index: u64, name: &[u8], value: &[u8]) {
    let field = table.get(index).unwrap();
    assert_eq!(table.resolve(field.name), Some(name));
    assert_eq!(table.resolve(field.value), Some(value));
}
