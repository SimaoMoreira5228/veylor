use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use veylor_http2::{
    DEFAULT_MAX_FRAME_SIZE, FRAME_HEADER_LEN, FrameDirection, FrameHeader, FrameType,
    H2FrameDriver, H2Role, HeadersFrameWriter, HpackEntrySlot, HpackFieldToEncode,
    HpackRepresentation, HpackTable, StreamActionKind, StreamSlot, StreamTable,
    decode_frame_header, encode_frame_header, encode_header_block,
};

struct CountAlloc;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);
static DEALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

#[allow(unsafe_code)]
unsafe impl GlobalAlloc for CountAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            ALLOCATIONS.fetch_add(1, Ordering::SeqCst);
            ALLOCATED_BYTES.fetch_add(layout.size(), Ordering::SeqCst);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        DEALLOCATIONS.fetch_add(1, Ordering::SeqCst);
    }
}

#[global_allocator]
static A: CountAlloc = CountAlloc;

fn reset_counts() {
    ALLOCATIONS.store(0, Ordering::SeqCst);
    ALLOCATED_BYTES.store(0, Ordering::SeqCst);
    DEALLOCATIONS.store(0, Ordering::SeqCst);
}

const ITERATIONS: usize = 64;

const FIELDS: [HpackFieldToEncode<'static>; 3] = [
    HpackFieldToEncode {
        name: b":method",
        value: b"GET",
        representation: HpackRepresentation::WithoutIndexing,
        huffman_name: false,
        huffman_value: false,
    },
    HpackFieldToEncode {
        name: b":path",
        value: b"/route/00042",
        representation: HpackRepresentation::WithoutIndexing,
        huffman_name: false,
        huffman_value: false,
    },
    HpackFieldToEncode {
        name: b"x-test",
        value: b"alloc",
        representation: HpackRepresentation::WithoutIndexing,
        huffman_name: false,
        huffman_value: false,
    },
];

fn establish(streams: &mut StreamTable<'_>) {
    let settings = FrameHeader {
        payload_length: 0,
        frame_type: FrameType::Settings,
        flags: 0,
        stream_id: 0,
    };
    assert_eq!(
        streams.on_frame(settings, FrameDirection::Sent, &[]).kind,
        StreamActionKind::Accept
    );
    assert_eq!(
        streams
            .on_frame(settings, FrameDirection::Received, &[])
            .kind,
        StreamActionKind::Accept
    );
}

fn run_once(
    driver: &mut H2FrameDriver<'_, '_, '_, '_>,
    block_table: &mut HpackTable<'_>,
    stream_id: u32,
) -> usize {
    let mut block = [0; 256];
    let block_length = encode_header_block(&FIELDS, block_table, &mut block).unwrap();
    assert!(block_length > 0);

    let header = FrameHeader {
        payload_length: block_length as u32,
        frame_type: FrameType::Headers,
        flags: 0,
        stream_id,
    };
    let mut header_bytes = [0; FRAME_HEADER_LEN];
    encode_frame_header(header, &mut header_bytes, DEFAULT_MAX_FRAME_SIZE).unwrap();
    let decoded = decode_frame_header(&header_bytes, DEFAULT_MAX_FRAME_SIZE).unwrap();
    assert_eq!(decoded, header);

    let mut output = [0; 512];
    let mut writer = HeadersFrameWriter::new(&mut output);
    let queued = writer
        .queue_headers(driver, stream_id, &block[..block_length], false)
        .unwrap();
    assert_eq!(queued.block_length, block_length);
    assert!(!writer.pending_bytes().is_empty());
    writer.acknowledge(queued.output_length).unwrap();
    assert!(writer.pending_bytes().is_empty());
    queued.output_length
}

// NOTE: unbounded allocator use on network-controlled input is a DoS vector.
// This gate pins the bounded-memory contract for the H2 encode/queue hot path.
#[test]
fn http2_hot_path_performs_no_heap_allocation() {
    let mut slots = [StreamSlot::EMPTY; 80];
    let mut header_block_storage = [0; 64];
    let mut streams = StreamTable::new(&mut slots, H2Role::Client, &mut header_block_storage, 64);
    establish(&mut streams);

    let mut decoder_entries = [HpackEntrySlot::EMPTY; 8];
    let mut decoder_storage = [0; 256];
    let mut decoder = HpackTable::new(&mut decoder_entries, &mut decoder_storage, 4_096);
    let mut driver_entries = [HpackEntrySlot::EMPTY; 8];
    let mut driver_storage = [0; 256];
    let mut driver_encoder = HpackTable::new(&mut driver_entries, &mut driver_storage, 4_096);
    let mut settings_ack_limits = [0; 4];
    let mut driver = H2FrameDriver::new(
        &mut streams,
        &mut decoder,
        &mut driver_encoder,
        &mut settings_ack_limits,
    )
    .unwrap();

    let mut block_entries = [HpackEntrySlot::EMPTY; 8];
    let mut block_storage = [0; 256];
    let mut block_table = HpackTable::new(&mut block_entries, &mut block_storage, 4_096);

    let warmed_bytes = run_once(&mut driver, &mut block_table, 1);
    assert!(warmed_bytes > 0);

    reset_counts();

    let mut total_bytes = 0;
    for index in 0..ITERATIONS {
        let stream_id = 3 + 2 * index as u32;
        total_bytes += run_once(&mut driver, &mut block_table, stream_id);
    }

    let allocations = ALLOCATIONS.load(Ordering::SeqCst);
    let allocated_bytes = ALLOCATED_BYTES.load(Ordering::SeqCst);
    let deallocations = DEALLOCATIONS.load(Ordering::SeqCst);
    assert_eq!(allocations, 0);
    assert_eq!(allocated_bytes, 0);
    assert_eq!(deallocations, 0);

    println!("http2: {ITERATIONS} iterations, {total_bytes} queued bytes, {allocations} allocs");
}
