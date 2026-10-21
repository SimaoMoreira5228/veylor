mod integer;
mod representation;
mod string;
mod table;

pub use integer::{HpackIntegerError, decode_hpack_integer, encode_hpack_integer};
pub use representation::{
    HpackFieldToEncode, HpackHeader, HpackRepresentation, decode_header_block, decode_header_field,
    encode_header_block, encode_indexed_header, encode_literal_header,
    encode_pending_table_size_updates, encode_table_size_update,
};
pub use string::huffman::{HuffmanError, decode_huffman, encode_huffman};
pub use string::{DecodedHpackString, HpackStringError, decode_hpack_string, encode_hpack_string};
pub use table::{
    HPACK_STATIC_TABLE_LEN, HpackBytes, HpackEntrySlot, HpackField, HpackSpan, HpackTable,
    HpackTableError, TableInsert,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HpackError {
    InvalidIndex,
    InvalidRepresentation,
    TableSizeUpdateAfterField,
    RequiredTableSizeUpdate { maximum: u64 },
    HeaderCapacity { required: usize },
    BufferTooSmall { required: usize },
    Integer(HpackIntegerError),
    String(HpackStringError),
    Table(HpackTableError),
}
