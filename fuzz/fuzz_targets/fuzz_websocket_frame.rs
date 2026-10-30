#![no_main]
use libfuzzer_sys::fuzz_target;
use veylor_websocket::{Limits, WebSocket};

fuzz_target!(|data: &[u8]| {
    let mut websocket = WebSocket::server(Limits::new(16 * 1024, 64 * 1024));
    let mut input = data.to_vec();
    let mut offset = 0;

    while offset < input.len() {
        let feed = match websocket.feed(&mut input[offset..]) {
            Ok(feed) => feed,
            Err(_) => break,
        };
        if feed.need_more || feed.consumed == 0 {
            break;
        }
        offset += feed.consumed;
    }
});
