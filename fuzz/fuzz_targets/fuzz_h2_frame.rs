#![no_main]
use libfuzzer_sys::fuzz_target;
use veylor_http2::{
    DEFAULT_MAX_FRAME_SIZE, FRAME_HEADER_LEN, FramePayloadError, FrameType, GOAWAY_FIXED_LEN,
    HEADERS_FLAG_PADDED, HEADERS_FLAG_PRIORITY, MAX_STREAM_ID, MAX_WINDOW_INCREMENT,
    PRIORITY_PAYLOAD_LEN, PUSH_PROMISE_FLAG_PADDED, RST_STREAM_PAYLOAD_LEN, SETTINGS_ENTRY_LEN,
    SettingsParameter, decode_continuation_payload, decode_data_payload, decode_frame_header,
    decode_goaway_payload, decode_headers_payload, decode_ping_payload, decode_priority_fields,
    decode_priority_payload, decode_push_promise_payload, decode_rst_stream_payload,
    decode_settings_payload, decode_window_update_payload, encode_frame_header,
    encode_priority_fields, encode_rst_stream_payload,
};




const MAX_INPUT: usize = FRAME_HEADER_LEN + DEFAULT_MAX_FRAME_SIZE as usize;

const SETTINGS_SLOTS: usize = 64;

fn is_subslice(whole: &[u8], part: &[u8]) -> bool {
    let base = whole.as_ptr() as usize;
    let addr = part.as_ptr() as usize;
    addr >= base && part.len() <= whole.len() && addr - base <= whole.len() - part.len()
}

fuzz_target!(|data: &[u8]| {
    let data = if data.len() > MAX_INPUT {
        &data[..MAX_INPUT]
    } else {
        data
    };
    let header = match decode_frame_header(data, DEFAULT_MAX_FRAME_SIZE) {
        Ok(header) => {
            assert!(
                header.payload_length <= DEFAULT_MAX_FRAME_SIZE,
                "payload length escapes configured max"
            );
            assert!(
                header.stream_id <= MAX_STREAM_ID,
                "stream id escapes 31 bits"
            );
            let mut wire = [0u8; FRAME_HEADER_LEN];
            match encode_frame_header(header, &mut wire, DEFAULT_MAX_FRAME_SIZE) {
                Ok(()) => {
                    assert_eq!(wire[..5], data[..5], "length/type/flags round trip");
                    assert_eq!(wire[5], data[5] & 0x7f, "reserved bit clears on re-encode");
                    assert_eq!(wire[6..], data[6..FRAME_HEADER_LEN], "stream id round trip");
                    assert_eq!(
                        decode_frame_header(&wire, DEFAULT_MAX_FRAME_SIZE),
                        Ok(header),
                        "header re-decode round trip"
                    );
                }
                Err(_) => {}
            }
            header
        }
        Err(_) => return,
    };
    let payload = &data[FRAME_HEADER_LEN..];
    match header.frame_type {
        FrameType::Data => match decode_data_payload(header, payload) {
            Ok(decoded) => {
                assert!(is_subslice(payload, decoded.data));
                match decoded.padding_length {
                    Some(padding) => assert_eq!(
                        decoded.data.len() + usize::from(padding) + 1,
                        payload.len(),
                        "data padding shape"
                    ),
                    None => assert_eq!(
                        decoded.data.len(),
                        payload.len(),
                        "unpadded data spans payload"
                    ),
                }
            }
            Err(_) => {}
        },
        FrameType::Headers => match decode_headers_payload(header, payload) {
            Ok(decoded) => {
                assert!(is_subslice(payload, decoded.fragment));
                assert_eq!(
                    decoded.padding_length.is_some(),
                    header.flags & HEADERS_FLAG_PADDED != 0,
                    "headers padding flag agrees with shape"
                );
                assert_eq!(
                    decoded.priority.is_some(),
                    header.flags & HEADERS_FLAG_PRIORITY != 0,
                    "headers priority flag agrees with shape"
                );
            }
            Err(_) => {}
        },
        FrameType::Priority => match decode_priority_payload(header, payload) {
            Ok(priority) => {
                let mut wire = [0u8; PRIORITY_PAYLOAD_LEN];
                match encode_priority_fields(priority, &mut wire) {
                    Ok(()) => assert_eq!(
                        decode_priority_fields(&wire),
                        Ok(priority),
                        "priority round trip"
                    ),
                    Err(_) => {}
                }
            }
            Err(_) => {}
        },
        FrameType::RstStream => match decode_rst_stream_payload(header, payload) {
            Ok(code) => {
                assert_eq!(
                    payload.len(),
                    RST_STREAM_PAYLOAD_LEN,
                    "rst-stream spans payload"
                );
                let mut wire = [0u8; RST_STREAM_PAYLOAD_LEN];
                match encode_rst_stream_payload(code, &mut wire) {
                    Ok(()) => assert_eq!(
                        decode_rst_stream_payload(header, &wire),
                        Ok(code),
                        "rst-stream round trip"
                    ),
                    Err(_) => {}
                }
            }
            Err(_) => {}
        },
        FrameType::Settings => {
            let mut parameters = [SettingsParameter { id: 0, value: 0 }; SETTINGS_SLOTS];
            match decode_settings_payload(header, payload, &mut parameters) {
                Ok(count) => {
                    assert!(count <= parameters.len(), "count escapes caller buffer");
                    assert_eq!(
                        count * SETTINGS_ENTRY_LEN,
                        payload.len(),
                        "settings entries span payload"
                    );
                }
                Err(FramePayloadError::BufferTooSmall { required }) => {
                    assert!(
                        required > parameters.len(),
                        "BufferTooSmall requires more than provided"
                    );
                    assert_eq!(
                        required * SETTINGS_ENTRY_LEN,
                        payload.len(),
                        "required entries span payload"
                    );
                }
                Err(_) => {}
            }
        }
        FrameType::Ping => match decode_ping_payload(header, payload) {
            Ok(opaque) => assert_eq!(opaque.as_slice(), payload, "ping spans payload"),
            Err(_) => {}
        },
        FrameType::GoAway => match decode_goaway_payload(header, payload) {
            Ok(goaway) => {
                assert!(
                    goaway.last_stream_id <= MAX_STREAM_ID,
                    "goaway stream id escapes 31 bits"
                );
                assert!(is_subslice(payload, goaway.debug_data));
                assert_eq!(
                    GOAWAY_FIXED_LEN + goaway.debug_data.len(),
                    payload.len(),
                    "goaway spans payload"
                );
            }
            Err(_) => {}
        },
        FrameType::WindowUpdate => match decode_window_update_payload(header, payload) {
            Ok(update) => assert!(
                (1..=MAX_WINDOW_INCREMENT).contains(&update.increment),
                "window increment escapes range"
            ),
            Err(_) => {}
        },
        FrameType::PushPromise => match decode_push_promise_payload(header, payload) {
            Ok(promised) => {
                assert_ne!(
                    promised.promised_stream_id, 0,
                    "promised stream id is nonzero"
                );
                assert!(is_subslice(payload, promised.fragment));
                assert_eq!(
                    promised.padding_length.is_some(),
                    header.flags & PUSH_PROMISE_FLAG_PADDED != 0,
                    "push-promise padding flag agrees with shape"
                );
            }
            Err(_) => {}
        },
        FrameType::Continuation => match decode_continuation_payload(header, payload) {
            Ok(fragment) => assert_eq!(fragment, payload, "continuation spans payload"),
            Err(_) => {}
        },
        FrameType::Unknown(_) => {}
    }
});
