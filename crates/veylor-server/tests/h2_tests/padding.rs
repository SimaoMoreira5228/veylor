use super::*;

const PADDED_FRAMES: usize = 400;
const PAD_LENGTH: usize = 254;

fn padded_payload() -> Vec<u8> {
    let mut payload = vec![PAD_LENGTH as u8, b'x'];
    payload.resize(2 + PAD_LENGTH, 0);
    payload
}

#[test]
fn h2_padded_data_flood_is_credited_back_and_never_stalls() {
    let server = managed_h2_body_server_with_cap(4_096);
    let host = server.host();
    let mut stream = h2_connect(&server);
    handshake(&mut stream);

    send_headers_len(&mut stream, 1, b"POST", b"/upload", None, false);
    let payload = padded_payload();
    let charged = (PADDED_FRAMES * payload.len()) as u64;
    assert!(charged > 65_535, "flood must exceed one connection window");
    for index in 0..PADDED_FRAMES {
        let mut flags = DATA_FLAG_PADDED;
        if index + 1 == PADDED_FRAMES {
            flags |= DATA_FLAG_END_STREAM;
        }
        write_frame(&mut stream, FrameType::Data, flags, 1, &payload);
    }

    let (event, _, _) = poll_h2_request(&host);
    assert_eq!(pull_all(&host, event.token), vec![b'x'; PADDED_FRAMES]);
    assert_eq!(
        host.respond(event.token, 200, b"OK", b"done"),
        RespondOutcome::Ok
    );

    let mut connection_credit = 0u64;
    let mut stream_credit = 0u64;
    let mut finished = false;
    while !finished {
        let (header, body) = read_frame(&mut stream);
        match header.frame_type {
            FrameType::WindowUpdate => {
                let increment = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
                if header.stream_id == 0 {
                    connection_credit += u64::from(increment);
                } else {
                    stream_credit += u64::from(increment);
                }
            }
            FrameType::GoAway | FrameType::RstStream => {
                panic!("padded flood killed the connection")
            }
            FrameType::Data => {
                assert_eq!(header.stream_id, 1);
                finished = header.flags & DATA_FLAG_END_STREAM != 0;
            }
            _ => {}
        }
    }

    let padding = (PADDED_FRAMES * (PAD_LENGTH + 1)) as u64;
    assert!(
        connection_credit >= padding,
        "connection credit {connection_credit}"
    );
    assert!(stream_credit >= padding, "stream credit {stream_credit}");

    send_headers(&mut stream, 3, b"GET", b"/hello", true);
    assert_eq!(read_response(&mut stream, 3).body, b"hello");
    drop(stream);
    stop_join(server);
}
