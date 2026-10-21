use super::*;

#[test]
fn server_validates_client_preface_incrementally_and_leaves_following_bytes() {
    let mut slots = [StreamSlot::EMPTY; 1];
    let mut header_storage = [0; 32];
    let mut table = StreamTable::new(&mut slots, H2Role::Server, &mut header_storage, 0);
    let mut input = [0; CLIENT_CONNECTION_PREFACE.len() + 9];
    input[..CLIENT_CONNECTION_PREFACE.len()].copy_from_slice(CLIENT_CONNECTION_PREFACE);

    let mut consumed = 0;
    for fragment in input[..CLIENT_CONNECTION_PREFACE.len()].chunks(5) {
        let read = table.receive_client_preface_fragment(fragment).unwrap();
        assert_eq!(read, fragment.len());
        consumed += read;
    }
    assert_eq!(consumed, CLIENT_CONNECTION_PREFACE.len());
    assert!(table.client_preface_complete());
    assert_eq!(
        table.receive_client_preface_fragment(&input[CLIENT_CONNECTION_PREFACE.len()..]),
        Err(ClientPrefaceError::AlreadyComplete)
    );

    assert_eq!(
        table.on_frame(
            header(FrameType::Settings, 0, 0, 0),
            FrameDirection::Received,
            &[],
        ),
        StreamAction::ACCEPT
    );
}

#[test]
fn preface_rejects_bad_magic_and_wrong_role() {
    let mut server_slots = [StreamSlot::EMPTY; 1];
    let mut server_storage = [0; 8];
    let mut server = StreamTable::new(&mut server_slots, H2Role::Server, &mut server_storage, 0);
    assert_eq!(
        server.receive_client_preface_fragment(&CLIENT_CONNECTION_PREFACE[..7]),
        Ok(7)
    );
    let mut invalid = [0; 4];
    invalid.copy_from_slice(b"X * ");
    assert_eq!(
        server.receive_client_preface_fragment(&invalid),
        Err(ClientPrefaceError::InvalidByte { offset: 7 })
    );
    assert_eq!(
        server.on_frame(
            header(FrameType::Settings, 0, 0, 0),
            FrameDirection::Received,
            &[],
        ),
        StreamAction::connection_error(ErrorCode::ProtocolError)
    );

    let mut client_slots = [StreamSlot::EMPTY; 1];
    let mut client_storage = [0; 8];
    let mut client = StreamTable::new(&mut client_slots, H2Role::Client, &mut client_storage, 0);
    assert_eq!(
        client.receive_client_preface_fragment(CLIENT_CONNECTION_PREFACE),
        Err(ClientPrefaceError::WrongRole)
    );
}

#[test]
fn first_frame_must_be_settings_in_both_directions() {
    let mut client_slots = [StreamSlot::EMPTY; 1];
    let mut client_storage = [0; 8];
    let mut client = StreamTable::new(&mut client_slots, H2Role::Client, &mut client_storage, 0);
    assert_eq!(
        client.on_frame(
            header(FrameType::Ping, 0, 0, 8),
            FrameDirection::Received,
            &[0; 8],
        ),
        StreamAction::connection_error(ErrorCode::ProtocolError)
    );
    assert_eq!(
        client.on_frame(
            header(FrameType::Headers, HEADERS_FLAG_END_HEADERS, 1, 0),
            FrameDirection::Sent,
            &[],
        ),
        StreamAction::REJECT
    );

    let mut server_slots = [StreamSlot::EMPTY; 1];
    let mut server_storage = [0; 8];
    let mut server = StreamTable::new(&mut server_slots, H2Role::Server, &mut server_storage, 0);
    assert_eq!(
        server.on_frame(
            header(FrameType::Settings, 0, 0, 0),
            FrameDirection::Received,
            &[],
        ),
        StreamAction::connection_error(ErrorCode::ProtocolError)
    );
}
