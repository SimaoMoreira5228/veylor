use super::MAX_STREAM_WRITE;
use super::METHOD_CAP;
use super::RequestTrailer;
use super::ResponsePhase;
use super::RouteSnapshot;
use super::STAGED_BODY_CAP;
use super::ServiceBodyPolicy;
use super::ServiceError;
use super::ServiceStreams;
use super::StreamRecord;
use super::TrailerOutcome;

fn route() -> RouteSnapshot<'static> {
    RouteSnapshot {
        route_id: 7,
        policy: ServiceBodyPolicy::Stream {
            max_body_bytes: 1_024,
        },
        permitted_trailers: TRAILERS,
    }
}

static TRAILERS: &[&[u8]] = &[b"x-checksum", b"x-count"];

fn streams() -> [StreamRecord<'static>; 4] {
    [StreamRecord::EMPTY; 4]
}

#[test]
fn open_get_matches_route_and_snapshots_policy() {
    let mut storage = streams();
    let mut service = ServiceStreams::new(&mut storage);
    let target = service
        .open_request(1, b"GET", b"/items/42", Some(route()), false)
        .unwrap();
    let record = service.get(target).unwrap();
    assert_eq!(record.method(), b"GET");
    assert_eq!(record.route_id, 7);
    assert_eq!(
        record.policy,
        ServiceBodyPolicy::Stream {
            max_body_bytes: 1_024,
        }
    );
    assert_eq!(record.phase, ResponsePhase::None);
    assert!(!record.end_received);
}

#[test]
fn open_refuses_non_origin_forms() {
    let mut storage = streams();
    let mut service = ServiceStreams::new(&mut storage);
    assert_eq!(
        service.open_request(1, b"GET", b"https://example.com/x", Some(route()), false),
        Err(ServiceError::NonOriginForm)
    );
    assert_eq!(
        service.open_request(1, b"CONNECT", b"/x", Some(route()), false),
        Err(ServiceError::ConnectRefused)
    );
    assert_eq!(
        service.open_request(1, b"OPTIONS", b"*", Some(route()), false),
        Err(ServiceError::NonOriginForm)
    );
    assert_eq!(
        service.open_request(1, b"GET", b"/x", None, false),
        Err(ServiceError::UnknownRoute)
    );
    assert!(
        service
            .get(super::RecordRef {
                stream_id: 1,
                generation: 1,
            })
            .is_err()
    );
}

#[test]
fn staged_data_grant_on_consume() {
    let mut storage = streams();
    let mut service = ServiceStreams::new(&mut storage);
    let target = service
        .open_request(1, b"POST", b"/upload", Some(route()), false)
        .unwrap();
    assert_eq!(service.stage_data(target, b"hello", false).unwrap(), 5);
    let mut out = [0; 8];
    let drain = service.take_staged(target, &mut out[..3]).unwrap();
    assert_eq!(drain.bytes, 3);
    assert_eq!(drain.grant_owed, 3);
    assert_eq!(drain.remaining, 2);
    assert_eq!(&out[..3], b"hel");
    let drain = service.take_staged(target, &mut out).unwrap();
    assert_eq!(drain.bytes, 2);
    assert_eq!(drain.grant_owed, 2);
    assert_eq!(drain.remaining, 0);
    assert_eq!(&out[..2], b"lo");
}

#[test]
fn over_cap_stage_refused() {
    let mut storage = streams();
    let mut service = ServiceStreams::new(&mut storage);
    let big = RouteSnapshot {
        route_id: 1,
        policy: ServiceBodyPolicy::Stream {
            max_body_bytes: u64::MAX,
        },
        permitted_trailers: &[],
    };
    let target = service
        .open_request(1, b"POST", b"/big", Some(big), false)
        .unwrap();
    let fill = [7; STAGED_BODY_CAP];
    assert_eq!(
        service.stage_data(target, &fill, false).unwrap(),
        STAGED_BODY_CAP
    );
    assert_eq!(
        service.stage_data(target, b"x", false),
        Err(ServiceError::BodyOverCap)
    );
    let mut out = [0; STAGED_BODY_CAP];
    let drain = service.take_staged(target, &mut out).unwrap();
    assert_eq!(drain.grant_owed, STAGED_BODY_CAP as u32);
    let reject = RouteSnapshot {
        route_id: 2,
        policy: ServiceBodyPolicy::Reject,
        permitted_trailers: &[],
    };
    let plain = service
        .open_request(3, b"GET", b"/x", Some(reject), true)
        .unwrap();
    assert_eq!(
        service.stage_data(plain, b"x", false),
        Err(ServiceError::BodyNotPermitted)
    );
}

#[test]
fn one_shot_200_with_end_stream() {
    let mut storage = streams();
    let mut service = ServiceStreams::new(&mut storage);
    let target = service
        .open_request(1, b"GET", b"/x", Some(route()), true)
        .unwrap();
    let started = service.start_response(target, 200, true).unwrap();
    assert!(started.ended_stream);
    assert_eq!(service.get(target).unwrap().phase, ResponsePhase::Complete);
    assert_eq!(
        service.start_response(target, 200, true),
        Err(ServiceError::ResponseAlreadyStarted)
    );
    assert_eq!(
        service.stage_data(target, b"x", false),
        Err(ServiceError::ResponseComplete)
    );
}

#[test]
fn informational_then_final() {
    let mut storage = streams();
    let mut service = ServiceStreams::new(&mut storage);
    let target = service
        .open_request(1, b"GET", b"/x", Some(route()), true)
        .unwrap();
    service.start_response(target, 103, false).unwrap();
    assert_eq!(
        service.get(target).unwrap().phase,
        ResponsePhase::Informational
    );
    assert_eq!(
        service.start_response(target, 101, false),
        Err(ServiceError::InvalidStatus)
    );
    service.start_response(target, 200, true).unwrap();
    assert_eq!(service.get(target).unwrap().phase, ResponsePhase::Complete);
}

#[test]
fn streaming_begin_write_end_pacing() {
    let mut storage = streams();
    let mut service = ServiceStreams::new(&mut storage);
    let long_method = [b'G'; METHOD_CAP + 1];
    assert_eq!(
        service.open_request(1, &long_method, b"/x", Some(route()), false),
        Err(ServiceError::MethodTooLong)
    );
    let target = service
        .open_request(1, b"GET", b"/x", Some(route()), false)
        .unwrap();
    service.begin_streaming(target, 200).unwrap();
    service.admit_write(target, 64).unwrap();
    assert_eq!(
        service.admit_write(target, 64),
        Err(ServiceError::NotWritable)
    );
    assert_eq!(
        service.admit_write(target, 0),
        Err(ServiceError::NotWritable)
    );
    service.notify_writable(target).unwrap();
    assert_eq!(
        service.admit_write(target, MAX_STREAM_WRITE + 1),
        Err(ServiceError::WriteTooLarge)
    );
    service.notify_writable(target).unwrap();
    service.admit_write(target, MAX_STREAM_WRITE).unwrap();
    service.end_streaming(target).unwrap();
    assert_eq!(service.get(target).unwrap().phase, ResponsePhase::Complete);
    assert_eq!(
        service.notify_writable(target),
        Err(ServiceError::ResponseNotStreaming)
    );
}

#[test]
fn trailer_allowlist_accept_reject() {
    let mut storage = streams();
    let mut service = ServiceStreams::new(&mut storage);
    let target = service
        .open_request(1, b"POST", b"/x", Some(route()), false)
        .unwrap();
    let fields = [RequestTrailer {
        name: b"x-checksum",
        value: b"abc123",
    }];
    let outcome = service.stage_request_trailers(target, &fields).unwrap();
    assert_eq!(outcome, TrailerOutcome::Staged { count: 1 });
    assert!(service.get(target).unwrap().end_received);
    assert_eq!(
        service.get(target).unwrap().trailer_name(0),
        Some(b"x-checksum".as_slice())
    );
    let other = service
        .open_request(3, b"POST", b"/y", Some(route()), false)
        .unwrap();
    let bad = [RequestTrailer {
        name: b"x-evil",
        value: b"nope",
    }];
    assert_eq!(
        service.stage_request_trailers(other, &bad),
        Err(ServiceError::TrailerRefused)
    );
    let pseudo = [RequestTrailer {
        name: b":status",
        value: b"200",
    }];
    assert_eq!(
        service.stage_request_trailers(other, &pseudo),
        Err(ServiceError::TrailerRefused)
    );
    service.begin_streaming(other, 200).unwrap();
    assert!(
        service
            .check_response_trailers(other, &[b"x-count".as_slice()])
            .is_ok()
    );
    assert_eq!(
        service.check_response_trailers(other, &[b"x-evil".as_slice()]),
        Err(ServiceError::TrailerRefused)
    );
}

#[test]
fn rst_teardown_isolates_siblings() {
    let mut storage = streams();
    let mut service = ServiceStreams::new(&mut storage);
    let first = service
        .open_request(1, b"POST", b"/a", Some(route()), false)
        .unwrap();
    let sibling = service
        .open_request(3, b"GET", b"/b", Some(route()), true)
        .unwrap();
    service.stage_data(first, b"staged", false).unwrap();
    let torn = service.teardown(1, false);
    assert!(torn.released);
    assert_eq!(torn.dropped_ungranted, 6);
    assert!(!service.teardown(1, false).released);
    assert_eq!(
        service.stage_data(first, b"x", false),
        Err(ServiceError::UnknownStream)
    );
    assert_eq!(service.stage_data(sibling, b"", false).unwrap(), 0);
    service.start_response(sibling, 200, true).unwrap();
    assert_eq!(service.get(sibling).unwrap().phase, ResponsePhase::Complete);
}

#[test]
fn stale_generation_fails_closed() {
    let mut storage = streams();
    let mut service = ServiceStreams::new(&mut storage);
    let first = service
        .open_request(1, b"GET", b"/a", Some(route()), true)
        .unwrap();
    service.close(first).unwrap();
    let second = service
        .open_request(1, b"GET", b"/a", Some(route()), true)
        .unwrap();
    assert_ne!(first.generation, second.generation);
    assert_eq!(
        service.get(first).unwrap_err(),
        ServiceError::StaleGeneration
    );
    assert_eq!(
        service.stage_data(first, b"x", false),
        Err(ServiceError::StaleGeneration)
    );
    assert_eq!(
        service.start_response(first, 200, true),
        Err(ServiceError::StaleGeneration)
    );
    service.start_response(second, 200, true).unwrap();
}

#[test]
fn bodyless_statuses_cannot_stream() {
    let mut storage = streams();
    let mut service = ServiceStreams::new(&mut storage);
    let no_content = service
        .open_request(1, b"GET", b"/x", Some(route()), true)
        .unwrap();
    assert_eq!(
        service.begin_streaming(no_content, 204),
        Err(ServiceError::InvalidStatus)
    );
    let not_modified = service
        .open_request(3, b"GET", b"/x", Some(route()), true)
        .unwrap();
    assert_eq!(
        service.begin_streaming(not_modified, 304),
        Err(ServiceError::InvalidStatus)
    );
    let head = service
        .open_request(5, b"HEAD", b"/x", Some(route()), true)
        .unwrap();
    assert_eq!(
        service.begin_streaming(head, 200),
        Err(ServiceError::HeadCannotStream)
    );
}
