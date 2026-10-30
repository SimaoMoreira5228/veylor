#![no_main]
use libfuzzer_sys::fuzz_target;
use veylor_http1::{BodyDecoder, BodyEvent, BodyLimits, Header, Limits, ParseError, ResponseBody};




const MAX_INPUT: usize = 16_384;

const TRAILER_SLOTS: [usize; 4] = [32, 8, 2, 0];

const MAX_TRAILERS: usize = 32;






const CALL_SLACK: usize = 16;

fn call_budget(stream_len: usize) -> usize {
    3 * stream_len + CALL_SLACK
}


#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Outcome {
    Complete {
        body_bytes: u64,
        trailers: usize,
        consumed: usize,
    },
    Failed {
        error: ParseError,
        body_bytes: u64,
        consumed: usize,
    },
}























fn same_outcome(left: Outcome, right: Outcome) -> bool {
    match (left, right) {
        (
            Outcome::Failed {
                body_bytes: left_bytes,
                ..
            },
            Outcome::Failed {
                body_bytes: right_bytes,
                ..
            },
        ) => left_bytes == right_bytes,
        _ => left == right,
    }
}

fn trailer_slots(config: u8) -> usize {
    TRAILER_SLOTS[usize::from((config >> 2) & 0x03)]
}



fn split_points(config: u8, len: usize) -> ([usize; 64], usize) {
    let mut points = [0usize; 64];
    let mut count = 0usize;
    let mut push = |end: usize| {
        if end > 0 && end < len && count < points.len() {
            

            if count == 0 || points[count - 1] != end {
                points[count] = end;
                count += 1;
            }
        }
    };
    match config & 0x03 {
        0 => push(len / 2),
        1 => {
            push(len / 3);
            push(2 * len / 3);
        }
        2 => {
            let stride = 1usize + usize::from(config >> 4);
            let mut end = stride;
            while end < len {
                push(end);
                end += stride;
            }
        }
        _ => {
            let stride = 17usize + usize::from(config >> 4);
            let mut end = stride;
            while end < len {
                push(end);
                end += stride;
            }
        }
    }
    (points, count)
}

fn check_data_span(input_len: usize, offset: usize, length: usize, consumed: usize) {
    assert!(
        offset
            .checked_add(length)
            .is_some_and(|end| end <= input_len),
        "data span {offset}+{length} escapes step input of len {input_len} (finding)"
    );
    assert_eq!(
        offset + length,
        consumed,
        "data span end does not match consumed (finding)"
    );
}

fn check_trailer_spans(input: &[u8], trailers: &[Header], count: usize, limits: Limits) {
    assert!(
        count <= trailers.len(),
        "trailer count {count} escapes caller buffer of len {} (finding)",
        trailers.len()
    );
    assert!(
        count <= limits.header_count,
        "trailer count {count} escapes Limits (finding)"
    );
    assert!(
        count <= BodyLimits::DEFAULT.trailer_count,
        "trailer count {count} escapes BodyLimits (finding)"
    );
    for header in trailers.iter().take(count) {
        assert!(
            header
                .name
                .offset
                .checked_add(header.name.length)
                .is_some_and(|end| end <= input.len()),
            "trailer name span escapes step input (finding)"
        );
        assert!(
            header
                .value
                .offset
                .checked_add(header.value.length)
                .is_some_and(|end| end <= input.len()),
            "trailer value span escapes step input (finding)"
        );
    }
}




fn check_absorption(
    decoder: &mut BodyDecoder,
    leftover: &[u8],
    trailers: &mut [Header],
    limits: Limits,
) {
    let progress = decoder
        .next(leftover, false, trailers, limits)
        .expect("post-terminal input must be absorbed, not rejected (finding)");
    assert!(
        progress.consumed == 0 && progress.event.is_none(),
        "post-terminal bytes not absorbed (finding)"
    );
    let tail = b"EXTRA-TAIL\r\n0\r\nX-Absorb: 1\r\n\r\n";
    let progress = decoder
        .next(tail, false, trailers, limits)
        .expect("post-terminal tail must be absorbed, not rejected (finding)");
    assert!(
        progress.consumed == 0 && progress.event.is_none(),
        "post-terminal tail not absorbed (finding)"
    );
    let progress = decoder
        .next(b"", true, trailers, limits)
        .expect("post-terminal EOF must be absorbed, not rejected (finding)");
    assert!(
        progress.consumed == 0 && progress.event.is_none(),
        "post-terminal EOF not absorbed (finding)"
    );
}





fn drive(stream: &[u8], points: &[usize; 64], point_count: usize, slots: usize) -> Outcome {
    let limits = Limits::default();
    let mut decoder = BodyDecoder::new(ResponseBody::Chunked);
    let mut trailers = [Header::default(); MAX_TRAILERS];
    


    let mut staging = [0u8; MAX_INPUT];
    let mut staged = 0usize;
    let mut next_byte = 0usize;
    let mut calls = 0usize;
    let mut consumed_total = 0usize;
    let mut body_bytes = 0u64;
    let mut trailer_count = 0usize;

    

    let mut ends = [stream.len(); 65];
    let mut end_count = 0usize;
    for point in points.iter().take(point_count) {
        let point = (*point).min(stream.len());
        if point > next_byte && point < stream.len() {
            ends[end_count] = point;
            end_count += 1;
            next_byte = point;
        }
    }
    ends[end_count] = stream.len();
    end_count += 1;

    next_byte = 0;
    for (index, segment_end) in ends.iter().enumerate().take(end_count) {
        let segment_end = *segment_end;
        if segment_end > next_byte {
            let fresh = segment_end - next_byte;
            staging[staged..staged + fresh].copy_from_slice(&stream[next_byte..segment_end]);
            staged += fresh;
            next_byte = segment_end;
        }
        let last_segment = index + 1 == end_count;

        
        loop {
            calls += 1;
            assert!(
                calls <= call_budget(stream.len()),
                "chunked driver looped without progress (finding)"
            );
            let input = &staging[..staged];
            match decoder.next(input, false, &mut trailers[..slots], limits) {
                Err(error) => {
                    assert_ne!(
                        error,
                        ParseError::BufferChanged,
                        "stable staging buffer reported BufferChanged (finding)"
                    );
                    
                    let retry = decoder.next(input, false, &mut trailers[..slots], limits);
                    assert_eq!(retry, Err(error), "terminal error did not stick (finding)");
                    return Outcome::Failed {
                        error,
                        body_bytes,
                        consumed: consumed_total,
                    };
                }
                Ok(progress) => {
                    assert!(
                        progress.consumed <= staged,
                        "consumed escapes step input (finding)"
                    );
                    match progress.event {
                        Some(BodyEvent::Data(span)) => {
                            assert!(
                                progress.consumed > 0,
                                "data event without progress (finding)"
                            );
                            check_data_span(staged, span.offset, span.length, progress.consumed);
                            body_bytes += span.length as u64;
                        }
                        Some(BodyEvent::Trailers { count }) => {
                            check_trailer_spans(input, &trailers[..slots], count, limits);
                            trailer_count = count;
                        }
                        Some(BodyEvent::Complete) => {
                            consumed_total += progress.consumed;
                            assert!(
                                consumed_total <= stream.len(),
                                "total consumed escapes stream (finding)"
                            );
                            let leftover = staged - progress.consumed;
                            check_absorption(
                                &mut decoder,
                                &staging[..leftover],
                                &mut trailers[..slots],
                                limits,
                            );
                            return Outcome::Complete {
                                body_bytes,
                                trailers: trailer_count,
                                consumed: consumed_total,
                            };
                        }
                        None => {
                            if progress.consumed == 0 {
                                break;
                            }
                        }
                    }
                    if progress.consumed > 0 {
                        staging.copy_within(progress.consumed..staged, 0);
                        staged -= progress.consumed;
                        consumed_total += progress.consumed;
                    }
                }
            }
        }

        if last_segment {
            break;
        }
    }

    







    loop {
        calls += 1;
        assert!(
            calls <= call_budget(stream.len()),
            "chunked EOF drain looped without progress (finding)"
        );
        let input = &staging[..staged];
        match decoder.next(input, true, &mut trailers[..slots], limits) {
            Err(error) => {
                assert_ne!(
                    error,
                    ParseError::BufferChanged,
                    "stable staging buffer reported BufferChanged at EOF (finding)"
                );
                return Outcome::Failed {
                    error,
                    body_bytes,
                    consumed: consumed_total,
                };
            }
            Ok(progress) => {
                assert!(
                    progress.consumed <= staged,
                    "consumed escapes step input at EOF (finding)"
                );
                match progress.event {
                    Some(BodyEvent::Data(span)) => {
                        check_data_span(staged, span.offset, span.length, progress.consumed);
                        body_bytes += span.length as u64;
                    }
                    Some(BodyEvent::Trailers { count }) => {
                        check_trailer_spans(input, &trailers[..slots], count, limits);
                        trailer_count = count;
                    }
                    Some(BodyEvent::Complete) => {
                        consumed_total += progress.consumed;
                        let leftover = staged - progress.consumed;
                        check_absorption(
                            &mut decoder,
                            &staging[..leftover],
                            &mut trailers[..slots],
                            limits,
                        );
                        return Outcome::Complete {
                            body_bytes,
                            trailers: trailer_count,
                            consumed: consumed_total,
                        };
                    }
                    None => {
                        


                    }
                }
                if progress.consumed > 0 {
                    staging.copy_within(progress.consumed..staged, 0);
                    staged -= progress.consumed;
                    consumed_total += progress.consumed;
                }
            }
        }
    }
}

fuzz_target!(|data: &[u8]| {
    let data = if data.len() > MAX_INPUT {
        &data[..MAX_INPUT]
    } else {
        data
    };
    let (config, stream) = match data.split_first() {
        Some((first, rest)) => (*first, rest),
        None => return,
    };
    let slots = trailer_slots(config);

    
    let (points, point_count) = split_points(config, stream.len());
    let incremental = drive(stream, &points, point_count, slots);

    
    let single = drive(stream, &[0usize; 64], 0, slots);

    


    assert!(
        same_outcome(incremental, single),
        "incremental vs single-shot disagreement (finding): {incremental:?} vs {single:?}"
    );
});
