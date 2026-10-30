#![no_main]
use libfuzzer_sys::fuzz_target;
use veylor_http1::{ClientResponse, Header, Limits, ResponseRequestMethod};


const MAX_INPUT: usize = 16_384;


const ITER_SLACK: usize = 8;

fn request_method(byte: u8) -> ResponseRequestMethod {
    match byte % 3 {
        0 => ResponseRequestMethod::Head,
        1 => ResponseRequestMethod::Connect,
        _ => ResponseRequestMethod::Other,
    }
}

fuzz_target!(|data: &[u8]| {
    let data = if data.len() > MAX_INPUT {
        &data[..MAX_INPUT]
    } else {
        data
    };
    

    let (method, stream) = match data.split_first() {
        Some((first, rest)) => (request_method(*first), rest),
        None => (ResponseRequestMethod::Other, &[][..]),
    };
    let limits = Limits::default();
    let mut client = ClientResponse::new(method, b"");
    let mut headers = [Header::default(); 32];
    let mut trailers = [Header::default(); 16];

    

    let mut offset = 0usize;
    let mut iters = 0usize;
    loop {
        iters += 1;
        assert!(
            iters <= stream.len() + ITER_SLACK,
            "client-response driver looped without progress (finding)"
        );
        let progress = match client.next(&stream[offset..], false, &mut headers, &mut trailers, limits) {
            Ok(progress) => progress,
            Err(_) => break,
        };
        assert!(
            progress.consumed <= stream.len() - offset,
            "consumed escapes remaining input"
        );
        if progress.consumed == 0 {
            break;
        }
        offset += progress.consumed;
    }

    
    let mut iters = 0usize;
    loop {
        iters += 1;
        assert!(
            iters <= ITER_SLACK,
            "client-response EOF drain looped without progress (finding)"
        );
        match client.next(&stream[offset..], true, &mut headers, &mut trailers, limits) {
            Ok(progress) => {
                assert!(
                    progress.consumed <= stream.len() - offset,
                    "consumed escapes remaining input at EOF"
                );
                if progress.consumed == 0 {
                    break;
                }
                offset += progress.consumed;
            }
            Err(_) => break,
        }
    }
});
