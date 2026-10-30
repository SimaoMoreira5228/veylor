#![no_main]
use libfuzzer_sys::fuzz_target;
use veylor_http1::{Header, Limits, ParseError, Parser, RequestHead};



const MAX_INPUT: usize = 16_384;

const SMALL_HEADERS: usize = 4;

const LARGE_HEADERS: usize = 128;

fn spans_within(input: &[u8], offset: usize, length: usize) {
    assert!(
        offset.checked_add(length).is_some_and(|end| end <= input.len()),
        "span {offset}+{length} escapes input of len {}",
        input.len()
    );
}

fn check_head(input: &[u8], head: RequestHead, headers: &[Header], limits: Limits) {
    assert!(head.length <= input.len(), "head length escapes input");
    assert!(
        head.header_count <= headers.len(),
        "header count escapes caller buffer"
    );
    assert!(
        head.header_count <= limits.header_count,
        "header count escapes Limits"
    );
    spans_within(input, head.method.offset, head.method.length);
    spans_within(input, head.target.offset, head.target.length);
    spans_within(input, head.version.offset, head.version.length);
    spans_within(input, head.authority.offset, head.authority.length);
    assert!(
        head.method(input).is_some()
            && head.target(input).is_some()
            && head.version(input).is_some(),
        "request-line accessors must resolve inside input"
    );
    for header in headers.iter().take(head.header_count) {
        spans_within(input, header.name.offset, header.name.length);
        spans_within(input, header.value.offset, header.value.length);
    }
}

fuzz_target!(|data: &[u8]| {
    let data = if data.len() > MAX_INPUT {
        &data[..MAX_INPUT]
    } else {
        data
    };
    let limits = Limits::default();
    let mut parser = Parser::new();
    let mut small = [Header::default(); SMALL_HEADERS];

    
    if data.len() > 1 {
        let _ = parser.parse(&data[..data.len() / 2], &mut small, limits);
    }

    match parser.parse(data, &mut small, limits) {
        Err(ParseError::HeaderCapacity) => {
            assert!(parser.required_header_capacity() > small.len());
            let mut large = [Header::default(); LARGE_HEADERS];
            match parser.parse(data, &mut large, limits) {
                Ok(Some(head)) => check_head(data, head, &large, limits),
                Ok(None) | Err(_) => {}
            }
        }
        Ok(Some(head)) => check_head(data, head, &small, limits),
        Ok(None) | Err(_) => {}
    }
});
