#![no_main]




















































use libfuzzer_sys::fuzz_target;
use veylor_http1::{
    ConnectionHeader, FixedResponseBodyWriter, ResponseEncodeError, ResponseFraming,
    ResponseToWrite, ResponseWriteCursor, encode_response_chunk, encode_response_final_chunk,
    encode_response_head, encode_response_head_with_required_length,
};
use veylor_http1::Field;


const MAX_INPUT: usize = 4_096;

const MAX_FIELDS: usize = 8;

const OUT_MAX: usize = 8_192;

const OUT_SMALL: usize = 8;

const METHOD_MAX: usize = 16;
const REASON_MAX: usize = 48;
const UPGRADE_MAX: usize = 32;
const NAME_MAX: usize = 32;
const VALUE_MAX: usize = 96;
const BODY_MAX: usize = 512;

const ITER_SLACK: usize = 16;

const METHODS: [&[u8]; 4] = [b"GET", b"HEAD", b"CONNECT", b"POST"];

fn framing(byte: u8) -> ResponseFraming {
    match byte & 0x03 {
        0 | 3 => ResponseFraming::FixedLength,
        1 => ResponseFraming::Chunked,
        _ => ResponseFraming::CloseDelimited,
    }
}

fn connection(byte: u8) -> ConnectionHeader {
    match (byte >> 2) & 0x03 {
        0 => ConnectionHeader::Omit,
        1 => ConnectionHeader::Close,
        2 => ConnectionHeader::KeepAlive,
        _ => ConnectionHeader::Upgrade,
    }
}

fn is_token_slice(bytes: &[u8]) -> bool {
    !bytes.is_empty() && bytes.iter().all(|b| veylor_http1::is_token(*b))
}

fn has_hostile_value(bytes: &[u8]) -> bool {
    bytes
        .iter()
        .any(|b| *b == b'\r' || *b == b'\n' || (*b < 0x20 && *b != b'\t') || *b == 0x7f)
}





fn body_allowed_expected(method: &[u8], status: u16) -> bool {
    status >= 200
        && method != b"HEAD"
        && status != 204
        && status != 205
        && status != 304
        && !(method == b"CONNECT" && (200..300).contains(&status))
}

fn content_length_expected(method: &[u8], status: u16) -> bool {
    status >= 200 && status != 204 && !(method == b"CONNECT" && (200..300).contains(&status))
}

fn take<'a>(stream: &mut &'a [u8], len: usize) -> &'a [u8] {
    let len = len.min(stream.len());
    let (head, tail) = stream.split_at(len);
    *stream = tail;
    head
}

fn parse_hex(bytes: &[u8]) -> Option<usize> {
    if bytes.is_empty() {
        return None;
    }
    let mut value = 0usize;
    for &b in bytes {
        let digit = match b {
            b'0'..=b'9' => b - b'0',
            b'a'..=b'f' => b - b'a' + 10,
            b'A'..=b'F' => b - b'A' + 10,
            _ => return None,
        };
        value = value
            .checked_mul(16)?
            .checked_add(usize::from(digit))?;
    }
    Some(value)
}

fn decimal_eq(value: u64, text: &[u8]) -> bool {
    let mut digits = [0u8; 20];
    let mut start = digits.len();
    let mut rest = value;
    loop {
        start -= 1;
        digits[start] = b'0' + (rest % 10) as u8;
        rest /= 10;
        if rest == 0 {
            break;
        }
    }
    &digits[start..] == text
}

fn check_no_bare_lf(head: &[u8]) {
    for (i, &b) in head.iter().enumerate() {
        if b == b'\n' {
            assert!(
                i > 0 && head[i - 1] == b'\r',
                "bare LF in emitted head at {i} (finding)"
            );
        }
    }
    assert!(
        head.ends_with(b"\r\n\r\n"),
        "emitted head missing CRLF terminator (finding)"
    );
}

fn find_header_value<'a>(head: &'a [u8], name: &[u8]) -> Option<&'a [u8]> {
    for line in head.split(|b| *b == b'\n') {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.len() > name.len()
            && line[..name.len()].eq_ignore_ascii_case(name)
            && line[name.len()] == b':'
        {
            let mut value = &line[name.len() + 1..];
            while value.first() == Some(&b' ') || value.first() == Some(&b'\t') {
                value = &value[1..];
            }
            return Some(value);
        }
    }
    None
}

fuzz_target!(|data: &[u8]| {
    let data = if data.len() > MAX_INPUT {
        &data[..MAX_INPUT]
    } else {
        data
    };
    if data.len() < 10 {
        return;
    }
    let status = u16::from_le_bytes([data[0], data[1]]);
    let method_sel = data[2];
    let mlen = usize::from(data[3]).min(METHOD_MAX);
    let config = data[4];
    let cl_lo = u16::from_le_bytes([data[5], data[6]]);
    let rlen = usize::from(data[7]).min(REASON_MAX);
    let nfields = usize::from(data[8]).min(MAX_FIELDS);
    let ulen = usize::from(data[9]).min(UPGRADE_MAX);
    let mut stream = &data[10..];

    let method: &[u8] = if mlen == 0 && method_sel < 4 {
        METHODS[usize::from(method_sel)]
    } else if mlen == 0 {
        METHODS[0]
    } else {
        take(&mut stream, mlen)
    };
    let reason = take(&mut stream, rlen);
    let requested_upgrade = take(&mut stream, ulen);

    let mut names: [&[u8]; MAX_FIELDS] = [&[]; MAX_FIELDS];
    let mut values: [&[u8]; MAX_FIELDS] = [&[]; MAX_FIELDS];
    let mut fields = [Field {
        name: b"",
        value: b"",
    }; MAX_FIELDS];
    for i in 0..nfields {
        let lens = take(&mut stream, 2);
        let (nlen, vlen) = if lens.len() == 2 {
            (
                usize::from(lens[0]).min(NAME_MAX),
                usize::from(lens[1]).min(VALUE_MAX),
            )
        } else {
            (0, 0)
        };
        names[i] = take(&mut stream, nlen);
        values[i] = take(&mut stream, vlen);
        fields[i] = Field {
            name: names[i],
            value: values[i],
        };
    }
    let body = take(&mut stream, BODY_MAX);

    let content_length: u64 = if config & 0x10 != 0 {
        u64::from(cl_lo) | 0xFFFF_FFFF_FFFF_0000
    } else {
        u64::from(cl_lo)
    };

    let response = ResponseToWrite {
        request_method: method,
        requested_upgrade,
        status,
        reason,
        fields: &fields[..nfields],
        framing: framing(config),
        content_length,
        connection: connection(config),
    };

    let hostile_method = !is_token_slice(method);
    let hostile_text = has_hostile_value(reason)
        || (0..nfields).any(|i| has_hostile_value(names[i]) || has_hostile_value(values[i]))
        || names[..nfields].iter().any(|n| n.is_empty());

    
    let mut output = [0u8; OUT_MAX];
    let mut required = 0usize;
    let full = encode_response_head_with_required_length(&response, &mut output, &mut required);

    
    let mut small = [0xa5u8; OUT_SMALL];
    let mut small_required = 0usize;
    let small_result =
        encode_response_head_with_required_length(&response, &mut small, &mut small_required);
    match (&full, &small_result) {
        (Ok(full_head), Ok(small_head)) => {
            assert_eq!(
                small_head.length, full_head.length,
                "short-buffer length disagrees (finding)"
            );
            assert_eq!(
                small_required, required,
                "short-buffer required_length disagrees (finding)"
            );
            assert_eq!(
                &small[..small_head.length.min(OUT_SMALL)],
                &output[..small_head.length.min(OUT_SMALL)],
                "short-buffer bytes disagree (finding)"
            );
        }
        


        (Ok(_), Err(ResponseEncodeError::BufferTooSmall)) => {
            assert!(
                required > OUT_SMALL,
                "short buffer rejected a fitting encoding (finding)"
            );
            assert!(
                small.iter().all(|b| *b == 0xa5),
                "short buffer touched on BufferTooSmall (finding)"
            );
            assert_eq!(
                small_required, required,
                "required_length unstable across buffer sizes (finding)"
            );
        }
        (Ok(_), Err(other)) => {
            panic!("unexpected short-buffer error {other:?} (finding)");
        }
        (Err(_), Err(_)) => {
            assert_eq!(
                small_required, required,
                "required_length unstable across buffer sizes (finding)"
            );
            if full == Err(ResponseEncodeError::BufferTooSmall) {
                assert!(
                    small.iter().all(|b| *b == 0xa5),
                    "short buffer touched on BufferTooSmall (finding)"
                );
            }
        }
        (Err(_), Ok(_)) => {
            panic!("short buffer accepted what the full buffer rejected (finding)");
        }
    }

    
    let mut output2 = [0u8; OUT_MAX];
    let plain = encode_response_head(&response, &mut output2);
    match (&full, &plain) {
        (Ok(a), Ok(b)) => {
            assert_eq!(a, b, "required_length variant disagrees (finding)");
            assert_eq!(
                &output[..a.length],
                &output2[..b.length],
                "variant bytes disagree (finding)"
            );
        }
        (Err(a), Err(b)) => assert_eq!(a, b, "variant error disagrees (finding)"),
        _ => panic!("variant accept/reject disagreement (finding)"),
    }

    match full {
        Err(_) => {
            

        }
        Ok(head) => {
            assert!(
                !hostile_method && !hostile_text,
                "hostile bytes encoded without rejection (finding)"
            );
            assert_eq!(required, head.length, "required_length != length (finding)");
            let bytes = &output[..head.length];

            
            assert!(
                bytes.starts_with(b"HTTP/1.1 "),
                "missing version prefix (finding)"
            );
            assert_eq!(
                &bytes[9..12],
                &[
                    b'0' + (u32::from(status) / 100) as u8,
                    b'0' + ((u32::from(status) / 10) % 10) as u8,
                    b'0' + (u32::from(status) % 10) as u8,
                ],
                "status digits mismatch (finding)"
            );
            check_no_bare_lf(bytes);

            
            let expected = body_allowed_expected(method, status);
            assert_eq!(
                head.body_allowed, expected,
                "body_allowed disagrees with method/status rule (finding)"
            );

            
            match response.framing {
                ResponseFraming::FixedLength if content_length_expected(method, status) => {
                    let value = find_header_value(bytes, b"content-length")
                        .expect("Content-Length missing on fixed response (finding)");
                    assert!(
                        decimal_eq(content_length, value),
                        "Content-Length mismatch (finding)"
                    );
                }
                ResponseFraming::FixedLength => {
                    assert!(
                        find_header_value(bytes, b"content-length").is_none(),
                        "unexpected Content-Length (finding)"
                    );
                }
                ResponseFraming::Chunked => {
                    let value = find_header_value(bytes, b"transfer-encoding")
                        .expect("Transfer-Encoding missing on chunked response (finding)");
                    assert!(
                        value.eq_ignore_ascii_case(b"chunked"),
                        "wrong Transfer-Encoding value (finding)"
                    );
                }
                ResponseFraming::CloseDelimited => {
                    assert!(
                        find_header_value(bytes, b"content-length").is_none()
                            && find_header_value(bytes, b"transfer-encoding").is_none(),
                        "framing header on close-delimited response (finding)"
                    );
                }
            }

            

            let mut cursor = ResponseWriteCursor::new(bytes);
            let half = head.length / 2;
            let mut iters = 0usize;
            let mut advanced = 0usize;
            while !cursor.is_complete() {
                iters += 1;
                assert!(iters <= ITER_SLACK, "cursor drain looped (finding)");
                let step = (half.max(1)).min(cursor.pending().len());
                let done = cursor
                    .advance_write(step)
                    .expect("cursor advance must succeed (finding)");
                advanced += step;
                assert_eq!(done, cursor.is_complete());
            }
            assert_eq!(advanced, head.length);
            let mut cursor = ResponseWriteCursor::new(bytes);
            if head.length > 0 {
                assert_eq!(
                    cursor.advance_write(head.length + 1),
                    Err(ResponseEncodeError::WriteOverrun),
                    "cursor overrun not reported (finding)"
                );
                assert!(cursor.pending().is_empty());
                assert_eq!(
                    cursor.advance_write(0),
                    Err(ResponseEncodeError::InvalidState),
                    "poisoned cursor reusable (finding)"
                );
            }

            
            match response.framing {
                ResponseFraming::FixedLength => {
                    let mut writer =
                        FixedResponseBodyWriter::new(content_length, head.body_allowed);
                    if !head.body_allowed {
                        

                        assert_eq!(
                            writer.queue(b"x"),
                            Err(ResponseEncodeError::InvalidState),
                            "body staged for bodyless response (finding)"
                        );
                        assert!(writer.finish().unwrap());
                    } else if content_length <= BODY_MAX as u64 {
                        
                        let staged = body.len().min(content_length as usize);
                        let mid = staged / 2;
                        let mut iters = 0usize;
                        for piece in [&body[..mid], &body[mid..staged]] {
                            iters += 1;
                            assert!(iters <= ITER_SLACK, "body queue looped (finding)");
                            if piece.is_empty() {
                                continue;
                            }
                            writer.queue(piece).expect("queue must accept (finding)");
                            while !writer.pending().is_empty() {
                                iters += 1;
                                assert!(iters <= ITER_SLACK, "body drain looped (finding)");
                                let n = writer.pending().len();
                                writer.advance_write(n).expect("advance must succeed (finding)");
                            }
                        }
                        if staged as u64 == content_length {
                            assert!(writer.finish().unwrap(), "full body must finish (finding)");
                        } else {
                            assert_eq!(
                                writer.finish(),
                                Err(ResponseEncodeError::ShortBody),
                                "short body must report ShortBody (finding)"
                            );
                        }
                    }
                }
                ResponseFraming::Chunked => {
                    
                    let mut chunk_out = [0u8; BODY_MAX + 32];
                    let pieces = 3usize;
                    let step = body.len().div_ceil(pieces.max(1));
                    let mut iters = 0usize;
                    let mut offset = 0usize;
                    while offset < body.len() {
                        iters += 1;
                        assert!(iters <= ITER_SLACK, "chunk loop looped (finding)");
                        let end = (offset + step.max(1)).min(body.len());
                        let n = encode_response_chunk(&body[offset..end], &mut chunk_out)
                            .expect("chunk encode must succeed (finding)");
                        let wire = &chunk_out[..n];
                        let hex_end = wire.iter().position(|b| *b == b'\r').expect("hex line (finding)");
                        assert_eq!(wire[hex_end..hex_end + 2], [b'\r', b'\n'], "chunk framing (finding)");
                        assert!(
                            wire.ends_with(b"\r\n"),
                            "chunk terminator (finding)"
                        );
                        let data_len = end - offset;
                        assert_eq!(
                            parse_hex(&wire[..hex_end]),
                            Some(data_len),
                            "chunk size mismatch (finding)"
                        );
                        assert_eq!(&wire[hex_end + 2..wire.len() - 2], &body[offset..end]);
                        offset = end;
                    }
                    
                    let trailers = &fields[..nfields.min(2)];
                    let mut final_out = [0u8; 1024];
                    match encode_response_final_chunk(&trailers, &mut final_out) {
                        Ok(n) => {
                            let wire = &final_out[..n];
                            assert!(wire.starts_with(b"0\r\n"), "final chunk mark (finding)");
                            assert!(wire.ends_with(b"\r\n\r\n"), "final terminator (finding)");
                            check_no_bare_lf(wire);
                        }
                        Err(
                            ResponseEncodeError::InvalidField
                            | ResponseEncodeError::InvalidFramingField,
                        ) => {}
                        Err(other) => panic!("unexpected final-chunk error {other:?} (finding)"),
                    }
                }
                ResponseFraming::CloseDelimited => {}
            }
        }
    }
});
