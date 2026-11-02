use std::borrow::Cow;

use crate::request::Reject;

const MAX_PARTS: usize = 256;
const MAX_PART_HEADER_BYTES: usize = 8 * 1024;
const MAX_BOUNDARY_BYTES: usize = 70;

const BAD: Reject = Reject::new(400, "invalid multipart body");
const MEDIA_TYPE: Reject = Reject::new(415, "expected multipart/form-data");

pub struct Multipart<'a> {
    boundary: Cow<'a, [u8]>,
    body: &'a [u8],
}

impl<'a> Multipart<'a> {
    pub fn parse(headers: &'a [(&'a [u8], &'a [u8])], body: &'a [u8]) -> Result<Self, Reject> {
        let boundary = boundary(headers)?;
        Ok(Self { boundary, body })
    }

    pub fn parts(&self) -> Parts<'a> {
        Parts {
            boundary: self.boundary.clone(),
            body: self.body,
            rest: None,
            count: 0,
            done: false,
        }
    }

    pub fn field(&self, name: &str) -> Option<&'a [u8]> {
        self.parts()
            .filter_map(Result::ok)
            .find(|part| part.name() == name.as_bytes())
            .map(|part| part.data)
    }

    pub fn file(&self, name: &str) -> Option<Part<'a>> {
        self.parts()
            .filter_map(Result::ok)
            .find(|part| part.name() == name.as_bytes() && part.filename().is_some())
    }
}

pub struct Part<'a> {
    name: Cow<'a, [u8]>,
    filename: Option<Cow<'a, [u8]>>,
    content_type: Option<&'a [u8]>,
    data: &'a [u8],
}

impl<'a> Part<'a> {
    pub fn name(&self) -> &[u8] {
        &self.name
    }

    pub fn filename(&self) -> Option<&[u8]> {
        self.filename.as_deref()
    }

    pub fn content_type(&self) -> Option<&'a [u8]> {
        self.content_type
    }

    pub fn data(&self) -> &'a [u8] {
        self.data
    }

    pub fn is_file(&self) -> bool {
        self.filename.is_some()
    }
}

pub struct Parts<'a> {
    boundary: Cow<'a, [u8]>,
    body: &'a [u8],
    rest: Option<&'a [u8]>,
    count: usize,
    done: bool,
}

impl<'a> Iterator for Parts<'a> {
    type Item = Result<Part<'a>, Reject>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let rest = match self.rest {
            Some(rest) => rest,
            None => match first_boundary(self.body, &self.boundary) {
                Some(rest) => rest,
                None => {
                    self.done = true;
                    return Some(Err(BAD));
                }
            },
        };

        self.count += 1;
        if self.count > MAX_PARTS {
            self.done = true;
            return Some(Err(BAD));
        }

        match part(rest, &self.boundary) {
            Ok(Some((part, next))) => {
                self.rest = Some(next);
                Some(Ok(part))
            }
            Ok(None) => {
                self.done = true;
                None
            }
            Err(reject) => {
                self.done = true;
                Some(Err(reject))
            }
        }
    }
}

fn boundary<'a>(headers: &'a [(&'a [u8], &'a [u8])]) -> Result<Cow<'a, [u8]>, Reject> {
    let value = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(b"content-type"))
        .map(|(_, value)| *value)
        .ok_or(MEDIA_TYPE)?;

    let semicolon = value
        .iter()
        .position(|byte| *byte == b';')
        .ok_or(MEDIA_TYPE)?;
    if !value[..semicolon]
        .trim_ascii()
        .eq_ignore_ascii_case(b"multipart/form-data")
    {
        return Err(MEDIA_TYPE);
    }

    let mut index = semicolon + 1;
    while index < value.len() {
        while index < value.len() && value[index].is_ascii_whitespace() {
            index += 1;
        }
        let start = index;
        while index < value.len() && value[index] != b'=' && value[index] != b';' {
            index += 1;
        }
        let key = value[start..index].trim_ascii();
        if index >= value.len() || value[index] != b'=' {
            continue;
        }
        index += 1;
        let (parameter, next) = parameter(value, index)?;
        index = next;
        if !key.eq_ignore_ascii_case(b"boundary") {
            continue;
        }
        if !valid_boundary(&parameter) {
            return Err(MEDIA_TYPE);
        }
        return Ok(parameter);
    }

    Err(MEDIA_TYPE)
}

fn parameter<'a>(value: &'a [u8], mut index: usize) -> Result<(Cow<'a, [u8]>, usize), Reject> {
    while index < value.len() && value[index].is_ascii_whitespace() {
        index += 1;
    }

    if value.get(index) == Some(&b'"') {
        index += 1;
        let start = index;
        let mut escaped = false;
        while index < value.len() && value[index] != b'"' {
            if value[index] == b'\\' {
                escaped = true;
                index += 1;
            }
            index += 1;
        }
        if index >= value.len() {
            return Err(BAD);
        }
        let raw = &value[start..index];
        let next = index + 1;
        if !escaped {
            return Ok((Cow::Borrowed(raw), next));
        }

        let mut out = Vec::with_capacity(raw.len());
        let mut bytes = raw.iter().copied();
        while let Some(byte) = bytes.next() {
            match byte {
                b'\\' => {
                    if let Some(escaped) = bytes.next() {
                        out.push(escaped);
                    }
                }
                byte => out.push(byte),
            }
        }
        return Ok((Cow::Owned(out), next));
    }

    let start = index;
    while index < value.len() && value[index] != b';' && !value[index].is_ascii_whitespace() {
        index += 1;
    }
    Ok((Cow::Borrowed(&value[start..index]), index))
}

fn valid_boundary(boundary: &[u8]) -> bool {
    (1..=MAX_BOUNDARY_BYTES).contains(&boundary.len())
        && !boundary.ends_with(b" ")
        && boundary
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || b"'()+_,-./:=? ".contains(byte))
}

fn first_boundary<'a>(body: &'a [u8], boundary: &[u8]) -> Option<&'a [u8]> {
    let mut offset = 0;
    while let Some(found) = find(&body[offset..], b"--") {
        let at = offset + found;
        let line_start = at == 0 || body[..at].ends_with(b"\r\n");
        if line_start && body.get(at + 2..at + 2 + boundary.len()) == Some(boundary) {
            return Some(&body[at..]);
        }
        offset = at + 2;
    }
    None
}

fn part<'a>(rest: &'a [u8], boundary: &[u8]) -> Result<Option<(Part<'a>, &'a [u8])>, Reject> {
    let after_boundary = 2 + boundary.len();
    if rest.len() < after_boundary
        || !rest.starts_with(b"--")
        || &rest[2..after_boundary] != boundary
    {
        return Err(BAD);
    }
    if rest[after_boundary..].starts_with(b"--") {
        return Ok(None);
    }

    let mut cursor = after_boundary;
    while matches!(rest.get(cursor), Some(b' ' | b'\t')) {
        cursor += 1;
    }
    if !rest[cursor..].starts_with(b"\r\n") {
        return Err(BAD);
    }
    cursor += 2;

    let headers_end = find(&rest[cursor..], b"\r\n\r\n").ok_or(BAD)? + cursor;
    if headers_end - cursor > MAX_PART_HEADER_BYTES {
        return Err(BAD);
    }
    let data_start = headers_end + 4;
    let (data_length, next) = next_boundary(&rest[data_start..], boundary).ok_or(BAD)?;
    let part = parse_part(
        &rest[cursor..headers_end],
        &rest[data_start..data_start + data_length],
    )?;
    Ok(Some((part, &rest[data_start + next..])))
}

fn next_boundary(haystack: &[u8], boundary: &[u8]) -> Option<(usize, usize)> {
    let mut offset = 0;
    while let Some(found) = find(&haystack[offset..], b"\r\n--") {
        let at = offset + found;
        let after = at + 2;
        if haystack.get(after + 2..after + 2 + boundary.len()) == Some(boundary) {
            return Some((at, after));
        }
        offset = after;
    }
    None
}

fn parse_part<'a>(headers: &'a [u8], data: &'a [u8]) -> Result<Part<'a>, Reject> {
    let mut name = None;
    let mut filename = None;
    let mut content_type = None;

    for line in headers.split(|byte| *byte == b'\n') {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.is_empty() {
            continue;
        }
        let colon = line.iter().position(|byte| *byte == b':').ok_or(BAD)?;
        let field = line[..colon].trim_ascii();
        let value = line[colon + 1..].trim_ascii();
        if field.eq_ignore_ascii_case(b"content-disposition") {
            let (field_name, field_filename) = disposition(value)?;
            name = Some(field_name);
            filename = field_filename;
        } else if field.eq_ignore_ascii_case(b"content-type") {
            content_type = Some(value);
        }
    }

    let name = name.ok_or(BAD)?;
    Ok(Part {
        name,
        filename,
        content_type,
        data,
    })
}

type Disposition<'a> = (Cow<'a, [u8]>, Option<Cow<'a, [u8]>>);

fn disposition<'a>(value: &'a [u8]) -> Result<Disposition<'a>, Reject> {
    let end = value
        .iter()
        .position(|byte| *byte == b';')
        .unwrap_or(value.len());
    if !value[..end].trim_ascii().eq_ignore_ascii_case(b"form-data") {
        return Err(BAD);
    }

    let mut name = None;
    let mut filename = None;
    let mut index = end;
    while index < value.len() {
        index += 1;
        while index < value.len() && value[index].is_ascii_whitespace() {
            index += 1;
        }
        let start = index;
        while index < value.len() && value[index] != b'=' && value[index] != b';' {
            index += 1;
        }
        let key = value[start..index].trim_ascii();
        if index >= value.len() || value[index] != b'=' {
            continue;
        }
        index += 1;
        let (parameter, next) = parameter(value, index)?;
        index = next;
        if key.eq_ignore_ascii_case(b"name") {
            name = Some(parameter);
        } else if key.eq_ignore_ascii_case(b"filename") {
            filename = Some(parameter);
        }
    }

    name.map(|name| (name, filename)).ok_or(BAD)
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    memchr::memmem::find(haystack, needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    const BODY: &[u8] = b"--XyZ\r\n\
Content-Disposition: form-data; name=\"title\"\r\n\
\r\n\
Hello world\r\n\
--XyZ\r\n\
Content-Disposition: form-data; name=\"file\"; filename=\"notes.txt\"\r\n\
Content-Type: text/plain\r\n\
\r\n\
line one\r\n\
line two\r\n\
--XyZ--\r\n";

    fn headers() -> Vec<(&'static [u8], &'static [u8])> {
        vec![(b"content-type", b"multipart/form-data; boundary=XyZ")]
    }

    #[test]
    fn parses_fields_and_files() {
        let headers = headers();
        let multipart = Multipart::parse(&headers, BODY).expect("parse");
        assert_eq!(multipart.field("title"), Some(&b"Hello world"[..]));

        let file = multipart.file("file").expect("file");
        assert_eq!(file.filename(), Some(&b"notes.txt"[..]));
        assert_eq!(file.content_type(), Some(&b"text/plain"[..]));
        assert_eq!(file.data(), b"line one\r\nline two");

        let parts: Vec<_> = multipart.parts().collect::<Result<_, _>>().expect("parts");
        assert_eq!(parts.len(), 2);
        assert!(parts[0].data() == b"Hello world");
        assert!(parts[1].is_file());
    }

    #[test]
    fn accepts_quoted_parameter_escapes() {
        let body = b"--b\r\n\
Content-Disposition: form-data; name=\"say \\\"hi\\\"\"\r\n\
\r\n\
value\r\n\
--b--\r\n";
        let headers = [(
            b"content-type".as_slice(),
            b"multipart/form-data; boundary=\"b\"".as_slice(),
        )];
        let multipart = Multipart::parse(&headers, body).expect("parse");
        assert_eq!(multipart.field("say \"hi\""), Some(&b"value"[..]));
    }

    #[test]
    fn rejects_missing_or_wrong_boundary() {
        let headers = [(b"content-type".as_slice(), b"application/json".as_slice())];
        assert_eq!(
            Multipart::parse(&headers, BODY)
                .err()
                .map(|reject| reject.status),
            Some(415)
        );

        let headers = [(
            b"content-type".as_slice(),
            b"multipart/form-data".as_slice(),
        )];
        assert_eq!(
            Multipart::parse(&headers, BODY)
                .err()
                .map(|reject| reject.status),
            Some(415)
        );
    }

    #[test]
    fn rejects_truncated_body() {
        let headers = headers();
        let body = b"--XyZ\r\n\
Content-Disposition: form-data; name=\"a\"\r\n\
\r\n\
value";
        let multipart = Multipart::parse(&headers, body).expect("parse");
        let mut parts = multipart.parts();
        assert!(parts.next().expect("error").is_err());
        assert!(parts.next().is_none());
    }

    #[test]
    fn accepts_preamble_and_epilogue() {
        let headers = headers();
        let body = b"preamble\r\n--XyZ\r\n\
Content-Disposition: form-data; name=\"a\"\r\n\
\r\n\
1\r\n\
--XyZ--\r\nepilogue";
        let multipart = Multipart::parse(&headers, body).expect("parse");
        assert_eq!(multipart.field("a"), Some(&b"1"[..]));
    }
}
