#[cfg(test)]
mod tests;

// NOTE: pure stream-to-service state. No sockets, timers, or HPACK work here. Callers bring
// validated request fields, project the frozen router into RouteSnapshot before open, and use
// H2ServerDriver for all byte encoding. Decisions out, bytes via the existing writers.
pub const STAGED_BODY_CAP: usize = 2048;
pub const METHOD_CAP: usize = 16;
pub const MAX_TRAILERS: usize = 32;
pub const TRAILER_DATA_CAP: usize = 1024;
pub const MAX_STREAM_WRITE: usize = 16_384;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceBodyPolicy {
    Reject,
    Stream { max_body_bytes: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponsePhase {
    None,
    Informational,
    Final,
    Streaming,
    Complete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceError {
    InvalidStreamId,
    UnknownStream,
    StaleGeneration,
    DuplicateStream,
    TableFull,
    InvalidMethod,
    MethodTooLong,
    NonOriginForm,
    ConnectRefused,
    UnknownRoute,
    BodyNotPermitted,
    BodyOverCap,
    ResponseAlreadyStarted,
    ResponseNotStarted,
    ResponseNotStreaming,
    ResponseComplete,
    InvalidStatus,
    HeadCannotStream,
    NotWritable,
    WriteTooLarge,
    EmptyWrite,
    TrailerRefused,
    TrailerCapacity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RouteSnapshot<'allow> {
    pub route_id: u64,
    pub policy: ServiceBodyPolicy,
    pub permitted_trailers: &'allow [&'allow [u8]],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecordRef {
    pub stream_id: u32,
    pub generation: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TrailerSpan {
    pub name_offset: usize,
    pub name_length: usize,
    pub value_offset: usize,
    pub value_length: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestTrailer<'field> {
    pub name: &'field [u8],
    pub value: &'field [u8],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StagedDrain {
    pub bytes: usize,
    pub grant_owed: u32,
    pub end_received: bool,
    pub remaining: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StartedResponse {
    pub ended_stream: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TrailerOutcome {
    Staged { count: usize },
    Ignored,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TornDown {
    pub released: bool,
    pub stream_id: u32,
    pub dropped_ungranted: u32,
}

#[derive(Debug)]
pub struct StreamRecord<'allow> {
    pub occupied: bool,
    pub stream_id: u32,
    pub generation: u32,
    pub method: [u8; METHOD_CAP],
    pub method_len: usize,
    pub staged: [u8; STAGED_BODY_CAP],
    pub staged_len: usize,
    pub ungranted: u32,
    pub end_received: bool,
    pub route_id: u64,
    pub policy: ServiceBodyPolicy,
    pub permitted_trailers: &'allow [&'allow [u8]],
    pub phase: ResponsePhase,
    pub writable_pending: bool,
    pub trailer_fields: [TrailerSpan; MAX_TRAILERS],
    pub trailer_count: usize,
    pub trailer_data: [u8; TRAILER_DATA_CAP],
    pub trailer_used: usize,
}

impl<'allow> StreamRecord<'allow> {
    pub const EMPTY: Self = Self {
        occupied: false,
        stream_id: 0,
        generation: 0,
        method: [0; METHOD_CAP],
        method_len: 0,
        staged: [0; STAGED_BODY_CAP],
        staged_len: 0,
        ungranted: 0,
        end_received: false,
        route_id: 0,
        policy: ServiceBodyPolicy::Reject,
        permitted_trailers: &[],
        phase: ResponsePhase::None,
        writable_pending: false,
        trailer_fields: [TrailerSpan {
            name_offset: 0,
            name_length: 0,
            value_offset: 0,
            value_length: 0,
        }; MAX_TRAILERS],
        trailer_count: 0,
        trailer_data: [0; TRAILER_DATA_CAP],
        trailer_used: 0,
    };

    pub fn method(&self) -> &[u8] {
        &self.method[..self.method_len]
    }

    pub fn staged_body(&self) -> &[u8] {
        &self.staged[..self.staged_len]
    }

    pub fn trailer_name(&self, index: usize) -> Option<&[u8]> {
        let field = *self.trailer_fields.get(index)?;
        if index >= self.trailer_count {
            return None;
        }
        self.trailer_data
            .get(field.name_offset..field.name_offset + field.name_length)
    }

    pub fn trailer_value(&self, index: usize) -> Option<&[u8]> {
        let field = *self.trailer_fields.get(index)?;
        if index >= self.trailer_count {
            return None;
        }
        self.trailer_data
            .get(field.value_offset..field.value_offset + field.value_length)
    }
}

pub struct ServiceStreams<'allow, 'records> {
    records: &'records mut [StreamRecord<'allow>],
    next_generation: u32,
}

impl<'allow, 'records> ServiceStreams<'allow, 'records> {
    pub fn new(records: &'records mut [StreamRecord<'allow>]) -> Self {
        records.fill_with(|| StreamRecord::EMPTY);
        Self {
            records,
            next_generation: 1,
        }
    }

    pub fn open_request(
        &mut self,
        stream_id: u32,
        method: &[u8],
        path: &[u8],
        route: Option<RouteSnapshot<'allow>>,
        end_stream: bool,
    ) -> Result<RecordRef, ServiceError> {
        if stream_id == 0 {
            return Err(ServiceError::InvalidStreamId);
        }
        if method.is_empty() || !method.iter().copied().all(is_token) {
            return Err(ServiceError::InvalidMethod);
        }
        if method.len() > METHOD_CAP {
            return Err(ServiceError::MethodTooLong);
        }
        if method == b"CONNECT" {
            return Err(ServiceError::ConnectRefused);
        }
        if !is_origin_form(path) {
            return Err(ServiceError::NonOriginForm);
        }
        let Some(route) = route else {
            return Err(ServiceError::UnknownRoute);
        };
        if self
            .records
            .iter()
            .any(|record| record.occupied && record.stream_id == stream_id)
        {
            return Err(ServiceError::DuplicateStream);
        }
        let Some(slot) = self.records.iter_mut().find(|record| !record.occupied) else {
            return Err(ServiceError::TableFull);
        };
        let generation = self.next_generation;
        self.next_generation = self.next_generation.wrapping_add(1).max(1);
        slot.occupied = true;
        slot.stream_id = stream_id;
        slot.generation = generation;
        slot.method[..method.len()].copy_from_slice(method);
        slot.method_len = method.len();
        slot.staged_len = 0;
        slot.ungranted = 0;
        slot.end_received = end_stream;
        slot.route_id = route.route_id;
        slot.policy = route.policy;
        slot.permitted_trailers = route.permitted_trailers;
        slot.phase = ResponsePhase::None;
        slot.writable_pending = false;
        slot.trailer_count = 0;
        slot.trailer_used = 0;
        Ok(RecordRef {
            stream_id,
            generation,
        })
    }

    pub fn stage_data(
        &mut self,
        target: RecordRef,
        bytes: &[u8],
        end_stream: bool,
    ) -> Result<usize, ServiceError> {
        let record = self.lookup_mut(target)?;
        if record.phase == ResponsePhase::Complete {
            return Err(ServiceError::ResponseComplete);
        }
        if matches!(record.policy, ServiceBodyPolicy::Reject) && !bytes.is_empty() {
            return Err(ServiceError::BodyNotPermitted);
        }
        if let ServiceBodyPolicy::Stream { max_body_bytes } = record.policy {
            let next = record.staged_len.saturating_add(bytes.len()) as u64;
            if next > max_body_bytes {
                return Err(ServiceError::BodyOverCap);
            }
        }
        if record.staged_len + bytes.len() > STAGED_BODY_CAP {
            return Err(ServiceError::BodyOverCap);
        }
        record.staged[record.staged_len..record.staged_len + bytes.len()].copy_from_slice(bytes);
        record.staged_len += bytes.len();
        record.ungranted = record.ungranted.saturating_add(bytes.len() as u32);
        if end_stream {
            record.end_received = true;
        }
        Ok(record.staged_len)
    }

    pub fn take_staged(
        &mut self,
        target: RecordRef,
        out: &mut [u8],
    ) -> Result<StagedDrain, ServiceError> {
        let record = self.lookup_mut(target)?;
        let bytes = record.staged_len.min(out.len());
        out[..bytes].copy_from_slice(&record.staged[..bytes]);
        record.staged.copy_within(bytes..record.staged_len, 0);
        record.staged_len -= bytes;
        let grant = bytes as u32;
        record.ungranted = record.ungranted.saturating_sub(grant);
        Ok(StagedDrain {
            bytes,
            grant_owed: grant,
            end_received: record.end_received,
            remaining: record.staged_len,
        })
    }

    pub fn start_response(
        &mut self,
        target: RecordRef,
        status: u16,
        end_stream: bool,
    ) -> Result<StartedResponse, ServiceError> {
        let record = self.lookup_mut(target)?;
        if !matches!(
            record.phase,
            ResponsePhase::None | ResponsePhase::Informational
        ) {
            return Err(ServiceError::ResponseAlreadyStarted);
        }
        if status == 101 {
            return Err(ServiceError::InvalidStatus);
        }
        if (100..200).contains(&status) {
            if end_stream {
                return Err(ServiceError::InvalidStatus);
            }
            record.phase = ResponsePhase::Informational;
            return Ok(StartedResponse {
                ended_stream: false,
            });
        }
        if !(200..=599).contains(&status) {
            return Err(ServiceError::InvalidStatus);
        }
        record.phase = if end_stream {
            ResponsePhase::Complete
        } else {
            ResponsePhase::Final
        };
        Ok(StartedResponse {
            ended_stream: end_stream,
        })
    }

    pub fn begin_streaming(&mut self, target: RecordRef, status: u16) -> Result<(), ServiceError> {
        let record = self.lookup_mut(target)?;
        if !matches!(
            record.phase,
            ResponsePhase::None | ResponsePhase::Informational
        ) {
            return Err(ServiceError::ResponseAlreadyStarted);
        }
        if !(200..=599).contains(&status) {
            return Err(ServiceError::InvalidStatus);
        }
        if status == 204 || status == 304 {
            return Err(ServiceError::InvalidStatus);
        }
        if record.method() == b"HEAD" {
            return Err(ServiceError::HeadCannotStream);
        }
        record.phase = ResponsePhase::Streaming;
        record.writable_pending = true;
        Ok(())
    }

    pub fn notify_writable(&mut self, target: RecordRef) -> Result<(), ServiceError> {
        let record = self.lookup_mut(target)?;
        if record.phase != ResponsePhase::Streaming {
            return Err(ServiceError::ResponseNotStreaming);
        }
        record.writable_pending = true;
        Ok(())
    }

    pub fn admit_write(&mut self, target: RecordRef, length: usize) -> Result<(), ServiceError> {
        let record = self.lookup_mut(target)?;
        if record.phase != ResponsePhase::Streaming {
            return Err(ServiceError::ResponseNotStreaming);
        }
        if !record.writable_pending {
            return Err(ServiceError::NotWritable);
        }
        if length == 0 {
            return Err(ServiceError::EmptyWrite);
        }
        if length > MAX_STREAM_WRITE {
            return Err(ServiceError::WriteTooLarge);
        }
        record.writable_pending = false;
        Ok(())
    }

    pub fn end_streaming(&mut self, target: RecordRef) -> Result<(), ServiceError> {
        let record = self.lookup_mut(target)?;
        if record.phase != ResponsePhase::Streaming {
            return Err(ServiceError::ResponseNotStreaming);
        }
        record.phase = ResponsePhase::Complete;
        record.writable_pending = false;
        Ok(())
    }

    pub fn stage_request_trailers(
        &mut self,
        target: RecordRef,
        fields: &[RequestTrailer<'_>],
    ) -> Result<TrailerOutcome, ServiceError> {
        let record = self.lookup_mut(target)?;
        if record.phase == ResponsePhase::Complete {
            return Ok(TrailerOutcome::Ignored);
        }
        for field in fields {
            if field.name.is_empty() || field.name.first() == Some(&b':') {
                return Err(ServiceError::TrailerRefused);
            }
            if !valid_trailer_name(field.name) {
                return Err(ServiceError::TrailerRefused);
            }
            if valid_field_value(field.value).is_err() {
                return Err(ServiceError::TrailerRefused);
            }
            if !trailer_allowed(record.permitted_trailers, field.name) {
                return Err(ServiceError::TrailerRefused);
            }
            if record.trailer_count >= MAX_TRAILERS {
                return Err(ServiceError::TrailerCapacity);
            }
            if record.trailer_used + field.name.len() + field.value.len() > TRAILER_DATA_CAP {
                return Err(ServiceError::TrailerCapacity);
            }
            let slot = &mut record.trailer_fields[record.trailer_count];
            slot.name_offset = record.trailer_used;
            slot.name_length = field.name.len();
            record.trailer_data[record.trailer_used..record.trailer_used + field.name.len()]
                .copy_from_slice(field.name);
            record.trailer_used += field.name.len();
            slot.value_offset = record.trailer_used;
            slot.value_length = field.value.len();
            record.trailer_data[record.trailer_used..record.trailer_used + field.value.len()]
                .copy_from_slice(field.value);
            record.trailer_used += field.value.len();
            record.trailer_count += 1;
        }
        record.end_received = true;
        Ok(TrailerOutcome::Staged {
            count: record.trailer_count,
        })
    }

    pub fn check_response_trailers(
        &self,
        target: RecordRef,
        names: &[&[u8]],
    ) -> Result<(), ServiceError> {
        let record = self.lookup(target)?;
        if record.phase != ResponsePhase::Streaming {
            return Err(ServiceError::ResponseNotStreaming);
        }
        if names.is_empty() {
            return Err(ServiceError::TrailerRefused);
        }
        for name in names {
            if name.is_empty() || !valid_trailer_name(name) {
                return Err(ServiceError::TrailerRefused);
            }
            if !trailer_allowed(record.permitted_trailers, name) {
                return Err(ServiceError::TrailerRefused);
            }
        }
        Ok(())
    }

    pub fn teardown(&mut self, stream_id: u32, keep_headers: bool) -> TornDown {
        let Some(record) = self
            .records
            .iter_mut()
            .find(|record| record.occupied && record.stream_id == stream_id)
        else {
            return TornDown {
                released: false,
                stream_id,
                dropped_ungranted: 0,
            };
        };
        let dropped = record.ungranted;
        record.occupied = false;
        record.staged_len = 0;
        record.ungranted = 0;
        record.writable_pending = false;
        if !keep_headers {
            record.method_len = 0;
            record.route_id = 0;
            record.trailer_count = 0;
            record.trailer_used = 0;
        }
        TornDown {
            released: true,
            stream_id,
            dropped_ungranted: dropped,
        }
    }

    pub fn close(&mut self, target: RecordRef) -> Result<TornDown, ServiceError> {
        let record = self.lookup_mut(target)?;
        let dropped = record.ungranted;
        let stream_id = record.stream_id;
        record.occupied = false;
        record.staged_len = 0;
        record.ungranted = 0;
        record.writable_pending = false;
        record.method_len = 0;
        record.route_id = 0;
        record.trailer_count = 0;
        record.trailer_used = 0;
        Ok(TornDown {
            released: true,
            stream_id,
            dropped_ungranted: dropped,
        })
    }

    pub fn get(&self, target: RecordRef) -> Result<&StreamRecord<'allow>, ServiceError> {
        self.lookup(target)
    }

    fn lookup(&self, target: RecordRef) -> Result<&StreamRecord<'allow>, ServiceError> {
        let Some(record) = self
            .records
            .iter()
            .find(|record| record.stream_id == target.stream_id && record.occupied)
        else {
            return Err(ServiceError::UnknownStream);
        };
        if record.generation != target.generation {
            return Err(ServiceError::StaleGeneration);
        }
        Ok(record)
    }

    fn lookup_mut(&mut self, target: RecordRef) -> Result<&mut StreamRecord<'allow>, ServiceError> {
        let Some(record) = self
            .records
            .iter_mut()
            .find(|record| record.stream_id == target.stream_id && record.occupied)
        else {
            return Err(ServiceError::UnknownStream);
        };
        if record.generation != target.generation {
            return Err(ServiceError::StaleGeneration);
        }
        Ok(record)
    }
}

fn is_origin_form(path: &[u8]) -> bool {
    if path.first() != Some(&b'/') {
        return false;
    }
    if path.starts_with(b"//") {
        return false;
    }
    !path.contains(&b'#')
}

fn trailer_allowed(allowlist: &[&[u8]], name: &[u8]) -> bool {
    allowlist
        .iter()
        .any(|allowed| ascii_case_insensitive_eq(allowed, name))
}

fn ascii_case_insensitive_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right.iter())
        .all(|(a, b)| a.eq_ignore_ascii_case(b))
}

fn is_token(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

fn valid_trailer_name(name: &[u8]) -> bool {
    !name.is_empty()
        && name
            .iter()
            .copied()
            .all(|byte| is_token(byte) && !byte.is_ascii_uppercase())
}

fn valid_field_value(value: &[u8]) -> Result<(), ServiceError> {
    if value.first().is_some_and(is_ows) || value.last().is_some_and(is_ows) {
        return Err(ServiceError::TrailerRefused);
    }
    if value.iter().any(|byte| {
        *byte == 0
            || *byte == b'\r'
            || *byte == b'\n'
            || *byte == 0x7f
            || (*byte < 0x20 && *byte != b'\t')
    }) {
        return Err(ServiceError::TrailerRefused);
    }
    Ok(())
}

fn is_ows(byte: &u8) -> bool {
    *byte == b' ' || *byte == b'\t'
}
